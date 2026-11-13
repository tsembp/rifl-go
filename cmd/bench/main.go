package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/tsembp/rifl-go/internal/benchrun"
)

var csvHeader = []string{
	"timestamp", "implementation", "scenario", "repeat", "requests", "concurrency", "virtual_clients",
	"payload_bytes", "retry_percent", "retry_transport", "connection_mode", "seed", "attempts", "attempt_errors", "successes", "errors", "duration_ms", "throughput_ops_s",
	"p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms", "p9999_ms", "max_ms", "batch_ack_p50_ms", "batch_ack_max_ms", "value_before", "value_after",
	"intended_effects", "actual_effects", "duplicate_effects", "completions_before",
	"completions_after", "clients_before", "clients_after", "db_bytes_before", "db_bytes_after",
}

var latencyHeader = []string{"timestamp", "implementation", "scenario", "repeat", "concurrency", "virtual_clients", "sample", "kind", "success", "latency_ns"}
var timelineHeader = []string{"timestamp", "implementation", "scenario", "repeat", "concurrency", "virtual_clients", "elapsed_ms", "attempts", "attempt_errors", "successes", "errors"}

func main() {
	address := flag.String("addr", "127.0.0.1:9000", "RPC server address")
	implementation := flag.String("implementation", "protected", "protected or baseline")
	scenario := flag.String("scenario", benchrun.ScenarioNormal, "normal, lost-response, duplicate-heavy, retry-rate, ack-gc, batch-ack, blocked-gap, restart-seed, or restart-replay")
	requests := flag.Int("requests", 1000, "logical requests per repeat; ignored when duration is positive")
	duration := flag.Duration("duration", 0, "time-based run duration, for example 30s")
	concurrency := flag.Int("concurrency", 16, "parallel workers")
	virtualClients := flag.Int("virtual-clients", 0, "logical clients; 0 uses concurrency")
	warmup := flag.Int("warmup", 100, "unmeasured normal requests before each repeat")
	repeats := flag.Int("repeats", 3, "measured repeats")
	repeatStart := flag.Int("repeat-start", 1, "label and seed offset for the first measured repeat")
	duplicateCopies := flag.Int("duplicate-copies", 4, "attempts per logical request in duplicate-heavy mode")
	retryPercent := flag.Float64("retry-percent", 1, "percentage of logical requests retried in retry-rate mode")
	retryTransport := flag.Bool("retry-transport", false, "retry ambiguous transport failures with the same request ID")
	retryDelay := flag.Duration("retry-delay", 25*time.Millisecond, "delay between transport retries")
	connectionMode := flag.String("connection-mode", benchrun.ConnectionPerAttempt, "per-attempt or persistent")
	payloadBytes := flag.Int("payload-bytes", 0, "transport-only padding bytes per request")
	seed := flag.Int64("seed", 1, "deterministic retry selection seed")
	delta := flag.Int64("delta", 1, "counter delta")
	timeout := flag.Duration("timeout", 5*time.Second, "per-RPC timeout")
	clientPrefix := flag.String("client-prefix", "bench", "stable logical client prefix; keep identical across restart seed/replay")
	output := flag.String("out", "-", "CSV output path, or - for stdout")
	latencyOutput := flag.String("latency-out", "", "optional raw per-request latency CSV")
	timelineOutput := flag.String("timeline-out", "", "optional one-second cumulative timeline CSV")
	verifyBatchAck := flag.Bool("verify-batch-ack", false, "after each batch-ack repeat, check stale rejection and final-result replay; fails the run on violation")
	flag.Parse()
	if *repeats <= 0 || *repeatStart <= 0 || *warmup < 0 {
		log.Fatal("positive repeats/repeat-start and nonnegative warmup are required")
	}

	writer, closeWriter, writeHeader, err := outputWriter(*output)
	if err != nil {
		log.Fatal(err)
	}
	defer closeWriter()
	csvWriter := csv.NewWriter(writer)
	if writeHeader {
		if err := csvWriter.Write(csvHeader); err != nil {
			log.Fatal(err)
		}
	}
	latencyWriter, closeLatency, err := optionalCSVWriter(*latencyOutput, latencyHeader)
	if err != nil {
		log.Fatal(err)
	}
	defer closeLatency()
	timelineWriter, closeTimeline, err := optionalCSVWriter(*timelineOutput, timelineHeader)
	if err != nil {
		log.Fatal(err)
	}
	defer closeTimeline()

	for repeat := *repeatStart; repeat < *repeatStart+*repeats; repeat++ {
		cfg := benchrun.Config{
			Address:         *address,
			Implementation:  *implementation,
			Scenario:        *scenario,
			Requests:        *requests,
			Duration:        *duration,
			Concurrency:     *concurrency,
			VirtualClients:  *virtualClients,
			Warmup:          *warmup,
			DuplicateCopies: *duplicateCopies,
			RetryPercent:    *retryPercent,
			RetryTransport:  *retryTransport,
			RetryDelay:      *retryDelay,
			ConnectionMode:  *connectionMode,
			PayloadBytes:    *payloadBytes,
			Seed:            *seed + int64(repeat-1),
			Delta:           *delta,
			Timeout:         *timeout,
			ClientPrefix:    fmt.Sprintf("%s-r%d", *clientPrefix, repeat),
		}
		result, err := benchrun.Run(context.Background(), cfg)
		if err != nil {
			log.Fatalf("repeat %d: %v", repeat, err)
		}
		if err := csvWriter.Write(csvRecord(result, repeat)); err != nil {
			log.Fatal(err)
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			log.Fatal(err)
		}
		if latencyWriter != nil {
			for index, sample := range result.Samples {
				if err := latencyWriter.Write(latencyRecord(result, repeat, index, sample)); err != nil {
					log.Fatal(err)
				}
			}
			latencyWriter.Flush()
			if err := latencyWriter.Error(); err != nil {
				log.Fatal(err)
			}
		}
		if timelineWriter != nil {
			for _, point := range result.Timeline {
				if err := timelineWriter.Write(timelineRecord(result, repeat, point)); err != nil {
					log.Fatal(err)
				}
			}
			timelineWriter.Flush()
			if err := timelineWriter.Error(); err != nil {
				log.Fatal(err)
			}
		}
		if *verifyBatchAck {
			if err := benchrun.VerifyBatchAck(context.Background(), cfg); err != nil {
				log.Fatalf("repeat %d: batch-ack verification failed: %v", repeat, err)
			}
			log.Printf("repeat %d: batch-ack verification passed", repeat)
		}
		log.Printf("repeat %d: %.1f ops/s p99=%.3fms duplicate_effects=%d completions=%d",
			repeat, result.Throughput, milliseconds(result.P99), result.DuplicateEffects, result.CompletionsAfter)
	}
}

func optionalCSVWriter(path string, header []string) (*csv.Writer, func(), error) {
	if path == "" {
		return nil, func() {}, nil
	}
	writer, closeWriter, writeHeader, err := outputWriter(path)
	if err != nil {
		return nil, nil, err
	}
	csvWriter := csv.NewWriter(writer)
	if writeHeader {
		if err := csvWriter.Write(header); err != nil {
			closeWriter()
			return nil, nil, err
		}
		csvWriter.Flush()
	}
	return csvWriter, closeWriter, nil
}

func outputWriter(path string) (io.Writer, func(), bool, error) {
	if path == "-" {
		return os.Stdout, func() {}, true, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, nil, false, fmt.Errorf("open output: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, false, fmt.Errorf("stat output: %w", err)
	}
	return file, func() { file.Close() }, info.Size() == 0, nil
}

func csvRecord(result benchrun.Result, repeat int) []string {
	return []string{
		result.Timestamp.Format(time.RFC3339Nano), result.Implementation, result.Scenario,
		strconv.Itoa(repeat), strconv.Itoa(result.Requests), strconv.Itoa(result.Concurrency),
		strconv.Itoa(result.VirtualClients), strconv.Itoa(result.PayloadBytes), floatString(result.RetryPercent),
		strconv.FormatBool(result.RetryTransport), result.ConnectionMode, strconv.FormatInt(result.Seed, 10), strconv.FormatInt(result.Attempts, 10),
		strconv.FormatInt(result.AttemptErrors, 10), strconv.FormatInt(result.Successes, 10),
		strconv.FormatInt(result.Errors, 10), floatString(milliseconds(result.Duration)),
		floatString(result.Throughput), floatString(milliseconds(result.P50)),
		floatString(milliseconds(result.P90)), floatString(milliseconds(result.P95)), floatString(milliseconds(result.P99)),
		floatString(milliseconds(result.P999)), floatString(milliseconds(result.P9999)), floatString(milliseconds(result.Max)),
		floatString(milliseconds(result.BatchAckP50)), floatString(milliseconds(result.BatchAckMax)), strconv.FormatInt(result.ValueBefore, 10),
		strconv.FormatInt(result.ValueAfter, 10), strconv.FormatInt(result.IntendedEffects, 10),
		strconv.FormatInt(result.ActualEffects, 10), strconv.FormatInt(result.DuplicateEffects, 10),
		strconv.Itoa(result.CompletionsBefore), strconv.Itoa(result.CompletionsAfter),
		strconv.Itoa(result.ClientsBefore), strconv.Itoa(result.ClientsAfter),
		strconv.FormatInt(result.DatabaseBytesBefore, 10), strconv.FormatInt(result.DatabaseBytesAfter, 10),
	}
}

func latencyRecord(result benchrun.Result, repeat, index int, sample benchrun.LatencySample) []string {
	return []string{result.Timestamp.Format(time.RFC3339Nano), result.Implementation, result.Scenario,
		strconv.Itoa(repeat), strconv.Itoa(result.Concurrency), strconv.Itoa(result.VirtualClients),
		strconv.Itoa(index), sample.Kind, strconv.FormatBool(sample.Success), strconv.FormatInt(sample.Elapsed.Nanoseconds(), 10)}
}

func timelineRecord(result benchrun.Result, repeat int, point benchrun.TimelinePoint) []string {
	return []string{result.Timestamp.Format(time.RFC3339Nano), result.Implementation, result.Scenario,
		strconv.Itoa(repeat), strconv.Itoa(result.Concurrency), strconv.Itoa(result.VirtualClients),
		floatString(milliseconds(point.Elapsed)), strconv.FormatInt(point.Attempts, 10), strconv.FormatInt(point.AttemptErrors, 10),
		strconv.FormatInt(point.Successes, 10), strconv.FormatInt(point.Errors, 10)}
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}
func floatString(value float64) string { return strconv.FormatFloat(value, 'f', 6, 64) }
