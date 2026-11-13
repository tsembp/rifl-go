// Package benchrun contains the reproducible load generator used by cmd/bench.
package benchrun

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

const (
	ScenarioNormal        = "normal"
	ScenarioLostResponse  = "lost-response"
	ScenarioDuplicate     = "duplicate-heavy"
	ScenarioRetryRate     = "retry-rate"
	ScenarioAckGC         = "ack-gc"
	ScenarioBatchAck      = "batch-ack"
	ScenarioBlockedGap    = "blocked-gap"
	ScenarioRestartSeed   = "restart-seed"
	ScenarioRestartReplay = "restart-replay"
	ConnectionPerAttempt  = "per-attempt"
	ConnectionPersistent  = "persistent"
)

type Config struct {
	Address         string
	Implementation  string
	Scenario        string
	Requests        int
	Duration        time.Duration
	Concurrency     int
	VirtualClients  int
	Warmup          int
	DuplicateCopies int
	RetryPercent    float64
	RetryTransport  bool
	RetryDelay      time.Duration
	PayloadBytes    int
	Seed            int64
	ConnectionMode  string
	Delta           int64
	Timeout         time.Duration
	ClientPrefix    string
	// Transport is "tcp" (net/rpc, default) or "shm" (shared-memory slots in
	// Region; workers use slots SlotBase.., plus one control slot after them).
	Transport string
	Region    string
	SlotBase  int
}

type LatencySample struct {
	Elapsed time.Duration
	Kind    string
	Success bool
}

type TimelinePoint struct {
	Elapsed       time.Duration
	Attempts      int64
	AttemptErrors int64
	Successes     int64
	Errors        int64
}

type Result struct {
	Timestamp           time.Time
	Implementation      string
	Scenario            string
	Requests            int
	Concurrency         int
	VirtualClients      int
	PayloadBytes        int
	RetryPercent        float64
	RetryTransport      bool
	Seed                int64
	ConnectionMode      string
	Attempts            int64
	AttemptErrors       int64
	Successes           int64
	Errors              int64
	Duration            time.Duration
	Throughput          float64
	P50                 time.Duration
	P90                 time.Duration
	P95                 time.Duration
	P99                 time.Duration
	P999                time.Duration
	P9999               time.Duration
	Max                 time.Duration
	BatchAckP50         time.Duration
	BatchAckMax         time.Duration
	ValueBefore         int64
	ValueAfter          int64
	IntendedEffects     int64
	ActualEffects       int64
	DuplicateEffects    int64
	CompletionsBefore   int
	CompletionsAfter    int
	ClientsBefore       int
	ClientsAfter        int
	DatabaseBytesBefore int64
	DatabaseBytesAfter  int64
	Samples             []LatencySample
	Timeline            []TimelinePoint
}

func (c Config) normalized() Config {
	if c.VirtualClients == 0 {
		c.VirtualClients = c.Concurrency
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 25 * time.Millisecond
	}
	if c.ConnectionMode == "" {
		c.ConnectionMode = ConnectionPerAttempt
	}
	if c.Transport == "" {
		c.Transport = TransportTCP
	}
	return c
}

func (c Config) Validate() error {
	c = c.normalized()
	if c.Concurrency <= 0 || c.Timeout <= 0 {
		return errors.New("positive concurrency and positive timeout are required")
	}
	switch c.Transport {
	case TransportTCP:
		if c.Address == "" {
			return errors.New("address is required for tcp transport")
		}
	case TransportSHM:
		if c.Region == "" || c.SlotBase < 0 {
			return errors.New("shm transport requires a region and a nonnegative slot base")
		}
		if c.Implementation != "protected" {
			return errors.New("shm transport serves only the protected implementation")
		}
	default:
		return errors.New("transport must be tcp or shm")
	}
	if c.Requests <= 0 && c.Duration <= 0 {
		return errors.New("positive requests or duration is required")
	}
	if c.Duration < 0 || c.Warmup < 0 || c.RetryDelay < 0 || c.PayloadBytes < 0 {
		return errors.New("duration, warmup, retry delay and payload bytes cannot be negative")
	}
	if c.VirtualClients < c.Concurrency {
		return errors.New("virtual clients must be at least concurrency")
	}
	if c.Delta <= 0 {
		return errors.New("benchmark delta must be positive")
	}
	if c.RetryPercent < 0 || c.RetryPercent > 100 {
		return errors.New("retry percent must be between 0 and 100")
	}
	if c.Implementation != "protected" && c.Implementation != "baseline" {
		return errors.New("implementation must be protected or baseline")
	}
	if c.ConnectionMode != ConnectionPerAttempt && c.ConnectionMode != ConnectionPersistent {
		return errors.New("connection mode must be per-attempt or persistent")
	}
	switch c.Scenario {
	case ScenarioNormal, ScenarioLostResponse, ScenarioDuplicate, ScenarioRetryRate,
		ScenarioAckGC, ScenarioBatchAck, ScenarioBlockedGap, ScenarioRestartSeed, ScenarioRestartReplay:
	default:
		return fmt.Errorf("unknown scenario %q", c.Scenario)
	}
	if c.DuplicateCopies < 2 {
		return errors.New("duplicate copies must be at least 2")
	}
	if c.ClientPrefix == "" {
		return errors.New("client prefix is required")
	}
	if (c.Scenario == ScenarioRestartSeed || c.Scenario == ScenarioRestartReplay || c.Scenario == ScenarioBlockedGap) && c.Warmup != 0 {
		return errors.New("restart and blocked-gap scenarios require warmup=0")
	}
	if (c.Scenario == ScenarioRestartSeed || c.Scenario == ScenarioRestartReplay || c.Scenario == ScenarioBatchAck) && c.Duration > 0 {
		return errors.New("restart and batch-ack scenarios require a fixed request count")
	}
	if c.Scenario == ScenarioBatchAck {
		if c.Implementation != "protected" {
			return errors.New("batch-ack measures protected completion reclamation")
		}
		if c.VirtualClients != c.Concurrency || c.Requests < c.Concurrency {
			return errors.New("batch-ack requires virtual-clients=concurrency and at least one request per worker")
		}
	}
	return nil
}

type clientState struct {
	id              string
	sequence        uint64
	firstIncomplete uint64
}

type workerState struct {
	clients []*clientState
	next    int
	random  *rand.Rand
	caller  caller
}

// Run executes one measured experiment. Controlled duplicate scenarios send the
// same request ID more than once; transport retry also retains that ID.
func Run(ctx context.Context, cfg Config) (Result, error) {
	cfg = cfg.normalized()
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	tr, err := openTransport(cfg)
	if err != nil {
		return Result{}, err
	}
	defer tr.close()
	states, err := newWorkerStates(cfg, tr)
	if err != nil {
		return Result{}, err
	}
	defer closeWorkerCallers(states)
	if cfg.Warmup > 0 {
		warm := cfg
		warm.Requests = cfg.Warmup
		warm.Duration = 0
		warm.Warmup = 0
		warm.Scenario = ScenarioNormal
		if _, err := execute(ctx, warm, tr, states); err != nil {
			return Result{}, fmt.Errorf("warmup: %w", err)
		}
	}
	return execute(ctx, cfg, tr, states)
}

func newWorkerStates(cfg Config, tr transport) ([]*workerState, error) {
	workers := make([]*workerState, cfg.Concurrency)
	for worker := range workers {
		workers[worker] = &workerState{
			random: rand.New(rand.NewSource(cfg.Seed + int64(worker))),
		}
		var err error
		if workers[worker].caller, err = tr.newCaller(worker); err != nil {
			closeWorkerCallers(workers[:worker])
			return nil, err
		}
	}
	for client := 0; client < cfg.VirtualClients; client++ {
		state := &clientState{id: fmt.Sprintf("%s-%d", cfg.ClientPrefix, client), firstIncomplete: 1}
		if cfg.Scenario == ScenarioBlockedGap {
			state.sequence = 1
		}
		worker := client % cfg.Concurrency
		workers[worker].clients = append(workers[worker].clients, state)
	}
	return workers, nil
}

func closeWorkerCallers(workers []*workerState) {
	for _, worker := range workers {
		worker.caller.close()
	}
}

func (w *workerState) nextClient() *clientState {
	client := w.clients[w.next]
	w.next = (w.next + 1) % len(w.clients)
	return client
}

type counters struct {
	attempts      atomic.Int64
	attemptErrors atomic.Int64
	successes     atomic.Int64
	errors        atomic.Int64
	logical       atomic.Int64
}

func execute(ctx context.Context, cfg Config, tr transport, states []*workerState) (Result, error) {
	before, err := tr.stats(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("read initial stats: %w", err)
	}
	if before.Protected != (cfg.Implementation == "protected") {
		return Result{}, fmt.Errorf("server mode does not match -implementation %s", cfg.Implementation)
	}
	started := time.Now()
	var runUntil time.Time
	if cfg.Duration > 0 {
		runUntil = started.Add(cfg.Duration)
	}
	var counts counters
	var samples []LatencySample
	var sampleMu sync.Mutex
	var timeline []TimelinePoint
	var timelineMu sync.Mutex
	stopMonitor := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				timelineMu.Lock()
				timeline = append(timeline, snapshot(started, &counts))
				timelineMu.Unlock()
			case <-stopMonitor:
				return
			}
		}
	}()

	payload := make([]byte, cfg.PayloadBytes)
	var workers sync.WaitGroup
	for worker := 0; worker < cfg.Concurrency; worker++ {
		count := workerRequestCount(cfg.Requests, cfg.Concurrency, worker)
		workers.Add(1)
		go func(worker, count int) {
			defer workers.Done()
			if cfg.Scenario == ScenarioBatchAck {
				runBatchAckWorker(ctx, cfg, states[worker], count, payload, &counts, &samples, &sampleMu)
				return
			}
			runWorker(ctx, cfg, states[worker], count, runUntil, payload, &counts, &samples, &sampleMu)
		}(worker, count)
	}
	workers.Wait()
	close(stopMonitor)
	<-monitorDone
	duration := time.Since(started)
	timelineMu.Lock()
	timeline = append(timeline, snapshot(started, &counts))
	timelineMu.Unlock()

	after, err := tr.stats(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("read final stats: %w", err)
	}
	actual := after.Value - before.Value
	intended := counts.successes.Load() * cfg.Delta
	if cfg.Scenario == ScenarioRestartReplay {
		intended = 0
	}
	successLatencies := sampleDurations(samples, "", true)
	ackLatencies := sampleDurations(samples, "batch-ack", true)
	return Result{
		Timestamp: time.Now().UTC(), Implementation: cfg.Implementation, Scenario: cfg.Scenario,
		Requests: int(counts.logical.Load()), Concurrency: cfg.Concurrency, VirtualClients: cfg.VirtualClients,
		PayloadBytes: cfg.PayloadBytes, RetryPercent: cfg.RetryPercent, RetryTransport: cfg.RetryTransport, Seed: cfg.Seed, ConnectionMode: cfg.ConnectionMode,
		Attempts: counts.attempts.Load(), AttemptErrors: counts.attemptErrors.Load(), Successes: counts.successes.Load(), Errors: counts.errors.Load(),
		Duration: duration, Throughput: float64(counts.successes.Load()) / duration.Seconds(),
		P50: percentile(successLatencies, 50), P90: percentile(successLatencies, 90), P95: percentile(successLatencies, 95),
		P99: percentile(successLatencies, 99), P999: percentile(successLatencies, 99.9), P9999: percentile(successLatencies, 99.99), Max: percentile(successLatencies, 100),
		BatchAckP50: percentile(ackLatencies, 50), BatchAckMax: percentile(ackLatencies, 100),
		ValueBefore: before.Value, ValueAfter: after.Value, IntendedEffects: intended, ActualEffects: actual, DuplicateEffects: actual - intended,
		CompletionsBefore: before.CompletionRecords, CompletionsAfter: after.CompletionRecords,
		ClientsBefore: before.ClientBoundaries, ClientsAfter: after.ClientBoundaries,
		DatabaseBytesBefore: before.DatabaseBytes, DatabaseBytesAfter: after.DatabaseBytes,
		Samples: samples, Timeline: timeline,
	}, nil
}

func runWorker(ctx context.Context, cfg Config, state *workerState, count int, runUntil time.Time, payload []byte, counts *counters, samples *[]LatencySample, sampleMu *sync.Mutex) {
	for operation := 0; ; operation++ {
		if ctx.Err() != nil || (!runUntil.IsZero() && !time.Now().Before(runUntil)) {
			return
		}
		if runUntil.IsZero() && operation >= count {
			return
		}
		client := state.nextClient()
		client.sequence++
		req := riflrpc.AddRequest{ID: riflrpc.RequestID{ClientID: client.id, Sequence: client.sequence}, Delta: cfg.Delta, FirstIncomplete: client.firstIncomplete, Padding: payload}
		copies := 1
		switch cfg.Scenario {
		case ScenarioLostResponse:
			copies = 2
		case ScenarioDuplicate:
			copies = cfg.DuplicateCopies
		case ScenarioRetryRate:
			if state.random.Float64()*100 < cfg.RetryPercent {
				copies = 2
			}
		}
		logicalStarted := time.Now()
		ok := perform(ctx, cfg, state.caller, req, copies, runUntil, counts)
		recordSample(samples, sampleMu, LatencySample{Elapsed: time.Since(logicalStarted), Kind: "operation", Success: ok})
		counts.logical.Add(1)
		if ok {
			counts.successes.Add(1)
			if cfg.Scenario != ScenarioBlockedGap && cfg.Scenario != ScenarioRestartSeed && cfg.Scenario != ScenarioRestartReplay {
				client.firstIncomplete = client.sequence + 1
			}
		} else {
			counts.errors.Add(1)
		}
	}
}

func runBatchAckWorker(ctx context.Context, cfg Config, state *workerState, count int, payload []byte, counts *counters, samples *[]LatencySample, sampleMu *sync.Mutex) {
	client := state.clients[0]
	for operation := 0; operation < count; operation++ {
		client.sequence++
		req := riflrpc.AddRequest{ID: riflrpc.RequestID{ClientID: client.id, Sequence: client.sequence}, Delta: cfg.Delta, FirstIncomplete: client.firstIncomplete, Padding: payload}
		started := time.Now()
		ok := perform(ctx, cfg, state.caller, req, 1, time.Time{}, counts)
		recordSample(samples, sampleMu, LatencySample{Elapsed: time.Since(started), Kind: "operation", Success: ok})
		counts.logical.Add(1)
		if !ok {
			counts.errors.Add(1)
			return
		}
		counts.successes.Add(1)
	}
	client.sequence++
	client.firstIncomplete = client.sequence
	req := riflrpc.AddRequest{ID: riflrpc.RequestID{ClientID: client.id, Sequence: client.sequence}, Delta: cfg.Delta, FirstIncomplete: client.firstIncomplete, Padding: payload}
	started := time.Now()
	ok := perform(ctx, cfg, state.caller, req, 1, time.Time{}, counts)
	recordSample(samples, sampleMu, LatencySample{Elapsed: time.Since(started), Kind: "batch-ack", Success: ok})
	counts.logical.Add(1)
	if ok {
		counts.successes.Add(1)
		client.firstIncomplete = client.sequence + 1
	} else {
		counts.errors.Add(1)
	}
}

func perform(ctx context.Context, cfg Config, caller caller, req riflrpc.AddRequest, copies int, runUntil time.Time, counts *counters) bool {
	for copy := 0; copy < copies; copy++ {
		for {
			counts.attempts.Add(1)
			_, err := caller.add(ctx, req)
			if err == nil {
				break
			}
			counts.attemptErrors.Add(1)
			if !cfg.RetryTransport || definitiveServerError(err) || ctx.Err() != nil || (!runUntil.IsZero() && !time.Now().Before(runUntil)) {
				return false
			}
			timer := time.NewTimer(cfg.RetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false
			case <-timer.C:
			}
		}
	}
	return true
}

type rpcCaller struct {
	address string
	timeout time.Duration
	mode    string
	conn    net.Conn
	client  *rpc.Client
}

func (c *rpcCaller) add(parent context.Context, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	if c.mode == ConnectionPerAttempt {
		return add(parent, c.address, c.timeout, req)
	}
	if err := c.connect(parent); err != nil {
		return riflrpc.AddReply{}, err
	}
	deadline := time.Now().Add(c.timeout)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		c.close()
		return riflrpc.AddReply{}, err
	}
	var reply riflrpc.AddReply
	if err := c.client.Call("Counter.Add", req, &reply); err != nil {
		c.close()
		return riflrpc.AddReply{}, err
	}
	return reply, nil
}

func (c *rpcCaller) connect(parent context.Context) error {
	if c.client != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.address)
	if err != nil {
		return err
	}
	c.conn = conn
	c.client = rpc.NewClient(conn)
	return nil
}

func (c *rpcCaller) close() {
	if c.client != nil {
		c.client.Close()
	} else if c.conn != nil {
		c.conn.Close()
	}
	c.client = nil
	c.conn = nil
}

func definitiveServerError(err error) bool {
	var serverError rpc.ServerError
	return errors.As(err, &serverError) && !strings.HasPrefix(string(serverError), "storage outcome unknown: ")
}

func recordSample(samples *[]LatencySample, mu *sync.Mutex, sample LatencySample) {
	mu.Lock()
	*samples = append(*samples, sample)
	mu.Unlock()
}

func sampleDurations(samples []LatencySample, kind string, success bool) []time.Duration {
	values := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		if sample.Success == success && (kind == "" || sample.Kind == kind) {
			values = append(values, sample.Elapsed)
		}
	}
	return values
}

func snapshot(started time.Time, counts *counters) TimelinePoint {
	return TimelinePoint{Elapsed: time.Since(started), Attempts: counts.attempts.Load(), AttemptErrors: counts.attemptErrors.Load(), Successes: counts.successes.Load(), Errors: counts.errors.Load()}
}

func workerRequestCount(total, concurrency, worker int) int {
	count := total / concurrency
	if worker < total%concurrency {
		count++
	}
	return count
}

func add(parent context.Context, address string, timeout time.Duration, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	var reply riflrpc.AddReply
	if err := call(parent, address, timeout, "Counter.Add", req, &reply); err != nil {
		return riflrpc.AddReply{}, err
	}
	return reply, nil
}

func stats(parent context.Context, address string, timeout time.Duration) (riflrpc.ServiceStats, error) {
	var reply riflrpc.ServiceStats
	if err := call(parent, address, timeout, "Counter.Stats", struct{}{}, &reply); err != nil {
		return riflrpc.ServiceStats{}, err
	}
	return reply, nil
}

func call(parent context.Context, address string, timeout time.Duration, method string, args, reply interface{}) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	return client.Call(method, args, reply)
}

func percentile(values []time.Duration, percent float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(math.Ceil(percent/100*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
