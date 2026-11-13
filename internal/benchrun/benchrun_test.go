package benchrun

import (
	"context"
	"net"
	"net/rpc"
	"path/filepath"
	"testing"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

type closeableService interface {
	Close() error
}

func serve(t *testing.T, service interface{}) (string, func()) {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("Counter", service); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return listener.Addr().String(), func() {
		listener.Close()
		<-done
		if closer, ok := service.(closeableService); ok {
			if err := closer.Close(); err != nil {
				t.Errorf("close service: %v", err)
			}
		}
	}
}

func config(address, implementation, scenario string) Config {
	return Config{
		Address:         address,
		Implementation:  implementation,
		Scenario:        scenario,
		Requests:        12,
		Concurrency:     3,
		DuplicateCopies: 4,
		Delta:           1,
		Timeout:         2 * time.Second,
		ClientPrefix:    "test",
	}
}

func TestLostResponseShowsOnlyBaselineDuplicateEffects(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	protectedAddress, stopProtected := serve(t, protected)
	protectedResult, err := Run(context.Background(), config(protectedAddress, "protected", ScenarioLostResponse))
	if err != nil {
		t.Fatal(err)
	}
	stopProtected()
	if protectedResult.ActualEffects != 12 || protectedResult.DuplicateEffects != 0 || protectedResult.Attempts != 24 {
		t.Fatalf("protected result = %+v", protectedResult)
	}

	baseline, err := riflrpc.OpenUnprotectedCounter(filepath.Join(t.TempDir(), "baseline.db"))
	if err != nil {
		t.Fatal(err)
	}
	baselineAddress, stopBaseline := serve(t, baseline)
	baselineResult, err := Run(context.Background(), config(baselineAddress, "baseline", ScenarioLostResponse))
	if err != nil {
		t.Fatal(err)
	}
	stopBaseline()
	if baselineResult.ActualEffects != 24 || baselineResult.DuplicateEffects != 12 || baselineResult.Attempts != 24 {
		t.Fatalf("baseline result = %+v", baselineResult)
	}
}

func TestBlockedGapRetainsProtectedCompletions(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, protected)
	defer stop()
	result, err := Run(context.Background(), config(address, "protected", ScenarioBlockedGap))
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletionsAfter-result.CompletionsBefore != 12 {
		t.Fatalf("retained completions = %d; want 12", result.CompletionsAfter-result.CompletionsBefore)
	}
}

func TestWarmupUsesSameClientsAndIsReclaimed(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, protected)
	defer stop()
	cfg := config(address, "protected", ScenarioAckGC)
	cfg.Warmup = 6
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletionsAfter != cfg.Concurrency {
		t.Fatalf("completion records after warmup and measurement = %d; want %d", result.CompletionsAfter, cfg.Concurrency)
	}
}

func TestServerModeMismatchIsRejected(t *testing.T) {
	baseline, err := riflrpc.OpenUnprotectedCounter(filepath.Join(t.TempDir(), "baseline.db"))
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, baseline)
	defer stop()
	_, err = Run(context.Background(), config(address, "protected", ScenarioNormal))
	if err == nil {
		t.Fatal("expected server mode mismatch")
	}
}

func TestRestartReplayHasNoProtectedEffect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "protected.db")
	protected, err := riflrpc.OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, protected)
	seed := config(address, "protected", ScenarioRestartSeed)
	if _, err := Run(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	stop()

	protected, err = riflrpc.OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	address, stop = serve(t, protected)
	defer stop()
	replay := config(address, "protected", ScenarioRestartReplay)
	result, err := Run(context.Background(), replay)
	if err != nil {
		t.Fatal(err)
	}
	if result.ActualEffects != 0 || result.DuplicateEffects != 0 {
		t.Fatalf("restart replay result = %+v", result)
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	values := []time.Duration{5, 1, 4, 2, 3}
	if got := percentile(values, 50); got != 3 {
		t.Fatalf("p50 = %v; want 3", got)
	}
	if got := percentile(values, 99); got != 5 {
		t.Fatalf("p99 = %v; want 5", got)
	}
	if got := percentile(values, 99.99); got != 5 {
		t.Fatalf("p99.99 = %v; want 5", got)
	}
}

func TestDurationVirtualClientsPayloadAndTimeline(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, protected)
	defer stop()
	cfg := config(address, "protected", ScenarioNormal)
	cfg.Duration = 100 * time.Millisecond
	cfg.VirtualClients = 6
	cfg.PayloadBytes = 256
	cfg.ConnectionMode = ConnectionPersistent
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requests == 0 || result.VirtualClients != 6 || result.PayloadBytes != 256 || result.ConnectionMode != ConnectionPersistent {
		t.Fatalf("duration result = %+v", result)
	}
	if len(result.Samples) != result.Requests || len(result.Timeline) == 0 {
		t.Fatalf("samples=%d requests=%d timeline=%d", len(result.Samples), result.Requests, len(result.Timeline))
	}
	if result.CompletionsAfter > cfg.VirtualClients {
		t.Fatalf("completion records = %d; want at most %d", result.CompletionsAfter, cfg.VirtualClients)
	}
}

func TestRetryRateShowsOnlyBaselineDuplicateEffects(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	protectedAddress, stopProtected := serve(t, protected)
	cfg := config(protectedAddress, "protected", ScenarioRetryRate)
	cfg.RetryPercent = 100
	protectedResult, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	stopProtected()
	if protectedResult.Attempts != 24 || protectedResult.DuplicateEffects != 0 {
		t.Fatalf("protected result = %+v", protectedResult)
	}

	baseline, err := riflrpc.OpenUnprotectedCounter(filepath.Join(t.TempDir(), "baseline.db"))
	if err != nil {
		t.Fatal(err)
	}
	baselineAddress, stopBaseline := serve(t, baseline)
	defer stopBaseline()
	cfg = config(baselineAddress, "baseline", ScenarioRetryRate)
	cfg.RetryPercent = 100
	baselineResult, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if baselineResult.Attempts != 24 || baselineResult.DuplicateEffects != 12 {
		t.Fatalf("baseline result = %+v", baselineResult)
	}
}

func TestBatchAckMeasuresReclamation(t *testing.T) {
	protected, err := riflrpc.OpenCounter(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	address, stop := serve(t, protected)
	defer stop()
	cfg := config(address, "protected", ScenarioBatchAck)
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requests != cfg.Requests+cfg.Concurrency {
		t.Fatalf("logical requests = %d; want %d", result.Requests, cfg.Requests+cfg.Concurrency)
	}
	if result.CompletionsAfter != cfg.Concurrency || result.BatchAckMax == 0 {
		t.Fatalf("batch ack result = %+v", result)
	}
	batchSamples := 0
	for _, sample := range result.Samples {
		if sample.Kind == "batch-ack" {
			batchSamples++
		}
	}
	if batchSamples != cfg.Concurrency {
		t.Fatalf("batch samples = %d; want %d", batchSamples, cfg.Concurrency)
	}
	if err := VerifyBatchAck(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	wrong := cfg
	wrong.ClientPrefix += "-unknown"
	if err := VerifyBatchAck(context.Background(), wrong); err == nil {
		t.Fatal("verification accepted clients that never ran")
	}
}
