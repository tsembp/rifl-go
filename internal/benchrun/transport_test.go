package benchrun

import (
	"context"
	"path/filepath"
	"testing"

	riflrpc "github.com/tsembp/rifl-go"
	"github.com/tsembp/rifl-go/shm"
)

// serveSHM runs a shared-memory owner in this process, with either the state in
// the region (configuration C) or the bbolt counter behind the slots (B).
func serveSHM(t *testing.T, store string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "region")
	region, err := shm.Create(path, shm.Config{Slots: 8, Clients: 64, Records: 32})
	if err != nil {
		t.Fatal(err)
	}
	epoch, ok, err := region.TryAcquire()
	if err != nil || !ok {
		t.Fatal("could not own region")
	}
	var handler shm.Handler
	if store == "bbolt" {
		counter, err := riflrpc.OpenCounter(filepath.Join(dir, "counter.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { counter.Close() })
		handler = shm.CounterHandler{Counter: counter}
	} else {
		state := shm.NewState(region, epoch)
		state.Recover()
		handler = state
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { shm.Serve(ctx, region, handler); close(done) }()
	t.Cleanup(func() { stop(); <-done; region.Close() })
	return path
}

func shmConfig(region, scenario string) Config {
	cfg := config("", "protected", scenario)
	cfg.Transport = TransportSHM
	cfg.Region = region
	return cfg
}

func TestSHMTransportScenarios(t *testing.T) {
	for _, store := range []string{"shm", "bbolt"} {
		t.Run(store, func(t *testing.T) {
			region := serveSHM(t, store)
			for _, scenario := range []string{ScenarioNormal, ScenarioLostResponse, ScenarioDuplicate, ScenarioRetryRate, ScenarioAckGC} {
				cfg := shmConfig(region, scenario)
				cfg.ClientPrefix = store + "-" + scenario
				cfg.RetryPercent = 50
				result, err := Run(context.Background(), cfg)
				if err != nil {
					t.Fatalf("%s: %v", scenario, err)
				}
				if result.Errors != 0 || result.DuplicateEffects != 0 || result.Successes != int64(cfg.Requests) {
					t.Fatalf("%s: %+v", scenario, result)
				}
			}
			cfg := shmConfig(region, ScenarioBlockedGap)
			cfg.ClientPrefix = store + "-gap"
			result, err := Run(context.Background(), cfg)
			if err != nil || result.CompletionsAfter-result.CompletionsBefore != cfg.Requests {
				t.Fatalf("blocked gap retained %d: %v", result.CompletionsAfter-result.CompletionsBefore, err)
			}
		})
	}
}

func TestSHMTransportBatchAckVerification(t *testing.T) {
	region := serveSHM(t, "shm")
	cfg := shmConfig(region, ScenarioBatchAck)
	cfg.Requests = 3 * 30 // within the 32-record window per client
	result, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletionsAfter != cfg.Concurrency || result.DuplicateEffects != 0 {
		t.Fatalf("batch ack: %+v", result)
	}
	if err := VerifyBatchAck(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestSHMTransportRejectsTooFewSlots(t *testing.T) {
	region := serveSHM(t, "shm")
	cfg := shmConfig(region, ScenarioNormal)
	cfg.Concurrency, cfg.VirtualClients = 8, 8 // 8 workers + 1 control > 8 slots
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("ran without a control slot")
	}
}
