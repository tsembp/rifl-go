package shm

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

const crashExit = 77

// TestOwnerCrashHelper runs as a separate owner process. It serves the region
// and exits abruptly at the requested point, without unmapping or unlocking:
// the kernel then releases its lock, as for a crashed compute node.
func TestOwnerCrashHelper(t *testing.T) {
	point := os.Getenv("RIFL_SHM_CRASH")
	if point == "" {
		return
	}
	r, err := Open(os.Getenv("RIFL_SHM_REGION"))
	if err != nil {
		t.Fatal(err)
	}
	epoch, ok, err := r.TryAcquire()
	if err != nil || !ok {
		t.Fatalf("helper could not own region: %v %v", ok, err)
	}
	crash := func() { os.Exit(crashExit) }
	var h Handler
	if os.Getenv("RIFL_SHM_MODE") == "bbolt" {
		counter, err := riflrpc.OpenCounter(os.Getenv("RIFL_SHM_DB"))
		if err != nil {
			t.Fatal(err)
		}
		h = crashAfterHandle{CounterHandler{counter}, crash}
	} else {
		s := NewState(r, epoch)
		switch point {
		case "before-commit":
			s.beforeCommit = crash
		case "after-commit":
			s.afterCommit = crash
		case "mid-apply":
			s.midApply = crash
		case "after-apply":
			s.afterApply = crash
		default:
			t.Fatalf("unknown crash point %q", point)
		}
		s.Recover()
		h = s
	}
	Serve(context.Background(), r, h)
}

// crashAfterHandle models a lost response: the operation is durable, but the
// owner dies before writing the reply slot.
type crashAfterHandle struct {
	h     Handler
	crash func()
}

func (c crashAfterHandle) Handle(r riflrpc.AddRequest) (riflrpc.AddReply, Code) {
	reply, code := c.h.Handle(r)
	if r.Delta == 99 {
		c.crash()
	}
	return reply, code
}

// startOwner launches a crashing owner and, when it dies, takes over in this
// process with a fresh epoch, recovery and a new server loop.
func startOwner(t *testing.T, path string, env ...string) (takeover <-chan Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOwnerCrashHelper$")
	cmd.Env = append(append(os.Environ(), "RIFL_SHM_REGION="+path), env...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan Handler, 1)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	serveCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { stop(); <-done })
	go func() {
		defer close(done)
		err := cmd.Wait()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
			t.Errorf("owner did not crash at the hook: %v", err)
			return
		}
		// A standby blocks on the lock, as a real one would.
		lockCtx, cancelLock := context.WithTimeout(context.Background(), 5*time.Second)
		started := time.Now()
		epoch, err := r.Acquire(lockCtx)
		cancelLock()
		if err != nil {
			t.Errorf("standby could not take over: %v", err)
			return
		}
		t.Logf("lock acquired %v after owner exit was observed", time.Since(started))
		var h Handler
		if db := envValue(env, "RIFL_SHM_DB"); db != "" {
			counter, err := riflrpc.OpenCounter(db)
			if err != nil {
				t.Error(err)
				return
			}
			defer counter.Close()
			h = CounterHandler{counter}
		} else {
			s := NewState(r, epoch)
			s.Recover()
			h = s
		}
		ready <- h
		Serve(serveCtx, r, h)
	}()
	return ready
}

func envValue(env []string, key string) string {
	for _, kv := range env {
		if len(kv) > len(key) && kv[:len(key)+1] == key+"=" {
			return kv[len(key)+1:]
		}
	}
	return ""
}

func TestOwnerCrashAndTakeoverAtEveryPoint(t *testing.T) {
	for _, point := range []string{"before-commit", "after-commit", "mid-apply", "after-apply"} {
		t.Run(point, func(t *testing.T) {
			r, path := newRegion(t, Config{Slots: 2, Clients: 4, Records: 4})
			// Seed an acknowledged record so the crashing request also advances
			// the boundary and reclaims it inside the same commit.
			seed := NewState(r, 0)
			mustHandle(t, seed, req("c", 1, 10, 1), 10, CodeOK)
			takeover := startOwner(t, path, "RIFL_SHM_CRASH="+point)
			client, err := NewClient(r, 0)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// The owner dies while handling this; the standby must finish it.
			reply, err := client.Call(ctx, req("c", 2, 5, 2))
			if err != nil || reply.Value != 15 {
				t.Fatalf("after takeover: %+v %v, want 15", reply, err)
			}
			h := <-takeover
			s := h.(*State)
			// Exactly one effect, the reclaimed record is stale, and a retry of
			// the crashed request replays its saved reply.
			if s.Counter() != 15 {
				t.Fatalf("counter %d, want 15 (duplicate or lost effect)", s.Counter())
			}
			if _, err := client.Call(ctx, req("c", 2, 5, 2)); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if _, err := client.Call(ctx, req("c", 1, 10, 2)); !errors.Is(err, CodeStale) {
				t.Fatalf("reclaimed record not stale: %v", err)
			}
			if s.Counter() != 15 {
				t.Fatalf("retries changed counter to %d", s.Counter())
			}
			if stats := s.Stats(); stats.CompletionRecords != 1 || stats.ClientBoundaries != 1 {
				t.Fatalf("stats %+v, want 1 record and 1 boundary", stats)
			}
		})
	}
}

// Configuration B: shared-memory slots in front of the bbolt counter. The
// owner commits to bbolt and dies before writing the reply; the standby reopens
// the same database and the pending slot gets the saved result.
func TestBboltBehindSlotsSurvivesLostReply(t *testing.T) {
	r, path := newRegion(t, Config{Slots: 1, Clients: 1, Records: 1})
	db := filepath.Join(t.TempDir(), "counter.db")
	takeover := startOwner(t, path, "RIFL_SHM_CRASH=lost-reply", "RIFL_SHM_MODE=bbolt", "RIFL_SHM_DB="+db)
	client, err := NewClient(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reply, err := client.Call(ctx, req("c", 1, 99, 1))
	if err != nil || reply.Value != 99 {
		t.Fatalf("after takeover: %+v %v, want saved 99", reply, err)
	}
	<-takeover
	if reply, err := client.Call(ctx, req("c", 2, 1, 2)); err != nil || reply.Value != 100 {
		t.Fatalf("next request: %+v %v, want 100 (no duplicate effect)", reply, err)
	}
}
