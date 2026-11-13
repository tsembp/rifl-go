package shm

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

func newRegion(t *testing.T, cfg Config) (*Region, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "region")
	r, err := Create(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, path
}

func req(client string, seq uint64, delta int64, first uint64) riflrpc.AddRequest {
	return riflrpc.AddRequest{ID: riflrpc.RequestID{ClientID: client, Sequence: seq}, Delta: delta, FirstIncomplete: first}
}

func mustHandle(t *testing.T, s *State, r riflrpc.AddRequest, want int64, wantCode Code) {
	t.Helper()
	reply, code := s.Handle(r)
	if code != wantCode || (code == CodeOK && reply.Value != want) {
		t.Fatalf("%+v: got %d %v, want %d %v", r, reply.Value, code, want, wantCode)
	}
}

func TestStateReplaysOriginalResultsAndRejectsStale(t *testing.T) {
	r, _ := newRegion(t, Config{Slots: 1, Clients: 8, Records: 4})
	s := NewState(r, 1)
	mustHandle(t, s, req("alice", 1, 5, 1), 5, CodeOK)
	mustHandle(t, s, req("bob", 1, 7, 1), 12, CodeOK)
	// A duplicate replays its own saved value, not the current counter.
	mustHandle(t, s, req("alice", 1, 5, 1), 5, CodeOK)
	mustHandle(t, s, req("alice", 1, 6, 1), 0, CodeConflict)
	// Acknowledging 1 reclaims it; the retry is then permanently stale.
	mustHandle(t, s, req("alice", 2, 1, 2), 13, CodeOK)
	mustHandle(t, s, req("alice", 1, 5, 2), 0, CodeStale)
	// A lower acknowledgement never moves the boundary back.
	mustHandle(t, s, req("alice", 1, 5, 1), 0, CodeStale)
	if got := s.Counter(); got != 13 {
		t.Fatalf("counter %d, want 13", got)
	}
	stats := s.Stats()
	if stats.ClientBoundaries != 2 || stats.CompletionRecords != 2 {
		t.Fatalf("stats %+v, want 2 boundaries and 2 records", stats)
	}
}

func TestStateRemembersOverflowAndRefusesFullWindow(t *testing.T) {
	r, _ := newRegion(t, Config{Slots: 1, Clients: 2, Records: 2})
	s := NewState(r, 1)
	mustHandle(t, s, req("c", 1, math.MaxInt64, 1), math.MaxInt64, CodeOK)
	mustHandle(t, s, req("c", 2, 1, 1), 0, CodeOverflow)
	mustHandle(t, s, req("c", 2, 1, 1), 0, CodeOverflow) // remembered, not re-executed
	// Records 1 and 2 are unacknowledged; 3 would overwrite record 1's slot.
	mustHandle(t, s, req("c", 3, -1, 1), 0, CodeRetry)
	if s.Counter() != math.MaxInt64 {
		t.Fatal("refused request changed state")
	}
	// After acknowledging, the same ID executes exactly once.
	mustHandle(t, s, req("c", 3, -1, 3), math.MaxInt64-1, CodeOK)
	mustHandle(t, s, req("c", 3, -1, 3), math.MaxInt64-1, CodeOK)
	// The client table is full after two clients; refusal is definitive and
	// leaves no state behind.
	mustHandle(t, s, req("d", 1, 1, 1), math.MaxInt64, CodeOK)
	mustHandle(t, s, req("e", 1, 1, 1), 0, CodeClientTableFull)
	mustHandle(t, s, req("", 1, 1, 1), 0, CodeInvalid)
	mustHandle(t, s, req(string(make([]byte, MaxClientIDBytes+1)), 1, 1, 1), 0, CodeInvalid)
}

func TestLargeAcknowledgementJumpClearsAtMostOneWindow(t *testing.T) {
	r, _ := newRegion(t, Config{Slots: 1, Clients: 1, Records: 8})
	s := NewState(r, 1)
	for seq := uint64(1); seq <= 8; seq++ {
		mustHandle(t, s, req("c", seq, 1, 1), int64(seq), CodeOK)
	}
	start := time.Now()
	mustHandle(t, s, req("c", math.MaxUint64-1, 1, math.MaxUint64-1), 9, CodeOK)
	if time.Since(start) > time.Second {
		t.Fatal("acknowledgement jump scanned far beyond the window")
	}
	if got := s.Stats().CompletionRecords; got != 1 {
		t.Fatalf("records %d, want 1", got)
	}
}

func TestRegionStatePersistsAcrossReopen(t *testing.T) {
	r, path := newRegion(t, Config{Slots: 1, Clients: 4, Records: 4})
	mustHandle(t, NewState(r, 1), req("c", 1, 3, 1), 3, CodeOK)
	again, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	s := NewState(again, 2)
	s.Recover()
	mustHandle(t, s, req("c", 1, 3, 1), 3, CodeOK)
	if s.Counter() != 3 {
		t.Fatalf("counter %d, want 3", s.Counter())
	}
	if _, err := Create(path, Config{Slots: 1, Clients: 1, Records: 1}); err == nil {
		t.Fatal("Create reinitialized an existing region")
	}
}

func TestServeAndClientTimeoutRetrySameID(t *testing.T) {
	r, _ := newRegion(t, Config{Slots: 2, Clients: 4, Records: 4})
	client, err := NewClient(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	// No server yet: the call times out ambiguously and stays in the slot.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = client.Call(ctx, req("c", 1, 4, 1))
	cancel()
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v, want timeout", err)
	}
	if _, ok, err := r.TryAcquire(); !ok || err != nil {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	s := NewState(r, 1)
	serveCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Serve(serveCtx, r, s); close(done) }()
	defer func() { stop(); <-done }()
	call := func(rq riflrpc.AddRequest) (riflrpc.AddReply, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return client.Call(ctx, rq)
	}
	// Retrying the same ID collects the original execution.
	if reply, err := call(req("c", 1, 4, 1)); err != nil || reply.Value != 4 {
		t.Fatalf("retry: %+v %v", reply, err)
	}
	if reply, err := call(req("c", 2, 1, 2)); err != nil || reply.Value != 5 {
		t.Fatalf("next: %+v %v", reply, err)
	}
	if _, err := call(req("c", 1, 4, 2)); !errors.Is(err, CodeStale) {
		t.Fatalf("stale: %v", err)
	}
}
