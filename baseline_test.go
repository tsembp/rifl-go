package riflrpc

import (
	"math"
	"path/filepath"
	"testing"
)

func TestUnprotectedCounterAppliesDuplicateInvocations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "baseline.db")
	counter, err := OpenUnprotectedCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	req := AddRequest{
		ID:              RequestID{ClientID: "baseline-client", Sequence: 1},
		Delta:           7,
		FirstIncomplete: 1,
	}
	var first, second AddReply
	if err := counter.Add(req, &first); err != nil {
		t.Fatal(err)
	}
	if err := counter.Add(req, &second); err != nil {
		t.Fatal(err)
	}
	if first.Value != 7 || second.Value != 14 {
		t.Fatalf("duplicate replies = %d, %d; want 7, 14", first.Value, second.Value)
	}
	var stats ServiceStats
	if err := counter.Stats(struct{}{}, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Value != 14 || stats.CompletionRecords != 0 || stats.ClientBoundaries != 0 || stats.DatabaseBytes == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if err := counter.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenUnprotectedCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var value int64
	if err := reopened.Value(struct{}{}, &value); err != nil {
		t.Fatal(err)
	}
	if value != 14 {
		t.Fatalf("value after reopen = %d; want 14", value)
	}
}

func TestUnprotectedCounterOverflowIsDefinitive(t *testing.T) {
	counter, err := OpenUnprotectedCounter(filepath.Join(t.TempDir(), "baseline.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer counter.Close()
	var reply AddReply
	if err := counter.Add(testRequest("overflow", 1, math.MaxInt64, 1), &reply); err != nil {
		t.Fatal(err)
	}
	err = counter.Add(testRequest("overflow", 2, 1, 2), &reply)
	if err == nil || err.Error() != "counter overflow" {
		t.Fatalf("overflow error = %v; want definitive counter overflow", err)
	}
}
