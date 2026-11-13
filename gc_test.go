package riflrpc

import (
	"math"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestPerClientGCUsesNumericOrderAndPreservesOtherClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var reply AddReply
	sequences := []uint64{1, 2, 9, 10, 255, 256, 65535, 65536, math.MaxUint64 - 1, math.MaxUint64}
	for _, sequence := range sequences {
		if err := c.Add(testRequest("Alice", sequence, 1, 1), &reply); err != nil {
			t.Fatal(err)
		}
	}
	// Raw bucket names must distinguish clients even with separators or quotes.
	for _, client := range []string{"Bob", "Alice/child", `Alice"quoted`} {
		if err := c.Add(testRequest(client, 1, 1, 1), &reply); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Add(testRequest("Alice", 65537, 1, 256), &reply); err != nil {
		t.Fatal(err)
	}
	if boundary, count := persistentClientState(t, c, "Alice"); boundary != 256 || count != 6 {
		t.Fatalf("Alice boundary=%d records=%d, want 256 and 6", boundary, count)
	}
	if err := c.db.View(func(tx *bolt.Tx) error {
		got := clientSequences(tx, "Alice")
		want := []uint64{256, 65535, 65536, 65537, math.MaxUint64 - 1, math.MaxUint64}
		if len(got) != len(want) {
			t.Fatalf("got ordered records %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("incorrect numeric key order at index %d: %v", i, got)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, client := range []string{"Bob", "Alice/child", `Alice"quoted`} {
		if boundary, count := persistentClientState(t, c, client); boundary != 1 || count != 1 {
			t.Fatalf("another client changed: %q boundary=%d records=%d", client, boundary, count)
		}
		if err := c.Add(testRequest(client, 1, 1, 1), &reply); err != nil || reply.Value != int64(11+i) {
			t.Fatalf("another client's original reply changed: %+v, %v", reply, err)
		}
	}
	if err := c.Add(testRequest("Alice", 255, 1, 1), &reply); err == nil || err.Error() != staleRequestError {
		t.Fatalf("deleted request was reexecuted: %v", err)
	}
	var stats ServiceStats
	if err := c.Stats(struct{}{}, &stats); err != nil || stats.Value != 14 || stats.CompletionRecords != 9 || stats.ClientBoundaries != 1 {
		t.Fatalf("nested stats: %+v, %v", stats, err)
	}
}

func TestPerClientGCDeletesBatchAcrossPages(t *testing.T) {
	c, err := OpenCounter(filepath.Join(t.TempDir(), "counter.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var reply AddReply
	for sequence := uint64(1); sequence <= 512; sequence++ {
		if err := c.Add(testRequest("client", sequence, 1, 1), &reply); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Add(testRequest("client", 513, 1, 513), &reply); err != nil || reply.Value != 513 {
		t.Fatalf("batch acknowledgement: %+v, %v", reply, err)
	}
	if boundary, count := persistentClientState(t, c, "client"); boundary != 513 || count != 1 {
		t.Fatalf("batch GC skipped records: boundary=%d count=%d", boundary, count)
	}
	for _, sequence := range []uint64{1, 255, 256, 512} {
		if err := c.Add(testRequest("client", sequence, 1, 1), &reply); err == nil || err.Error() != staleRequestError {
			t.Fatalf("sequence %d not stale after batch GC: %v", sequence, err)
		}
	}
}

// Neighbouring clients sort immediately before and after the target in the flat
// completion bucket. A partial acknowledgement across many leaves must delete
// exactly the target's lower records and nothing on either side.
func TestPartialBatchGCKeepsNeighboursAndUpperRecords(t *testing.T) {
	c, err := OpenCounter(filepath.Join(t.TempDir(), "counter.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	long := strings.Repeat("x", 48)
	before, target, after := long+"-a", long+"-b", long+"-c"
	var reply AddReply
	for _, client := range []string{before, after} {
		for sequence := uint64(1); sequence <= 50; sequence++ {
			if err := c.Add(testRequest(client, sequence, 1, 1), &reply); err != nil {
				t.Fatal(err)
			}
		}
	}
	for sequence := uint64(1); sequence <= 3000; sequence++ {
		if err := c.Add(testRequest(target, sequence, 1, 1), &reply); err != nil {
			t.Fatal(err)
		}
	}
	// Acknowledge 1..1500 on a retry of an existing record: GC only, no new effect.
	if err := c.Add(testRequest(target, 1501, 1, 1501), &reply); err != nil || reply.Value != 1601 {
		t.Fatalf("partial acknowledgement replay: %+v, %v", reply, err)
	}
	if err := c.db.View(func(tx *bolt.Tx) error {
		got := clientSequences(tx, target)
		if len(got) != 1500 || got[0] != 1501 || got[len(got)-1] != 3000 {
			t.Fatalf("target kept %d records [%v..%v], want 1501..3000", len(got), got[0], got[len(got)-1])
		}
		for i, sequence := range got {
			if sequence != uint64(1501+i) {
				t.Fatalf("target record gap at index %d: %d", i, sequence)
			}
		}
		for _, client := range []string{before, after} {
			if n := len(clientSequences(tx, client)); n != 50 {
				t.Fatalf("neighbour %q lost records: %d left", client[len(client)-2:], n)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
