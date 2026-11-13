package riflrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestPersistentReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	req := testRequest("client", 1, math.MaxInt64, 1)
	overflow := testRequest("client", 2, 1, 1)
	var reply AddReply
	if err := c.Add(req, &reply); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(overflow, &reply); err == nil {
		t.Fatal("overflow accepted")
	}
	if err := c.Add(testRequest("client", 3, -1, 1), &reply); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Add(req, &reply); err != nil || reply.Value != math.MaxInt64 {
		t.Fatalf("original reply not restored: %+v, %v", reply, err)
	}
	if err := c.Add(overflow, &reply); err == nil {
		t.Fatal("forgot original overflow error")
	}
	conflict := req
	conflict.Delta = 1
	if err := c.Add(conflict, &reply); err == nil {
		t.Fatal("conflicting ID accepted after reopen")
	}
	if err := c.Add(AddRequest{}, &reply); err == nil {
		t.Fatal("invalid request accepted")
	}
	var value int64
	if err := c.Value(struct{}{}, &value); err != nil || value != math.MaxInt64-1 {
		t.Fatalf("value=%d, err=%v", value, err)
	}
}

func TestPersistentConcurrentLostResponse(t *testing.T) {
	c, err := OpenCounter(filepath.Join(t.TempDir(), "counter.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() }) // Registered before server cleanup: connections close first.
	addr, attempts := startServer(t, c, true)
	client := newTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := client.NewRequest(10)
	if reply, err := client.Add(ctx, req); err != nil || reply.Value != 10 {
		t.Fatalf("lost response: %+v, %v", reply, err)
	}
	if attempts.Load() < 2 {
		t.Fatal("expected retry after lost response")
	}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if reply, err := rpcAdd(addr, req); err != nil || reply.Value != 10 {
				t.Errorf("duplicate: %+v, %v", reply, err)
			}
			unique := testRequest(fmt.Sprintf("persistent-unique-%d", i), 1, 1, 1)
			if _, err := rpcAdd(addr, unique); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	var value int64
	if err := c.Value(struct{}{}, &value); err != nil || value != 34 {
		t.Fatalf("value=%d, err=%v", value, err)
	}
}

func persistentClientState(t *testing.T, c *PersistentCounter, clientID string) (uint64, int) {
	t.Helper()
	var firstIncomplete uint64
	count := 0
	err := c.db.View(func(tx *bolt.Tx) error {
		var err error
		firstIncomplete, err = persistentFirstIncomplete(tx.Bucket(clientsBucket), clientID)
		if err != nil {
			return err
		}
		count = len(clientSequences(tx, clientID))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return firstIncomplete, count
}

func TestPersistentAcknowledgementSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	var reply AddReply
	first := testRequest("client", 1, 10, 1)
	second := testRequest("client", 2, 5, 2)
	if err := c.Add(first, &reply); err != nil || reply.Value != 10 {
		t.Fatalf("first: %+v, %v", reply, err)
	}
	if err := c.Add(second, &reply); err != nil || reply.Value != 15 {
		t.Fatalf("second: %+v, %v", reply, err)
	}
	if boundary, count := persistentClientState(t, c, "client"); boundary != 2 || count != 1 {
		t.Fatalf("boundary=%d completions=%d, want 2 and 1", boundary, count)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Add(first, &reply); err == nil || err.Error() != staleRequestError {
		t.Fatalf("reclaimed request was not stale after restart: %v", err)
	}
	third := testRequest("client", 3, 2, 1) // Stale acknowledgement must not lower the boundary.
	if err := c.Add(third, &reply); err != nil || reply.Value != 17 {
		t.Fatalf("third: %+v, %v", reply, err)
	}
	if boundary, count := persistentClientState(t, c, "client"); boundary != 2 || count != 2 {
		t.Fatalf("boundary=%d completions=%d, want 2 and 2", boundary, count)
	}
	fourth := testRequest("client", 4, 1, 4)
	if err := c.Add(fourth, &reply); err != nil || reply.Value != 18 {
		t.Fatalf("fourth: %+v, %v", reply, err)
	}
	if boundary, count := persistentClientState(t, c, "client"); boundary != 4 || count != 1 {
		t.Fatalf("boundary=%d completions=%d, want 4 and 1", boundary, count)
	}
	overflow := testRequest("client", 5, math.MaxInt64, 4)
	if err := c.Add(overflow, &reply); err == nil {
		t.Fatal("overflow accepted")
	}
	sixth := testRequest("client", 6, -1, 6)
	if err := c.Add(sixth, &reply); err != nil || reply.Value != 17 {
		t.Fatalf("sixth: %+v, %v", reply, err)
	}
	if err := c.Add(overflow, &reply); err == nil || err.Error() != staleRequestError {
		t.Fatalf("acknowledged overflow was not stale: %v", err)
	}
}

func TestOpenCounterMigratesAcknowledgementBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		state, err := tx.CreateBucket(stateBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucket(legacyResultsBucket); err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], 7)
		return state.Put(valueKey, value[:])
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(clientsBucket) == nil {
			return errors.New("clients bucket was not created")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var value int64
	if err := c.Value(struct{}{}, &value); err != nil || value != 7 {
		t.Fatalf("value=%d, err=%v", value, err)
	}
}

func TestStorageFailureRemainsAmbiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	addr, attempts := startServer(t, c, false)
	client := newTestClient(t, addr)
	req := client.NewRequest(10)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := client.Add(ctx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("storage error must not be a definitive rejection: %v", err)
	}
	if got := client.prepareRequest(req).FirstIncomplete; got != req.ID.Sequence {
		t.Fatalf("ambiguous storage failure acknowledged request: first incomplete=%d", got)
	}
	if attempts.Load() < 2 {
		t.Fatal("storage failure was not retried")
	}
	reopened, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var value int64
	if err := reopened.Value(struct{}{}, &value); err != nil || value != 0 {
		t.Fatalf("failed storage operation changed value: %d, %v", value, err)
	}
	if err := reopened.Add(req, &AddReply{}); err != nil {
		t.Fatal(err)
	}
}

// TestCrashServerHelper runs only in a subprocess. os.Exit deliberately bypasses
// Close and all defers, at a transaction boundary or after commit before reply.
func TestCrashServerHelper(t *testing.T) {
	if os.Getenv("RIFL_CRASH_HELPER") != "1" {
		return
	}
	c, err := OpenCounter(os.Getenv("RIFL_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	crash := func() { os.Exit(79) }
	switch os.Getenv("RIFL_CRASH_POINT") {
	case "before":
		c.beforeCommit = crash
	case "after":
		c.afterCommit = crash
	}
	s := rpc.NewServer()
	if err := s.RegisterName("Counter", c); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("RIFL_ADDR=" + l.Addr().String())
	for {
		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		go s.ServeConn(conn)
	}
}

type testProcess struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error
	stderr bytes.Buffer
}

func launchCrashServer(t *testing.T, path, crashPoint string) (string, *testProcess) {
	t.Helper()
	p := &testProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestCrashServerHelper$"), done: make(chan struct{})}
	p.cmd.Env = append(os.Environ(), "RIFL_CRASH_HELPER=1", "RIFL_TEST_DB="+path, "RIFL_CRASH_POINT="+crashPoint)
	p.cmd.Stderr = &p.stderr
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if addr, ok := strings.CutPrefix(scanner.Text(), "RIFL_ADDR="); ok {
				ready <- addr
				return
			}
		}
		ready <- ""
	}()
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("subprocess failed to exit")
		}
	})
	select {
	case addr := <-ready:
		if addr == "" {
			t.Fatal("subprocess exited before listening")
		}
		return addr, p
	case <-time.After(5 * time.Second):
		t.Fatal("subprocess startup timeout")
	}
	return "", nil
}

func TestCrashRecovery(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "counter.db")
			addr, crashed := launchCrashServer(t, path, point)
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			client := rpc.NewClient(conn)
			req := testRequest("surviving-client", 42, 10, 1)
			var reply AddReply
			err = client.Call("Counter.Add", req, &reply)
			client.Close()
			if err == nil {
				t.Fatal("crashed server returned success")
			}
			select {
			case <-crashed.done:
				var exit *exec.ExitError
				if !errors.As(crashed.err, &exit) || exit.ExitCode() != 79 {
					t.Fatalf("wrong crash: %v, %s", crashed.err, crashed.stderr.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("crash did not terminate server")
			}
			addr, _ = launchCrashServer(t, path, "")
			probe, err := rpc.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer probe.Close()
			var value int64
			if err := probe.Call("Counter.Value", struct{}{}, &value); err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if point == "after" {
				want = 10
			}
			if value != want {
				t.Fatalf("recovered value=%d, want %d", value, want)
			}
			retrying := newTestClient(t, addr)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if reply, err := retrying.Add(ctx, req); err != nil || reply.Value != 10 {
				t.Fatalf("retry after restart: %+v, %v", reply, err)
			}
			if _, err := retrying.Add(ctx, retrying.NewRequest(5)); err != nil {
				t.Fatal(err)
			}
			if reply, err := retrying.Add(ctx, req); err != nil || reply.Value != 10 {
				t.Fatalf("original reply after later mutation: %+v, %v", reply, err)
			}
			if err := probe.Call("Counter.Value", struct{}{}, &value); err != nil || value != 15 {
				t.Fatalf("final value=%d, err=%v", value, err)
			}
		})
	}
}

func TestAcknowledgementCrashRecovery(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "counter.db")
			counter, err := OpenCounter(path)
			if err != nil {
				t.Fatal(err)
			}
			first := testRequest("ack-client", 1, 10, 1)
			var reply AddReply
			if err := counter.Add(first, &reply); err != nil {
				t.Fatal(err)
			}
			if err := counter.Close(); err != nil {
				t.Fatal(err)
			}

			addr, crashed := launchCrashServer(t, path, point)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			second := testRequest("ack-client", 2, 5, 2)
			err = client.Call("Counter.Add", second, &reply)
			client.Close()
			if err == nil {
				t.Fatal("crashed server returned success")
			}
			select {
			case <-crashed.done:
				var exit *exec.ExitError
				if !errors.As(crashed.err, &exit) || exit.ExitCode() != 79 {
					t.Fatalf("wrong crash: %v, %s", crashed.err, crashed.stderr.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("crash did not terminate server")
			}

			counter, err = OpenCounter(path)
			if err != nil {
				t.Fatal(err)
			}
			defer counter.Close()
			var value int64
			if err := counter.Value(struct{}{}, &value); err != nil {
				t.Fatal(err)
			}
			wantValue, wantBoundary := int64(10), uint64(1)
			if point == "after" {
				wantValue, wantBoundary = 15, 2
			}
			boundary, count := persistentClientState(t, counter, "ack-client")
			if value != wantValue || boundary != wantBoundary || count != 1 {
				t.Fatalf("value=%d boundary=%d completions=%d; want %d, %d, 1",
					value, boundary, count, wantValue, wantBoundary)
			}
			if err := counter.Add(second, &reply); err != nil || reply.Value != 15 {
				t.Fatalf("retry second: %+v, %v", reply, err)
			}
			if err := counter.Add(first, &reply); err == nil || err.Error() != staleRequestError {
				t.Fatalf("acknowledged first request was not stale: %v", err)
			}
		})
	}
}

// clientSequences returns one client's stored sequences in key order, checking
// that the client's records are contiguous in the flat completion bucket.
func clientSequences(tx *bolt.Tx, clientID string) []uint64 {
	prefix := completionPrefix(clientID)
	var sequences []uint64
	cursor := tx.Bucket(resultsBucket).Cursor()
	for key, _ := cursor.Seek(prefix); key != nil; key, _ = cursor.Next() {
		sequence, ok := completionSequence(key, prefix)
		if !ok {
			break
		}
		sequences = append(sequences, sequence)
	}
	return sequences
}
