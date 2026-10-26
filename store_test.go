package riflrpc

import (
	"bufio"
	"bytes"
	"context"
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
)

func TestPersistentReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "counter.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	req := AddRequest{RequestID{"client", 1}, math.MaxInt64}
	overflow := AddRequest{RequestID{"client", 2}, 1}
	var reply AddReply
	if err := c.Add(req, &reply); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(overflow, &reply); err == nil {
		t.Fatal("overflow accepted")
	}
	if err := c.Add(AddRequest{RequestID{"client", 3}, -1}, &reply); err != nil {
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
		go func() {
			defer wg.Done()
			if reply, err := client.Add(ctx, req); err != nil || reply.Value != 10 {
				t.Errorf("duplicate: %+v, %v", reply, err)
			}
			if _, err := client.Add(ctx, client.NewRequest(1)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var value int64
	if err := c.Value(struct{}{}, &value); err != nil || value != 34 {
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
			req := AddRequest{RequestID{"surviving-client", 42}, 10}
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
