package riflrpc

import (
	"context"
	"errors"
	"math"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dropReplyConn fails the first server write. net/rpc writes the response only
// after Add has executed, so this loses the reply after the mutation completes.
type dropReplyConn struct{ net.Conn }

func (c dropReplyConn) Write(_ []byte) (int, error) {
	c.Conn.Close()
	return 0, errors.New("injected lost response")
}

func startServer(t *testing.T, counter any, dropFirst bool) (string, *atomic.Int64) {
	t.Helper()
	s := rpc.NewServer()
	if err := s.RegisterName("Counter", counter); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections atomic.Int64
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			n := connections.Add(1)
			if dropFirst && n == 1 {
				conn = dropReplyConn{conn}
			}
			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()
				s.ServeConn(conn)
			}(conn)
		}
	}()
	t.Cleanup(func() { l.Close(); <-done; wg.Wait() })
	return l.Addr().String(), &connections
}

func newTestClient(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := NewClient(addr, 100*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestLostResponseRetriesWithoutDuplicateEffect(t *testing.T) {
	counter := &Counter{}
	addr, connections := startServer(t, counter, true)
	c := newTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := c.NewRequest(10)
	reply, err := c.Add(ctx, req)
	if err != nil || reply.Value != 10 {
		t.Fatalf("reply=%+v err=%v", reply, err)
	}
	if connections.Load() < 2 {
		t.Fatal("lost response did not trigger a reconnect/retry")
	}
	if _, err := c.Add(ctx, c.NewRequest(5)); err != nil {
		t.Fatal(err)
	}
	reply, err = c.Add(ctx, req)
	if err != nil || reply.Value != 10 {
		t.Fatalf("duplicate must return original result, got %+v, %v", reply, err)
	}
	var value int64
	counter.Value(struct{}{}, &value)
	if value != 15 {
		t.Fatalf("expected two logical increments (15), got %d", value)
	}
}

func TestConcurrentDuplicatesAndUniqueCalls(t *testing.T) {
	counter := &Counter{}
	addr, _ := startServer(t, counter, false)
	c := newTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	duplicate := c.NewRequest(1)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, req := range []AddRequest{duplicate, c.NewRequest(1)} {
				if _, err := c.Add(ctx, req); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	var value int64
	counter.Value(struct{}{}, &value)
	if value != 33 {
		t.Fatalf("expected 33 logical effects, got %d", value)
	}
}

func TestInvalidAndConflictingRequestsAreDefinitive(t *testing.T) {
	counter := &Counter{}
	addr, connections := startServer(t, counter, false)
	c := newTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := c.NewRequest(7)
	if _, err := c.Add(ctx, req); err != nil {
		t.Fatal(err)
	}
	conflict := req
	conflict.Delta = 8
	for _, bad := range []AddRequest{conflict, {}, {ID: RequestID{ClientID: "x"}}} {
		before := connections.Load()
		_, err := c.Add(ctx, bad)
		var serverErr rpc.ServerError
		if !errors.As(err, &serverErr) {
			t.Fatalf("expected application error, got %v", err)
		}
		if connections.Load() != before+1 {
			t.Fatal("application error was retried")
		}
	}
	var value int64
	counter.Value(struct{}{}, &value)
	if value != 7 {
		t.Fatalf("invalid requests changed state to %d", value)
	}
}

func TestOverflowResultIsRemembered(t *testing.T) {
	c := &Counter{}
	call := func(seq uint64, delta int64) error {
		return c.Add(AddRequest{ID: RequestID{"client", seq}, Delta: delta}, &AddReply{})
	}
	if err := call(1, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if err := call(2, 1); err == nil {
		t.Fatal("overflow accepted")
	}
	if err := call(3, -1); err != nil {
		t.Fatal(err)
	}
	if err := call(2, 1); err == nil {
		t.Fatal("retry changed original failure into success")
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			// Read and discard request bytes, but never reply.
			go func() {
				defer conn.Close()
				buf := make([]byte, 4096)
				for {
					if _, err := conn.Read(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
	defer func() { l.Close(); <-done }()
	c, err := NewClient(l.Addr().String(), 20*time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := c.Add(ctx, c.NewRequest(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if accepted.Load() < 2 {
		t.Fatal("timeout did not cause retry")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := c.Add(cancelled, c.NewRequest(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
