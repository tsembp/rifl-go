package shm

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
)

// Acquire blocks until this process holds the region's exclusive lock, then
// starts a new owner epoch. The kernel releases the lock when the owner process
// dies, which gives an exact failure detector on one machine; real multi-host
// CXL would need leases with fencing instead.
func (r *Region) Acquire(ctx context.Context) (uint64, error) {
	done := make(chan error, 1)
	go func() { done <- syscall.Flock(int(r.file.Fd()), syscall.LOCK_EX) }()
	select {
	case err := <-done:
		if err != nil {
			return 0, err
		}
	case <-ctx.Done():
		// The blocked flock cannot be cancelled; the caller should exit.
		return 0, ctx.Err()
	}
	epoch := atomic.AddUint64(r.word(hEpoch), 1)
	atomic.StoreUint64(r.word(hPid), uint64(os.Getpid()))
	return epoch, nil
}

// TryAcquire is Acquire without blocking; it reports false if another process
// owns the region.
func (r *Region) TryAcquire() (uint64, bool, error) {
	if err := syscall.Flock(int(r.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if err == syscall.EWOULDBLOCK {
			return 0, false, nil
		}
		return 0, false, err
	}
	epoch := atomic.AddUint64(r.word(hEpoch), 1)
	atomic.StoreUint64(r.word(hPid), uint64(os.Getpid()))
	return epoch, true, nil
}

// Serve processes request slots until ctx ends. The caller must own the region
// and, for the shared-memory state, must have called State.Recover first.
// One goroutine handles every slot in turn, so the handler is single-writer.
func Serve(ctx context.Context, r *Region, h Handler) {
	idle := 0
	for ctx.Err() == nil {
		worked := false
		for i := 0; i < r.cfg.Slots; i++ {
			slot := r.slot(i)
			if atomic.LoadUint64(r.word(slot+sState)) != slotRequest {
				continue
			}
			worked = true
			if r.get(slot+sOp) == opStats {
				stats := h.Stats()
				r.put(slot+sValue, uint64(stats.Value))
				r.put(slot+sSequence, uint64(stats.CompletionRecords))
				r.put(slot+sDelta, uint64(stats.ClientBoundaries))
				r.put(slot+sFirst, uint64(stats.DatabaseBytes))
				r.put(slot+sCode, b2u(!stats.Protected)) // 0 protected, 1 baseline
				atomic.StoreUint64(r.word(slot+sState), slotReply)
				continue
			}
			req := riflrpc.AddRequest{
				ID:              riflrpc.RequestID{ClientID: r.getID(slot+sIDLen, slot+sID), Sequence: r.get(slot + sSequence)},
				Delta:           int64(r.get(slot + sDelta)),
				FirstIncomplete: r.get(slot + sFirst),
			}
			reply, code := h.Handle(req)
			r.put(slot+sValue, uint64(reply.Value))
			r.put(slot+sCode, uint64(code))
			atomic.StoreUint64(r.word(slot+sState), slotReply)
		}
		if worked {
			idle = 0
			continue
		}
		// Poll with backoff: spin briefly, then yield, then sleep.
		idle++
		switch {
		case idle < 64:
		case idle < 1024:
			runtime.Gosched()
		default:
			time.Sleep(20 * time.Microsecond)
		}
	}
}

// Client issues requests through one slot. A slot must be used by exactly one
// client goroutine at a time.
type Client struct {
	r       *Region
	slot    int
	pending *riflrpc.RequestID // request left in the slot by a timed-out call
}

// NewClient binds a client to a slot index.
func NewClient(r *Region, slot int) (*Client, error) {
	if slot < 0 || slot >= r.cfg.Slots {
		return nil, CodeInvalid
	}
	return &Client{r: r, slot: slot}, nil
}

// ErrTimeout is ambiguous: the request stays in the slot and may still run.
// Retrying the same request ID waits for and returns that same execution.
var ErrTimeout = timeoutError{}

type timeoutError struct{}

func (timeoutError) Error() string { return "shared-memory call timed out: outcome unknown" }

// Call submits req and waits for its reply or ctx's end. It returns the reply,
// or a Code error (definitive except CodeRetry), or ErrTimeout.
func (c *Client) Call(ctx context.Context, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	if len(req.ID.ClientID) > MaxClientIDBytes {
		return riflrpc.AddReply{}, CodeInvalid
	}
	r, slot := c.r, c.r.slot(c.slot)
	if c.pending != nil {
		// A previous call timed out. Its request is still in the slot.
		if err := c.wait(ctx, slot); err != nil {
			return riflrpc.AddReply{}, err
		}
		same := *c.pending == req.ID
		c.pending = nil
		if same {
			return c.take(slot)
		}
		atomic.StoreUint64(r.word(slot+sState), slotEmpty)
	}
	r.put(slot+sOp, opAdd)
	r.putID(slot+sIDLen, slot+sID, req.ID.ClientID)
	r.put(slot+sSequence, req.ID.Sequence)
	r.put(slot+sDelta, uint64(req.Delta))
	r.put(slot+sFirst, req.FirstIncomplete)
	atomic.StoreUint64(r.word(slot+sState), slotRequest)
	if err := c.wait(ctx, slot); err != nil {
		id := req.ID
		c.pending = &id
		return riflrpc.AddReply{}, err
	}
	return c.take(slot)
}

// Stats asks the owner for read-only service statistics. Use a slot with no
// request outstanding.
func (c *Client) Stats(ctx context.Context) (riflrpc.ServiceStats, error) {
	r, slot := c.r, c.r.slot(c.slot)
	if c.pending != nil || atomic.LoadUint64(r.word(slot+sState)) != slotEmpty {
		return riflrpc.ServiceStats{}, CodeInvalid
	}
	r.put(slot+sOp, opStats)
	atomic.StoreUint64(r.word(slot+sState), slotRequest)
	if err := c.wait(ctx, slot); err != nil {
		// Stats has no effect; drop it once the owner answers.
		c.pending = &riflrpc.RequestID{}
		return riflrpc.ServiceStats{}, err
	}
	stats := riflrpc.ServiceStats{
		Protected:         r.get(slot+sCode) == 0,
		Value:             int64(r.get(slot + sValue)),
		CompletionRecords: int(r.get(slot + sSequence)),
		ClientBoundaries:  int(r.get(slot + sDelta)),
		DatabaseBytes:     int64(r.get(slot + sFirst)),
	}
	atomic.StoreUint64(r.word(slot+sState), slotEmpty)
	return stats, nil
}

func (c *Client) wait(ctx context.Context, slot int) error {
	for spins := 0; atomic.LoadUint64(c.r.word(slot+sState)) != slotReply; spins++ {
		if ctx.Err() != nil {
			return ErrTimeout
		}
		switch {
		case spins < 64:
		case spins < 1024:
			runtime.Gosched()
		default:
			time.Sleep(5 * time.Microsecond)
		}
	}
	return nil
}

func (c *Client) take(slot int) (riflrpc.AddReply, error) {
	value, code := int64(c.r.get(slot+sValue)), Code(c.r.get(slot+sCode))
	atomic.StoreUint64(c.r.word(slot+sState), slotEmpty)
	if code != CodeOK {
		return riflrpc.AddReply{}, code
	}
	return riflrpc.AddReply{Value: value}, nil
}
