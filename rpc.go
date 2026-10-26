// Package riflrpc provides a small RIFL-inspired RPC experiment, with in-memory
// and durable counter implementations.
package riflrpc

import (
	"errors"
	"math"
	"sync"
)

// RequestID identifies a logical operation, independently of RPC connections.
type RequestID struct {
	ClientID string
	Sequence uint64
}

type AddRequest struct {
	ID    RequestID
	Delta int64
}

type AddReply struct {
	Value int64
}

type completion struct {
	delta int64
	reply AddReply
	err   string
}

// Counter serializes mutations and completion tracking in one critical section.
// Create it once per server lifetime; creating it per connection loses deduplication.
// The zero value is ready to use. Records are retained until the server exits.
type Counter struct {
	mu        sync.Mutex
	value     int64
	completed map[RequestID]completion
}

// Add executes a logical increment once, or returns its original result.
// Reusing an ID with different arguments is rejected.
func (c *Counter) Add(req AddRequest, reply *AddReply) error {
	if req.ID.ClientID == "" || req.ID.Sequence == 0 {
		return errors.New("client ID and nonzero sequence are required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if result, ok := c.completed[req.ID]; ok {
		if result.delta != req.Delta {
			return errors.New("request ID reused with different arguments")
		}
		if result.err != "" {
			return errors.New(result.err)
		}
		*reply = result.reply
		return nil
	}
	if c.completed == nil {
		c.completed = make(map[RequestID]completion)
	}
	result := completion{delta: req.Delta}
	result.reply.Value, result.err = addValue(c.value, req.Delta)
	if result.err == "" {
		c.value = result.reply.Value
	}
	c.completed[req.ID] = result
	if result.err != "" {
		return errors.New(result.err)
	}
	*reply = result.reply
	return nil
}

func addValue(value, delta int64) (int64, string) {
	if (delta > 0 && value > math.MaxInt64-delta) ||
		(delta < 0 && value < math.MinInt64-delta) {
		return 0, "counter overflow"
	}
	return value + delta, ""
}

// Value returns the current counter for inspection; it does not change state.
func (c *Counter) Value(_ struct{}, reply *int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*reply = c.value
	return nil
}
