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
	// FirstIncomplete acknowledges every lower sequence for this client.
	FirstIncomplete uint64
	// Padding is transport-only benchmark data and does not affect Add semantics.
	Padding []byte
}

type AddReply struct {
	Value int64
}

// ServiceStats exposes read-only measurements used by the benchmark runner.
// CompletionRecords and ClientBoundaries are zero for an unprotected service.
type ServiceStats struct {
	Protected         bool
	Value             int64
	CompletionRecords int
	ClientBoundaries  int
	DatabaseBytes     int64
}

type completion struct {
	delta int64
	reply AddReply
	err   string
}

// Counter serializes mutations and completion tracking in one critical section.
// Create it once per server lifetime; creating it per connection loses deduplication.
// The zero value is ready to use. Records are retained until acknowledged.
type Counter struct {
	mu              sync.Mutex
	value           int64
	completed       map[RequestID]completion
	firstIncomplete map[string]uint64
}

// Add processes the client's acknowledgement, then executes a logical increment
// once or returns its original result. Reclaimed requests are rejected as stale.
func (c *Counter) Add(req AddRequest, reply *AddReply) error {
	if err := validateAddRequest(req); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processAck(req.ID.ClientID, req.FirstIncomplete)
	if req.ID.Sequence < c.clientFirstIncomplete(req.ID.ClientID) {
		return errors.New(staleRequestError)
	}
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

const staleRequestError = "stale request"

func validateAddRequest(req AddRequest) error {
	if req.ID.ClientID == "" || req.ID.Sequence == 0 || req.FirstIncomplete == 0 {
		return errors.New("client ID, nonzero sequence and nonzero first incomplete are required")
	}
	return nil
}

func (c *Counter) clientFirstIncomplete(clientID string) uint64 {
	if first := c.firstIncomplete[clientID]; first != 0 {
		return first
	}
	return 1
}

func (c *Counter) processAck(clientID string, firstIncomplete uint64) {
	if firstIncomplete <= c.clientFirstIncomplete(clientID) {
		return
	}
	if c.firstIncomplete == nil {
		c.firstIncomplete = make(map[string]uint64)
	}
	c.firstIncomplete[clientID] = firstIncomplete
	for id := range c.completed {
		if id.ClientID == clientID && id.Sequence < firstIncomplete {
			delete(c.completed, id)
		}
	}
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
