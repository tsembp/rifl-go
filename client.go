package riflrpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync/atomic"
	"time"
)

// Client is safe for concurrent use. Each attempt uses a separate TCP connection,
// so timing out one call cannot close another call's connection.
type Client struct {
	address        string
	id             string
	sequence       atomic.Uint64
	attemptTimeout time.Duration
	retryDelay     time.Duration
}

func NewClient(address string, attemptTimeout, retryDelay time.Duration) (*Client, error) {
	if address == "" || attemptTimeout <= 0 || retryDelay < 0 {
		return nil, errors.New("address, positive attempt timeout and nonnegative retry delay required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("generate client ID: %w", err)
	}
	return &Client{address: address, id: hex.EncodeToString(id[:]),
		attemptTimeout: attemptTimeout, retryDelay: retryDelay}, nil
}

// NewRequest allocates an ID once. Keep this request if a call's outcome is unknown.
func (c *Client) NewRequest(delta int64) AddRequest {
	seq := c.sequence.Add(1)
	if seq == 0 {
		panic("request sequence exhausted")
	}
	return AddRequest{ID: RequestID{ClientID: c.id, Sequence: seq}, Delta: delta}
}

// Add retries the same request until a definitive reply or context expiration.
// Cancellation does not undo server execution. After an ambiguous failure, retry
// this exact request; creating a fresh ID represents another logical operation.
func (c *Client) Add(ctx context.Context, req AddRequest) (AddReply, error) {
	for {
		if err := ctx.Err(); err != nil {
			return AddReply{}, err
		}
		reply, err := c.attempt(ctx, req)
		if err == nil {
			return reply, nil
		}
		var serverErr rpc.ServerError
		if errors.As(err, &serverErr) {
			return AddReply{}, err
		}
		timer := time.NewTimer(c.retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return AddReply{}, fmt.Errorf("RPC outcome unknown (last attempt: %v): %w", err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (c *Client) attempt(ctx context.Context, req AddRequest) (AddReply, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(attemptCtx, "tcp", c.address)
	if err != nil {
		return AddReply{}, err
	}
	deadline, _ := attemptCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		conn.Close()
		return AddReply{}, err
	}
	client := rpc.NewClient(conn)
	defer client.Close()
	var reply AddReply
	call := client.Go("Counter.Add", req, &reply, make(chan *rpc.Call, 1))
	select {
	case <-attemptCtx.Done():
		client.Close()
		<-call.Done // Wait before accessing reply or returning: decoder may be writing it.
		return AddReply{}, attemptCtx.Err()
	case done := <-call.Done:
		return reply, done.Error
	}
}
