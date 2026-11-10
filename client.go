package riflrpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// Client is safe for concurrent use. Each attempt uses a separate TCP connection,
// so timing out one call cannot close another call's connection.
type Client struct {
	address        string
	id             string
	attemptTimeout time.Duration
	retryDelay     time.Duration

	mu              sync.Mutex
	sequence        uint64
	firstIncomplete uint64
	unfinished      map[uint64]struct{}
}

func NewClient(address string, attemptTimeout, retryDelay time.Duration) (*Client, error) {
	if address == "" || attemptTimeout <= 0 || retryDelay < 0 {
		return nil, errors.New("address, positive attempt timeout and nonnegative retry delay required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("generate client ID: %w", err)
	}
	return &Client{
		address:         address,
		id:              hex.EncodeToString(id[:]),
		attemptTimeout:  attemptTimeout,
		retryDelay:      retryDelay,
		firstIncomplete: 1,
		unfinished:      make(map[uint64]struct{}),
	}, nil
}

// NewRequest allocates an ID and marks its sequence unfinished. Keep this request
// if a call's outcome is unknown; abandoning it prevents later acknowledgements
// from advancing past its sequence.
func (c *Client) NewRequest(delta int64) AddRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sequence == ^uint64(0) {
		panic("request sequence exhausted")
	}
	c.sequence++
	c.unfinished[c.sequence] = struct{}{}
	return AddRequest{
		ID:              RequestID{ClientID: c.id, Sequence: c.sequence},
		Delta:           delta,
		FirstIncomplete: c.firstIncomplete,
	}
}

// Add retries the same request until a definitive reply or context expiration.
// A definitive reply marks the sequence complete and allows a later attempt to
// acknowledge it. Cancellation does not undo server execution. After an ambiguous
// failure, retry this exact request; creating a fresh ID is another operation.
func (c *Client) Add(ctx context.Context, req AddRequest) (AddReply, error) {
	for {
		if err := ctx.Err(); err != nil {
			return AddReply{}, err
		}
		attemptReq := c.prepareRequest(req)
		reply, err := c.attempt(ctx, attemptReq)
		if err == nil {
			c.requestCompleted(req.ID)
			return reply, nil
		}
		var serverErr rpc.ServerError
		if errors.As(err, &serverErr) && !strings.HasPrefix(string(serverErr), storageErrorPrefix) {
			c.requestCompleted(req.ID)
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

func (c *Client) prepareRequest(req AddRequest) AddRequest {
	if req.ID.ClientID != c.id {
		return req
	}
	c.mu.Lock()
	req.FirstIncomplete = c.firstIncomplete
	c.mu.Unlock()
	return req
}

func (c *Client) requestCompleted(id RequestID) {
	if id.ClientID != c.id {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.unfinished[id.Sequence]; !ok {
		return
	}
	delete(c.unfinished, id.Sequence)
	for c.firstIncomplete <= c.sequence {
		if _, unfinished := c.unfinished[c.firstIncomplete]; unfinished {
			break
		}
		c.firstIncomplete++
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
