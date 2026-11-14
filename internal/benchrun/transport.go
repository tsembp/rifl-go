package benchrun

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"time"

	riflrpc "github.com/tsembp/rifl-go"
	"github.com/tsembp/rifl-go/shm"
)

const (
	TransportTCP = "tcp"
	TransportSHM = "shm"
)

// caller sends one worker's requests. Errors follow the net/rpc convention used
// by perform: an rpc.ServerError is definitive unless it carries the storage
// prefix; anything else is an ambiguous transport failure.
type caller interface {
	add(context.Context, riflrpc.AddRequest) (riflrpc.AddReply, error)
	close()
}

type transport interface {
	newCaller(worker int) (caller, error)
	stats(context.Context) (riflrpc.ServiceStats, error)
	add(context.Context, riflrpc.AddRequest) (riflrpc.AddReply, error) // one-off, for checks
	close()
}

func openTransport(cfg Config) (transport, error) {
	if cfg.Transport != TransportSHM {
		return tcpTransport{address: cfg.Address, timeout: cfg.Timeout, mode: cfg.ConnectionMode}, nil
	}
	region, err := shm.Open(cfg.Region)
	if err != nil {
		return nil, err
	}
	// One control slot after the workers' slots carries stats and checks.
	if need := cfg.SlotBase + cfg.Concurrency + 1; need > region.Config().Slots {
		region.Close()
		return nil, fmt.Errorf("region has %d slots; need %d (slot base + workers + 1 control)", region.Config().Slots, need)
	}
	control, err := shm.NewClient(region, cfg.SlotBase+cfg.Concurrency)
	if err != nil {
		region.Close()
		return nil, err
	}
	return &shmTransport{region: region, slotBase: cfg.SlotBase, timeout: cfg.Timeout,
		control: &shmCaller{client: control, timeout: cfg.Timeout}}, nil
}

type tcpTransport struct {
	address string
	timeout time.Duration
	mode    string
}

func (t tcpTransport) newCaller(int) (caller, error) {
	return &rpcCaller{address: t.address, timeout: t.timeout, mode: t.mode}, nil
}
func (t tcpTransport) stats(ctx context.Context) (riflrpc.ServiceStats, error) {
	return stats(ctx, t.address, t.timeout)
}
func (t tcpTransport) add(ctx context.Context, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	return add(ctx, t.address, t.timeout, req)
}
func (tcpTransport) close() {}

type shmTransport struct {
	region   *shm.Region
	slotBase int
	timeout  time.Duration
	control  *shmCaller
}

func (t *shmTransport) newCaller(worker int) (caller, error) {
	client, err := shm.NewClient(t.region, t.slotBase+worker)
	if err != nil {
		return nil, err
	}
	return &shmCaller{client: client, timeout: t.timeout}, nil
}
func (t *shmTransport) stats(parent context.Context) (riflrpc.ServiceStats, error) {
	ctx, cancel := context.WithTimeout(parent, t.timeout)
	defer cancel()
	return t.control.client.Stats(ctx)
}
func (t *shmTransport) add(ctx context.Context, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	return t.control.add(ctx, req)
}
func (t *shmTransport) close() { t.region.Close() }

type shmCaller struct {
	client  *shm.Client
	timeout time.Duration
}

func (c *shmCaller) add(parent context.Context, req riflrpc.AddRequest) (riflrpc.AddReply, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	reply, err := c.client.Call(ctx, req)
	var code shm.Code
	if errors.As(err, &code) {
		if code == shm.CodeRetry {
			// Not executed yet but may run later: ambiguous, retry the same ID.
			return reply, rpc.ServerError("storage outcome unknown: " + code.Error())
		}
		return reply, rpc.ServerError(code.Error())
	}
	return reply, err // nil or shm.ErrTimeout (ambiguous)
}

// The slot stays bound to this worker; a pending timed-out request is collected
// by the next call with the same ID.
func (c *shmCaller) close() {}
