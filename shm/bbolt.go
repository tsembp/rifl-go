package shm

import (
	"strings"

	riflrpc "github.com/tsembp/rifl-go"
)

// CounterHandler serves the shared-memory slots with an existing counter,
// normally the bbolt-backed riflrpc.PersistentCounter (configuration B: shared
// memory transport, durable disk state).
type CounterHandler struct {
	Counter interface {
		Add(riflrpc.AddRequest, *riflrpc.AddReply) error
		Stats(struct{}, *riflrpc.ServiceStats) error
	}
}

func (h CounterHandler) Stats() riflrpc.ServiceStats {
	var stats riflrpc.ServiceStats
	h.Counter.Stats(struct{}{}, &stats)
	return stats
}

func (h CounterHandler) Handle(req riflrpc.AddRequest) (riflrpc.AddReply, Code) {
	var reply riflrpc.AddReply
	err := h.Counter.Add(req, &reply)
	if err == nil {
		return reply, CodeOK
	}
	switch message := err.Error(); {
	case strings.HasPrefix(message, "storage outcome unknown: "):
		return riflrpc.AddReply{}, CodeRetry // ambiguous storage failure
	case message == CodeStale.Error():
		return riflrpc.AddReply{}, CodeStale
	case message == CodeOverflow.Error():
		return riflrpc.AddReply{}, CodeOverflow
	case message == CodeConflict.Error():
		return riflrpc.AddReply{}, CodeConflict
	case strings.HasPrefix(message, "client ID, nonzero sequence"):
		return riflrpc.AddReply{}, CodeInvalid
	default:
		return riflrpc.AddReply{}, CodeServerError
	}
}
