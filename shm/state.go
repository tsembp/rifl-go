package shm

import (
	"hash/fnv"
	"math"
	"sync/atomic"

	riflrpc "github.com/tsembp/rifl-go"
)

// Code is a reply outcome stored in a slot.
type Code uint64

const (
	CodeOK Code = iota
	CodeStale
	CodeOverflow
	CodeConflict
	// CodeRetry is ambiguous: the request was not executed now, but may be
	// later. The client must keep the ID unfinished and retry the same ID.
	CodeRetry
	CodeInvalid
	CodeClientTableFull
	CodeServerError
)

var codeMessages = map[Code]string{
	CodeStale:           "stale request",
	CodeOverflow:        "counter overflow",
	CodeConflict:        "request ID reused with different arguments",
	CodeRetry:           "retry later: outcome unknown",
	CodeInvalid:         "invalid request",
	CodeClientTableFull: "client table full",
	CodeServerError:     "server error",
}

func (c Code) Error() string { return codeMessages[c] }

// Handler executes one request for the server loop.
type Handler interface {
	Handle(riflrpc.AddRequest) (riflrpc.AddReply, Code)
}

// State is the RIFL service whose entire state lives in the shared region.
// Only the current owner (the process holding the region lock) may call
// Handle or Recover. Mutation, completion record, acknowledgement advance and
// reclamation become visible together at one commit point in the redo log.
type State struct {
	r     *Region
	epoch uint64
	// Test-only crash points; never exposed as RPC controls.
	beforeCommit func()
	afterCommit  func() // committed, nothing applied yet
	midApply     func() // boundary/reclamation applied, record and counter not yet
	afterApply   func() // applied, reply not yet written
}

// NewState wraps a region for the owner with the given epoch.
func NewState(r *Region, epoch uint64) *State { return &State{r: r, epoch: epoch} }

// Recover finishes a committed but unapplied operation. Values in the log are
// absolute, so reapplying an operation that was partly applied is idempotent.
func (s *State) Recover() {
	if atomic.LoadUint64(s.r.word(logOff+lState)) == logCommitted {
		s.apply()
	}
}

// Counter returns the shared counter value.
func (s *State) Counter() int64 { return int64(s.r.get(hCounter)) }

func (s *State) Handle(req riflrpc.AddRequest) (riflrpc.AddReply, Code) {
	id := req.ID.ClientID
	if id == "" || len(id) > MaxClientIDBytes || req.ID.Sequence == 0 || req.FirstIncomplete == 0 {
		return riflrpc.AddReply{}, CodeInvalid
	}
	index, found := s.findClient(id)
	if index < 0 {
		// Refused before execution and permanent (entries are never freed), so
		// a later duplicate is refused too: definitive is safe.
		return riflrpc.AddReply{}, CodeClientTableFull
	}
	boundary := uint64(1)
	if found {
		boundary = s.r.get(s.r.client(index) + cBoundary)
	}
	newBoundary := boundary
	if req.FirstIncomplete > boundary {
		newBoundary = req.FirstIncomplete
	}
	ack := log{client: index, insert: !found, id: id, boundary: newBoundary, clearFrom: boundary,
		counter: s.Counter()}
	advanced := newBoundary > boundary || !found

	if req.ID.Sequence < newBoundary {
		s.commitIf(advanced, ack)
		return riflrpc.AddReply{}, CodeStale
	}
	records := uint64(s.r.cfg.Records)
	if found {
		rec := s.r.record(index, req.ID.Sequence)
		if s.r.get(rec+rSeq) == req.ID.Sequence {
			s.commitIf(advanced, ack)
			if int64(s.r.get(rec+rDelta)) != req.Delta {
				return riflrpc.AddReply{}, CodeConflict
			}
			return riflrpc.AddReply{Value: int64(s.r.get(rec + rValue))}, Code(s.r.get(rec + rCode))
		}
	}
	if req.ID.Sequence-newBoundary >= records {
		// Executing would overwrite an unacknowledged record. Never evict it:
		// refuse before execution with an ambiguous code so the client retries.
		s.commitIf(advanced, ack)
		return riflrpc.AddReply{}, CodeRetry
	}
	value, code := s.Counter(), CodeOK
	if (req.Delta > 0 && value > math.MaxInt64-req.Delta) || (req.Delta < 0 && value < math.MinInt64-req.Delta) {
		code = CodeOverflow
	} else {
		value += req.Delta
	}
	op := ack
	op.hasRecord = true
	op.seq, op.delta, op.value, op.code = req.ID.Sequence, req.Delta, value, code
	if code == CodeOK {
		op.counter = value
	}
	s.commit(op)
	if code != CodeOK {
		return riflrpc.AddReply{}, code
	}
	return riflrpc.AddReply{Value: value}, CodeOK
}

type log struct {
	client              int
	insert              bool
	id                  string
	boundary, clearFrom uint64
	counter             int64
	hasRecord           bool
	seq                 uint64
	delta, value        int64
	code                Code
}

func (s *State) commitIf(advanced bool, op log) {
	if advanced {
		s.commit(op)
	}
}

// commit writes the log entry, publishes it with one atomic store (the commit
// point), then applies it.
func (s *State) commit(op log) {
	r, l := s.r, logOff
	r.put(l+lEpoch, s.epoch)
	r.put(l+lClient, uint64(op.client))
	r.put(l+lInsert, b2u(op.insert))
	r.putID(l+lIDLen, l+lID, op.id)
	r.put(l+lBoundary, op.boundary)
	r.put(l+lClearFrom, op.clearFrom)
	r.put(l+lClearTo, op.boundary)
	r.put(l+lCounter, uint64(op.counter))
	r.put(l+lHasRecord, b2u(op.hasRecord))
	r.put(l+lRecSeq, op.seq)
	r.put(l+lRecDelta, uint64(op.delta))
	r.put(l+lRecValue, uint64(op.value))
	r.put(l+lRecCode, uint64(op.code))
	if s.beforeCommit != nil {
		s.beforeCommit()
	}
	atomic.StoreUint64(r.word(l+lState), logCommitted)
	if s.afterCommit != nil {
		s.afterCommit()
	}
	s.apply()
}

func (s *State) apply() {
	r, l := s.r, logOff
	index := int(r.get(l + lClient))
	entry := r.client(index)
	if r.get(l+lInsert) == 1 {
		r.putID(entry+cIDLen, entry+cID, r.getID(l+lIDLen, l+lID))
		r.put(entry+cUsed, 1)
	}
	boundary := r.get(l + lBoundary)
	r.put(entry+cBoundary, boundary)
	// Live records lie in [clearFrom, clearFrom+Records); clear at most one window.
	from, to := r.get(l+lClearFrom), r.get(l+lClearTo)
	if to-from > uint64(r.cfg.Records) {
		to = from + uint64(r.cfg.Records)
	}
	for seq := from; seq < to; seq++ {
		if rec := r.record(index, seq); r.get(rec+rSeq) == seq {
			r.put(rec+rSeq, 0)
		}
	}
	if s.midApply != nil {
		s.midApply()
	}
	if r.get(l+lHasRecord) == 1 {
		seq := r.get(l + lRecSeq)
		rec := r.record(index, seq)
		r.put(rec+rDelta, r.get(l+lRecDelta))
		r.put(rec+rValue, r.get(l+lRecValue))
		r.put(rec+rCode, r.get(l+lRecCode))
		r.put(rec+rSeq, seq)
	}
	r.put(hCounter, r.get(l+lCounter))
	atomic.StoreUint64(r.word(l+lState), logIdle)
	if s.afterApply != nil {
		s.afterApply()
	}
}

// findClient returns the client's table index and whether it exists, or the
// free index where it would be inserted; -1 means the table is full.
func (s *State) findClient(id string) (int, bool) {
	h := fnv.New64a()
	h.Write([]byte(id))
	n := s.r.cfg.Clients
	start := int(h.Sum64() % uint64(n))
	for probe := 0; probe < n; probe++ {
		i := (start + probe) % n
		entry := s.r.client(i)
		if s.r.get(entry+cUsed) == 0 {
			return i, false
		}
		if s.r.getID(entry+cIDLen, entry+cID) == id {
			return i, true
		}
	}
	return -1, false
}

// Stats counts live state; it is read-only and for experiments.
func (s *State) Stats() riflrpc.ServiceStats {
	stats := riflrpc.ServiceStats{Protected: true, Value: s.Counter()}
	for i := 0; i < s.r.cfg.Clients; i++ {
		entry := s.r.client(i)
		if s.r.get(entry+cUsed) == 0 {
			continue
		}
		stats.ClientBoundaries++
		for j := 0; j < s.r.cfg.Records; j++ {
			if s.r.get(s.r.records+(i*s.r.cfg.Records+j)*recordSize+rSeq) != 0 {
				stats.CompletionRecords++
			}
		}
	}
	stats.DatabaseBytes = int64(len(s.r.mem))
	return stats
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
