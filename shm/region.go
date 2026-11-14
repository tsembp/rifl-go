// Package shm models CXL-style shared memory with a Linux MAP_SHARED file
// (normally under /dev/shm). Separate OS processes stand in for compute nodes.
// The region holds request/reply slots and, for the shared-memory service, the
// RIFL state itself (counter, client boundaries, completion records, redo log),
// so a replacement server process can take over without a disk.
//
// Memory model assumption: on one machine, sync/atomic operations on the shared
// mapping are coherent between processes, and plain field writes made before an
// atomic store are visible after an atomic load that observes it. This holds for
// one host; real multi-host CXL coherence is not modelled.
package shm

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	magic         = 0x5249464c53484d31 // "RIFLSHM1"
	layoutVersion = 1
	// MaxClientIDBytes bounds the fixed-size client ID fields.
	MaxClientIDBytes = 64
)

// Config fixes the region's capacities when it is created.
type Config struct {
	Slots   int // request/reply slots, one per concurrent client worker
	Clients int // logical clients with durable boundaries (never evicted)
	Records int // completion records per client: unacknowledged window size
}

func (c Config) validate() error {
	if c.Slots <= 0 || c.Clients <= 0 || c.Records <= 0 {
		return errors.New("slots, clients and records must be positive")
	}
	if c.Slots > 1<<20 || c.Clients > 1<<24 || c.Records > 1<<24 {
		return errors.New("capacity too large")
	}
	return nil
}

// Layout offsets. Every word is 8-byte aligned; mmap returns page-aligned memory.
const (
	// Header.
	hMagic   = 0
	hVersion = 8
	hSlots   = 16
	hClients = 24
	hRecords = 32
	hEpoch   = 40 // owner epoch, incremented on every takeover
	hPid     = 48 // current owner pid (informational)
	hCounter = 56 // counter value (int64)
	hSize    = 64

	// Redo log entry (one; the owner applies one operation at a time).
	logOff       = hSize
	lState       = 0 // logIdle or logCommitted
	lEpoch       = 8
	lClient      = 16 // client table index
	lInsert      = 24 // 1 if the client entry must be created
	lIDLen       = 32
	lID          = 40 // MaxClientIDBytes
	lBoundary    = 104
	lClearFrom   = 112 // clear ring records with sequence in [from, to)
	lClearTo     = 120
	lCounter     = 128
	lHasRecord   = 136
	lRecSeq      = 144
	lRecDelta    = 152
	lRecValue    = 160
	lRecCode     = 168
	logEntrySize = 192

	// Request/reply slot.
	slotsOff  = logOff + logEntrySize
	sState    = 0 // slotEmpty, slotRequest or slotReply
	sIDLen    = 8
	sID       = 16
	sSequence = 80
	sDelta    = 88
	sFirst    = 96
	sValue    = 104
	sCode     = 112
	sOp       = 120 // opAdd or opStats
	slotSize  = 128

	// A stats reply reuses the request fields: sValue=Value,
	// sSequence=CompletionRecords, sDelta=ClientBoundaries, sFirst=DatabaseBytes.

	// Client table entry (open addressing, never deleted).
	cUsed      = 0
	cIDLen     = 8
	cID        = 16
	cBoundary  = 80 // durable FirstIncomplete
	clientSize = 96

	// Completion record; sequence 0 means empty.
	rSeq       = 0
	rDelta     = 8
	rValue     = 16
	rCode      = 24
	recordSize = 32
)

const (
	logIdle      = 0
	logCommitted = 1

	slotEmpty   = 0
	slotRequest = 1
	slotReply   = 2

	opAdd   = 0
	opStats = 1
)

// Region is one process's mapping of the shared file.
type Region struct {
	file    *os.File
	mem     []byte
	cfg     Config
	clients int // byte offset of the client table
	records int // byte offset of the completion rings
}

func sizeFor(cfg Config) (clients, records, total int) {
	clients = slotsOff + cfg.Slots*slotSize
	records = clients + cfg.Clients*clientSize
	total = records + cfg.Clients*cfg.Records*recordSize
	return
}

// Create makes a new, zeroed region file. It fails if the file exists, so a
// region holding live RIFL state is never reinitialized by accident.
func Create(path string, cfg Config) (*Region, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("create region: %w", err)
	}
	_, _, total := sizeFor(cfg)
	if err := file.Truncate(int64(total)); err != nil {
		file.Close()
		return nil, err
	}
	r, err := mapRegion(file, total, cfg)
	if err != nil {
		file.Close()
		return nil, err
	}
	r.put(hVersion, layoutVersion)
	r.put(hSlots, uint64(cfg.Slots))
	r.put(hClients, uint64(cfg.Clients))
	r.put(hRecords, uint64(cfg.Records))
	// Publish the header last; Open rejects a region without the magic.
	atomic.StoreUint64(r.word(hMagic), magic)
	return r, nil
}

// Open attaches to an existing region and validates its header.
func Open(path string) (*Region, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open region: %w", err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() < hSize {
		file.Close()
		return nil, errors.New("region file too small")
	}
	header, err := syscall.Mmap(int(file.Fd()), 0, hSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}
	word := func(off int) uint64 { return atomic.LoadUint64((*uint64)(unsafe.Pointer(&header[off]))) }
	cfg := Config{Slots: int(word(hSlots)), Clients: int(word(hClients)), Records: int(word(hRecords))}
	valid := word(hMagic) == magic && word(hVersion) == layoutVersion
	syscall.Munmap(header)
	if !valid {
		file.Close()
		return nil, errors.New("not a RIFL shared-memory region or unsupported layout")
	}
	if err := cfg.validate(); err != nil {
		file.Close()
		return nil, err
	}
	_, _, total := sizeFor(cfg)
	if info.Size() != int64(total) {
		file.Close()
		return nil, errors.New("region size does not match its header")
	}
	r, err := mapRegion(file, total, cfg)
	if err != nil {
		file.Close()
	}
	return r, err
}

func mapRegion(file *os.File, total int, cfg Config) (*Region, error) {
	mem, err := syscall.Mmap(int(file.Fd()), 0, total, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map region: %w", err)
	}
	clients, records, _ := sizeFor(cfg)
	return &Region{file: file, mem: mem, cfg: cfg, clients: clients, records: records}, nil
}

// Config reports the region's capacities.
func (r *Region) Config() Config { return r.cfg }

// Close unmaps the region. It does not change shared state.
func (r *Region) Close() error {
	err := syscall.Munmap(r.mem)
	if cerr := r.file.Close(); err == nil {
		err = cerr
	}
	return err
}

func (r *Region) word(off int) *uint64 { return (*uint64)(unsafe.Pointer(&r.mem[off])) }

// get/put are plain accesses. They are ordered by the atomic state words
// (slot state, log state) that publish them.
func (r *Region) get(off int) uint64    { return *r.word(off) }
func (r *Region) put(off int, v uint64) { *r.word(off) = v }

func (r *Region) getID(lenOff, idOff int) string {
	n := int(r.get(lenOff))
	if n > MaxClientIDBytes {
		n = MaxClientIDBytes
	}
	return string(r.mem[idOff : idOff+n])
}

func (r *Region) putID(lenOff, idOff int, id string) {
	copy(r.mem[idOff:idOff+MaxClientIDBytes], id)
	r.put(lenOff, uint64(len(id)))
}

func (r *Region) slot(i int) int   { return slotsOff + i*slotSize }
func (r *Region) client(i int) int { return r.clients + i*clientSize }
func (r *Region) record(client int, sequence uint64) int {
	return r.records + (client*r.cfg.Records+int(sequence%uint64(r.cfg.Records)))*recordSize
}
