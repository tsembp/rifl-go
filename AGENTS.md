# Contributor and AI-agent context

Read README.md first. This is a small CS452 experiment adapting RIFL ideas to Go
net/rpc. The former trading-system design is superseded. Financial operations
may become workloads; an exchange, order book, UI, or real trading integration is
outside the current implementation.

## Current design

- The root package is riflrpc; rpc.go holds wire types and the counter service.
- client.go handles logical IDs, per-attempt deadlines, retries and reconnection.
- cmd/server and cmd/client are minimal runnable examples.
- Counter uses one mutex for state mutation and completion recording. Deduplication
  state belongs to the service, not the TCP connection.
- store.go provides PersistentCounter via OpenCounter(path). The server CLI uses
  this durable implementation by default, with a configurable -db path.
- Durable completions use one flat completions-v3 bucket keyed by
  <2-byte ID length><client ID><8-byte big-endian sequence>. GC seeks to the
  client's prefix, deletes while advancing (never re-seek per delete; emptied
  leaves stay until commit) and stops at FirstIncomplete. Saved completion values
  remain JSON; the in-memory Counter still uses its original map.
- Do not reintroduce nested per-client buckets: they cost an extra dirty page per
  commit (the measured four-client regression). Client IDs longer than
  maxClientIDBytes are rejected definitively.
- schema.go creates version 3 or atomically migrates unversioned completions-v1
  and version 2 nested completions-v2 databases. Preserve saved bytes, counter
  state and durable boundaries. Update schema tests for storage changes; old
  binaries cannot read upgraded databases.
- Stats counts completion keys in the flat bucket. ClientBoundaries still counts
  only durable boundary entries.
- baseline.go provides the durable retry-only UnprotectedCounter used solely as
  the fair comparison baseline. cmd/bench generates CSV and cmd/benchplot creates
  dependency-free SVG summaries; see BENCHMARKS.md for experiment rules.
- The benchmark supports duration runs, independent virtual-client and worker
  counts, payload padding, retry-rate and transport-recovery workloads, raw latency
  samples, timelines, and batched acknowledgements. CloudLab uses native static
  binaries on bare metal; see cloudlab/README.md.
- PersistentCounter uses one bbolt write transaction for the state change and
  completion record; do not enable NoSync or add a separate result cache.
- Replies are saved per logical request. Do not substitute the current counter
  value when replaying an old request.
- Transport failures are ambiguous and retryable. rpc.ServerError is definitive
  except for the reserved storageErrorPrefix, which signals an ambiguous storage
  failure. Keep that distinction in the client and server.
- A timed-out request may still execute. Never allocate a new sequence on retry.
- Each attempt has its own connection and reply variable. Wait for decoder
  completion before returning from a cancelled attempt.
- The client tracks unfinished sequences and piggybacks `FirstIncomplete`; only a
  definitive reply marks a sequence complete. Ambiguous failures remain unfinished.
- A server acknowledgement boundary only moves forward. Advancing it, deleting
  lower completion records, and applying the incoming operation share one bbolt
  transaction. Any request below the stored boundary is permanently stale.

## Shared-memory (CXL-inspired) extension

SHM_DESIGN_PROPOSAL.md is the agreed design. The shm package models CXL memory
as a Linux MAP_SHARED file (/dev/shm) shared by processes on ONE machine:

- Configuration A is the existing TCP + bbolt path; B is shm slots + bbolt
  (shm.CounterHandler); C keeps the RIFL state in the region (shm.State).
- shm.State: one owner at a time (flock on the region file; a standby blocks on
  it). The one-entry redo log's atomic COMMITTED store is the commit point; log
  values are absolute so recovery reapplies idempotently. Keep mutation, record,
  boundary advance and reclamation in one log entry.
- Never evict an unacknowledged record. A full per-client ring returns the
  ambiguous CodeRetry before execution, never a definitive refusal.
- Client timeouts leave the request in its slot; retrying the same ID collects
  that execution. Test-only crash hooks in shm.State must not become controls.
- Claims: exactly-once across server process crashes and disk-free takeover on
  one host. Not claimed: CXL hardware performance, multi-host coherence, or
  power-loss survival for C (CXL memory is not persistent).
- cmd/server -standby-lock gives A the same flock standby. Both servers log
  microsecond UTC timestamps, so the runner can record kill -> lock and
  kill -> serving per run. Measure takeover with these, not with the longest
  request: the longest request was mostly unrelated disk or scheduling stalls.
  That was a correction to shm-20261113.
- Results are in benchmark-results/shm-20261113 (A/B/C),
  shm-failover-20261113 (20-repetition takeover) and shm-tmpfs-20261114 (A/B
  with bbolt on /dev/shm, a stand-in for a CXL-backed filesystem; not a CXL
  measurement).
- Keep C's per-request latency files out of git: they reach GBs. Failover runs
  use --no-failover-latency.
- Refer to course staff only as "the instructor".

## Scope and correctness

The zero-value Counter is in-memory only. PersistentCounter survives server-process
restart using the same intact database, with bbolt's default sync behavior. Tests
do not establish power-loss or corruption tolerance. Do not claim generic
transactional RPC, bounded metadata, replication, or full RIFL support.
Do not evict completion records except through a valid acknowledgement, or
introduce TTL cleanup: a late retry could execute again. Preserve atomic mutation,
result, acknowledgement, and reclamation commits. Per-client durable boundaries
remain until leases are implemented. Test-only hooks in PersistentCounter allow
subprocess termination before commit and after commit before response; never
expose them as production RPC controls.

EXPERIMENT_RESULTS.md describes the old flat-layout measurements. Paired
CloudLab runs under benchmark-results/README.md cover the v2 per-client layout
(client-count and concurrency sweeps, batch GC, and the profiled four-client
regression) and the v3 composite-key layout that removes that regression while
keeping high-client scaling. Preserve the recorded regression, the superseded
first v3 run, the remaining ~2% four-client gap and repetition variability; do
not describe any layout as a universal speedup or replace the historical tables.

The lost-response test wraps a server connection and fails its response write
after net/rpc has executed the handler. Keep this test exercising a real RPC
connection, not just invoking the counter directly.

## Verification and workflow

Use gofmt on changed Go files, then run:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Preserve user changes. Do not commit, push, add dependencies, expand into shared
memory, or add financial-service complexity without a corresponding request.
Treat loopback TCP tests as protocol correctness checks, not CXL measurements
or performance claims. Check course rules with the team before preparing graded
submission material.
