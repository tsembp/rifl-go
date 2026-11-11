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
- Durable completions use completions-v2/<client ID>/<8-byte big-endian sequence>.
  GC visits only the target client's bucket and stops at FirstIncomplete. Saved
  completion values remain JSON; the in-memory Counter still uses its original map.
- schema.go creates version 2 or atomically migrates unversioned completions-v1
  databases. Preserve saved bytes, counter state and durable boundaries. Update
  schema tests for storage changes; old binaries cannot read upgraded databases.
- Stats must count completion entries inside client buckets, not the buckets
  themselves. ClientBoundaries still counts only durable boundary entries.
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

EXPERIMENT_RESULTS.md describes the old flat-layout measurements. The per-client
optimization is implemented and paired client-count and fixed-client concurrency
sweeps are saved under benchmark-results/README.md, including raw data and source
snapshots. Preserve the four-client regression and repetition variability in
reports; do not describe the optimization as a universal speedup or replace the
historical tables. Further profiling and batch-GC measurements remain next work.

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
