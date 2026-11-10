# rifl-go

A small RIFL-inspired exactly-once RPC experiment using Go's `net/rpc`, developed
for EPL452 / CS452 Datacenter Computing at the University of Cyprus.

## Problem

A server may execute a request and then lose its response. The client cannot tell
whether the operation happened. Retrying an increment, payment, or trading order
without duplicate detection can repeat its side effects.

This project studies how unique logical request IDs and saved results make
retries safe, and what coordination and performance costs they introduce.
The initial implementation uses conventional TCP RPC. A comparison with shared
memory between Go processes, inspired by CXL-SHM, is a possible later extension.

## Current prototype

- A counter with `Counter.Add` and read-only `Counter.Value` RPC methods.
- Random 128-bit client IDs and monotonically increasing request sequences.
- A client wrapper that reconnects and retries the same request on transport
  failures and per-attempt timeouts, bounded by a caller-provided context.
- Client-side tracking of the lowest unfinished sequence (`FirstIncomplete`),
  piggybacked on requests as an acknowledgement of all lower sequences.
- Server-side duplicate suppression and replay of the original result, including
  counter-overflow errors. Reusing an ID with different arguments is rejected.
- A bbolt-backed counter that commits the mutation and completion record together,
  preserving duplicate protection and original results across server restarts.
- Durable per-client acknowledgement boundaries, atomic completion-record
  reclamation, and stale-request rejection after records have been deleted.
- The original in-memory counter, retained for comparison and tests.
- Tests over loopback TCP, including lost responses, concurrent duplicates,
  database reopen, and abrupt server-process termination at transaction boundaries.
- A durable retry-only baseline plus a reproducible benchmark runner and SVG
  plotter for throughput, tail latency, duplicate effects, metadata, database size,
  retry rates, payload sizes, client scale, acknowledgement batches, and outages.

### Guarantee and limitations

**The server stores state and completion records in a bbolt database.** An increment
and its result are committed in one transaction before success is returned. A crash
before commit leaves neither visible; a crash after commit preserves both. Restart
with the same database to replay results without repeating their side effects.

The guarantee assumes the database remains intact and the filesystem/device honors
sync operations. bbolt's default synchronization stays enabled. Tests cover process
crashes, not power failures or storage corruption. One process owns the database;
another opener fails after a bounded lock wait. There is no replication or failover.
The zero-value `Counter` still uses memory only and loses everything on restart;
`OpenCounter(path)` creates the persistent service used by the server CLI.

A request is identified by `(ClientID, Sequence)`. Retrying that ID with the same
arguments returns its original result even after other requests change the
counter, provided the client has not acknowledged receiving that result. A timeout
or cancellation does not undo an operation: its outcome can remain unknown. Retain
the same request object if you need to retry afterward. Calling `NewRequest` again
creates another logical operation.

When a definitive reply arrives, the client marks that sequence complete. Each
attempt carries the lowest still-unfinished sequence; all lower requests are
therefore acknowledged. The server advances this boundary monotonically and
deletes lower completion records in the same bbolt transaction. A delayed request
below the durable boundary is rejected as stale, including after server restart.

Clients must retain the original request ID and arguments across retries, including
after server restarts. The client CLI creates a new ID on every invocation; it is
not a tool for replaying a previous invocation. Application errors such as overflow
are remembered, while storage errors remain retryable/ambiguous rather than being
treated as definitive rejections. A storage error is signaled with the reserved
`storage outcome unknown: ` RPC error prefix.

An allocated request that never receives a definitive reply prevents the boundary
from advancing past it, so later completion records remain safe. Per-client
acknowledgement boundaries are retained indefinitely because client leases and
crash cleanup are not implemented. The final completion for an idle client also
remains until a later request carries its acknowledgement. The benchmark tooling
is implemented, but final reported measurements still need to be collected under
controlled conditions. Financial workloads are future work. No CXL hardware or
emulation is implemented yet. The client and benchmark open one TCP connection per
attempt for simplicity; this is not an optimized transport baseline.

## Run

Requires Go 1.22 or newer. Uses `go.etcd.io/bbolt` for embedded transactional storage.

Start a server:

```sh
go run ./cmd/server -addr 127.0.0.1:9000 -db ./rifl.db
```

In another terminal:

```sh
go run ./cmd/client -addr 127.0.0.1:9000 -delta 10
```

The demo sends one request successfully, then deliberately sends it again after
its reply was acknowledged. The second call is rejected as stale and the counter
is incremented once. Each new CLI invocation creates a new client identity.

Restarting the server with the same `-db` path preserves the counter and results.
Do not delete the database to recover a failed server: doing so also deletes the
deduplication history. `/rifl.db` is ignored by Git; keep other database paths
outside the repository or add their exact paths to your local ignore rules.

## Test

```sh
go test ./...
go test -race ./...
go vet ./...
```

The tests verify lost-response retry, original-result replay before acknowledgement,
concurrent duplicate and unique requests, out-of-order client completion, request
validation, stale rejection, monotonic acknowledgement boundaries, remembered
overflow errors, timeout retries, and cancellation. Storage tests also verify
reclamation and stale protection across reopen, migration of existing databases,
recovery before/after mutation and acknowledgement commits using a subprocess,
and ambiguous storage failures. They do not simulate power loss or replace a
formal correctness proof.

## Benchmark

The server accepts `-mode protected` (default) or `-mode baseline`. The benchmark
client supports normal, response-loss simulation, duplicate-heavy, configurable
retry-rate, acknowledgement/GC, batched acknowledgement, blocked-gap, restart,
and transport-retry workloads. It supports fixed-duration runs, separate
worker/logical-client counts, payload sweeps, raw per-request latency, and
one-second timelines. `cmd/benchplot` turns summary rows into an SVG comparison.

Use a laptop for smoke tests and CloudLab for final controlled measurements. The
full fairness rules, scenario matrix, commands, restart procedure, output schema,
and interpretation guidance are in [BENCHMARKS.md](BENCHMARKS.md). A three-node
bare-metal profile and native deployment workflow are in
[cloudlab/README.md](cloudlab/README.md); Docker is not required.

## Next steps

1. Instantiate the supplied CloudLab profile, run the documented benchmark matrix,
   and analyze the aggregate, raw-latency, timeline, and node-metric CSV files.
2. Add a small financial demonstration; consider a shared-memory transport if
   time permits and the instructor confirms its scope.

## References

- [RIFL: Implementing Linearizability at Large Scale and Low Latency (SOSP 2015)](https://sigops.org/s/conferences/sosp/2015/current/2015-Monterey/126-lee-online.pdf)
- [CXL-SHM: Partial Failure Resilient Memory Management System (SOSP 2023)](https://doi.org/10.1145/3600006.3613135)

Contributor and AI-agent guidance is in [AGENTS.md](AGENTS.md).
