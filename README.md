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
- Server-side duplicate suppression and replay of the original result, including
  counter-overflow errors. Reusing an ID with different arguments is rejected.
- A mutex covering mutation and result recording, preventing concurrent attempts
  from executing the same operation twice.
- Tests over loopback TCP, including a response deliberately lost after execution.

### Guarantee and limitations

**State and completion records are in memory. The guarantee covers one server
lifetime only.** Both are lost on restart. This is an initial demonstration,
not a complete implementation of RIFL or durable exactly-once RPC.

A request is identified by `(ClientID, Sequence)`. Retrying that ID with the same
arguments returns its original result even after other requests change the
counter. A timeout or cancellation does not undo an operation: its outcome can
remain unknown. Retain the same request object if you need to retry afterward.
Calling `NewRequest` again creates another logical operation.

Completion records currently grow without bound. Persistence, acknowledgement
tracking, garbage collection, client leases, failure recovery, financial workloads,
and performance experiments are future work. No CXL hardware or emulation is
implemented yet. The client opens one TCP connection per attempt for simplicity;
this is not an optimized networking baseline.

## Run

Requires Go 1.22 or newer. There are no external dependencies.

Start a server:

```sh
go run ./cmd/server -addr 127.0.0.1:9000
```

In another terminal:

```sh
go run ./cmd/client -addr 127.0.0.1:9000 -delta 10
```

The demo sends the **same request twice**. On a fresh server both replies show
`10`, and the counter is incremented once. Each new invocation of the CLI creates
a new client identity and therefore a new logical operation.

## Test

```sh
go test ./...
go test -race ./...
go vet ./...
```

The tests verify lost-response retry, original-result replay, concurrent duplicate
and unique requests, request validation, conflicting ID reuse, remembered overflow
errors, timeout retries, and cancellation. They do not establish crash durability
or substitute for a formal correctness proof.

## Next steps

1. Add atomic durable storage of counter mutations and completion records, with
   restart tests at explicitly defined crash points.
2. Add acknowledgements and safe record reclamation, preserving stale-request
   detection after results are removed.
3. Evaluate equivalent unprotected and protected RPC workloads under the same
   transport and storage configuration.
4. Add a small financial demonstration; consider a shared-memory transport if
   time permits and the instructor confirms its scope.

## References

- [RIFL: Implementing Linearizability at Large Scale and Low Latency (SOSP 2015)](https://sigops.org/s/conferences/sosp/2015/current/2015-Monterey/126-lee-online.pdf)
- [CXL-SHM: Partial Failure Resilient Memory Management System (SOSP 2023)](https://doi.org/10.1145/3600006.3613135)

Contributor and AI-agent guidance is in [AGENTS.md](AGENTS.md).
