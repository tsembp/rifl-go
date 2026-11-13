# Agent Handoff

Last updated: 11 November 2026

## Project objective

This repository adapts RIFL-style duplicate suppression to Go's `net/rpc`
package. The implemented service is a counter because repeated side effects are
easy to detect. The system protects a logical request when a server executes it
but its reply is lost: the client retries the same request ID, and the server
returns the saved result instead of applying the operation again.

The current claim is exactly-once effects for the implemented counter RPC under
the tested process-crash, timeout, connection-loss, and lost-response model. Do
not describe this as exactly-once network delivery, generic transactional RPC,
full RIFL, replication, or power-loss tolerance.

## Current implementation

The conventional message-based implementation and its benchmark suite are
complete.

- `rpc.go` defines request and reply types and the in-memory `Counter`.
- `client.go` assigns a random 128-bit client ID and increasing sequence number,
  retries ambiguous failures with the same logical ID, and tracks the lowest
  unfinished sequence as `FirstIncomplete`.
- `store.go` implements the bbolt-backed `PersistentCounter`. A counter update,
  saved result, acknowledgement-boundary advancement, and completion-record
  reclamation occur in one bbolt write transaction.
- `baseline.go` provides the durable retry-only `UnprotectedCounter` used for a
  fair comparison. It intentionally executes every retry.
- `cmd/server` and `cmd/client` provide runnable examples.
- `internal/benchrun`, `cmd/bench`, and `cmd/benchplot` implement the benchmark
  workloads, CSV output, latency samples, timelines, and SVG summaries.
- `cloudlab/` contains the three-node bare-metal profile and deployment scripts.
- `BENCHMARKS.md` documents the experiment rules and commands.
- `EXPERIMENT_RESULTS.md` contains the completed CloudLab results.

## Correctness invariants

Preserve these rules when changing the storage layout or adding workloads:

1. A retry must reuse the original `(ClientID, Sequence)` and arguments.
2. A state mutation and its completion record must commit atomically.
3. Duplicate requests must replay the original saved result, not the current
   counter value.
4. Only a definitive reply marks a client request complete. Transport failures,
   timeouts, and reserved storage errors remain ambiguous and retryable.
5. The server acknowledgement boundary moves only forward.
6. Boundary advancement, deletion of acknowledged records, and the incoming
   operation must share one bbolt transaction.
7. A request below the durable boundary remains stale after restart and after
   its completion record has been reclaimed.
8. Do not add TTL-based eviction or unacknowledged-record eviction. A late retry
   could otherwise execute twice.
9. Keep bbolt's default synchronous durability. Do not enable `NoSync` or add a
   separate in-memory result cache to improve benchmark numbers.

## Test coverage

The tests cover:

- a response lost after a real `net/rpc` handler has executed;
- duplicate and concurrent requests;
- timeout, cancellation, reconnection, and retry behaviour;
- replay of the original result and remembered application errors;
- acknowledgement advancement, garbage collection, and stale rejection;
- database reopen and durable boundaries across restart;
- subprocess termination before commit and after commit before response;
- ambiguous storage failures;
- restart, retry-rate, blocked-gap, batch-ack, and lost-response benchmark cases.

Before handing off a change, run:

```sh
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
```

## Completed CloudLab evaluation

Measurements were collected on three CloudLab `d430` bare-metal nodes running
Ubuntu 22.04. Each ordinary performance point used five independent repetitions
with a fresh server and database. bbolt synchronous durability remained enabled.

Main findings:

- At 1 and 4 workers, protected throughput overhead was 4.6% and 2.6%.
- At 64 workers, protected overhead increased to 27.3%.
- At a 100% controlled retry rate, the baseline produced 16,697 extra effects;
  the protected server produced zero.
- Replaying 50,000 completed IDs after restart produced 50,000 extra baseline
  mutations and zero protected mutations.
- Payloads between 0 and 16 KiB had little effect in this setup.
- Continuous acknowledgement retained four completion records after runs of up
  to 10,000 requests.
- With four workers and 1,024 logical clients, throughput was 2,227.6 ops/s for
  the baseline and 372.0 ops/s for the protected server.

The last result exposed a scan bottleneck in commit 6a50b38: processPersistentAck
walked the entire shared completions-v1 bucket and JSON-decoded every request key.
The current change uses per-client buckets. Paired client-count and fixed-client
concurrency sweeps, with raw data and verification, are in benchmark-results/README.md.
The first sweep improves 1,024-client throughput by 2.70x but regresses at four
clients. Preserve this trade-off rather than claiming a universal speedup.

## Next work

The two-task GC follow-up in [HANDOFF_GC_FOLLOWUP.md](HANDOFF_GC_FOLLOWUP.md) is
complete. Batch-GC measurements and the four-client diagnosis are in
benchmark-results/gcfollowup-20261111/REPORT.md: the v2 nested buckets wrote one
extra 4 KiB page per commit, with CPU per operation unchanged. Schema v3 (flat
composite keys) removes that page. Its paired three-way comparison is in
benchmark-results/composite2-20261111/REPORT.md.

### 1. Profile remaining overhead and finish GC evaluation

Implemented: completions-v3 is one flat bucket whose keys are the client-ID
length, client ID and 8-byte big-endian sequence (v2 used nested per-client
buckets and is migrated automatically). Durable GC only visits that client's reclaimable records.
The RPC protocol and JSON completion values are unchanged. The in-memory counter
retains its original map-based implementation.

The change includes:

- schema version 3 and atomic migration of both older unversioned layouts and v2;
- atomic lookup, insertion, acknowledgement advancement, and deletion;
- correct `Stats` accounting of completion keys;
- tests for migration, duplicate replay, stale protection, crash boundaries,
  blocked gaps, and multiple clients;
- unchanged durability settings and invariants listed above.

Review store.go, schema.go, gc_test.go, schema_test.go and the adapted store_test.go.
The maintainers authorized the paired client-count and concurrency measurements.
The concurrency sweep keeps 64 clients fixed while varying workers, so it differs
from the old experiment where worker and client counts increased together.

Completed paired sweeps:

- active clients: 4, 16, 64, 256, and 1,024 with four workers;
- concurrency: 1, 4, 16, 32, and 64 workers with 64 fixed clients.

Remaining measurements:

- acknowledgement/GC and blocked-gap scenarios;
- batch acknowledgement at 100, 1,000, and 10,000 records.

Use the same CloudLab node type, duration, repeat count, payload, storage sync,
and connection settings as the existing results. Keep the old results rather
than overwriting them, and report before/after medians.

### 2. Add a small financial workload

Add an account-transfer service as a second non-idempotent workload. Keep its
scope small: durable account balances, validation, and atomic transfer plus
completion recording. Do not build an exchange, order book, UI, or real trading
integration. Reuse the logical request, retry, acknowledgement, and stale-request
rules instead of creating a separate reliability protocol.

### 3. Implement the CXL/shared-memory comparison

The planned next phase is a shared-memory transport between Go processes,
inspired by CXL-SHM. Treat CXL as process-shared memory for the prototype; do not
claim CXL hardware measurements unless real CXL hardware is used. First define
the request/reply queues, ownership rules, synchronization primitives, crash
recovery metadata, and how saved results and acknowledgements remain durable.
Then compare the message-based and shared-memory versions using equivalent
workloads and durability assumptions.

### 4. Update documentation and prepare the final report

README.md now links the completed paired sweeps through benchmark-results/README.md.
Add the follow-up findings there without overwriting historical data. The final
report should distinguish measured facts from future work and retain the
limitations in EXPERIMENT_RESULTS.md.

## Repository state at handoff

Spyros's implementation and experiment tooling are committed in 6a50b38. The
per-client GC work, migration tests, README clarification and both paired benchmark
datasets (including raw archives) are committed in b561748. Review git status
before staging; leave unrelated IDE files and databases alone. Preserve committed
measurement data and put follow-up experiments in a separate results folder.

Do not commit, push, add dependencies, or change the project scope without the
maintainers' approval.
