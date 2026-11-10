# Agent Handoff

Last updated: 10 November 2026

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

The last result exposes the main performance problem. `processPersistentAck` in
`store.go` scans the entire shared `completions-v1` bucket and JSON-decodes every
request key to find records for one client. Its cost therefore grows with total
active-client metadata rather than with that client's acknowledged records.

## Next work

### 1. Remove the acknowledgement scan bottleneck

Change completion storage so one client's acknowledgement only visits that
client's records. A practical bbolt layout is a top-level completion bucket with
one nested bucket per client and sequence numbers encoded as ordered 8-byte
big-endian keys. Another acceptable design is a separate per-client index that
points to the existing completion keys.

The change must include:

- an explicit database schema/version and migration or compatibility strategy
  for existing `completions-v1` databases;
- atomic lookup, insertion, acknowledgement advancement, and deletion;
- correct `Stats` accounting across nested buckets or the new index;
- tests for migration, duplicate replay, stale protection, crash boundaries,
  blocked gaps, and multiple clients;
- no weakening of any invariant listed above.

After the change, rerun at minimum:

- active clients: 4, 16, 64, 256, and 1,024 with four workers;
- concurrency: 1, 4, 16, 32, and 64 workers;
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

`README.md` still says the CloudLab matrix needs to be collected and describes
CXL as only a possible extension. Update it to match the completed experiments
and the current plan. The final report should distinguish measured facts from
future work and retain the limitations in `EXPERIMENT_RESULTS.md`.

## Repository state at handoff

The working tree contains a large set of modified and untracked project files;
the implementation and experiment work has not yet been committed as one clean
snapshot. Review `git status` before staging. In particular, `.idea/`, local
generated graph data, Word reports, databases, binaries, and raw benchmark output
should only be committed if intentionally part of the repository.

Do not commit, push, add dependencies, or change the project scope without the
maintainers' approval.
