# Agent Handoff

Last updated: 14 November 2026

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
Per-client buckets (v2) fixed the scan, improving 1,024-client throughput by
2.70x, but regressed by about 16% at four clients. The current flat composite-key
layout (v3) keeps the scaling and leaves a gap of about 2% at four clients. The
paired sweeps, with raw data and verification, are listed in
benchmark-results/README.md. Preserve these trade-offs rather than claiming a
universal speedup.

## Status and next work

### Done

- **GC follow-up** ([HANDOFF_GC_FOLLOWUP.md](HANDOFF_GC_FOLLOWUP.md)). The
  batch-GC comparison and four-client diagnosis are in
  benchmark-results/gcfollowup-20261111/REPORT.md: the v2 nested buckets wrote
  one extra 4 KiB page per commit, with CPU per operation unchanged.
- **Schema v3 (flat composite keys).** It removes that page while keeping
  high-client scaling; see benchmark-results/composite2-20261111/REPORT.md.
  It was merged to main in PR #1.
- **Shared-memory (CXL-inspired) extension**, as designed in
  SHM_DESIGN_PROPOSAL.md: configurations A (TCP + bbolt), B (shm slots + bbolt)
  and C (shm slots + shm state), with owner/standby takeover. It is on branch
  `shm-design`, PR #2. Results, all on one CloudLab d430 node:
  - benchmark-results/shm-20261113: throughput, batch acknowledgement and a
    first failover run.
  - benchmark-results/shm-failover-20261113: 20-repetition takeover timing.
    It also corrects the first run's reading of the longest request as the
    takeover.
  - benchmark-results/shm-tmpfs-20261114: A and B with the bbolt file on
    `/dev/shm`, a stand-in for the instructor's suggestion of bbolt on a
    filesystem backed by CXL memory.
- **The failover limitation is documented in README.md.** The message-based
  version supports a restart or a standby on the same machine
  (`cmd/server -standby-lock`), not failover to another machine.

### Remaining

1. **Final report.** Check the course rules with the team first. Build it from
   the REPORT.md files above, distinguishing measured facts from future work and
   keeping the limitations.
2. **Optional: a small financial workload.** An account-transfer service as a
   second non-idempotent workload: durable balances, validation, and an atomic
   transfer plus completion record. It should reuse the existing request,
   retry, acknowledgement and stale-request rules. No exchange, order book, UI
   or trading integration.
3. **Not implemented (future work):**
   - Cross-machine takeover. It needs a lease or fencing mechanism in place of
     the local `flock`, and storage the replacement machine can reach.
   - Leases for reclaiming per-client boundaries.

## Repository state at handoff

- main includes PR #1: schema v3 and the GC follow-up results.
- Branch `shm-design` (PR #2, open) adds the shared-memory package, the
  `cmd/shmserver` owner/standby server, `cmd/bench -transport shm`,
  scripts/shm/failover.sh, scripts/cloudlab/shm_compare.py and the three shm
  result folders.
- The full raw archive of shm-20261113 (about 434 MB) exceeds GitHub's file
  limit. Only `results-reduced.tar.gz` is committed.

Preserve committed measurement data and put new experiments in a new results
folder. Do not commit, push, add dependencies, or change the project scope
without the maintainers' approval.
