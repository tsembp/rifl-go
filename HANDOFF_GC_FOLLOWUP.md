# Handoff: remaining GC evaluation

> **Status: complete (11 November 2026).**
> - Task 1 and Task 2: benchmark-results/gcfollowup-20261111/REPORT.md.
> - Fix (schema v3): benchmark-results/composite2-20261111/REPORT.md, merged in
>   PR #1.
>
> This file is kept as the record of the task.

This is the focused task for Spyros's agent. Read AGENTS.md, README.md, and
AGENT_HANDOFF.md for the existing architecture and correctness invariants.
The GC optimization, migration, tests and raw benchmark data are in commit
`b561748` (`optimize gc per client and add benchmark results`).

## Completed work

- Persistent results now live in completions-v2/<client ID>/<ordered sequence>.
- schema.go atomically upgrades both previous flat completions-v1 layouts.
- Counter mutation, result, ACK-boundary advancement and GC share one transaction.
- Correctness, migration, subprocess crash tests, race tests and go vet pass.
- Paired client-count and fixed-client concurrency sweeps are complete.
- Reports, raw CSV archives, source snapshots, binary hashes and environment
  captures are under [benchmark-results](benchmark-results/README.md).

Original protected implementation: commit `6a50b38`. Optimized implementation:
commit `b561748`; its measured source was uncommitted when the snapshots were
captured. The retry-only baseline is deliberately unprotected. Do not confuse it
with the original protected implementation when measuring optimization benefits.

## Task 1: batch-GC comparison

Compare the original and optimized protected servers when one ACK releases many
completion records. Use the same benchmark client against both server binaries.

- Start with batches of 100, 1,000 and 10,000 records per client, using one worker
  and then four workers. Use at least five independent repetitions per point.
- The existing `batch-ack` scenario supports this. `-requests` is the total across
  workers: for B records per client and W workers, set requests=B*W and
  virtual-clients=W. Existing run-metadata-matrix.sh uses totals; label them
  accurately if you reuse that script.
- Use fixed request counts, warmup=0, payload-bytes=100, persistent connections,
  fresh server/database for each independent point, and unchanged bbolt sync.
- The batch ACK is piggybacked on one final Add per worker. Its measured latency
  includes the new counter operation and durable commit; it is not pure deletion
  time and there is no separate ACK RPC yet.
- Record batch_ack_p50_ms, batch_ack_max_ms, completion counts and database size,
  plus raw latency CSVs. Report repetition variability; a handful of ACK samples
  does not support strong high-percentile claims.
- Check that acknowledged records disappear, each worker's final result remains,
  and stale requests stay rejected. bbolt file size need not shrink after GC.
- Include an ack-gc/blocked-gap sanity comparison: an unfinished low sequence must
  keep later results safe, rather than allowing premature collection.

## Task 2: explain the four-client regression

The regression is at **four logical clients and four workers**. It is not the
four-worker point in the concurrency sweep, which held 64 clients fixed.

In the client-count sweep, median old protected throughput was 1,967.6 ops/s and
optimized throughput was 1,645.4 ops/s (about 16% lower). At 1,024 clients, the
optimization was 2.70x faster. Both observations must remain in the report.

- Reproduce the four-client/four-worker case before drawing conclusions. Match
  15-second measured runs, 2,048 warmup requests, five repetitions, 100-byte
  payload, persistent connections, fresh databases, compiler and sync settings.
- Profile CPU/allocation costs and examine transaction/I/O costs in separate
  diagnostic runs. Nested-bucket overhead is a hypothesis, not a proven cause.
  Do not mix profiling runs into the uninstrumented performance summaries.
- Explain the measured trade-off with evidence. An honest explanation is an
  acceptable outcome; another optimization is not required.
- If proposing a fix, keep it separate and small, rerun correctness/migration/
  crash tests, and verify that high-client scaling is not lost.

## Measurement and handoff rules

Use the same currently allocated machines for paired old/new comparisons.
Recorded SSH addresses belong to the November 11 allocation; verify ownership
and readiness before reusing them. Do not compare only against historical
Ubuntu 22.04 numbers. The paired November 11 runs used Ubuntu 24.04 and Go 1.27.0.

Preserve the existing datasets. Save new summaries, raw samples, diagnostics,
source revision, settings, hashes and environment in a new named results folder.
Document failures/outliers rather than silently discarding them. The concurrency
run already documents a between-point control-SSH stall and a noisy baseline run.

Do not disable sync, add TTL eviction, change retry IDs, remove stale boundaries,
or add caching to improve numbers. Cross-machine failover is a documented
limitation approved by the instructor; it is not part of these two tasks.
Do not expand into financial workloads or shared memory during this follow-up.

Deliver a short report for each task, raw supporting data, and any clearly scoped
code changes. Follow AGENTS.md for tests and obtain maintainer approval before
committing or pushing new follow-up work.
