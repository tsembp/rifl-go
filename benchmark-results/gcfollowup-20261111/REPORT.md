# GC follow-up: batch-GC comparison and four-client regression

Run `gcfollowup-20261111`, 11 November 2026. Three CloudLab `d430` nodes at
Emulab: server pc750 (10.10.1.1), client pc745, controller pc741. Ubuntu
24.04.5 and an Intel Xeon E5-2630 v3 on all three nodes. Static binaries were
built with Go 1.27.0 (CGO off). bbolt used its default sync behaviour. Each
point used a fresh database, persistent connections and a 100-byte payload.
The order of the two versions alternated between points.

- **Original:** the protected server at commit `6a50b38`.
- **Optimized:** the protected server at commit `4ab1b7a` (per-client layout from
  `b561748`). Both builds share the same `cmd/server/main.go`, which adds opt-in
  profiling flags. One benchmark binary was used against both servers.

Hashes are in `build/build.json`, and the exact measured source is in
`build/optimized-source.tar.gz`. Raw summaries, latencies and timelines are in
`*-summary.csv` and `client-results.tar.gz`. Server logs, metrics, profiles and
`/proc/<pid>/io` snapshots are in `server-results.tar.gz`. No points failed and
none were discarded. All 106 points passed the runner's validation, with zero
errors and zero duplicate effects.

## Task 1: batch-ack GC

B is the number of records per client, released by one acknowledgement that is
piggybacked on each worker's final Add. W is the number of workers, and there
is one logical client per worker. Five repetitions per point. Values are
medians with the [min–max] range.

| B | W | Original ACK p50 (ms) | Optimized ACK p50 (ms) | Original ACK max (ms) | Optimized ACK max (ms) | DB size, orig / opt |
|---|---|---|---|---|---|---|
| 100 | 1 | 1.24 [1.01–1.30] | 0.97 [0.79–1.12] | 1.24 | 0.97 | 64 / 64 KiB |
| 100 | 4 | 2.55 [1.82–3.25] | 1.82 [1.58–1.95] | 2.92 [2.50–3.80] | 2.46 [2.00–2.65] | 256 / 128 KiB |
| 1,000 | 1 | 2.97 [2.22–3.53] | 1.58 [1.40–1.68] | 2.97 | 1.58 | 512 / 256 KiB |
| 1,000 | 4 | 4.98 [4.65–6.82] | 2.10 [1.70–2.41] | 9.49 [9.19–10.24] | 2.67 [2.50–3.88] | 2,048 / 1,024 KiB |
| 10,000 | 1 | 25.82 [14.26–27.29] | 7.21 [4.76–7.54] | 25.82 | 7.21 | 4,096 / 2,048 KiB |
| 10,000 | 4 | 46.29 [27.95–60.69] | 7.96 [5.83–8.70] | 75.68 [59.70–86.14] | 14.46 [8.89–15.42] | 16,384 / 8,192 KiB |

- With W=1, there is only one ACK sample per repetition, so p50 and max are the
  same value. Even with W=4, there are only four samples per repetition. The
  table supports comparing medians, not high-percentile claims.
- The ACK latency includes a new Add and its durable commit. It is not pure
  deletion time.
- The batch release gets cheaper as B grows: about 1.3× faster at B=100 and
  about 3.6–5.8× faster at B=10,000. The final database file is about half the
  size. bbolt does not shrink the file after GC, so the size reflects the
  file's peak, not what remains live.
- `bench -verify-batch-ack` passed on every point. Acknowledged sequences
  (the first and the last) were rejected as stale, each worker's final request
  replayed its saved result, and exactly W completion records remained. The
  counter value did not change during verification.

**Sanity check** (1,000 requests, 4 clients and 4 workers, 5 repetitions):

- `ack-gc` kept exactly 4 records on both versions.
- `blocked-gap` kept all 1,000 records on both versions. With a low sequence
  left unfinished, neither version collected later results too early.

## Task 2: four-client / four-worker regression

Settings matched the earlier sweep: 15-second runs, 2,048 warmup requests,
5 repetitions.

| Version | Throughput per repetition (ops/s) | Median | p50 latency |
|---|---|---|---|
| Original | 1953, 1961, 1953, 1953, 1979 | 1,953.5 | 2.037 ms |
| Optimized | 1638, 1636, 1633, 1642, 1647 | 1,638.4 | 2.312 ms |

The regression reproduces: the optimized version is 16.1% slower, compared
with 16.4% in the November 11 sweep (1,967.6 vs 1,645.4). The variation
between repetitions is under 2%.

### Diagnosis

The diagnostic runs were separate: 3 profiled and 3 I/O-counted repetitions
per version. They are not part of the throughput numbers above.

Per operation, measured from `/proc/<pid>/io` across warmup and measured
requests:

| | Original | Optimized |
|---|---|---|
| Bytes written per op | 12,288 (3 pages) | 16,384 (4 pages) |
| Write syscalls per op | 4.00 | 5.00 |
| Read syscalls per op | 2.00 | 2.00 |

These values were identical in all three repetitions.

- **CPU is not the cause.** CPU time per operation was about the same:
  12.86 s over about 31.3k ops (0.41 ms) for the original, and 11.29 s over
  about 26.6k ops (0.42 ms) for the optimized version.
- Allocation per operation was about the same as well: about 11.6 KB (original)
  vs about 10.9 KB (optimized).
- In both CPU profiles, more than 40% of samples are in syscalls, mostly in
  bbolt `Commit` → `fdatasync`/`pwrite`. The server is limited by synchronous
  commits.
- **Explanation.** With four clients, each commit in the per-client layout
  dirties one more page and issues one more `pwrite` than in the flat layout:
  4 data pages instead of 3. The extra page and write are measured. Attributing
  them to the nested client bucket is an inference from the layout change: the
  client bucket's page is dirtied in addition to the parent `completions-v2`
  entry. We did not inspect bbolt's page structure directly. Every operation
  pays for this extra page inside the synchronous commit. `fdatasync` calls
  were not counted separately: `/proc/<pid>/io` does not include them in
  `syscw`. The longer commit matches both the 16% throughput loss and the
  +0.28 ms median latency.
- The idea that nested buckets add *CPU* overhead is not supported. The cost is
  a write-amplification effect on synchronous I/O.
- **Why it reverses with many clients.** The per-commit cost of the flat layout
  grows with the number of retained records it touches. Earlier measurements
  showed it 2.70× slower at 1,024 clients, and in Task 1 it was 3.6–5.8× slower
  for large batches. The extra page is a fixed cost that is small next to that
  growth.
- Both observations stand: one extra page per commit at small client counts,
  and much better scaling at large client and batch counts.

No fix was attempted. Removing the extra page would mean changing the storage
layout. That would need new migration, crash and scaling validation, and the
handoff marks it as optional.
