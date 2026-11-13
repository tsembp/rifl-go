# Flat composite-key completions (schema v3): three-way comparison

Run `composite2-20261111`, 11 November 2026. The setup was the same as in
`gcfollowup-20261111`:

- **Nodes:** three CloudLab `d430` nodes at Emulab. The server was pc750
  (10.10.1.1), the client pc745, and the controller pc741.
- **Software:** Ubuntu 24.04.5 and Go 1.27.0 static builds, with bbolt's
  default sync behaviour.
- **Runs:** a fresh database for each point, persistent connections and a
  100-byte payload. The order of the three versions rotated between points.

The three versions:

- **original:** `6a50b38`. A flat bucket keyed by JSON `RequestID`. GC scans the
  whole bucket.
- **optimized:** `4ab1b7a`. Nested `completions-v2/<client>/<seq>` buckets.
- **candidate:** branch `flat-composite-completions`. One flat
  `completions-v3` bucket whose key is a 2-byte client-ID length, the client ID,
  and an 8-byte big-endian sequence. GC seeks to the client's prefix and deletes
  while advancing, stopping at `FirstIncomplete`.

All three builds share the same `cmd/server/main.go`. Hashes are in
`build/build.json`, and the exact source is in
`build/working-tree-source.tar.gz`. All 174 points passed validation: zero
errors, zero duplicate effects, the expected completion counts, and
`-verify-batch-ack` on every batch point. No points were discarded.

The earlier run `../composite-20261111` used a first candidate that re-seeked
after every delete. That walked emptied leaves again and again, which is
quadratic in the batch size: the B=10,000 ACK took about 155 ms. Its data is
kept as a record of that defect. This run uses the corrected loop.

## Four clients / four workers

15-second runs, 2,048 warmup requests, 5 repetitions.

| Version | Throughput per repetition (ops/s) | Median | p50 latency |
|---|---|---|---|
| Original | 1959, 1972, 1959, 1961, 1953 | 1,959.5 | 2.031 ms |
| Optimized | 1641, 1634, 1641, 1640, 1638 | 1,640.4 (−16.3%) | 2.306 ms |
| Candidate | 1918, 1916, 1925, 1925, 1929 | 1,925.0 (−1.8%) | 2.066 ms |

Writes per operation, from `/proc/<pid>/io` over 3 diagnostic repetitions. The
values were identical in every repetition:

| | Original | Optimized | Candidate |
|---|---|---|---|
| Bytes written per op | 12,288 | 16,384 | 12,288 |
| Write syscalls per op | 4.00 | 5.00 | 4.00 |

The candidate removes the extra page per commit and recovers most of the
regression. A gap of about 1.8% to the original remains. It was consistent
across the repetitions but is small, and we did not investigate it further.

## 1,024 clients / four workers

Same settings as above.

| Version | Median throughput (ops/s) | p50 latency |
|---|---|---|
| Original | 595.5 | 6.081 ms |
| Optimized | 1,600.6 | 2.453 ms |
| Candidate | 1,587.8 | 2.466 ms |

High-client scaling is preserved: the candidate is within 1% of the optimized
version and 2.7× the original.

## Batch-ack GC

Five repetitions per point. ACK p50 is the median of the per-repetition values,
with the [min–max] range. DB size is the final file size; bbolt does not shrink
the file after GC.

| B | W | Original | Optimized | Candidate | DB size, orig / opt / cand |
|---|---|---|---|---|---|
| 100 | 1 | 1.12 [1.06–1.22] | 1.04 [0.96–1.72] | 0.94 [0.82–1.03] | 64 / 64 / 64 KiB |
| 100 | 4 | 2.42 [1.88–3.02] | 2.31 [1.84–2.42] | 1.77 [1.20–2.03] | 256 / 128 / 128 KiB |
| 1,000 | 1 | 3.47 [2.53–3.62] | 1.51 [1.14–1.62] | 1.75 [1.11–1.81] | 512 / 256 / 512 KiB |
| 1,000 | 4 | 5.52 [3.52–7.01] | 2.04 [1.51–2.49] | 2.55 [2.07–3.76] | 2,048 / 1,024 / 2,048 KiB |
| 10,000 | 1 | 27.55 [18.35–30.58] | 6.79 [4.63–8.59] | 8.23 [6.52–10.13] | 4,096 / 2,048 / 4,096 KiB |
| 10,000 | 4 | 46.56 [44.01–46.99] | 8.49 [6.93–10.34] | 7.87 [6.88–10.29] | 16,384 / 8,192 / 16,384 KiB |

All values are in milliseconds except DB size. The ACK samples are few (1–4 per
repetition), so these support comparing medians only.

The candidate's batch release is in the same range as the optimized version and
about 3–6× faster than the original at B=10,000. Its database file is about
twice the optimized size, the same as the original's. Every completion key
repeats the client ID: about 50 bytes in these benchmark IDs, against an 8-byte
key in the nested layout. That is a disk-space cost; it did not show up as a
throughput cost here.

## Sanity checks

1,000 requests, 4 clients and 4 workers, 5 repetitions.

- `ack-gc` kept exactly 4 records on every version.
- `blocked-gap` kept all 1,000 records on every version, so no version collected
  records too early.

## Correctness work on the branch

- **Schema v3.** It migrates both the unversioned v1 layout and the v2 nested
  layout in one transaction. Saved bytes, the counter value and boundaries are
  preserved.
- **Migration tests.** v1 and v2 migration, a malformed v2 database rolling back,
  and a subprocess crash before and after the migration commit for both sources.
- **Overlong client IDs.** IDs longer than bbolt's key limit allows (32,758
  bytes) are now rejected definitively. They are not reported as ambiguous
  storage failures.
- **GC test.** A partial batch acknowledgement across many leaves, with
  neighbouring clients on both sides in key order, deletes exactly the target's
  lower records.
- `go test ./...`, `go test -race ./...`, `go vet ./...` and `gofmt` all pass.
