# Shared-memory (CXL-inspired) comparison: configurations A, B and C

Run `shm-20261113`, 13 November 2026. One CloudLab `d430` node at Emulab
(pc755): Intel Xeon E5-2630 v3 with 32 CPUs and Ubuntu 24.04.5. Static
binaries were built with Go 1.27.0; hashes are in `build/build.json` and the
exact source is in `build/source.tar.gz`. bbolt files were on the local disk
(`/var/tmp`, ext3) with bbolt's default sync behaviour. Shared regions were in
`/dev/shm`. Every point used a fresh database and region, and the order of the
configurations rotated between points.

| Config | Requests travel over | RIFL state lives in | Survives |
|---|---|---|---|
| A | TCP `net/rpc`, **loopback** | bbolt file, synced | process crash, power loss |
| B | shared-memory slots | bbolt file, synced | process crash, power loss |
| C | shared-memory slots | shared-memory region | process crash only |

**Scope of the measurements:**

- All three configurations ran the server and the benchmark on the same
  machine, because C cannot span hosts in this model. A's numbers are therefore
  loopback numbers, not the two-machine numbers in earlier runs.
- These are measurements of process-shared memory on one host. They are not
  CXL hardware measurements.
- C is not durable across power loss or a machine crash.

Raw data is in `results-reduced.tar.gz`: every summary, timeline and log, plus
the per-request latency samples for A and B. C's per-request samples
(`throughput-C-latency.csv` and `failover-C-r*/latency.csv`, about 5.9 GB
uncompressed) exceed GitHub's file limit and are kept outside the repository.
Every figure in this report comes from the committed summary CSVs.

All 138 points passed validation: zero errors, zero duplicate effects, and the
expected completion counts. Every batch point also passed
`-verify-batch-ack`. No points were discarded.

## Throughput

4 workers, 15-second runs, 2,048 warmup requests, 100-byte payload,
5 repetitions. Throughput is the median with the [min–max] range; latency is
the median of the per-repetition values.

| Clients | Config | Throughput (ops/s) | p50 | p99 |
|---|---|---|---|---|
| 4 | A | 1,803.3 [1,782.3–1,914.8] | 2.198 ms | 3.013 ms |
| 4 | B | 2,319.0 [2,303.1–2,339.1] | 1.538 ms | 2.593 ms |
| 4 | C | 375,175 [366,614–389,195] | 0.006 ms | 0.031 ms |
| 1,024 | A | 1,638.2 [1,555.8–1,749.2] | 2.398 ms | 3.419 ms |
| 1,024 | B | 1,962.3 [1,955.2–1,978.0] | 1.852 ms | 3.029 ms |
| 1,024 | C | 325,482 [315,709–335,087] | 0.008 ms | 0.035 ms |

- **A vs B (the cost of the transport).** Replacing loopback TCP with shared
  slots gives 1.29× the throughput at 4 clients and 1.20× at 1,024. The median
  latency drops by about 0.66 ms (4 clients) and 0.55 ms (1,024 clients).
- **B vs C (the cost of storage).** Removing the synchronous disk commit gives
  about 160× (4 clients) and 165× (1,024 clients) more throughput. B is still
  bound by bbolt's disk syncs.
- C's speed comes from **weaker durability**: its state survives server process
  crashes, but not power loss or a machine crash. It does not come from a
  better protocol.

## Failover

The owner was killed with SIGKILL 5 seconds into a 10-second run, with 4
workers and 4 clients. A standby process was already waiting on the same kind
of kernel `flock` failure detector in all three configurations. The benchmark
retried ambiguous failures using the same request IDs. 5 repetitions per
configuration.

| Config | Longest single request in each run (ms) | Requests retried | Errors | Duplicates |
|---|---|---|---|---|
| A | 9.22, 8.38, 9.19, 9.92, 9.53 | 4–7 per run | 0 | 0 |
| B | 9.51, 8.86, 8.93, 8.04, 9.26 | 0 | 0 | 0 |
| C | 3.27, 4.15, 10.14, 11.02, 10.25 | 0 | 0 | 0 |

- **A standby took over in all 15 runs.** For B and C, the standby logged that
  it was ready 2–3 ms after the kill time recorded by the script. A's logs record
  whole seconds only, but its standby took the lock in every run.
- **Clients saw this differently.** In A, the TCP connection broke, so 4–7
  requests failed and were retried with the same IDs. In B and C, the requests
  stayed in their shared-memory slots and the new owner answered them; no client
  had to retry.
- **Every configuration kept exactly-once effects** across the takeover.
- **How the longest request relates to the takeover.** The longest request is
  an upper bound on the client-visible interruption. For C, normal p99 latency
  is 0.03 ms, so a 3–11 ms request can only be the takeover. For A and B, normal
  p99 is about 3 ms, so most of their maximum is also the takeover. With 5
  samples per configuration, these numbers support "about 10 ms or less on one
  host", not finer comparisons.
- **What this shows.** Disk-free takeover (C) works and keeps the RIFL state
  without a disk restart. On one host, A and B recover just as quickly from the
  same local file. C's distinctive property only matters when the replacement
  runs on a *different* machine that can reach the shared memory but not the
  failed machine's disk. That is the CXL setting, and this model cannot test it.

## Batch acknowledgement

ACK p50 is the median of the per-repetition values in milliseconds, with the
[min–max] range. Five repetitions per point; there are only 1–4 ACK samples per
repetition, so these support comparing medians only.

| B | W | A | B | C |
|---|---|---|---|---|
| 100 | 1 | 0.941 [0.774–0.963] | 0.401 [0.361–0.536] | 0.009 [0.004–0.015] |
| 100 | 4 | 2.321 [1.559–2.656] | 1.400 [1.393–1.546] | 0.008 [0.006–0.009] |
| 1,000 | 1 | 1.344 [1.011–1.591] | 0.907 [0.791–2.071] | 0.034 [0.032–0.042] |
| 1,000 | 4 | 3.236 [2.991–4.459] | 2.796 [2.022–3.130] | 0.032 [0.026–0.036] |
| 10,000 | 1 | 8.484 [6.038–9.265] | 8.040 [4.129–8.388] | 0.280 [0.174–0.299] |
| 10,000 | 4 | 13.996 [9.431–16.323] | 8.352 [5.633–12.959] | 0.266 [0.204–0.300] |

- In C, releasing 10,000 records takes about 0.27 ms. It clears ring entries in
  memory and writes nothing to disk.
- In A and B, the cost is dominated by bbolt deleting and rewriting pages
  inside a synchronous commit.

## Claims supported by this run

- **Exactly-once effects across takeover.** Owner process crashes with standby
  takeover produced exactly-once effects in all three configurations. This
  complements the crash-point subprocess tests in `shm/`, which kill the owner
  before the commit, after it, mid-apply, and before the reply.
- **Disk-free takeover.** Keeping the RIFL state in shared memory allows
  takeover without a disk, with a client-visible interruption of about 10 ms or
  less on one host.
- **Where the cost lies.** On this machine, synchronous disk commits, not the
  transport, are the dominant cost of durable RIFL.

## Claims not supported

- CXL hardware latency or bandwidth.
- Multi-host coherence or cross-machine takeover.
- Power-loss durability for C.
- Two-machine network costs. For those, see `../composite2-20261111`, which
  used the same storage code.
