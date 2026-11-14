# bbolt on a memory-backed filesystem: configurations A and B

Run `shm-tmpfs-20261114`, 14 November 2026. Same CloudLab `d430` node
(pc755) and allocation as `../shm-20261113` and `../shm-failover-20261113`.
Go 1.27.0 static builds; hashes are in `build/build.json` and the exact source
is in `build/source.tar.gz`.

## Why this run

Our instructor suggested that, for the RIFL-inspired implementation in a CXL
environment, a filesystem that stores its data in CXL memory would let us use
bbolt as it is. This run is a stand-in for that idea: the **unmodified** A and B
servers, with the bbolt file on `/dev/shm` (tmpfs, a filesystem in RAM) instead
of the local disk.

- **Nothing in bbolt changed.** It still calls `fdatasync` on every commit (no
  `NoSync`). On tmpfs that call returns almost immediately, because there is no
  device to flush.
- **C is not repeated.** It does not use bbolt; its numbers come from
  `../shm-20261113`.

**What tmpfs is not:**

- It is local DRAM, not CXL memory. CXL-attached memory is slower than local
  DRAM.
- A real CXL-backed filesystem would also need cache flushes for persistence and
  coordination between machines.
- The data does not survive power loss or a machine crash, as for C.

So these numbers are an optimistic bound on what "bbolt on memory" could give,
not a CXL measurement.

Same workloads as the disk runs:

- **Throughput:** 4 workers, 15-second runs, 2,048 warmup requests, 100-byte
  payload, 5 repetitions.
- **Batch acknowledgement:** B = 100, 1,000 and 10,000 records per client,
  W = 1 or 4 workers, 5 repetitions.
- **Failover:** 20 repetitions, SIGKILL after 5 seconds of a 10-second run.

All 122 points passed validation: zero errors and zero duplicate effects,
expected completion counts, and `-verify-batch-ack` on every batch point. No
points were discarded.

## Throughput

Median ops/s with the [min–max] range over 5 repetitions. The disk column is
`../shm-20261113`, which used the same node and settings.

| Clients | Config | bbolt on disk | bbolt on tmpfs | Speed-up | p50 / p99 on tmpfs |
|---|---|---|---|---|---|
| 4 | A | 1,803.3 | 7,757.9 [7,692.5–7,787.3] | 4.3× | 0.516 / 1.533 ms |
| 4 | B | 2,319.0 | 20,299.1 [19,911.8–21,394.9] | 8.8× | 0.163 / 0.719 ms |
| 1,024 | A | 1,638.2 | 5,552.6 [5,471.7–5,583.8] | 3.4× | 0.711 / 1.931 ms |
| 1,024 | B | 1,962.3 | 10,661.9 [10,560.2–10,813.1] | 5.4× | 0.314 / 1.241 ms |

- **Unmodified bbolt gets 3.4–8.8× faster** once its file is in memory. The
  synchronous disk flush was the dominant cost on disk.
- **The transport now matters more.** With the disk gone, B (shared-memory
  slots) is 2.6× A (loopback TCP) at 4 clients and 1.9× at 1,024. On disk the
  gap was 1.2–1.3×.
- **C is still well ahead:** 375,175 ops/s at 4 clients and 325,482 at 1,024,
  which is 18× and 31× B on tmpfs. What remains in B is bbolt itself:
  copy-on-write B+tree pages, JSON-encoded completion values and the commit
  path. Keeping RIFL state in purpose-built shared-memory structures is
  therefore much cheaper than running a general key-value store on memory.

## Batch acknowledgement

ACK p50 in milliseconds: median of the per-repetition values, with the [min–max]
range. B is the number of records released per client, W the number of workers.

| B | W | A disk | A tmpfs | B disk | B tmpfs |
|---|---|---|---|---|---|
| 100 | 1 | 0.941 | 0.365 [0.202–0.540] | 0.401 | 0.125 [0.101–0.157] |
| 100 | 4 | 2.321 | 0.354 [0.304–0.695] | 1.400 | 0.317 [0.203–0.433] |
| 1,000 | 1 | 1.344 | 1.006 [0.958–2.093] | 0.907 | 0.686 [0.553–1.798] |
| 1,000 | 4 | 3.236 | 1.089 [0.982–1.407] | 2.796 | 2.639 [1.917–3.563] |
| 10,000 | 1 | 8.484 | 8.181 [7.321–8.772] | 8.040 | 8.113 [7.108–9.110] |
| 10,000 | 4 | 13.996 | 8.727 [7.969–9.399] | 8.352 | 7.581 [6.582–8.664] |

- Small batches become much cheaper, because their cost was mostly the flush.
- Releasing 10,000 records still takes about 7.5–8.7 ms on tmpfs, close to the
  disk numbers. That cost is bbolt's CPU work deleting and rewriting B+tree
  pages, not I/O. C releases 10,000 records in about 0.27 ms.
- With only 1–4 ACK samples per repetition, these support comparing medians
  only.

## Failover

20 repetitions per configuration. Medians with the [min–max] range, in
milliseconds from the recorded kill time. The disk column is
`../shm-failover-20261113`.

| Config | Kill → lock | Lock → serving (tmpfs) | Lock → serving (disk) | Kill → serving (tmpfs) | Kill → serving (disk) | Longest request (tmpfs / disk) | Retries per run |
|---|---|---|---|---|---|---|---|
| A | 2.75 [2.36–3.09] | 0.78 [0.63–0.94] | 1.15 | 3.52 [3.09–3.94] | 3.69 | 5.75 / 9.35 | 4 |
| B | 2.19 [1.68–3.23] | 0.32 [0.18–0.55] | 0.60 | 2.49 [2.01–3.52] | 2.61 | 2.80 / 8.89 | 0 |

- **Takeover time barely changes.** Most of it is the failure detector: the
  kernel releasing the dead owner's `flock`. Opening bbolt from tmpfs is about
  0.3–0.4 ms faster than from disk.
- **The longest request drops sharply:** from 8.9–9.4 ms on disk to 2.8–5.8 ms.
  This supports the finding in `../shm-failover-20261113` that the slowest
  requests on disk were disk-sync stalls, not the takeover.
- **B still needs no client retries.** A's clients still see a broken TCP
  connection and retry 4 requests per run with the same IDs.
- **Exactly-once effects held** in all 40 takeovers.

## Claims supported by this run

- **The instructor's suggestion works for our implementation without code
  changes.** The bbolt-based RIFL server (A or B) runs unchanged on a
  memory-backed filesystem, keeps exactly-once effects across standby takeover,
  and gains 3.4–8.8× throughput because the disk flush disappears.
- **Remaining cost is the storage engine, not the medium.** Even on memory,
  bbolt-based B is 18–31× slower than C's purpose-built shared-memory state.

## Claims not supported

- **CXL hardware.** CXL memory latency, bandwidth and persistence were not
  measured; tmpfs is local DRAM and is optimistic.
- **A CXL-backed filesystem shared across machines.** Neither the filesystem nor
  takeover from another host was tested. That would also need a lease or
  fencing mechanism instead of a local `flock`.
- **Durability.** A tmpfs database does not survive power loss or a machine
  crash.
