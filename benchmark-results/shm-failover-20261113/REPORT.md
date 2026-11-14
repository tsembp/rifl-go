# Failover timing: configurations A, B and C (20 repetitions)

Run `shm-failover-20261113`, 13 November 2026. Same CloudLab `d430` node
(pc755) and allocation as `../shm-20261113`, Go 1.27.0 static builds (hashes in
`build/build.json`, exact source in `build/source.tar.gz`). bbolt files on the
local disk (`/var/tmp`), regions in `/dev/shm`.

This follow-up adds two things the first run lacked:

- **Takeover timestamps.** `cmd/server` now logs microsecond UTC timestamps,
  like `cmd/shmserver`, so A's takeover can be timed. The first run's A logs
  had whole seconds only.
- **20 repetitions** per configuration instead of 5.

Each run is `scripts/shm/failover.sh`: an owner and a standby blocked on the
same kernel `flock`, a 10-second benchmark with 4 workers and 4 clients, and
SIGKILL of the owner after 5 seconds. The benchmark retries ambiguous failures
with the same request IDs (`-retry-transport -retry-delay 1ms`). Configuration
order rotated between repetitions. Per-request latency files were not written
(`LATENCY=0`); each summary still has p50 to max.

All 60 runs passed: zero errors and zero duplicate effects, and the standby took
over in every run. Raw logs are in `results.tar.gz`; per-run values are in
`failover-takeover.csv`.

## Takeover time

Medians over 20 runs, with the [min–max] range, in milliseconds from the
recorded kill time:

| Config | Kill → standby holds lock | Lock → serving | Kill → serving | Requests retried per run |
|---|---|---|---|---|
| A (TCP + bbolt) | 2.50 [1.90–3.10] | 1.15 [0.92–1.95] | 3.69 [2.85–4.44] | 4 [4–8] |
| B (shm + bbolt) | 2.00 [1.40–3.18] | 0.60 [0.45–0.88] | 2.61 [1.94–4.07] | 0 |
| C (shm state) | 2.06 [1.54–3.50] | 0.07 [0.01–0.13] | 2.11 [1.55–3.58] | 0 |

- **Kill → lock** is the failure detector: the kernel releasing the dead
  owner's `flock`. It is the same mechanism in all three configurations, so it
  is the same order of magnitude in each. The kill time is written by `date`
  just before `kill` runs, so this column also includes some script overhead.
- **Lock → serving** is the cost that differs:
  - **A** opens the bbolt file and starts a TCP listener (1.15 ms).
  - **B** opens the same bbolt file behind the shared slots (0.60 ms).
  - **C** only checks the one-entry redo log in the region (0.07 ms) — no disk
    access.
- **Clients:** in A, the TCP connection broke, so 4–8 requests per run failed
  and were retried with the same IDs after reconnecting. In B and C the requests
  stayed in their shared-memory slots, and the new owner answered them without
  any client retry.

## The longest request is not a takeover measurement

The first run (`../shm-20261113`) used each run's longest request as an upper
bound on the takeover and argued that, for C, a 3–11 ms request "can only be
the takeover". That inference was wrong. In that run's per-request samples, the
slowest requests are spread across the whole run, not clustered at the kill:

- **C:** in 3 of the 5 runs, the slowest request completed well before or well
  after the kill (about 29%, 36% and 86% of the way through a run whose kill is
  at about 50%). C has occasional 1–11 ms stalls throughout. These are rare
  enough to sit above its 0.03 ms p99.
- **A and B:** the 8–10 ms maxima also fall at arbitrary points, consistent with
  periodic disk-sync stalls.

This run shows the same picture with more data: the median longest request is
9.35 ms (A), 8.89 ms (B) and 11.55 ms (C), while the standby was serving
2.1–3.7 ms after the kill. The longest request is still a valid upper bound on
what any client waited, but it is dominated by stalls unrelated to the takeover.
The kill → serving column is the takeover measurement.

## What this shows

- **Takeover on one host is a few milliseconds in all three configurations**,
  and the shared kernel failure detector accounts for most of it.
- **Disk-free recovery is the measurable difference.** C's standby is ready
  about 0.07 ms after it gets the lock, against 0.60 ms (B) and 1.15 ms (A) for
  opening bbolt. On one host that difference is small next to detection.
- **Shared-memory slots hide the takeover from clients:** no retries in B and
  C, against 4–8 per run in A.

## Not shown

- Takeover by a process on a different machine (the CXL case), or with a
  failure detector other than a local `flock`.
- CXL hardware timing, or power-loss durability for C.
