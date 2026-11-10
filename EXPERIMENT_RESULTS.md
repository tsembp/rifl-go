# CloudLab experiment results

These results were collected on three CloudLab `d430` bare-metal nodes running
Ubuntu 22.04 on November 1–2, 2026. The server and client communicated over the
private LAN. Performance runs used persistent TCP connections per worker, bbolt's
default synchronous durability, a 100-byte payload unless varied, and a fresh
server process and database for every independent repetition.

Each performance point has five measured repetitions. Tables report medians.
All ordinary performance rows had zero logical and transport errors.

## Concurrency

Each repetition ran for 30 seconds.

| Workers | Baseline ops/s | Protected ops/s | Protected overhead | Baseline p99 ms | Protected p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,215.0 | 1,159.1 | 4.6% | 0.989 | 1.028 |
| 4 | 2,224.7 | 2,167.3 | 2.6% | 2.758 | 2.787 |
| 16 | 2,376.9 | 2,415.9 | -1.6% | 9.152 | 9.068 |
| 32 | 2,372.8 | 2,066.2 | 12.9% | 17.779 | 20.181 |
| 64 | 2,364.6 | 1,719.5 | 27.3% | 30.944 | 47.554 |

The small reversal at 16 workers is within run-to-run system variation and is
not evidence that protection makes RPC intrinsically faster.

## Controlled retry rate

Four workers ran for 15 seconds. A selected logical request was sent twice with
the same request ID.

| Retry rate | Baseline ops/s | Protected ops/s | Baseline extra effects | Protected extra effects |
| ---: | ---: | ---: | ---: | ---: |
| 0% | 2,217.4 | 2,157.1 | 0 | 0 |
| 0.1% | 2,226.9 | 2,167.7 | 28 | 0 |
| 1% | 2,199.1 | 2,147.8 | 329 | 0 |
| 5% | 2,116.2 | 2,093.3 | 1,582 | 0 |
| 10% | 2,034.8 | 2,036.0 | 2,998 | 0 |
| 100% | 1,113.0 | 1,254.1 | 16,697 | 0 |

The protected server prevented every controlled repeated side effect. Both
implementations still paid for the additional RPC attempt.

## Payload size

Sixteen workers ran for 15 seconds.

| Payload bytes | Baseline ops/s | Protected ops/s | Baseline p99 ms | Protected p99 ms |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 2,377.5 | 2,405.6 | 9.148 | 8.985 |
| 100 | 2,377.5 | 2,417.4 | 9.451 | 8.922 |
| 1,024 | 2,367.9 | 2,430.5 | 9.948 | 8.952 |
| 4,096 | 2,376.2 | 2,413.4 | 9.120 | 9.070 |
| 16,384 | 2,351.5 | 2,335.8 | 8.992 | 8.860 |

Payloads through 16 KiB caused little change. Local durable storage and request
coordination dominate this range.

## Active logical clients

Worker concurrency stayed at four while logical clients increased. Each run
lasted 15 seconds.

| Logical clients | Baseline ops/s | Protected ops/s | Protected overhead | Retained protected completions | Protected p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 2,217.5 | 2,170.4 | 2.1% | 4 | 2.775 |
| 16 | 2,234.2 | 2,055.7 | 8.0% | 16 | 2.847 |
| 64 | 2,226.3 | 1,504.4 | 32.4% | 64 | 3.702 |
| 256 | 2,227.7 | 1,031.7 | 53.7% | 256 | 5.639 |
| 1,024 | 2,227.6 | 372.0 | 83.3% | 1,024 | 14.772 |

This is the clearest implementation bottleneck. The current acknowledgement
path scans the shared completion bucket to find records for one client, so its
cost grows with active-client metadata. A per-client completion index is the
most important future optimization.

## Acknowledgement and garbage collection

With continuous acknowledgements, protected runs retained four completion
records regardless of whether the run executed 100, 1,000, or 10,000 requests;
the database stayed at 32 KiB. When sequence 1 was deliberately omitted, the
server safely retained 100, 1,000, and 10,000 records, and database size grew to
64 KiB, 512 KiB, and 4 MiB respectively.

Batch acknowledgement reclaimed the same record counts atomically. Median
batch-ack latency was 2.241 ms for 100 records, 2.347 ms for 1,000, and 18.588 ms
for 10,000. The median maximum among four simultaneous batch acknowledgements at
10,000 records was 41.238 ms. bbolt file size remained at its high-water mark
after logical deletion, which is expected and is distinct from live metadata.

## Restart and live outage recovery

The protected server seeded 50,000 completion records, restarted with the same
database, and replayed all request IDs. Replay caused zero new mutations. The
retry-only baseline replay caused 50,000 additional mutations.

In three 30-second live-outage repetitions, the server was unavailable for two
seconds and clients retried ambiguous transport failures using the same request
IDs. Protected runs recovered from 10,780 total failed attempts with zero
duplicate effects. Baseline runs recovered from 10,389 failed attempts and
produced one duplicate effect. A timed process kill does not guarantee landing
inside the commit-before-response window, so deterministic crash hooks and the
lost-response RPC test remain the stronger correctness tests for that boundary.

## Scope

These experiments evaluate one Go `net/rpc` server with local bbolt storage.
They do not establish power-loss tolerance, replication, transactional RPC,
bounded metadata, CXL performance, or full RIFL behavior. Compare mechanisms
and trends with RIFL rather than absolute performance numbers.
