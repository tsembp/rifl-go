# Client-count comparison: old versus optimized GC

Three CloudLab d430 nodes, Ubuntu 24.04; server and client on the private LAN.
Five independent repetitions per point, 15 seconds each, four workers, 100-byte payload,
persistent TCP connections, warmup 2048, default synchronous bbolt durability.
Every repetition starts a fresh server process and database. Variant order rotates.

Baseline and original protected binaries come from commit 6a50b38. Optimized code is
the uncommitted source snapshot archived with this run. Compiler and hashes are in manifest.json.

| Clients | Baseline ops/s | Old protected ops/s | Optimized ops/s | Optimized / old | Old p99 ms | Optimized p99 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 2,041.2 | 1,967.6 | 1,645.4 | 0.84x | 2.862 | 3.385 |
| 16 | 2,038.1 | 1,842.4 | 1,885.5 | 1.02x | 3.041 | 2.907 |
| 64 | 2,036.2 | 1,493.8 | 1,726.2 | 1.16x | 3.627 | 3.160 |
| 256 | 2,032.7 | 1,192.9 | 1,722.8 | 1.44x | 4.868 | 3.176 |
| 1024 | 2,042.7 | 585.0 | 1,581.7 | 2.70x | 10.329 | 3.401 |

All 75 measurement points had zero reported logical errors, attempt errors, and duplicate effects.
Values above are medians across repetitions; min/max ranges are in comparison.csv.

The largest measured improvement is at 1,024 clients: 2.70x throughput and about
67% lower p99. At four clients the optimized layout has about 16% lower throughput
than the old layout. The change improves client-count scaling but is not a
universal speedup; the remaining overhead should be investigated separately.

Independent raw-data verification covered 1,939,089 successful logical requests.
All sample counts matched the summaries, and all 75 p99 values were recomputed
successfully from raw samples. See raw-verification.json. Raw archives are saved
locally and on controller pc705 under /tmp/gc-20261111-QtN7Qx/.

These results compare durable TCP RPC implementations. They are not CXL or failover measurements.
The warmup differs from the earlier reported runs (2048 instead of 1000), so use this paired
old/new comparison rather than attributing differences from the old report solely to GC.
The raw-data archives contain samples, timelines, server logs, and process metrics.
