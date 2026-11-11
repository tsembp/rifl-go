# Concurrency sweep with 64 fixed logical clients

Server pc713, client pc708, controller pc705; CloudLab d430, Ubuntu 24.04.
Five independent 15-second repetitions per point; 64 logical clients; workers varied.
100-byte payload, 2048 warmup requests, persistent TCP per worker, synchronous bbolt.
Fresh server process and database per point; variant order rotates. Binaries match
the preceding client-count sweep exactly (hashes and source snapshots are included).

| Workers | Baseline ops/s | Old protected ops/s | Optimized ops/s | Optimized / old | Baseline p99 ms | Old p99 ms | Optimized p99 ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,092.4 | 915.4 | 996.3 | 1.09x | 1.071 | 1.412 | 1.252 |
| 4 | 2,033.4 | 1,509.5 | 1,753.4 | 1.16x | 2.668 | 3.629 | 3.115 |
| 16 | 2,260.6 | 1,644.7 | 1,876.1 | 1.14x | 9.195 | 12.165 | 10.727 |
| 32 | 2,249.3 | 1,589.6 | 1,868.9 | 1.18x | 18.102 | 23.652 | 21.370 |
| 64 | 2,253.0 | 1,642.3 | 1,849.2 | 1.13x | 33.353 | 45.177 | 41.080 |

All 75 points had zero reported logical errors, attempt errors, and duplicate effects.
Independent raw-data checks verified counts and recomputed p99 for 1,909,226 logical requests.
Tables show medians across repetitions; min/max ranges are in comparison.csv.

This is a closed-loop benchmark: each worker waits for its reply before sending again.
Increasing workers tests concurrent request pressure while client metadata stays fixed.
The single bbolt writer can saturate: more workers may then add queueing latency rather
than throughput. Measurements alone do not identify every component of that latency.
These are TCP/bbolt results, not CXL, power-loss, or cross-server failover measurements.

The controller SSH connection stalled between points after 25 valid measurements.
The server was confirmed idle and the next database absent. The remaining points
resumed with fresh SSH control sockets and unchanged binaries/settings; completed
points were retained. This was not an in-flight benchmark RPC failure. See manifest.json.

## Repetition variability

The following groups contain a repetition more than 20% from their median.
Those points are retained, not discarded. These are descriptive medians, not
confidence intervals; additional repetitions would help characterize variability.

- baseline, 16 workers: 1462.3–2280.4 ops/s; median 2260.6.

## Repeated point from the first sweep

Both sweeps include four workers and 64 clients, with the same binaries and settings.
This provides a cross-run reproducibility check; it is not a confidence interval.

| Variant | First sweep ops/s | This sweep ops/s | Difference |
|---|---:|---:|---:|
| baseline | 2,036.2 | 2,033.4 | -0.1% |
| original | 1,493.8 | 1,509.5 | +1.1% |
| optimized | 1,726.2 | 1,753.4 | +1.6% |
