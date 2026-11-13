# Benchmark results

These runs compare the retry-only baseline, the original protected server from
commit `6a50b38`, and the uncommitted per-client GC optimization. They use the same
CloudLab d430 allocation and binaries, Ubuntu 24.04, private-network TCP RPC, and
bbolt's normal synchronous durability. Each point has five repetitions, a fresh
database/server process, 100-byte payload, persistent worker connections, and
2,048 warmup requests. Measurements last 15 seconds per repetition.

| Experiment | Varied | Held fixed | Report |
|---|---|---|---|
| Client-count sweep | 4, 16, 64, 256, 1,024 logical clients | 4 workers | [Results](gc-20261111-QtN7Qx/comparison.md) |
| Concurrency sweep | 1, 4, 16, 32, 64 workers | 64 logical clients | [Results](concurrency-20261111-qwUdyA/comparison.md) |

Follow-up runs. These use the same machine type and settings, but a new
allocation and the runner in `scripts/cloudlab/gc_followup.py`:

| Experiment | Compared | Report |
|---|---|---|
| Batch GC and four-client diagnosis | original vs v2 per-client | [Report](gcfollowup-20261111/REPORT.md) |
| First v3 candidate (superseded: quadratic batch GC) | original, v2, v3 | [Note](composite-20261111/NOTE.md) |
| v3 flat composite keys | original, v2, v3 | [Report](composite2-20261111/REPORT.md) |

Follow-up folders keep `build/build.json` (binary hashes), the working-tree patch
and source archive, but not the compiled binaries.

Each run directory contains:

- `comparison.md` and `comparison.csv`: medians and per-point min/max ranges.
- `*-summary.csv`: the individual measured repetitions.
- `client-results.tar.gz`: raw latency samples and timelines (CSV), plus summaries.
- `server-results.tar.gz`: server logs and process/resource samples (CSV).
- `raw-verification.json`: sample-count checks and p99 recalculation from raw data.
- `manifest.json`, environment captures, source snapshots and scripts: provenance.

Inspect archive contents with `tar -tzf client-results.tar.gz` or extract into a
separate directory. No database files or compiled binaries are included.
The original source archive is a plain `.tar`; the optimized snapshot is `.tar.gz`.
Run scripts record the specific node addresses and paths for this allocation;
adjust those before reusing them elsewhere.

The first sweep exposes a trade-off: per-client GC improves throughput at high
client counts but regresses at four clients. The follow-up traced this to one
extra page written per commit, and the v3 layout removes it. These results are not CXL or
cross-server failover measurements. The older Ubuntu 22.04 measurements in the
root `EXPERIMENT_RESULTS.md` remain historical results and should not be treated
as a paired comparison with these runs.
