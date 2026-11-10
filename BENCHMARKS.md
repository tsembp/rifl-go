# Benchmark guide

The benchmark compares two durable services under the same Go `net/rpc`, TCP,
request payload, connection policy, bbolt version, write transaction pattern,
and default synchronous commit settings:

- `protected`: `PersistentCounter`, with request IDs, saved replies,
  acknowledgements, record reclamation, and stale-request rejection.
- `baseline`: `UnprotectedCounter`, which durably applies every invocation and
  therefore repeats side effects when a logical request is retried.

The baseline is intentionally not exactly-once. Its purpose is to measure both
the overhead of protection and the duplicate effects that protection prevents.

Use `-connection-mode persistent` for throughput and latency experiments. It
keeps one `net/rpc` connection per worker and prevents TCP handshake/TIME_WAIT
costs and ephemeral-port exhaustion from dominating the service measurement.
Use `per-attempt` for failure experiments whose purpose is reconnection behavior.

## Laptop or CloudLab?

Use a laptop for development, correctness checks, and short smoke tests. Use
CloudLab for the final reported measurements. Run the client and server on two
separate, otherwise idle nodes of the same hardware type; a third controller node
can store results without joining the measured path. Put each bbolt database on
local storage rather than a shared home directory. Record the CloudLab profile,
node type, CPU, memory, storage device/filesystem, operating system, Go version,
Git revision, private-network topology, and binary checksums with the results.

CloudLab does not make a benchmark automatically valid: use the same two nodes,
swap only the server mode and database path, repeat every point, and avoid other
load during a run. Laptop results are useful but should be labelled preliminary.

The ready-to-import profile, native build/deployment scripts, environment capture,
server metrics, and exact workflow are in [cloudlab/README.md](cloudlab/README.md).
Docker is not used for final measurements because it would add configuration and
possible networking/storage effects without helping on reserved bare-metal nodes.

## Build

Build once so compilation is outside the measurements:

```sh
go build -o /tmp/rifl-server ./cmd/server
go build -o /tmp/rifl-bench ./cmd/bench
go build -o /tmp/rifl-benchplot ./cmd/benchplot
```

On the server node, choose a fresh database path for each implementation and
scenario. For example:

```sh
/tmp/rifl-server -mode protected -addr 0.0.0.0:9000 -db /tmp/protected-normal.db
```

On the client node, replace `SERVER_PRIVATE_IP`:

```sh
/tmp/rifl-bench \
  -addr SERVER_PRIVATE_IP:9000 \
  -implementation protected \
  -scenario normal \
  -requests 10000 \
  -concurrency 16 \
  -warmup 1000 \
  -repeats 5 \
  -client-prefix protected-normal-c16 \
  -out protected.csv
```

Stop that server, start `-mode baseline` with a different fresh database, and
run the identical client command with only `implementation` and `client-prefix`
changed. Append paired results to the same CSV if convenient. Never point the two
modes at the same database.

## Experiment matrix

Run both implementations with concurrency `1, 4, 16, 32, 64`, at least five
measured repeats per point, and preferably `-duration 30s` or longer after a
warmup. Keep duration, warmup, timeout, delta, payload, logical-client count,
hardware, and connection policy equal. Randomize or alternate protected/baseline
run order to reduce time-dependent machine bias. Give every measured repeat a
fresh database and server process; otherwise durable client boundaries from an
earlier repeat can change the cost of a later repeat. The CloudLab matrix scripts
use `-repeat-start` so independently launched repetitions retain distinct labels
and deterministic seed offsets in the combined CSV.

Scenarios:

- `normal`: one invocation for each logical request.
- `lost-response`: the runner receives but deliberately discards the first valid
  reply, then reconnects and retries the same request ID. This is a controlled
  ambiguity simulation, not packet loss; the real lost-response RPC test remains
  the protocol correctness check.
- `duplicate-heavy`: sends `-duplicate-copies` identical invocations per logical
  request. Four copies is the default.
- `retry-rate`: deterministically retries `-retry-percent` of logical requests
  once, using `-seed` for reproducible selection.
- `ack-gc`: normal successful traffic, labelled separately to inspect the number
  of retained completion records and database growth after acknowledgement.
- `batch-ack`: accumulates completion records behind an acknowledgement, then
  measures the request that acknowledges and deletes the batch. It is a
  protected-only, fixed-request workload with one logical client per worker.
- `blocked-gap`: never sends sequence 1, so the protected acknowledgement boundary
  stays at 1 and later completion records cannot be reclaimed safely. Use
  `-warmup 0` for this scenario.
- `restart-seed` and `restart-replay`: verify durable protection across restart.

Use a distinct database per scenario so one scenario's retained metadata does not
change another scenario's cost. The exception is the restart pair, which must use
the same intact database.

Beyond the primary concurrency sweep, run these experiment families:

- Payload bytes: `0, 100, 1024, 4096, 16384`. Padding changes wire-input size only;
  it is deliberately not part of the counter operation's semantics.
- Retry rate: `0, 0.1, 1, 5, 10, 100` percent with a fixed recorded seed.
- Active logical clients: keep worker concurrency fixed and sweep
  `-virtual-clients` to measure per-client boundary and completion metadata.
- Metadata/GC: compare `ack-gc`, `blocked-gap`, and fixed-size `batch-ack` runs.
- Recovery: restart replay and a timed outage using `-retry-transport`. Transport
  retries preserve the request ID and appear separately as `attempt_errors`.

Example matrix point:

```sh
/tmp/rifl-bench -addr SERVER_PRIVATE_IP:9000 \
  -implementation protected -scenario duplicate-heavy \
  -duplicate-copies 4 -requests 10000 -concurrency 32 \
  -warmup 1000 -repeats 5 -client-prefix protected-duplicates-c32 \
  -out protected.csv
```

## Restart experiment

Start the protected server with a fresh database and seed deterministic IDs:

```sh
/tmp/rifl-bench -addr SERVER_PRIVATE_IP:9000 \
  -implementation protected -scenario restart-seed \
  -requests 1000 -concurrency 16 -warmup 0 -repeats 5 \
  -client-prefix protected-restart -out protected-restart.csv
```

Stop and restart the server with the same database, then replay exactly those IDs:

```sh
/tmp/rifl-bench -addr SERVER_PRIVATE_IP:9000 \
  -implementation protected -scenario restart-replay \
  -requests 1000 -concurrency 16 -warmup 0 -repeats 5 \
  -client-prefix protected-restart -out protected-restart.csv
```

The protected replay should report `actual_effects=0` and
`duplicate_effects=0`. Repeat with the baseline and its own intact database; the
baseline replay demonstrates repeated side effects.

## Output and graphs

Each summary row contains throughput, p50/p90/p95/p99/p99.9/p99.99/max successful
logical-request latency, invocation and attempt-error counts, before/after values,
duplicate effects, completion/client metadata counts, and database size.
`-latency-out` preserves every logical-request sample, including failures and
batch-ack labels; `-timeline-out` writes cumulative one-second counters. A nonzero
logical `errors` column invalidates an ordinary performance point. Attempt errors
are expected only during an intentional recovery experiment.

Plot a CSV containing both implementations (or concatenate files while retaining
their headers; the plotter accepts repeated headers):

```sh
/tmp/rifl-benchplot -in combined.csv -out benchmark-results.svg
```

The SVG averages repeats for each `(scenario, concurrency, implementation)` and
plots throughput, p99 latency, and duplicate side effects. Preserve the raw CSV;
the graph is a summary, not a substitute for the measurements.

For parameter sweeps, select the numeric CSV column used on the x-axis:

```sh
/tmp/rifl-benchplot -in retry.csv -out retry.svg -x retry_percent -title "Retry-rate comparison"
/tmp/rifl-benchplot -in payload.csv -out payload.svg -x payload_bytes -title "Payload-size comparison"
/tmp/rifl-benchplot -in clients.csv -out clients.svg -x virtual_clients -title "Active logical-client comparison"
```

For the report, compare medians or confidence intervals across repeated runs,
state the selected connection policy, and avoid claiming CXL or production
performance from these TCP/bbolt experiments. Use `persistent` connections for
the throughput matrix so ephemeral-port exhaustion is not the bottleneck; keep
`per-attempt` available for reconnect and failure experiments.

## Relationship to the RIFL paper

The matrix follows the paper's useful questions—latency/throughput overhead,
scaling with concurrency and active clients, and duplicate-request behavior—but
the systems are not equivalent. This implementation has one Go server, TCP
`net/rpc`, and local synchronous bbolt commits. It has no replication, lease
manager, migration, transactions, or RAMCloud transport. Compare trends and
mechanism costs, not absolute latency numbers as if the platforms were the same.
