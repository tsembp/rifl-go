# CloudLab experiment deployment

Use native static Go binaries on bare metal. Docker is intentionally not in the
measurement path: it adds another network namespace, filesystem layer, resource
configuration, and software version without helping this two-process experiment.

## 1. Instantiate the profile

Create a CloudLab profile from `cloudlab/profile.py`. It requests three identical
bare-metal nodes on a private experiment LAN:

- `server` (`10.10.1.1`): RPC service and local bbolt database.
- `client` (`10.10.1.2`): benchmark load generator.
- `controller` (`10.10.1.3`): results and plotting, kept off the measured path.

The default `m510` type is a reasonable x86 starting point, not a requirement.
If unavailable, choose another type, but pin and record one type for the entire
comparison. Also pin the disk image/profile version before collecting final data.

## 2. Build and deploy

From the repository on the laptop, use the SSH names shown by CloudLab:

```sh
scripts/cloudlab/deploy.sh \
  USER@SERVER_CONTROL_HOST \
  USER@CLIENT_CONTROL_HOST \
  USER@CONTROLLER_CONTROL_HOST
```

The script builds `CGO_ENABLED=0` Linux/amd64 binaries and copies only the needed
programs. SSH/SCP uses the control network; benchmark `-addr` must use
`10.10.1.1:9000`, the isolated experiment network.

Capture each node before the first run:

```sh
~/rifl-bin/capture-env.sh ~/rifl-results/environment.txt
```

Copy the repository Git revision and CloudLab experiment/profile identifiers into
the controller's experiment notes as well; the deployed binary has no `.git`
directory from which to recover them.

## 3. Run one clean measurement point

On `server`, use a unique database name for every point and run it on local disk:

```sh
mkdir -p /tmp/rifl-data ~/rifl-results
~/rifl-bin/rifl-server \
  -mode protected -addr 10.10.1.1:9000 \
  -db /tmp/rifl-data/protected-normal-c16-r1.db \
  >~/rifl-results/protected-normal-c16-r1.server.log 2>&1 &
server_pid=$!
~/rifl-bin/collect-server-metrics.sh "$server_pid" \
  /tmp/rifl-data/protected-normal-c16-r1.db \
  ~/rifl-results/protected-normal-c16-r1.metrics.csv &
```

On `client`, run a duration-based point and preserve aggregate, raw-latency, and
timeline files:

```sh
~/rifl-bin/rifl-bench \
  -addr 10.10.1.1:9000 -implementation protected -scenario normal \
  -duration 30s -concurrency 16 -virtual-clients 16 -warmup 1000 \
  -repeats 5 -timeout 5s -seed 452 \
  -client-prefix protected-normal-c16 \
  -out ~/rifl-results/summary.csv \
  -latency-out ~/rifl-results/latency.csv \
  -timeline-out ~/rifl-results/timeline.csv
```

Stop the exact recorded server PID after the point. Start the baseline with a new
database and change only `-mode`, `-implementation`, and the client prefix. Do not
reuse a database across modes or unrelated points. Copy all result directories to
the controller and then to the laptop before the CloudLab experiment expires.

## 4. Required experiment families

Run at least five repeats after a warmup, in randomized protected/baseline order:

1. Normal throughput and latency versus concurrency `1, 4, 16, 32, 64`.
2. Controlled retry rates `0, 0.1, 1, 5, 10, 100` percent using `retry-rate`.
3. Payload sizes `0, 100, 1024, 4096, 16384` bytes.
4. Logical-client scale with fixed concurrency and increasing `-virtual-clients`.
5. Completion growth (`blocked-gap`) and batched reclamation (`batch-ack`).
6. Restart replay using the same intact database, plus a live outage run with
   `-retry-transport` while the server is stopped and restarted.

Use `BENCHMARKS.md` for scenario semantics, controls, validity rules, and expected
interpretation. Never present these results as replication, CXL, power-loss, or
generic transactional-RPC measurements.
