#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
    printf 'usage: %s SERVER_SSH CLIENT_SSH RUN_ID\n' "$0" >&2
    exit 2
fi

server_host=$1
client_host=$2
run_id=$3
server_ip=${SERVER_IP:-10.10.1.1}
requests=${REQUESTS:-10000}
concurrency=${CONCURRENCY:-16}
payload_bytes=${PAYLOAD_BYTES:-100}
seed=${SEED:-452}
current_pid=

stop_server() {
    if [ -n "$current_pid" ]; then
        ssh "$server_host" "kill $current_pid 2>/dev/null || true" || true
        current_pid=
    fi
}
trap stop_server EXIT INT TERM

start_server() {
    mode=$1
    db_path=$2
    server_log=$3
    metrics_file=$4
    current_pid=$(ssh "$server_host" "nohup \"\$HOME/rifl-bin/rifl-server\" -mode $mode -addr $server_ip:9000 -db $db_path >\"$server_log\" 2>&1 </dev/null & echo \$!")
    sleep 2
    ssh "$server_host" "nohup \"\$HOME/rifl-bin/collect-server-metrics.sh\" $current_pid $db_path \"$metrics_file\" >/dev/null 2>&1 </dev/null &" || true
}

ssh "$server_host" "mkdir -p /tmp/rifl-data/$run_id \"\$HOME/rifl-results/$run_id\""
ssh "$client_host" "mkdir -p \"\$HOME/rifl-results/$run_id\""

for mode in protected baseline; do
    tag="restart-$mode"
    db_path="/tmp/rifl-data/$run_id/$tag.db"
    prefix="$run_id-$tag"

    printf 'starting %s seed at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_server "$mode" "$db_path" "\$HOME/rifl-results/$run_id/$tag-seed.server.log" "\$HOME/rifl-results/$run_id/$tag-seed.metrics.csv"
    ssh "$client_host" "\"\$HOME/rifl-bin/rifl-bench\" \
      -addr $server_ip:9000 -implementation $mode -scenario restart-seed \
      -requests $requests -concurrency $concurrency -virtual-clients $concurrency \
      -warmup 0 -repeats 5 -timeout 5s -payload-bytes $payload_bytes \
      -connection-mode persistent -seed $seed -client-prefix $prefix \
      -out \"\$HOME/rifl-results/$run_id/restart-summary.csv\""
    stop_server

    printf 'starting %s replay after restart at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_server "$mode" "$db_path" "\$HOME/rifl-results/$run_id/$tag-replay.server.log" "\$HOME/rifl-results/$run_id/$tag-replay.metrics.csv"
    ssh "$client_host" "\"\$HOME/rifl-bin/rifl-bench\" \
      -addr $server_ip:9000 -implementation $mode -scenario restart-replay \
      -requests $requests -concurrency $concurrency -virtual-clients $concurrency \
      -warmup 0 -repeats 5 -timeout 5s -payload-bytes $payload_bytes \
      -connection-mode persistent -seed $seed -client-prefix $prefix \
      -out \"\$HOME/rifl-results/$run_id/restart-summary.csv\""
    stop_server
done

point=0
for repeat in 1 2 3; do
    point=$((point + 1))
    if [ $((point % 2)) -eq 1 ]; then modes="protected baseline"; else modes="baseline protected"; fi
    for mode in $modes; do
        tag="outage-${mode}-n${repeat}"
        db_path="/tmp/rifl-data/$run_id/$tag.db"
        printf 'starting %s at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        start_server "$mode" "$db_path" "\$HOME/rifl-results/$run_id/$tag-before.server.log" "\$HOME/rifl-results/$run_id/$tag-before.metrics.csv"

        ssh "$client_host" "\"\$HOME/rifl-bin/rifl-bench\" \
          -addr $server_ip:9000 -implementation $mode -scenario normal \
          -duration 30s -concurrency $concurrency -virtual-clients $concurrency \
          -warmup 1000 -repeats 1 -repeat-start $repeat -timeout 1s \
          -retry-transport -retry-delay 25ms -payload-bytes $payload_bytes \
          -connection-mode persistent -seed $seed -client-prefix $run_id-$tag \
          -out \"\$HOME/rifl-results/$run_id/outage-summary.csv\" \
          -timeline-out \"\$HOME/rifl-results/$run_id/outage-timeline.csv\"" &
        bench_pid=$!
        sleep 10
        stop_server
        sleep 2
        start_server "$mode" "$db_path" "\$HOME/rifl-results/$run_id/$tag-after.server.log" "\$HOME/rifl-results/$run_id/$tag-after.metrics.csv"
        wait "$bench_pid"
        stop_server
        printf 'finished %s at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        sleep 2
    done
done

trap - EXIT INT TERM
printf 'recovery matrix complete: %s\n' "$run_id"
