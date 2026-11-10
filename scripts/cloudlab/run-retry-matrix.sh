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
duration=${DURATION:-15s}
repeats=${REPEATS:-5}
repeat_start=${REPEAT_START:-1}
retry_values=${RETRY_VALUES:-"0 0.1 1 5 10 100"}
warmup=${WARMUP:-1000}
concurrency=${CONCURRENCY:-4}
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

ssh "$server_host" "mkdir -p /tmp/rifl-data/$run_id \"\$HOME/rifl-results/$run_id\""
ssh "$client_host" "mkdir -p \"\$HOME/rifl-results/$run_id\""

point=0
for retry_percent in $retry_values; do
    repeat=$repeat_start
    while [ "$repeat" -le "$repeats" ]; do
        point=$((point + 1))
        if [ $((point % 2)) -eq 1 ]; then
            modes="protected baseline"
        else
            modes="baseline protected"
        fi
        for mode in $modes; do
            tag="retry-${mode}-r${retry_percent}-n${repeat}"
            db_path="/tmp/rifl-data/$run_id/$tag.db"
            server_log="\$HOME/rifl-results/$run_id/$tag.server.log"
            metrics_file="\$HOME/rifl-results/$run_id/$tag.metrics.csv"

            printf 'starting %s at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            current_pid=$(ssh "$server_host" "nohup \"\$HOME/rifl-bin/rifl-server\" -mode $mode -addr $server_ip:9000 -db $db_path >\"$server_log\" 2>&1 </dev/null & echo \$!")
            sleep 2
            ssh "$server_host" "nohup \"\$HOME/rifl-bin/collect-server-metrics.sh\" $current_pid $db_path \"$metrics_file\" >/dev/null 2>&1 </dev/null &" || true

        ssh "$client_host" "\"\$HOME/rifl-bin/rifl-bench\" \
          -addr $server_ip:9000 -implementation $mode -scenario retry-rate \
          -duration $duration -concurrency $concurrency -virtual-clients $concurrency \
          -warmup $warmup -repeats 1 -repeat-start $repeat -timeout 5s -payload-bytes $payload_bytes \
          -retry-percent $retry_percent -connection-mode persistent \
          -seed $seed -client-prefix $run_id-$tag \
          -out \"\$HOME/rifl-results/$run_id/retry-summary.csv\" \
          -latency-out \"\$HOME/rifl-results/$run_id/retry-latency.csv\" \
          -timeline-out \"\$HOME/rifl-results/$run_id/retry-timeline.csv\""

            stop_server
            printf 'finished %s at %s\n' "$tag" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            sleep 2
        done
        repeat=$((repeat + 1))
    done
done

trap - EXIT INT TERM
printf 'retry matrix complete: %s\n' "$run_id"
