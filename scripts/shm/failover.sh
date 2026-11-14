#!/bin/sh
# Failover experiment on one machine: an owner and a waiting standby serve a
# duration benchmark; the owner is killed with SIGKILL partway through. The
# standby takes over through the same kernel flock detector in every config.
#
#   A  TCP net/rpc + bbolt   (cmd/server -standby-lock)
#   B  shm slots   + bbolt   (cmd/shmserver -store bbolt)
#   C  shm slots   + shm     (cmd/shmserver -store shm)
#
# usage: failover.sh A|B|C BIN_DIR OUT_DIR
# env:   DURATION (10s) KILL_AFTER (5) WORKERS (4) CLIENTS (4)
#        DATA_DIR (OUT_DIR; must be a real disk for A/B durability)
#        REGION_DIR (/dev/shm)
#        LATENCY (1; 0 skips the per-request latency file, which is several
#        hundred MB per run for C; summary.csv still has p50..max)
set -eu
[ "$#" -eq 3 ] || { echo "usage: $0 A|B|C BIN_DIR OUT_DIR" >&2; exit 2; }
config=$1 bin=$2 out=$3
duration=${DURATION:-10s} kill_after=${KILL_AFTER:-5}
workers=${WORKERS:-4} clients=${CLIENTS:-4}
latency_out=
[ "${LATENCY:-1}" = 0 ] || latency_out="-latency-out $out/latency.csv"
mkdir "$out"   # refuse to reuse an existing results directory
data=${DATA_DIR:-$out}
region=${REGION_DIR:-/dev/shm}/rifl-failover-$$
db=$data/counter-$$.db
port=$((20000 + $$ % 20000))
owner= standby=

cleanup() {
    for pid in $owner $standby; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -f "$region"
}
trap cleanup EXIT INT TERM

start() { # role
    case $config in
    A) "$bin/rifl-server" -addr "127.0.0.1:$port" -db "$db" -standby-lock "$data/lock-$$" >"$out/$1.log" 2>&1 & ;;
    B) "$bin/rifl-shmserver" -region "$region" -create -slots $((workers + 1)) -clients "$clients" -records 64 \
           -store bbolt -db "$db" >"$out/$1.log" 2>&1 & ;;
    C) "$bin/rifl-shmserver" -region "$region" -create -slots $((workers + 1)) -clients "$clients" -records 64 \
           -store shm >"$out/$1.log" 2>&1 & ;;
    *) echo "unknown config $config" >&2; exit 2 ;;
    esac
}

ready() { # wait until the owner serves
    for _ in $(seq 1 200); do
        case $config in
        A) grep -q "listening" "$out/owner.log" 2>/dev/null && return ;;
        *) grep -q "owner epoch" "$out/owner.log" 2>/dev/null && return ;;
        esac
        sleep 0.05
    done
    echo "owner not ready" >&2; exit 1
}

start owner; owner=$!
ready
start standby; standby=$!
sleep 0.5

if [ "$config" = A ]; then
    transport="-transport tcp -addr 127.0.0.1:$port -connection-mode persistent"
else
    transport="-transport shm -region $region"
fi
# shellcheck disable=SC2086
"$bin/rifl-bench" $transport -implementation protected -scenario normal \
    -duration "$duration" -concurrency "$workers" -virtual-clients "$clients" -warmup 100 \
    -repeats 1 -retry-transport -retry-delay 1ms -timeout 5s -payload-bytes 100 -seed 452 \
    -client-prefix "failover-$config-$$" -out "$out/summary.csv" \
    $latency_out -timeline-out "$out/timeline.csv" 2>"$out/bench.log" &
bench=$!
sleep "$kill_after"
date -u +%Y-%m-%dT%H:%M:%S.%NZ >"$out/kill-time.txt"
kill -KILL "$owner"; owner=
wait "$bench"
printf 'config=%s workers=%s clients=%s duration=%s kill_after=%s\n' \
    "$config" "$workers" "$clients" "$duration" "$kill_after" >"$out/settings.txt"
tail -n 1 "$out/summary.csv"
