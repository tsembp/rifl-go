#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
    printf 'usage: %s SERVER_PID DATABASE_PATH OUTPUT_CSV\n' "$0" >&2
    exit 2
fi

server_pid=$1
database_path=$2
output=$3
mkdir -p "$(dirname -- "$output")"
printf 'timestamp_utc,rss_kb,cpu_percent,db_bytes,network_rx_bytes,network_tx_bytes\n' >"$output"

while kill -0 "$server_pid" 2>/dev/null; do
    rss=$(ps -o rss= -p "$server_pid" | tr -d ' ')
    cpu=$(ps -o %cpu= -p "$server_pid" | tr -d ' ')
    db_bytes=$(stat -c %s "$database_path" 2>/dev/null || printf '0')
    set -- $(awk -F '[: ]+' 'NR > 2 {rx += $3; tx += $11} END {print rx+0, tx+0}' /proc/net/dev)
    printf '%s,%s,%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "${rss:-0}" "${cpu:-0}" "$db_bytes" "$1" "$2" >>"$output"
    sleep 1
done
