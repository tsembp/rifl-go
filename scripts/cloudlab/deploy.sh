#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
    printf 'usage: %s SERVER_SSH CLIENT_SSH CONTROLLER_SSH\n' "$0" >&2
    exit 2
fi

server_host=$1
client_host=$2
controller_host=$3
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
binary_dir="$repo_dir/dist/linux-amd64"

"$script_dir/build.sh"
for host in "$server_host" "$client_host" "$controller_host"; do
    ssh "$host" 'mkdir -p "$HOME/rifl-bin" "$HOME/rifl-results"'
done

scp "$binary_dir/rifl-server" "$server_host:rifl-bin/"
scp "$binary_dir/rifl-bench" "$client_host:rifl-bin/"
scp "$binary_dir/rifl-benchplot" "$controller_host:rifl-bin/"
scp "$script_dir/capture-env.sh" "$script_dir/collect-server-metrics.sh" "$server_host:rifl-bin/"
scp "$script_dir/capture-env.sh" "$client_host:rifl-bin/"
scp "$script_dir/capture-env.sh" "$controller_host:rifl-bin/"
printf '%s\n' "deployed native binaries; use the private 10.10.1.0/24 experiment LAN for benchmark traffic"
