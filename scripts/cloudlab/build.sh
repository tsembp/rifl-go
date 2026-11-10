#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
output_dir="$repo_dir/dist/linux-amd64"

mkdir -p "$output_dir"
cd "$repo_dir"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$output_dir/rifl-server" ./cmd/server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$output_dir/rifl-bench" ./cmd/bench
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$output_dir/rifl-benchplot" ./cmd/benchplot
printf '%s\n' "built native Linux binaries in $output_dir"
