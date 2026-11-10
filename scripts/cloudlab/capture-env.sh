#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    printf 'usage: %s OUTPUT_FILE\n' "$0" >&2
    exit 2
fi

output=$1
mkdir -p "$(dirname -- "$output")"
{
    printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'hostname=%s\n' "$(hostname)"
    printf 'kernel=%s\n' "$(uname -srmo)"
    printf '\n[lscpu]\n'
    lscpu
    printf '\n[memory]\n'
    free -h
    printf '\n[block-devices]\n'
    lsblk -o NAME,MODEL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS
    printf '\n[filesystems]\n'
    df -hT
    printf '\n[network]\n'
    ip -brief address
    printf '\n[binary-checksums]\n'
    sha256sum "$HOME"/rifl-bin/rifl-* 2>/dev/null || true
} >"$output"
printf '%s\n' "wrote $output"
