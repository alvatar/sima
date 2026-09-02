#!/usr/bin/env bash
# Builds the static sima bootstrap artifact and places it beside release sima.
set -euo pipefail

if (( $# != 0 )); then
    echo "usage: scripts/build-sima-static.sh" >&2
    exit 2
fi

if ! command -v x86_64-linux-musl-gcc >/dev/null 2>&1; then
    echo "x86_64-linux-musl-gcc is required; install musl on Arch or musl-tools on Debian" >&2
    exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

target="x86_64-unknown-linux-musl"
if ! rustup target list --installed | grep -Fxq "$target"; then
    rustup target add "$target"
fi

cargo build --release --target "$target" -p sima

built="target/$target/release/sima"
ldd_output="$(ldd "$built" 2>&1)" && ldd_status=0 || ldd_status=$?
# ldd distinguishes static executables from static PIEs by message and status.
if ! { (( ldd_status != 0 )) && [[ "$ldd_output" == *"not a dynamic executable"* ]]; } \
    && ! { (( ldd_status == 0 )) && [[ "$ldd_output" == *"statically linked"* ]]; }; then
    echo "expected $built to be static; ldd reported: $ldd_output" >&2
    exit 1
fi

placed="target/release/sima-static"
mkdir -p "$(dirname "$placed")"
cp "$built" "$placed"
echo "$placed"
