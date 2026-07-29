#!/usr/bin/env bash
# Builds the sima image from the working tree and pushes it to the registry a
# rented machine pulls from, tagged `latest` and with the current commit.
#
# Run it from an interactive shell. An agent session has `/usr` mounted
# `nosuid`, so the kernel ignores the `cap_setuid` capability on `newuidmap`
# and rootless podman cannot map its subuid range.
#
# Authentication is podman's own, stored by `podman login ghcr.io`; this script
# never handles a credential. If the push is refused, the stored token lacks
# `write:packages`.
set -euo pipefail

IMAGE="${SIMA_IMAGE:-ghcr.io/alvatar/sima}"
RUNTIME="${SIMA_RUNTIME:-podman}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

commit="$(git rev-parse --short HEAD)"

"$RUNTIME" build -t "$IMAGE:latest" -t "$IMAGE:$commit" -f containers/sima/Containerfile .
"$RUNTIME" push "$IMAGE:latest"
"$RUNTIME" push "$IMAGE:$commit"

echo "published $IMAGE:latest and $IMAGE:$commit"
