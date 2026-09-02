#!/bin/sh
# Runs once per payload digest, as /bin/sh, at the payload root.
# Vulkan loader, the libraries the NVIDIA ICD dlopens, the GLSL compiler, and
# a gate proving the program opens the GPU here.
set -eu
cd "${SIMA_PAYLOAD_DIR:-$PWD}"
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
    libvulkan1 vulkan-tools libegl1 libgl1 libxext6 libx11-6 glslc >/dev/null
chmod +x bin/program
vulkaninfo --summary 2>&1 | tr -cd '\11\12\15\40-\176' | grep -E 'deviceName|driverVersion' | head -4
./bin/program render --scene scenes/demo --out out/install-check --width 64 --height 48 --samples 1
echo "install: ok"
