# sima-worker container image

A stateless executor host. It runs `sima-worker` as its entrypoint and speaks
the framed stdio protocol over the container's stdin/stdout, so the
orchestrator drives it exactly as a local worker — `docker run --rm -i` (or
`podman run --rm -i`) is the whole invocation. The image carries no store and
no run state; every task input and output crosses the pipe.

`podman` and `docker` accept the same arguments here, so every single-runtime
example below runs verbatim under either — read `podman` as `docker` throughout.
Delivery names both together, one per machine.

## Build

From the workspace root:

```
podman build -t sima-worker -f containers/worker/Containerfile .
```

The worker is compiled inside the image against bookworm's glibc, so the
binary matches the runtime stage regardless of the host's glibc.

## Device access

The Vulkan loader and the Mesa ICDs (Intel and AMD) are baked into the image.
An Intel or AMD GPU is reached by passing the render node:

```
podman run --rm -i --device /dev/dri sima-worker --enumerate
```

NVIDIA user-space libraries are not baked — they must match the host kernel
driver — so the host's nvidia-container-toolkit injects them at container
start through CDI:

```
podman run --rm -i --device nvidia.com/gpu=all sima-worker --enumerate
```

`--enumerate` prints one JSON device per line and exits; it is the probe the
orchestrator runs to resolve a remote's device selectors.

## Delivery to a manually provisioned host

Save the image with the local machine's runtime and load it with the remote's,
over ssh:

```
podman save sima-worker | ssh <host> docker load
```

Each verb runs where its runtime lives: `podman save` on this machine writes the
image to the pipe, `docker load` on `<host>` reads it — each name is that
machine's own runtime. The same image pushes to a registry when automatic
provisioning arrives.
