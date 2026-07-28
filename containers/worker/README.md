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
podman build -t localhost/sima:latest -f containers/worker/Containerfile .
```

The worker is compiled inside the image against bookworm's glibc, so the
binary matches the runtime stage regardless of the host's glibc.

## Device access

The Vulkan loader and the Mesa ICDs (Intel and AMD) are baked into the image.
An Intel or AMD GPU is reached by passing the render node:

```
podman run --rm -i --device /dev/dri localhost/sima:latest --enumerate <format>
```

NVIDIA user-space libraries are not baked — they must match the host kernel
driver — so the host's nvidia-container-toolkit injects them at container
start through CDI:

```
podman run --rm -i --device nvidia.com/gpu=all localhost/sima:latest --enumerate <format>
```

`--enumerate` prints one JSON device per line and exits; it is the probe the
orchestrator runs to resolve a machine's device selectors. It takes the run's
format id, because that names the execution backend to enumerate and a backend
reaches only the devices its own driver stack exposes.

## What a config names

A machine of yours runs its workers in this image, and names it by tag. The
default is `localhost/sima:latest`, so a build tagged that way needs no `image`
key at all:

```toml
[host.gpubox]                             # reached at "gpubox" over ssh
workers  = 4
# image  = "localhost/sima:latest"        # default as shown
# runtime = "podman"                      # docker | podman; default docker
run_args = ["--device", "nvidia.com/gpu=all"]
```

The same keys on `[orchestrator]` put the pool in a container on this machine
instead, with no ssh hop. There the image has no default: naming one is what
asks for a container, and without it the workers are plain subprocesses.

A rented machine names its image too, but as a registry reference the provider
host can pull — `ghcr.io/alvatar/sima-worker:latest` by default — since nothing
is delivered to a machine that did not exist a minute ago.

## Delivery to a manually provisioned host

Save the image with the local machine's runtime and load it with the remote's,
over ssh:

```
podman save localhost/sima:latest | ssh <host> docker load
```

Each verb runs where its runtime lives: `podman save` on this machine writes the
image to the pipe, `docker load` on `<host>` reads it — each name is that
machine's own runtime. The same image pushes to a registry when automatic
provisioning arrives.
