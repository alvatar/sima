# sima container image

A stateless executor host. It runs `sima-worker` as its entrypoint and speaks
the framed stdio protocol over the container's stdin/stdout, so the
orchestrator drives it exactly as a local worker — `docker run --rm -i` (or
`podman run --rm -i`) is the whole invocation. The image carries no store and
no search state; every task input and output crosses the pipe.

`sima` is on the path beside the worker, because a machine rented to host a
migrated search drives it from inside this image: the far side runs `sima search`
there, and answers `sima sync-serve` and `sima follow-serve` over the hop the
migration arrived on. A machine of yours does not need it — its own `sima` runs
outside the container, on the machine itself — but one image serves both, so
there is one thing to build and one thing to publish.

`podman` and `docker` accept the same arguments here, so every single-runtime
example below runs verbatim under either — read `podman` as `docker` throughout.
Delivery names both together, one per machine.

## Build

From the workspace root:

```
podman build -t localhost/sima:latest -f containers/sima/Containerfile .
```

Both binaries are compiled inside the image against bookworm's glibc, so they
match the runtime stage regardless of the host's glibc.

**Build and publish it from an interactive shell**, not from an agent session.
An agent runs with `/usr` mounted `nosuid`, which makes the kernel ignore the
`cap_setuid` file capability on `/usr/sbin/newuidmap`, so rootless podman cannot
map its subuid range and every build fails at `newuidmap: Could not set caps`.
Substituting a single-uid namespace with `unshare -Ur` gets past that and then
fails mounting an overlay over the build context. Neither is a property of the
machine, so an interactive shell builds normally:

```
podman login ghcr.io -u <user>          # once; a token carrying write:packages
containers/sima/publish.sh              # every time the image needs rebuilding
```

`publish.sh` builds from the working tree and pushes two tags, `latest` and the
current commit, so a rented machine can be pointed at either. It handles no
credential of its own: authentication is whatever `podman login` stored, and a
refused push means that token lacks `write:packages`. `SIMA_IMAGE` and
`SIMA_RUNTIME` override the registry path and the runtime.

Rebuild whenever the image's contents change — a commit touching `crates/`,
`Cargo.lock`, or the `Containerfile` — since a rented machine runs the published
copy and nothing delivers a local build to it.

Pushing to `ghcr.io` costs nothing. What a private repository pays for is
Actions compute, which is why the workflow below is a convenience over this
path rather than the only way to publish.

## Device access

The Vulkan loader and the Mesa ICDs (Intel and AMD) are baked into the image.
An Intel or AMD GPU is reached by passing the render node:

```
podman run --rm -i --device /dev/dri localhost/sima:latest --enumerate-devices <format>
```

NVIDIA user-space libraries are not baked — they must match the host kernel
driver — so the host's nvidia-container-toolkit injects them at container
start through CDI:

```
podman run --rm -i --device nvidia.com/gpu=all localhost/sima:latest \
  --enumerate-devices <format>
```

`--enumerate-devices` prints one JSON device per line and exits; it is the probe the
orchestrator runs to resolve a machine's device selectors. It takes the search's
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
host can pull — `ghcr.io/alvatar/sima:latest` by default — since nothing is
delivered to a machine that did not exist a minute ago. That is also the image a
migration onto a rented machine expects to find `sima` in.

## Publishing

The registry copy is built and pushed by `.github/workflows/image.yml`, on a
push that touches the image or its inputs and on demand with
`gh workflow run image.yml`. It builds for `linux/amd64`, pushes to
`ghcr.io/<owner>/sima`, and then checks the image it just pushed: both binaries
present and executable, and `sima-worker --enumerate-devices stub.v1` answering from
inside it. A published image that fails that check fails the workflow.

CI publishing is preferred where it is available, because the registry copy is
what every rented machine pulls and CI builds it from the repository's own
source at a known commit. It is not the only path: an interactive shell pushes
the same image with the commands under **Build**, and does so without the
Actions minutes a private repository is billed for.

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
