# tidelet

`tidelet` is a host-local sandbox daemon for AI agents. It provides performance-accurate sandbox multiplexing for LLM agents.  `tideletd` runs sandboxes as OCI containers through containerd and runc under cgroup v2. It supports:

- **Forking**: copy a running sandbox, with its processes, memory and filesystem, in about 150 ms and a megabyte of
  memory per copy.
- **Policies**: one pluggable policy decides what runs, from every sandbox's CPU, memory, swap and pressure.
- **A model-API proxy**: agents' model calls pass through the daemon, so a policy can schedule on them.

It features a gRPC API with a REST gateway, a CLI (`tidelet`), a Python package, and Prometheus metrics.

## Get Started

```bash
pip install tidelet     # the daemon, the CLI and a Python client (Linux, x86_64 or aarch64)
```

```bash
tidelet daemon start --container-checkpoints      # asks for sudo; `tidelet daemon stop` ends it
tidelet run -d --name box --image docker.io/library/python:3.12-slim --cpus 1 --memory 1Gi -- sleep infinity
tidelet exec box -- python3 -c 'print("hi")'
```

The host needs containerd 2 with runc, cgroup v2, the CNI plugins in `/opt/cni/bin`, and CRIU 4.1 for forking.
[python/README.md](python/README.md) documents the Python client.

## Build

Go 1.27 on Linux (arm64 or amd64):

```sh
make build      # bin/tideletd, bin/tidelet (the CLI), bin/tidelet-init (the sandbox init)
make test
```

Running sandboxes needs root, containerd 2 with runc, cgroup v2, the CNI plugins in `/opt/cni/bin`, and for forking
and checkpoints (`--container-checkpoints`) CRIU 4.1.

## Run

```sh
tidelet daemon start --container-checkpoints
tidelet run -d --name box --image docker.io/library/python:3.12-slim --cpus 1 --memory 1Gi -- sleep infinity
tidelet exec box -- python3 -c 'print("hi")'
tidelet stats box; tidelet ls; tidelet rm -f box
tidelet daemon status                               # also: daemon logs -f, daemon restart, daemon stop
```

`tidelet daemon start` runs `tideletd` in the background with the flags it is given, under sudo, and lets your group
use the API, so the other commands need no sudo. Stopping the daemon leaves the sandboxes running; the next one picks
them up.

A daemon with a confined pool, the TideSand policy and the model-API proxy:

```sh
tidelet daemon start --policy tidesand --container-checkpoints \
    --pool-cgroup auto --pool-cpuset 0-55 --pool-memory 112Gi --sample-interval 1s \
    --llm-proxy-listen 0.0.0.0:18800 --llm-upstream https://api.anthropic.com --llm-max-defer 1m
```

Agents in sandboxes send their model calls to the proxy (`--llm-proxy-listen`), which forwards them to
`--llm-upstream`, or to the sandbox's `tidelet.io/llm-upstream` label when it has one. `--containerd-namespace moby`
shares Docker's images, and `--socket-group GROUP` lets a group besides root use the API. Use `tideletd --help` to
list all flags.

## Forking

A fork is a copy of a running sandbox at one instant: the same processes, memory and files, with its own ID, IP
address, cgroup and writable filesystem layer. The original keeps running; it is paused only while the snapshot is
taken.

```sh
# a sandbox that counts once a second
tidelet run -d --name counter --image docker.io/library/python:3.12-slim --cpus 1 --memory 1Gi -- \
    python3 -u -c 'import itertools, time; [print(i, flush=True) or time.sleep(1) for i in itertools.count()]'

tidelet fork counter --name left --name right      # two copies; counter keeps running
tidelet ls

tidelet fork counter -n 10                         # ten more; prints their IDs
tidelet logs left                                  # the count continues from where counter was
tidelet ls                                         # every sandbox: counter and its twelve copies

tidelet snapshot create counter --name counter_warm        # or keep the snapshot, and start from it later
tidelet snapshot ls
tidelet run -d --snapshot counter_warm --name worker --cpus 2 --memory 2Gi
tidelet rm -f worker; tidelet snapshot rm counter_warm    # a snapshot goes once no sandbox created from it is left
```

A snapshot is one CRIU dump, rewritten so that every restore maps the same memory files: copies share the pages they
only read, and the kernel copies a page when a sandbox first writes it. With a `python:3.12-slim` sandbox holding
256 MiB on a 64-core arm64 host (AWS c7g.metal):

| | |
|---|---|
| taking a snapshot (the sandbox is paused) | 0.5 to 0.65 s |
| creating one sandbox from a snapshot | 0.17 s |
| `fork -n 10` / `fork -n 50`, snapshot included | 1.0 s / 2.5 s |
| memory of each copy when it starts | 4 to 6 MiB |

- Snapshots live in memory (`--snapshot-dir`, a tmpfs): they survive a daemon restart, not a reboot.
- A copy has a new IP address and hostname, and its TCP connections come back closed. Its ID is in `/.tidelet/ctl/id`.
- Commands running under `tidelet exec` keep running in the copies, with their output going to the copy's log.
- A copy cannot have a different image, command, environment or mounts: its processes are already running.
- Allocators that rely on `MADV_DONTNEED` returning zeros (jemalloc, mimalloc) are not supported in copies; glibc,
  CPython and Go are.

## Policies

`tideletd --policy NAME [--policy-opt key=value ...]`:

| policy | what it does |
|---|---|
| `strict` (default) | strict reservation: a sandbox is admitted only when its CPU and memory limits fit the capacity left |
| `overcommit` | threshold overcommit: a sandbox is admitted while the pool's measured CPU and memory use are both below `threshold` (0.8) |
| `agentcgroup` | the [AgentCgroup](https://arxiv.org/abs/2602.09345): per-tool-call resource domains, priority classes and protection windows; `gate=theta` adds overcommit's admission rule |
| `tidesand` | TideSand: a model response that carries tool calls is released when its predicted CPU and memory-bandwidth demand fits the budgets, idle sandboxes are swapped out speculatively, and a tool call that would starve the others is rolled back and rerun; needs `--container-checkpoints` and `--llm-proxy-listen` |

Each policy's package comment (`pkg/policy/<name>/`) documents its rules and options. To write one, implement
`policy.Policy` and register it in `pkg/policy/registry`; `strict` is the minimal example.

## API

gRPC on a unix socket (`/run/tidelet/tidelet.sock`, `api/tidelet/v1alpha1/tidelet.proto`); a REST gateway
(`127.0.0.1:7474`, `/v1alpha1/...`) and Prometheus metrics (`/metrics`) serve the same API. `pkg/rpc/client` is a Go
client, and the pip package's `tidelet.client` a Python one.

## Layout

```
cmd/tideletd, cmd/tidelet, cmd/tidelet-init     the daemon, the CLI, the sandbox init
api/tidelet/v1alpha1                            the gRPC API (make proto regenerates the code)
pkg/manager                                     the sandbox and snapshot tables, sampler, event loop, policy actions
pkg/policy                                      the policy contract; strict, overcommit, agentcgroup, tidesand
pkg/llmproxy                                    the model-API proxy
pkg/runtime                                     containerd/runc backend, snapshots and forks, checkpoints, networking
pkg/runtime/criuimg                             rewrites CRIU images so restores share memory
pkg/host                                        capacity, pressure, the pool cgroup, the memory-traffic meter
python/                                         the pip package: the binaries, tidelet.client, tidelet.daemon
```
