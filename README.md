# tidelet

tidelet is a host-local sandbox daemon for AI agents. `tideletd` runs sandboxes as OCI containers through containerd
and runc under cgroup v2, and adds what running many agents on one machine needs:

- **Forking.** Copy a running sandbox, with its processes, memory and filesystem, in about 150 ms and about a megabyte
  of memory per copy, however large the sandbox is. The original keeps running.
- **Policies.** One pluggable policy decides what runs: it sees every sandbox's CPU, memory, swap and pressure, and
  answers with actions (admit or queue a sandbox, reshape a cgroup, reclaim or prefetch memory, checkpoint, roll back).
- **A model-API proxy.** Agents' model calls pass through the daemon, so a policy can schedule on them: it knows when
  an agent is waiting for its model and when a tool call is about to start.

It has a gRPC API with a REST gateway, a CLI (`tidelet`), and Prometheus metrics.

## Quick start

Requirements: Linux (arm64 or amd64) with cgroup v2, root, containerd 2 with runc, the CNI plugins in `/opt/cni/bin`,
Go 1.27 to build, and CRIU 4.1 for forking and checkpoints.

```sh
make build      # bin/tideletd, bin/tidelet (the CLI), bin/tidelet-init (the sandbox init)
sudo bin/tideletd --container-checkpoints &

sudo bin/tidelet run -d --name box --image docker.io/library/python:3.12-slim --cpus 1 --memory 1Gi -- sleep infinity
sudo bin/tidelet exec box -- python3 -c 'print("hi")'
sudo bin/tidelet ls
sudo bin/tidelet rm -f box
```

## Forking sandboxes

A fork is a copy of a running sandbox at one instant: the same processes, at the same instruction, with the same
memory and files, and its own ID, IP address, cgroup and writable filesystem layer. The sandbox that was forked keeps
running with its connections and running commands untouched; it is paused only while the snapshot is taken.

```sh
# a sandbox holding 256 MiB that counts once a second
sudo bin/tidelet run -d --name agent --image docker.io/library/python:3.12-slim --cpus 1 --memory 1Gi -- \
    python3 -u -c 'import itertools, time; heap = bytearray(256 << 20); [print(i, flush=True) or time.sleep(1) for i in itertools.count()]'

sudo bin/tidelet fork agent                 # one copy; prints its ID
sudo bin/tidelet fork agent -n 10           # ten more
sudo bin/tidelet fork agent --name left --name right
sudo bin/tidelet logs left                  # the count continues from where agent was
sudo bin/tidelet fork left                  # copies can be forked too
```

To make copies later, or after the original is gone, take a **snapshot** and create sandboxes from it. This is the
way to keep a warmed-up environment (dependencies installed, services started, caches loaded) and start from it:

```sh
sudo bin/tidelet snapshot create agent --name warm
sudo bin/tidelet run -d --snapshot warm --name worker1
sudo bin/tidelet run -d --snapshot warm --name worker2 --cpus 2 --memory 2Gi   # a copy may have its own limits
sudo bin/tidelet snapshot ls
sudo bin/tidelet snapshot rm warm           # once no sandbox created from it is left
```

`fork` is exactly that: a snapshot that is deleted with its last copy, plus a `run --snapshot` per copy. A sandbox
created from a snapshot is an ordinary sandbox: it passes the policy's admission, and `exec`, `logs`, `stats`, `stop`
and `rm` work on it as on any other. What it cannot have is a different image, command, environment or mounts, since
its processes are already running.

Measured on a 64-core arm64 host (AWS c7g.metal) with a `python:3.12-slim` sandbox holding 256 MiB:

| | |
|---|---|
| taking a snapshot (the sandbox is paused) | 0.5 to 0.65 s |
| creating one sandbox from a snapshot | 0.17 s |
| `fork -n 10` / `fork -n 50`, snapshot included | 1.0 s / 2.5 s |
| memory of the original | 266 MiB |
| memory of each copy when it starts | 4 to 6 MiB |
| 100 copies together | 0.3 GiB, plus 0.26 GiB for the snapshot |
| snapshot of a copy (only what it changed) | 0.3 s, 3.5 MiB |

### How it works

- **One CRIU dump, many restores.** A snapshot is a CRIU dump of the sandbox. Before it is used, the dump is rewritten
  so that the processes' private memory lives in sparse files on a tmpfs instead of in the image, and every memory
  area becomes a private mapping of its file.
- **Restores map, they do not copy.** Creating a sandbox from the snapshot is a CRIU restore that maps those files.
  Pages a sandbox only reads are the page-cache pages every other copy reads; the kernel gives a sandbox its own copy
  of a page when it first writes it. Nothing sits on the page-fault path: no daemon, no userfaultfd, and neither CRIU
  nor the kernel is patched.
- **The filesystem is layered.** The snapshot's writable layer is committed as a containerd snapshot, and each copy
  gets a fresh writable layer on top.
- **Copies of copies stay small.** A snapshot of a sandbox that itself came from a snapshot holds only the pages it
  has written since. Once those outgrow a quarter of the shared pages, the memory files are written anew.

`pkg/runtime/criuimg` is the image rewriter; `pkg/runtime/backend/containerd/snapshot.go` the rest.

### Things to know

- Snapshots are kept in memory (`--snapshot-dir`, a tmpfs) and do not survive a reboot. They do survive a daemon
  restart. Their memory is not charged to any sandbox's cgroup.
- A copy's TCP connections come back closed: the peer belongs to the original. Programs reconnect. A copy has a new
  IP address and hostname; its own ID is in `/.tidelet/ctl/id`, for a program that wants to know it was copied.
- Commands that were running under `tidelet exec` when the snapshot was taken keep running in the copies, with their
  output going to the copy's log.
- Inside a copy, memory is a private file mapping, so `MADV_DONTNEED` brings the snapshot's bytes back instead of
  zeros. glibc is handled (sandboxes see `vm.overcommit_memory = 2`, which makes it shrink heaps with `mmap`), and
  CPython and Go do not rely on it, but allocators that do, such as jemalloc and mimalloc, are not supported in copies.
  A copy's `/proc/self/maps` shows its heap as files under `/.tidelet/mem`.
- MicroVM sandboxes and sandboxes started with a terminal or an open stdin (`run -t`, `run -i`) cannot be snapshotted.

## Policies

`tideletd --policy NAME [--policy-opt key=value ...]`:

| policy | what it does |
|---|---|
| `strict` (default) | strict reservation: a sandbox is admitted only when its CPU and memory limits fit the capacity left |
| `overcommit` | threshold overcommit: a sandbox is admitted while the pool's measured CPU and memory use are both below `threshold` (0.8) |
| `agentcgroup` | AgentCgroup (arXiv 2602.09345) as a cgroup-v2 control loop: per-tool-call resource domains, priority classes and protection windows; `gate=theta` adds overcommit's admission rule |
| `tidesand` | agent-aware scheduling: a model response that carries tool calls is released when its predicted CPU and memory-bandwidth demand fits the budgets; idle sandboxes are swapped out speculatively and brought back before they are needed; a tool call that would starve the others is rolled back to a checkpoint and rerun later. Needs `--container-checkpoints` and `--llm-proxy-listen` |

Each policy's package comment (`pkg/policy/<name>/`) documents its rules and options. To write one, implement
`policy.Policy` and register it in `pkg/policy/registry`; `strict` is the minimal example.

## Running the daemon

```sh
sudo bin/tideletd --policy tidesand --container-checkpoints \
    --pool-cgroup auto --pool-cpuset 0-55 --pool-memory 112Gi --sample-interval 1s \
    --llm-proxy-listen 0.0.0.0:18800 --llm-max-defer 30m
```

Flags worth knowing (`tideletd --help` lists all):

- `--container-checkpoints`: make sandboxes forkable and checkpointable as a whole (processes with CRIU, filesystem,
  memory); `tidelet-init` runs as each sandbox's init.
- `--pool-cgroup auto --pool-cpuset C --pool-memory M`: confine the sandboxes to one cgroup with these CPUs and this
  much memory; the capacity the policy sees, and its node samples, are the pool's.
- `--llm-proxy-listen ADDR`: the model-API proxy. A sandbox sends its model calls to it (its base-URL setting), and
  the proxy forwards each to the sandbox's upstream (the label `tidelet.io/llm-upstream`, else `--llm-upstream`),
  reporting requests and responses to the policy, which may hold a response's tool calls back (`--llm-max-defer`).
- `--containerd-namespace NS`: the containerd namespace sandboxes and images live in (`moby` shares Docker's images).
- `--sandbox-swap limit`: a sandbox whose policy leaves its swap unlimited may swap at most its memory limit.
- `--sample-interval`: how often every sandbox is sampled for the policy.

## API

gRPC on a unix socket (`/run/tidelet/tidelet.sock`), defined in `api/tidelet/v1alpha1/tidelet.proto`; a REST gateway
on `127.0.0.1:7474` (`/v1alpha1/...`) and Prometheus metrics at `/metrics` serve the same API.

| | |
|---|---|
| `SandboxService` | `CreateSandbox`, `StartSandbox`, `StopSandbox`, `DeleteSandbox`, `GetSandbox`, `ListSandboxes`, `ForkSandbox`, `Exec`, `Attach`, `Logs`, `GetStats`, `Watch` |
| `SnapshotService` | `CreateSnapshot`, `GetSnapshot`, `ListSnapshots`, `DeleteSnapshot` |
| `NodeService` | `GetNode`, `GetVersion` |

`CreateSandbox` with `spec.snapshot` set creates the sandbox from a snapshot; `ForkSandbox` is the shorthand for
snapshotting a sandbox and creating copies from it.

```sh
curl -s -X POST localhost:7474/v1alpha1/sandboxes/agent:fork -d '{"count": 3}'
curl -s -X POST localhost:7474/v1alpha1/snapshots -d '{"sandbox": "agent", "name": "warm"}'
curl -s -X POST localhost:7474/v1alpha1/sandboxes -d '{"name": "worker", "spec": {"snapshot": "warm"}}'
```

`pkg/rpc/client` is a Go client.

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
```

## Development

```sh
make build test     # go test -race ./...
make vet fmt
make proto          # after editing the .proto
```

## License

Apache 2.0.
