# tidelet

tidelet is a host-local sandbox daemon for AI agents, and the system the TideSand paper's scheduling policy runs in.
`tideletd` runs sandboxes as OCI containers through containerd and runc under cgroup v2, samples every sandbox's CPU,
memory, swap and pressure, meters the host's memory traffic, and runs a pass-through proxy for the agents' model API.
A **policy** decides what runs: it sees resource samples, the proxy's model-request and model-response events, and
sandbox lifecycle events, and answers with actions (admit or queue a sandbox, hold or release a model response,
reshape a cgroup, reclaim or prefetch memory, checkpoint, roll back, restore).

**To reproduce the paper's results, follow [tidebox/README.md](https://github.com/TideSand/tidebox)**: TideBox,
the replay harness, is cloned into `tidebox/` inside this checkout and drives tidelet through every experiment.

## Policies

`tideletd --policy NAME [--policy-opt key=value ...]`:

| policy | what it does |
|---|---|
| `strict` | strict reservation: a sandbox is admitted only when its CPU and memory limits fit the capacity left |
| `overcommit` | threshold overcommit: a sandbox is admitted while the pool's measured CPU and memory use are both below `threshold` (0.8) |
| `agentcgroup` | the AgentCgroup baseline (arXiv 2602.09345) as its cgroup-v2 control loop: per-tool-call resource domains, priority classes and protection windows; `gate=theta` adds overcommit's admission rule |
| `tidesand` | TideSand: agent-aware scheduling (a model response that carries tool calls is released when its predicted CPU and memory-bandwidth demand fits the budgets, alpha `target` 0.7 of the cores and beta `bwTarget` 0.8 of 250 GB/s), speculative swapping of idle sandboxes, and selective rollback with whole-container checkpoints (rollback threshold `pressure` 0.85); needs `--container-checkpoints` and CRIU |

Each policy's package comment (`pkg/policy/<name>/`) documents its rules and options; the defaults are the paper's
configuration. `mechanisms=defer|speculate|rollback` runs TideSand with one mechanism.

## Build

Go 1.27 on Linux (arm64 or amd64):

```sh
make build      # bin/tideletd, bin/tidelet (the CLI), bin/tidelet-init (a checkpointable sandbox's init)
make test
```

Running sandboxes needs root, containerd 2 with runc, cgroup v2, the CNI plugins in `/opt/cni/bin`, and for
`--container-checkpoints` CRIU 4.1 (`tidebox/scripts/setup.sh prepare` installs all of it).

## Run

```sh
sudo bin/tideletd --policy tidesand --container-checkpoints --containerd-namespace moby \
    --pool-cgroup auto --pool-cpuset 0-55 --pool-memory 112Gi --sample-interval 1s \
    --llm-proxy-listen 0.0.0.0:18800 --llm-max-defer 30m
sudo docker pull python:3.12     # images come from containerd's moby namespace, Docker's own image store
sudo bin/tidelet run -d --name box1 --image docker.io/library/python:3.12 --cpus 2 --memory 4Gi -- sleep infinity
sudo bin/tidelet exec -i box1 -- python3 -c 'print("hi")'
sudo bin/tidelet stats box1; sudo bin/tidelet node; sudo bin/tidelet rm -f box1
```

Flags worth knowing (`tideletd --help` lists all):

- `--pool-cgroup auto --pool-cpuset C --pool-memory M`: confine the sandboxes to one cgroup with these CPUs and this
  much memory; the capacity the policy sees, and its node samples, are the pool's.
- `--llm-proxy-listen ADDR`: the model-API proxy. A sandbox sends its model calls to it (its base-URL setting), and
  the proxy forwards each to the sandbox's upstream (the label `tidelet.io/llm-upstream`, else `--llm-upstream`),
  reporting requests and responses to the policy, which may hold a response's tool calls back (`--llm-max-defer`).
- `--container-checkpoints`: make sandboxes checkpointable as a whole (processes with CRIU, the writable layer of the
  filesystem); `tidelet-init` runs as each sandbox's init.
- `--sandbox-swap limit`: a sandbox whose policy leaves its swap unlimited may swap at most its memory limit.
- `--sample-interval`: how often every sandbox is sampled for the policy (the policies want 1 s).

A REST gateway (`127.0.0.1:7474`, `/v1alpha1/...`) and Prometheus metrics (`/metrics`) serve the same API as the gRPC
socket.

## Layout

```
cmd/tideletd, cmd/tidelet, cmd/tidelet-init     the daemon, the CLI, the sandbox init for checkpoints
api/tidelet/v1alpha1                            the gRPC API (make proto regenerates the code)
pkg/manager                                     the sandbox table, sampler, event loop and the policy's actions
pkg/policy                                      the policy contract; strict, overcommit, agentcgroup, tidesand; meter
pkg/llmproxy                                    the model-API proxy
pkg/runtime                                     containerd/runc backend, checkpoints, swap-in, CNI networking
pkg/host                                        capacity, pressure, the pool cgroup, the memory-traffic meter
```

## License

Apache 2.0.
