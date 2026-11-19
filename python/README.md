# tidelet (Python)

tidelet is a sandbox daemon for AI agents: containers you can run commands in and **fork** in about 150 ms and
a megabyte of memory per copy. This package bundles the daemon and gives it a Python interface.

```sh
pip install tidelet
```

It does three things:

- **Installs the binaries.** `tideletd` (the daemon) and `tidelet` (its command line) are on PATH after the
  install; `tidelet.binary("tideletd")` gives a binary's path.
- **`tidelet.client`** talks to a running daemon.
- **`tidelet.daemon`** runs a daemon from Python.

The daemon runs on Linux (x86_64 or aarch64) as root and needs containerd 2 with runc, the CNI plugins in
`/opt/cni/bin`, and CRIU 4.1 for forking. The client alone has no such needs.

## Client

```python
from tidelet.client import Client

client = Client()                       # /run/tidelet/tidelet.sock, or $TIDELET_SOCKET, or Client("tcp://host:port")

box = client.run("docker.io/library/python:3.12-slim", ["sleep", "infinity"],
                 name="agent", cpus=1, memory="1Gi")
result = box.exec(["python3", "-c", "print(6 * 7)"])
print(result.exit_code, result.stdout)                      # 0 42
box.exec(["pip", "install", "requests"], check=True, timeout=300)

# Fork: running copies of the sandbox as it is now. The original keeps running.
left, right = box.fork(names=["left", "right"])
more = box.fork(10)

# Or keep this moment as a snapshot, and start sandboxes from it later.
warm = box.snapshot(name="warm")
worker = warm.run(name="worker", cpus=2, memory="2Gi")      # also client.run(snapshot="warm")

print(worker.stats().memory_bytes, worker.ip, worker.phase)
for line in worker.logs(tail=10):
    print(line.stream, line.text, end="")

for sandbox in client.sandboxes():
    sandbox.delete()
warm.delete()
```

| | |
|---|---|
| `Client` | `run`, `create`, `sandbox`, `sandboxes`, `snapshot`, `snapshots`, `node`, `version`, `ping`, `events` |
| `Sandbox` | `exec`, `exec_stream`, `fork`, `snapshot`, `logs`, `output`, `stats`, `start`, `stop`, `delete`, `wait`, `refresh`; attributes `id`, `name`, `phase`, `ip`, `cpus`, `memory`, `exit_code`, `snapshot_id` |
| `Snapshot` | `run`, `create`, `delete`, `refresh`; attributes `id`, `name`, `shared_bytes`, `private_bytes`, `sandboxes` |

A `Sandbox` is a context manager that deletes the sandbox on exit. Errors are `NotFound`, `Invalid`, `Conflict`
(wrong state, for example deleting a snapshot that still has sandboxes), `Rejected` (the policy has no room now) and
`ExecError` (from `exec(..., check=True)`), all `TideletError`; an unreachable daemon raises `ConnectionError`.

CPU quantities are cores (`1`, `0.5`, `"500m"`); memory is bytes or a string such as `"512Mi"` or `"2Gi"`.

## Daemon

```python
from tidelet.daemon import Daemon

with Daemon.isolated("demo") as daemon:           # its own state, socket and containerd namespace
    client = daemon.client()
    with client.run("docker.io/library/alpine:3.20", ["sleep", "infinity"]) as box:
        print(box.exec(["uname", "-a"]).stdout)
```

`Daemon(...)` with no arguments is the system daemon (state in `/var/lib/tidelet`, socket
`/run/tidelet/tidelet.sock`); `Daemon.isolated(name)` shares nothing with it. Options cover the policy
(`policy`, `policy_opts`), the sandbox pool (`pool_cpuset`, `pool_memory`), the model-API proxy
(`llm_proxy_listen`), and `args` for any other `tideletd` flag. `start()` returns once the API answers; `stop()`
ends the daemon and leaves the sandboxes running, for the next daemon with the same state to pick up.

From a process that is not root the daemon is started with `sudo -n` and opens its socket to the caller's group
(`tideletd --socket-group`), so the client needs no root.

To run the daemon as a service, use the installed `tideletd` command in a systemd unit.

## Building

The wheel holds static Go binaries and is built from the tidelet repository:

```sh
cd python
uv build --wheel                 # or: python -m build --wheel. Needs Go; runs `go build` for the three binaries
TIDELET_ARCH=x86_64 uv build --wheel   # for another architecture (x86_64 or aarch64) than this machine's
scripts/gen_proto.sh             # after the .proto changed (needs grpcio-tools)
pytest                           # unit tests; TIDELET_E2E=1 pytest runs against a real daemon
```
