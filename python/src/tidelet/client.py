"""A client for the tidelet daemon.

    from tidelet.client import Client

    client = Client()                                   # /run/tidelet/tidelet.sock
    box = client.run("docker.io/library/python:3.12-slim", ["sleep", "infinity"],
                     name="agent", cpus=1, memory="1Gi")
    print(box.exec(["python3", "-c", "print(6 * 7)"]).stdout)

    left, right = box.fork(names=["left", "right"])     # running copies; box keeps running
    warm = box.snapshot(name="warm")                    # or keep the moment, and start from it later
    worker = warm.run(name="worker", cpus=2, memory="2Gi")

    for sandbox in (left, right, worker, box):
        sandbox.delete()
    warm.delete()

The client speaks the daemon's gRPC API (``api/tidelet/v1alpha1/tidelet.proto``) over its unix socket
or TCP. CPU quantities are cores (``1``, ``0.5``, ``"500m"``); memory quantities are bytes or strings
such as ``"512Mi"`` and ``"2Gi"``. The daemon's socket belongs to root unless the daemon was started
otherwise.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator, Mapping, Sequence

import grpc

from tidelet._proto import tidelet_pb2 as pb
from tidelet._proto import tidelet_pb2_grpc as rpc

DEFAULT_SOCKET = "/run/tidelet/tidelet.sock"

__all__ = [
    "Client", "Sandbox", "Snapshot", "ExecResult", "Stats", "Node", "Event", "LogEntry",
    "TideletError", "NotFound", "Invalid", "Conflict", "Rejected", "ExecError",
    "parse_cpu", "parse_memory", "DEFAULT_SOCKET",
]


# ---------------------------------------------------------------- errors

class TideletError(Exception):
    """The daemon refused or failed a request."""

    def __init__(self, message: str, code: str = "UNKNOWN") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class NotFound(TideletError):
    """No such sandbox or snapshot."""


class Invalid(TideletError):
    """The request is not valid."""


class Conflict(TideletError):
    """The sandbox or snapshot is not in a state that allows the request."""


class Rejected(TideletError):
    """The policy has no room for the sandbox now; trying again later may work."""


class ExecError(TideletError):
    """A command run with ``check=True`` exited with a non-zero status."""

    def __init__(self, result: "ExecResult") -> None:
        super().__init__(f"command exited with status {result.exit_code}: {result.stderr.strip()[-500:]}", "EXEC")
        self.result = result


_ERRORS = {
    grpc.StatusCode.NOT_FOUND: NotFound,
    grpc.StatusCode.INVALID_ARGUMENT: Invalid,
    grpc.StatusCode.FAILED_PRECONDITION: Conflict,
    grpc.StatusCode.RESOURCE_EXHAUSTED: Rejected,
}


def _raise(err: grpc.RpcError):
    code = err.code()
    if code == grpc.StatusCode.UNAVAILABLE:
        raise ConnectionError(f"tidelet daemon unavailable: {err.details()}") from None
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise TimeoutError(err.details() or "deadline exceeded") from None
    raise _ERRORS.get(code, TideletError)(err.details() or str(code), code.name) from None


# ------------------------------------------------------------ quantities

_MEMORY_UNITS = {
    "": 1, "k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
}


def parse_cpu(value: int | float | str | None) -> int:
    """Cores (``1``, ``0.5``, ``"2"``) or millicores (``"500m"``) to millicores; None is 0 (unset)."""
    if value is None or value == "":
        return 0
    if isinstance(value, str):
        text = value.strip()
        millis = float(text[:-1]) if text.endswith("m") else float(text) * 1000
    else:
        millis = float(value) * 1000
    if millis < 0:
        raise ValueError(f"negative CPU quantity {value!r}")
    return round(millis)


def parse_memory(value: int | str | None) -> int:
    """Bytes (an int) or a quantity such as ``"512Mi"``, ``"2Gi"``, ``"1G"`` to bytes; None is 0 (unset)."""
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        raise ValueError(f"invalid memory quantity {value!r}")
    if isinstance(value, (int, float)):
        number, unit = float(value), ""
    else:
        text = value.strip()
        digits = len(text.rstrip("KMGTkib"))
        number, unit = float(text[:digits]), text[digits:]
        if unit == "K":
            unit = "k"
        if unit not in _MEMORY_UNITS:
            raise ValueError(f"invalid memory quantity {value!r}")
    if number < 0:
        raise ValueError(f"negative memory quantity {value!r}")
    return round(number * _MEMORY_UNITS[unit])


def _resources(cpus, memory, cpu_request, memory_request) -> pb.Resources | None:
    if cpus is None and memory is None and cpu_request is None and memory_request is None:
        return None
    return pb.Resources(
        limits=pb.ResourceList(cpu_millicores=parse_cpu(cpus), memory_bytes=parse_memory(memory)),
        requests=pb.ResourceList(cpu_millicores=parse_cpu(cpu_request), memory_bytes=parse_memory(memory_request)),
    )


_NETWORKS = {
    "bridge": pb.NETWORK_MODE_BRIDGE, "none": pb.NETWORK_MODE_NONE,
    "host": pb.NETWORK_MODE_HOST, "join": pb.NETWORK_MODE_JOIN,
}


def _mounts(mounts) -> list[pb.Mount]:
    out = []
    for m in mounts or ():
        if isinstance(m, str):
            parts = m.split(":")
            if len(parts) not in (2, 3) or (len(parts) == 3 and parts[2] not in ("ro", "rw")):
                raise ValueError(f"invalid mount {m!r}: host:container[:ro]")
            out.append(pb.Mount(host_path=parts[0], container_path=parts[1], read_only=len(parts) == 3 and parts[2] == "ro"))
        else:
            host, container, *rest = m
            out.append(pb.Mount(host_path=str(host), container_path=str(container), read_only=bool(rest and rest[0])))
    return out


def _time(ts) -> datetime | None:
    return ts.ToDatetime() if ts is not None and (ts.seconds or ts.nanos) else None


def _phase(p: int) -> str:
    return pb.Phase.Name(p).removeprefix("PHASE_").capitalize()


# ---------------------------------------------------------------- values

@dataclass
class ExecResult:
    """The outcome of a command run in a sandbox."""
    exit_code: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    def check(self) -> "ExecResult":
        """Raise :class:`ExecError` unless the command exited with status 0."""
        if self.exit_code != 0:
            raise ExecError(self)
        return self


@dataclass
class Stats:
    """A sandbox's resource usage at one instant."""
    timestamp: datetime | None
    cpu_seconds: float
    cpu_throttled_seconds: float
    memory_bytes: int
    memory_limit_bytes: int
    memory_working_set_bytes: int
    swap_bytes: int
    pids: int


@dataclass
class Node:
    """The node as the daemon's policy sees it. CPU in cores, memory in bytes."""
    policy: str
    runtime: str
    capacity_cpus: float
    capacity_memory: int
    allocatable_cpus: float
    allocatable_memory: int
    allocated_cpus: float
    allocated_memory: int
    sandboxes: int
    queued: int


@dataclass
class LogEntry:
    stream: str  # "stdout" or "stderr"
    data: bytes
    timestamp: datetime | None

    @property
    def text(self) -> str:
        return self.data.decode(errors="replace")


@dataclass
class Event:
    type: str  # queued, created, started, frozen, thawed, exited, failed, deleted
    timestamp: datetime | None
    sandbox: "Sandbox"


@dataclass
class Snapshot:
    """A running sandbox captured at one instant; sandboxes created from it start as copies of it."""
    id: str
    name: str
    sandbox_id: str
    sandbox_name: str
    created_at: datetime | None
    shared_bytes: int
    private_bytes: int
    took_seconds: float
    ephemeral: bool
    sandboxes: int
    _client: "Client" = field(repr=False, compare=False, default=None)

    @classmethod
    def _of(cls, client: "Client", s: pb.Snapshot) -> "Snapshot":
        return cls(s.id, s.name, s.sandbox, s.sandbox_name, _time(s.created_at), s.shared_bytes, s.private_bytes,
                   s.took_seconds, s.ephemeral, s.sandboxes, client)

    def run(self, **kwargs) -> "Sandbox":
        """Create and start a sandbox from this snapshot. Takes ``name``, ``cpus``, ``memory``,
        ``cpu_request``, ``memory_request``, ``labels``, ``hostname`` and ``priority``."""
        return self._client.run(snapshot=self.id, **kwargs)

    def create(self, **kwargs) -> "Sandbox":
        """Like :meth:`run` without starting the sandbox."""
        return self._client.create(snapshot=self.id, **kwargs)

    def refresh(self) -> "Snapshot":
        fresh = self._client.snapshot(self.id)
        self.__dict__.update({k: v for k, v in fresh.__dict__.items() if k != "_client"})
        return self

    def delete(self) -> None:
        """Delete the snapshot. Refused (:class:`Conflict`) while sandboxes created from it exist."""
        self._client._call(self._client._snapshots.DeleteSnapshot, pb.DeleteSnapshotRequest(snapshot=self.id))


class Sandbox:
    """A sandbox. Attributes hold the state the daemon last reported; :meth:`refresh` updates them."""

    def __init__(self, client: "Client", message: pb.Sandbox) -> None:
        self._client = client
        self._set(message)

    def _set(self, m: pb.Sandbox) -> None:
        self.raw = m
        self.id: str = m.id
        self.name: str = m.name
        self.image: str = m.spec.image
        self.snapshot_id: str = m.spec.snapshot
        self.labels: dict[str, str] = dict(m.spec.labels)
        self.phase: str = _phase(m.status.phase)
        self.message: str = m.status.message
        self.exit_code: int | None = m.status.exit_code if m.status.HasField("exit_code") else None
        self.created_at = _time(m.created_at)
        self.started_at = _time(m.status.started_at)
        self.finished_at = _time(m.status.finished_at)
        ips = list(m.status.network.ip_addresses)
        self.ip: str | None = ips[0].split("/")[0] if ips else None
        lim = m.status.allocated.limits
        self.cpus: float = lim.cpu_millicores / 1000
        self.memory: int = lim.memory_bytes

    def __repr__(self) -> str:
        return f"Sandbox(id={self.id[:12]!r}, name={self.name!r}, phase={self.phase!r})"

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, *exc) -> None:
        try:
            self.delete()
        except NotFound:
            pass

    @property
    def running(self) -> bool:
        return self.phase == "Running"

    def refresh(self) -> "Sandbox":
        resp = self._client._call(self._client._sandboxes.GetSandbox, pb.GetSandboxRequest(sandbox=self.id))
        self._set(resp.sandbox)
        return self

    def start(self) -> "Sandbox":
        resp = self._client._call(self._client._sandboxes.StartSandbox, pb.StartSandboxRequest(sandbox=self.id))
        self._set(resp.sandbox)
        return self

    def stop(self, timeout: float = 0) -> "Sandbox":
        """SIGTERM, then SIGKILL after ``timeout`` seconds (0: the daemon's default)."""
        resp = self._client._call(self._client._sandboxes.StopSandbox,
                                  pb.StopSandboxRequest(sandbox=self.id, timeout_seconds=int(timeout)))
        self._set(resp.sandbox)
        return self

    def delete(self, force: bool = True) -> None:
        """Remove the sandbox; with ``force`` (the default) a running one is stopped first."""
        self._client._call(self._client._sandboxes.DeleteSandbox, pb.DeleteSandboxRequest(sandbox=self.id, force=force))
        self.phase = "Deleted"

    def wait(self, phase: str | Sequence[str] = ("Exited", "Failed"), timeout: float | None = None, poll: float = 0.1) -> "Sandbox":
        """Block until the sandbox is in one of the given phases."""
        want = {phase} if isinstance(phase, str) else set(phase)
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.refresh().phase not in want:
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(f"sandbox {self.id[:12]} is {self.phase}, not {' or '.join(sorted(want))}")
            time.sleep(poll)
        return self

    def exec(self, command: Sequence[str], *, env: Mapping[str, str] | None = None, workdir: str = "",
             stdin: str | bytes | None = None, timeout: float | None = None, check: bool = False) -> ExecResult:
        """Run a command in the sandbox and wait for it.

        ``stdin`` is given to the command and then closed. After ``timeout`` seconds the command is
        killed and TimeoutError raised. With ``check`` a non-zero exit status raises :class:`ExecError`.
        """
        out, err, code = bytearray(), bytearray(), None
        for stream, data in self.exec_stream(command, env=env, workdir=workdir, stdin=stdin, timeout=timeout):
            if stream == "stdout":
                out += data
            elif stream == "stderr":
                err += data
            else:
                code = data
        result = ExecResult(code if code is not None else -1, out.decode(errors="replace"), err.decode(errors="replace"))
        return result.check() if check else result

    def exec_stream(self, command: Sequence[str], *, env: Mapping[str, str] | None = None, workdir: str = "",
                    stdin: str | bytes | None = None, timeout: float | None = None) -> Iterator[tuple[str, bytes | int]]:
        """Run a command and yield its output as it arrives: ``("stdout", bytes)``,
        ``("stderr", bytes)``, and last ``("exit", code)``."""
        if isinstance(command, str):
            raise TypeError("command must be a list of arguments, e.g. ['sh', '-c', '...']")
        data = stdin.encode() if isinstance(stdin, str) else stdin

        def requests():
            yield pb.ExecRequest(start=pb.ExecStart(sandbox=self.id, command=list(command), env=dict(env or {}),
                                                    working_dir=workdir, stdin=data is not None))
            if data is not None:
                for i in range(0, len(data), 1 << 16):
                    yield pb.ExecRequest(stdin=data[i:i + (1 << 16)])
                yield pb.ExecRequest(close_stdin=True)

        try:
            for resp in self._client._sandboxes.Exec(requests(), timeout=timeout):
                kind = resp.WhichOneof("msg")
                if kind == "exit":
                    yield "exit", resp.exit.code
                elif kind in ("stdout", "stderr"):
                    yield kind, getattr(resp, kind)
        except grpc.RpcError as e:
            _raise(e)

    def logs(self, *, follow: bool = False, tail: int = 0, since: datetime | None = None) -> Iterator[LogEntry]:
        """The output of the sandbox's main process. With ``follow`` the iterator goes on until the
        sandbox exits."""
        req = pb.LogsRequest(sandbox=self.id, follow=follow, tail_lines=tail)
        if since is not None:
            req.since.FromDatetime(since)
        try:
            for resp in self._client._sandboxes.Logs(req):
                yield LogEntry("stderr" if resp.stream == pb.LOG_STREAM_STDERR else "stdout", resp.data, _time(resp.timestamp))
        except grpc.RpcError as e:
            _raise(e)

    def output(self, tail: int = 0) -> str:
        """The main process's output so far, stdout and stderr together, as text."""
        return "".join(entry.text for entry in self.logs(tail=tail))

    def stats(self) -> Stats:
        s = self._client._call(self._client._sandboxes.GetStats, pb.GetStatsRequest(sandbox=self.id)).stats
        return Stats(_time(s.timestamp), s.cpu_usage_nanos / 1e9, s.cpu_throttled_nanos / 1e9, s.memory_usage_bytes,
                     s.memory_limit_bytes, s.memory_working_set_bytes, s.memory_swap_usage_bytes, s.pids)

    def fork(self, count: int | None = None, *, names: Sequence[str] | None = None) -> list["Sandbox"]:
        """Make running copies of this running sandbox: same processes, memory and filesystem, each
        continuing from this instant with its own identity. The sandbox keeps running. Returns the
        copies: ``count`` of them (default: one per name, or 1)."""
        req = pb.ForkSandboxRequest(sandbox=self.id, count=count or 0, names=list(names or ()))
        resp = self._client._call(self._client._sandboxes.ForkSandbox, req)
        return [Sandbox(self._client, s) for s in resp.sandboxes]

    def snapshot(self, name: str = "") -> Snapshot:
        """Snapshot this running sandbox, which keeps running. Create sandboxes from the snapshot
        with :meth:`Snapshot.run`."""
        resp = self._client._call(self._client._snapshots.CreateSnapshot, pb.CreateSnapshotRequest(sandbox=self.id, name=name))
        return Snapshot._of(self._client, resp.snapshot)


# ---------------------------------------------------------------- client

class Client:
    """A connection to a tidelet daemon.

    ``address`` is a unix socket path or ``tcp://host:port``; it defaults to $TIDELET_SOCKET, then
    ``/run/tidelet/tidelet.sock``. ``timeout`` (seconds) bounds each call that is not a stream.
    """

    def __init__(self, address: str | None = None, *, timeout: float | None = None) -> None:
        self.address = address or os.environ.get("TIDELET_SOCKET") or DEFAULT_SOCKET
        self.timeout = timeout
        if self.address.startswith("tcp://"):
            target = self.address[len("tcp://"):]
        elif self.address.startswith("unix://"):
            target = "unix:" + self.address[len("unix://"):]
        else:
            target = "unix:" + self.address
        self._channel = grpc.insecure_channel(target)
        self._sandboxes = rpc.SandboxServiceStub(self._channel)
        self._snapshots = rpc.SnapshotServiceStub(self._channel)
        self._node = rpc.NodeServiceStub(self._channel)

    def __repr__(self) -> str:
        return f"Client({self.address!r})"

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._channel.close()

    def _call(self, method, request, timeout: float | None = None):
        try:
            return method(request, timeout=timeout if timeout is not None else self.timeout)
        except grpc.RpcError as e:
            _raise(e)

    # -- node

    def version(self) -> str:
        """The daemon's version."""
        return self._call(self._node.GetVersion, pb.GetVersionRequest()).version

    def ping(self, timeout: float = 2) -> bool:
        """Whether a daemon answers at the address."""
        try:
            self._call(self._node.GetVersion, pb.GetVersionRequest(), timeout=timeout)
            return True
        except (ConnectionError, TimeoutError, TideletError):
            return False

    def node(self) -> Node:
        n = self._call(self._node.GetNode, pb.GetNodeRequest()).node
        return Node(n.policy, n.runtime, n.capacity.cpu_millicores / 1000, n.capacity.memory_bytes,
                    n.allocatable.cpu_millicores / 1000, n.allocatable.memory_bytes,
                    n.allocated.cpu_millicores / 1000, n.allocated.memory_bytes, n.sandbox_count, n.queued_count)

    # -- sandboxes

    def create(self, image: str = "", command: Sequence[str] | None = None, *, name: str = "", snapshot: str = "",
               cpus: int | float | str | None = None, memory: int | str | None = None,
               cpu_request: int | float | str | None = None, memory_request: int | str | None = None,
               env: Mapping[str, str] | None = None, workdir: str = "",
               mounts: Iterable[str | tuple] | None = None, labels: Mapping[str, str] | None = None,
               network: str | None = None, hostname: str = "", priority: int = 0, microvm: bool = False) -> Sandbox:
        """Create a sandbox without starting it (see :meth:`run`).

        Depending on the daemon's policy the sandbox comes back Created or Queued; a queued sandbox
        is created when the policy has room for it.
        """
        if isinstance(command, str):
            raise TypeError("command must be a list of arguments, e.g. ['sh', '-c', '...']")
        if snapshot:
            if image or command or env or workdir or mounts or network or microvm:
                raise ValueError("a sandbox created from a snapshot continues the snapshot's processes: only name, "
                                 "resources, labels, hostname and priority can be given")
            spec = pb.SandboxSpec(snapshot=snapshot, labels=dict(labels or {}), hostname=hostname, priority=priority)
        else:
            if not image:
                raise ValueError("image is required (or snapshot)")
            mode = network or "bridge"
            if mode not in _NETWORKS:
                raise ValueError(f"unknown network mode {mode!r}: one of {', '.join(_NETWORKS)}")
            spec = pb.SandboxSpec(
                image=image, command=list(command or ()), env=dict(env or {}), working_dir=workdir,
                mounts=_mounts(mounts), labels=dict(labels or {}), network=_NETWORKS[mode], hostname=hostname,
                priority=priority, runtime=pb.RUNTIME_CLASS_MICROVM if microvm else pb.RUNTIME_CLASS_CONTAINER)
        res = _resources(cpus, memory, cpu_request, memory_request)
        if res is not None:
            spec.resources.CopyFrom(res)
        resp = self._call(self._sandboxes.CreateSandbox, pb.CreateSandboxRequest(name=name, spec=spec))
        return Sandbox(self, resp.sandbox)

    def run(self, image: str = "", command: Sequence[str] | None = None, **kwargs) -> Sandbox:
        """Create a sandbox and start it.

        ``image`` is an OCI image reference with its registry (``docker.io/library/python:3.12-slim``)
        and ``command`` replaces the image's entrypoint. ``cpus`` and ``memory`` are hard limits;
        ``cpu_request`` and ``memory_request`` what the policy accounts (default: the limits).
        ``mounts`` are ``"host:container[:ro]"`` strings or tuples; ``network`` is ``"bridge"`` (the
        default), ``"none"`` or ``"host"``.

        With ``snapshot`` (an ID or name) the sandbox is instead a copy of the snapshotted sandbox,
        and only ``name``, the resources, ``labels``, ``hostname`` and ``priority`` apply.

        A sandbox the policy queued is returned Queued; it starts when the policy dispatches it.
        """
        return self.create(image, command, **kwargs).start()

    def sandbox(self, ref: str) -> Sandbox:
        """A sandbox by ID, name, or unique ID prefix."""
        return Sandbox(self, self._call(self._sandboxes.GetSandbox, pb.GetSandboxRequest(sandbox=ref)).sandbox)

    def sandboxes(self) -> list[Sandbox]:
        """All sandboxes, newest first."""
        resp = self._call(self._sandboxes.ListSandboxes, pb.ListSandboxesRequest())
        return [Sandbox(self, s) for s in resp.sandboxes]

    def events(self) -> Iterator[Event]:
        """Sandbox lifecycle events as they happen. The iterator does not end; break out of it."""
        try:
            for resp in self._sandboxes.Watch(pb.WatchRequest()):
                kind = pb.EventType.Name(resp.type).removeprefix("EVENT_TYPE_").lower()
                yield Event(kind, _time(resp.timestamp), Sandbox(self, resp.sandbox))
        except grpc.RpcError as e:
            _raise(e)

    # -- snapshots

    def snapshot(self, ref: str) -> Snapshot:
        """A snapshot by ID, name, or unique ID prefix."""
        return Snapshot._of(self, self._call(self._snapshots.GetSnapshot, pb.GetSnapshotRequest(snapshot=ref)).snapshot)

    def snapshots(self) -> list[Snapshot]:
        """All snapshots, newest first."""
        resp = self._call(self._snapshots.ListSnapshots, pb.ListSnapshotsRequest())
        return [Snapshot._of(self, s) for s in resp.snapshots]
