"""Run the tidelet daemon from Python.

    from tidelet.daemon import Daemon

    with Daemon(policy="strict") as daemon:        # starts tideletd, stops it at the end
        client = daemon.client()
        box = client.run("docker.io/library/python:3.12-slim", ["sleep", "infinity"])
        ...

The daemon needs root, containerd with runc, the CNI plugins, and CRIU for forking. Started from a
process that is not root it runs under ``sudo -n`` (passwordless sudo) and opens its socket to the
calling user's group, so :meth:`Daemon.client` works without root.

``Daemon(...)`` with default arguments is the system daemon: state in /var/lib/tidelet, socket at
/run/tidelet/tidelet.sock. :meth:`Daemon.isolated` gives one with its own state, socket and containerd
namespace, for tests and for running several side by side. To run the daemon in the foreground from
a shell, use the ``tideletd`` command this package installs.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Mapping, Sequence

from tidelet._bin import binary
from tidelet.client import DEFAULT_SOCKET, Client

__all__ = ["Daemon", "DaemonError"]


class DaemonError(RuntimeError):
    """The daemon could not be started or stopped."""


class Daemon:
    """A tideletd process.

    Arguments left None keep the daemon's own defaults.

    - ``policy`` and ``policy_opts``: the policy that decides what runs (``strict``, ``overcommit``,
      ``agentcgroup``, ``tidesand``) and its options.
    - ``checkpoints``: make sandboxes forkable and checkpointable (needs CRIU). On by default.
    - ``socket``: the gRPC unix socket. ``http_listen``: the REST gateway's ``host:port``, "" to
      disable it.
    - ``state_dir``, ``run_dir``: where the daemon keeps persistent state and runtime files.
    - ``containerd_namespace``: the containerd namespace for sandboxes and images.
    - ``pool_cpuset``, ``pool_memory``: confine all sandboxes to these CPUs (``"0-7"``) and this much
      memory (``"16Gi"``).
    - ``llm_proxy_listen``: ``host:port`` of the model-API proxy.
    - ``sample_interval``: seconds between usage samples handed to the policy.
    - ``args``: any further tideletd flags, as a list.
    - ``log``: a file for the daemon's log (default ``<run_dir>/tideletd.log`` for an isolated daemon,
      else discarded unless given).
    - ``sudo``: run under ``sudo -n``; default: when the calling process is not root.
    """

    def __init__(self, *, policy: str | None = None, policy_opts: Mapping[str, object] | None = None,
                 checkpoints: bool = True, socket: str | None = None, http_listen: str | None = None,
                 state_dir: str | os.PathLike | None = None, run_dir: str | os.PathLike | None = None,
                 containerd_namespace: str | None = None, pool_cpuset: str | None = None,
                 pool_memory: str | None = None, llm_proxy_listen: str | None = None,
                 sample_interval: float | None = None, log_level: str | None = None,
                 args: Sequence[str] = (), log: str | os.PathLike | None = None, sudo: bool | None = None) -> None:
        self.socket = socket or DEFAULT_SOCKET
        self.sudo = os.geteuid() != 0 if sudo is None else sudo
        self.log = Path(log) if log is not None else None
        self._proc: subprocess.Popen | None = None
        self._log_file = None
        flags: list[str] = []

        def flag(name: str, value) -> None:
            if value is not None:
                flags.extend([name, str(value)])

        flag("--listen", socket)
        flag("--http-listen", http_listen)
        flag("--state-dir", state_dir)
        flag("--run-dir", run_dir)
        flag("--containerd-namespace", containerd_namespace)
        flag("--policy", policy)
        for key, value in (policy_opts or {}).items():
            flags.extend(["--policy-opt", f"{key}={value}"])
        if checkpoints:
            flags.append("--container-checkpoints")
        if pool_cpuset is not None or pool_memory is not None:
            flags.extend(["--pool-cgroup", "auto"])
            flag("--pool-cpuset", pool_cpuset)
            flag("--pool-memory", pool_memory)
        flag("--llm-proxy-listen", llm_proxy_listen)
        if sample_interval is not None:
            flags.extend(["--sample-interval", f"{sample_interval}s"])
        flag("--log-level", log_level)
        if self.sudo and "--socket-group" not in args:
            flags.extend(["--socket-group", str(os.getgid())])
        flags.extend(args)
        self.flags = flags

    @classmethod
    def isolated(cls, name: str = "tidelet-py", *, base: str | os.PathLike | None = None, **kwargs) -> "Daemon":
        """A daemon that shares nothing with the system one: state under ``base`` (default
        ``/var/lib/<name>``), runtime files and socket under ``/run/<name>``, the containerd namespace
        ``<name>``, and no REST gateway unless ``http_listen`` is given."""
        run_dir = Path("/run") / name
        kwargs.setdefault("state_dir", Path(base) if base is not None else Path("/var/lib") / name)
        kwargs.setdefault("run_dir", run_dir)
        kwargs.setdefault("socket", str(run_dir / "tidelet.sock"))
        kwargs.setdefault("containerd_namespace", name)
        kwargs.setdefault("http_listen", "")
        return cls(**kwargs)

    # -- lifecycle

    @property
    def command(self) -> list[str]:
        """The command line that starts the daemon."""
        cmd = [binary("tideletd"), *self.flags]
        return ["sudo", "-n", *cmd] if self.sudo else cmd

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def client(self, **kwargs) -> Client:
        """A client connected to this daemon."""
        return Client(self.socket, **kwargs)

    def start(self, timeout: float = 60) -> "Daemon":
        """Start the daemon and wait until it serves its API."""
        if self.running:
            return self
        if self.sudo:
            if shutil.which("sudo") is None:
                raise DaemonError("tideletd needs root, and sudo is not installed")
            if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0:
                raise DaemonError("tideletd needs root: run as root, or allow passwordless sudo")
        with Client(self.socket) as probe:
            if probe.ping(timeout=0.5):
                raise DaemonError(f"a daemon is already serving {self.socket}")
        out = subprocess.DEVNULL
        if self.log is not None:
            self.log.parent.mkdir(parents=True, exist_ok=True)
            self._log_file = out = open(self.log, "ab")
        try:
            self._proc = subprocess.Popen(self.command, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                          start_new_session=True)
        except FileNotFoundError as e:
            self._close_log()
            raise DaemonError(str(e)) from None
        deadline = time.monotonic() + timeout
        with Client(self.socket) as probe:
            while not probe.ping(timeout=1):
                if self._proc.poll() is not None:
                    code = self._proc.returncode
                    self._proc = None
                    self._close_log()
                    raise DaemonError(f"tideletd exited with status {code}{self._log_tail()}")
                if time.monotonic() > deadline:
                    self.stop()
                    raise DaemonError(f"tideletd did not serve {self.socket} within {timeout:g} s{self._log_tail()}")
                time.sleep(0.05)
        return self

    def stop(self, timeout: float = 30) -> None:
        """Stop the daemon (SIGTERM, then SIGKILL after ``timeout`` seconds). Sandboxes are containerd
        containers and keep running; a daemon started again with the same state picks them up."""
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            if proc.poll() is None:
                self._signal(proc, signal.SIGTERM)
                try:
                    proc.wait(timeout)
                except subprocess.TimeoutExpired:
                    self._signal(proc, signal.SIGKILL)
                    proc.wait(10)
        finally:
            self._close_log()

    def _signal(self, proc: subprocess.Popen, sig: signal.Signals) -> None:
        if not self.sudo:
            proc.send_signal(sig)
            return
        # The process is root's (sudo): only root may signal it. sudo passes TERM on to the daemon;
        # KILL goes to the whole session, since sudo cannot pass that one on.
        target = ["--", f"-{os.getsid(proc.pid)}"] if sig == signal.SIGKILL else [str(proc.pid)]
        subprocess.run(["sudo", "-n", "kill", f"-{sig.name.removeprefix('SIG')}", *target], capture_output=True)

    def _close_log(self) -> None:
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    def _log_tail(self) -> str:
        if self.log is None or not self.log.exists():
            return " (pass log=... to keep its output)"
        lines = self.log.read_text(errors="replace").strip().splitlines()[-8:]
        return ":\n" + "\n".join(lines)

    def __enter__(self) -> "Daemon":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def __repr__(self) -> str:
        state = f"pid {self.pid}" if self.running else "stopped"
        return f"Daemon(socket={self.socket!r}, {state})"
