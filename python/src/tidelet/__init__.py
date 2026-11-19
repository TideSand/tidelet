"""tidelet: a sandbox daemon for AI agents, with containers you can fork in milliseconds.

The package bundles the daemon (``tideletd``) and its command line (``tidelet``); installing it puts
both on PATH. From Python:

- :mod:`tidelet.client` talks to a running daemon: create sandboxes, run commands in them, fork them.
- :mod:`tidelet.daemon` runs a daemon.

    from tidelet.daemon import Daemon

    with Daemon() as daemon:                      # needs root (or passwordless sudo)
        client = daemon.client()
        box = client.run("docker.io/library/python:3.12-slim", ["sleep", "infinity"], cpus=1, memory="1Gi")
        print(box.exec(["python3", "-c", "print(6 * 7)"]).stdout)
        copies = box.fork(3)
"""
from importlib.metadata import PackageNotFoundError, version

from tidelet._bin import binary

try:
    __version__ = version("tidelet")
except PackageNotFoundError:  # running from a source tree
    __version__ = "0.0.0"

__all__ = ["binary", "__version__"]
