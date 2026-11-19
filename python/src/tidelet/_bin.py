"""Finding and running the bundled binaries."""
import os
import shutil
import sys
from pathlib import Path

_NAMES = ("tideletd", "tidelet", "tidelet-init")


def binary(name: str) -> str:
    """Path of a tidelet binary: ``tideletd``, ``tidelet`` or ``tidelet-init``.

    Looked for in $TIDELET_BIN_DIR, then in the package (where a wheel puts them), then in the bin/
    folder of a source checkout, then on PATH.
    """
    if name not in _NAMES:
        raise ValueError(f"unknown binary {name!r}: one of {', '.join(_NAMES)}")
    here = Path(__file__).resolve().parent
    candidates = []
    if os.environ.get("TIDELET_BIN_DIR"):
        candidates.append(Path(os.environ["TIDELET_BIN_DIR"]) / name)
    candidates.append(here / "bin" / name)
    candidates.append(here.parents[2] / "bin" / name)  # <repo>/python/src/tidelet -> <repo>/bin
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)
    # Not the console script this package installs (it would call itself).
    scripts = Path(sys.argv[0]).resolve().parent if sys.argv and sys.argv[0] else None
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        if not directory or (scripts and Path(directory).resolve() == scripts):
            continue
        found = shutil.which(name, path=directory)
        if found:
            return found
    raise FileNotFoundError(
        f"{name} not found: install the tidelet wheel for this platform, or set TIDELET_BIN_DIR to the "
        "directory holding tideletd, tidelet and tidelet-init")


def _exec(name: str) -> None:
    path = binary(name)
    os.execv(path, [path, *sys.argv[1:]])


def run_tidelet() -> None:
    """Console script ``tidelet``: the command-line client."""
    _exec("tidelet")


def run_tideletd() -> None:
    """Console script ``tideletd``: the daemon."""
    _exec("tideletd")
