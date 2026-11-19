"""Build hook: put the Go binaries into the wheel and tag it for the platform they were built for.

The binaries (tideletd, tidelet, tidelet-init) are built from the repository this folder sits in,
statically (CGO_ENABLED=0), into src/tidelet/bin. Binaries already there are used as they are when the
Go sources or toolchain are missing (set TIDELET_SKIP_GO_BUILD=1 to force that).
TIDELET_ARCH (x86_64 or aarch64) builds the wheel for another architecture than the build machine's.
"""
import os
import platform
import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

BINARIES = {"tideletd": "./cmd/tideletd", "tidelet": "./cmd/tidelet", "tidelet-init": "./cmd/tidelet-init"}
ARCH = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}
GOARCH = {"x86_64": "amd64", "aarch64": "arm64"}


class GoBinaries(BuildHookInterface):
    def initialize(self, version, build_data):
        root = Path(self.root)
        repo = root.parent
        out = root / "src" / "tidelet" / "bin"
        machine = (os.environ.get("TIDELET_ARCH") or platform.machine()).lower()
        if platform.system() != "Linux" or machine not in ARCH:
            raise RuntimeError("tidelet runs on Linux (x86_64 or aarch64) only")
        go = shutil.which("go") or ("/usr/local/go/bin/go" if os.path.exists("/usr/local/go/bin/go") else None)
        have = all((out / name).is_file() for name in BINARIES)
        if os.environ.get("TIDELET_SKIP_GO_BUILD") or not (repo / "go.mod").is_file() or go is None:
            if not have:
                raise RuntimeError(
                    f"the tidelet binaries are not in {out} and cannot be built: "
                    "build the wheel from the tidelet repository's python/ folder with Go installed")
        else:
            out.mkdir(parents=True, exist_ok=True)
            pyver = self.metadata.version
            ldflags = f"-s -w -X github.com/TideSand/tidelet/pkg/version.Version={pyver}"
            env = {**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": GOARCH[ARCH[machine]]}
            for name, pkg in BINARIES.items():
                self.app.display_info(f"go build {pkg}")
                subprocess.run([go, "build", "-trimpath", "-ldflags", ldflags, "-o", str(out / name), pkg],
                               cwd=repo, env=env, check=True)
        build_data["pure_python"] = False
        # Static Go binaries: any Linux of this architecture.
        build_data["tag"] = f"py3-none-manylinux_2_17_{ARCH[machine]}"
