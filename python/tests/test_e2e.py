"""End to end against a real daemon: needs root or passwordless sudo, containerd, CNI plugins and CRIU.
Run with TIDELET_E2E=1 (and TIDELET_E2E_ARGS for extra tideletd flags)."""
import os
import shlex
import time

import pytest

from tidelet.client import Conflict, NotFound
from tidelet.daemon import Daemon

pytestmark = pytest.mark.skipif(not os.environ.get("TIDELET_E2E"), reason="set TIDELET_E2E=1 to run against a real daemon")
IMAGE = os.environ.get("TIDELET_E2E_IMAGE", "docker.io/library/python:3.12-slim")
COUNTER = ["python3", "-u", "-c", "import itertools, time\nheap = bytearray(64 << 20)\nfor i in itertools.count(): print(i, flush=True); time.sleep(0.2)"]


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    base = tmp_path_factory.mktemp("tidelet")
    daemon = Daemon.isolated("tidelet-pytest", base=base / "state", log=base / "tideletd.log",
                             args=shlex.split(os.environ.get("TIDELET_E2E_ARGS", "")))
    with daemon:
        with daemon.client() as c:
            yield c
            for sb in c.sandboxes():
                sb.delete()
            for snap in c.snapshots():
                snap.delete()


def test_run_exec_fork_snapshot(client):
    assert client.version()
    assert client.node().policy == "strict"
    with client.run(IMAGE, COUNTER, name="src", cpus=1, memory="512Mi") as box:
        assert box.running and box.cpus == 1 and box.memory == 512 << 20
        assert client.sandbox("src").id == box.id
        r = box.exec(["sh", "-c", "cat; echo err >&2; exit 3"], stdin="hello")
        assert (r.exit_code, r.stdout, r.stderr) == (3, "hello", "err\n")
        assert box.exec(["sh", "-c", "echo $A"], env={"A": "b"}, check=True).stdout == "b\n"
        box.exec(["sh", "-c", "echo parent > /marker"], check=True)
        time.sleep(1)

        left, right = box.fork(names=["left", "right"])
        assert left.running and left.snapshot_id and left.snapshot_id == right.snapshot_id
        assert left.exec(["cat", "/marker"]).stdout == "parent\n"
        left.exec(["sh", "-c", "echo child > /marker"], check=True)
        assert box.exec(["cat", "/marker"]).stdout == "parent\n"
        assert left.stats().memory_bytes < box.stats().memory_bytes / 2      # the copy shares the heap
        time.sleep(1)
        assert int(left.output(tail=1)) > 3                                  # and goes on counting

        warm = box.snapshot(name="warm")
        assert warm.shared_bytes > 32 << 20 and client.snapshot("warm").id == warm.id
        worker = warm.run(name="worker", cpus=0.5, memory="256Mi")
        assert worker.running and worker.cpus == 0.5 and worker.snapshot_id == warm.id
        with pytest.raises(Conflict):
            warm.delete()
        for sb in (left, right, worker):
            sb.delete()
        assert warm.refresh().sandboxes == 0
        warm.delete()
        assert [s for s in client.snapshots() if not s.ephemeral] == []
    with pytest.raises(NotFound):
        client.sandbox("src")
