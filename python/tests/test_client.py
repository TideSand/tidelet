import pytest

from tidelet import binary
from tidelet._proto import tidelet_pb2 as pb
from tidelet.client import Client, ExecError, ExecResult, _mounts, _resources, parse_cpu, parse_memory
from tidelet.daemon import Daemon


def test_parse_cpu():
    assert parse_cpu(None) == 0
    assert parse_cpu(1) == 1000
    assert parse_cpu(0.5) == 500
    assert parse_cpu("2") == 2000
    assert parse_cpu("250m") == 250
    with pytest.raises(ValueError):
        parse_cpu(-1)


def test_parse_memory():
    assert parse_memory(None) == 0
    assert parse_memory(4096) == 4096
    assert parse_memory("512Mi") == 512 << 20
    assert parse_memory("2Gi") == 2 << 30
    assert parse_memory("1G") == 10**9
    assert parse_memory("1.5Gi") == 3 << 29
    for bad in ("12Q", "Gi", True):
        with pytest.raises(ValueError):
            parse_memory(bad)


def test_resources_and_mounts():
    assert _resources(None, None, None, None) is None
    r = _resources(1, "1Gi", None, None)
    assert r.limits.cpu_millicores == 1000 and r.limits.memory_bytes == 1 << 30 and r.requests.cpu_millicores == 0
    m = _mounts(["/a:/b", "/c:/d:ro", ("/e", "/f", True)])
    assert [(x.host_path, x.container_path, x.read_only) for x in m] == [("/a", "/b", False), ("/c", "/d", True), ("/e", "/f", True)]
    with pytest.raises(ValueError):
        _mounts(["/only"])


def test_exec_result():
    assert ExecResult(0, "out", "").check().ok
    with pytest.raises(ExecError) as e:
        ExecResult(3, "", "boom").check()
    assert e.value.result.exit_code == 3


def test_create_validates_before_calling():
    c = Client("/nonexistent/tidelet.sock")
    with pytest.raises(ValueError):
        c.create()                                   # no image, no snapshot
    with pytest.raises(ValueError):
        c.create(snapshot="warm", image="x")         # a copy cannot change its image
    with pytest.raises(TypeError):
        c.create("img", "sleep 1")                   # a command is a list
    with pytest.raises(ConnectionError):
        c.version()
    assert c.ping(timeout=0.2) is False
    c.close()


def test_proto_package_is_the_daemons():
    assert pb.DESCRIPTOR.package == "tidelet.v1alpha1"
    assert pb.SandboxSpec(snapshot="s").snapshot == "s"


def test_daemon_command_line():
    d = Daemon(policy="overcommit", policy_opts={"threshold": 0.7}, pool_cpuset="0-3", sample_interval=1, sudo=False,
               args=["--log-level", "debug"])
    assert d.command[0] == binary("tideletd")
    flags = d.command[1:]
    assert flags == ["--policy", "overcommit", "--policy-opt", "threshold=0.7", "--container-checkpoints",
                     "--pool-cgroup", "auto", "--pool-cpuset", "0-3", "--sample-interval", "1s", "--log-level", "debug"]
    iso = Daemon.isolated("t1", sudo=True, checkpoints=False)
    assert iso.socket == "/run/t1/tidelet.sock" and iso.command[:2] == ["sudo", "-n"]
    assert "--socket-group" in iso.flags and iso.flags[iso.flags.index("--containerd-namespace") + 1] == "t1"
    assert not iso.running and iso.pid is None
