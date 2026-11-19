import datetime

from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NetworkMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NETWORK_MODE_UNSPECIFIED: _ClassVar[NetworkMode]
    NETWORK_MODE_NONE: _ClassVar[NetworkMode]
    NETWORK_MODE_HOST: _ClassVar[NetworkMode]
    NETWORK_MODE_BRIDGE: _ClassVar[NetworkMode]
    NETWORK_MODE_JOIN: _ClassVar[NetworkMode]

class RuntimeClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUNTIME_CLASS_UNSPECIFIED: _ClassVar[RuntimeClass]
    RUNTIME_CLASS_CONTAINER: _ClassVar[RuntimeClass]
    RUNTIME_CLASS_MICROVM: _ClassVar[RuntimeClass]

class Phase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PHASE_UNSPECIFIED: _ClassVar[Phase]
    PHASE_PENDING: _ClassVar[Phase]
    PHASE_QUEUED: _ClassVar[Phase]
    PHASE_CREATED: _ClassVar[Phase]
    PHASE_RUNNING: _ClassVar[Phase]
    PHASE_FROZEN: _ClassVar[Phase]
    PHASE_EXITED: _ClassVar[Phase]
    PHASE_FAILED: _ClassVar[Phase]

class LogStream(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOG_STREAM_UNSPECIFIED: _ClassVar[LogStream]
    LOG_STREAM_STDOUT: _ClassVar[LogStream]
    LOG_STREAM_STDERR: _ClassVar[LogStream]

class EventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EVENT_TYPE_UNSPECIFIED: _ClassVar[EventType]
    EVENT_TYPE_QUEUED: _ClassVar[EventType]
    EVENT_TYPE_CREATED: _ClassVar[EventType]
    EVENT_TYPE_STARTED: _ClassVar[EventType]
    EVENT_TYPE_FROZEN: _ClassVar[EventType]
    EVENT_TYPE_THAWED: _ClassVar[EventType]
    EVENT_TYPE_EXITED: _ClassVar[EventType]
    EVENT_TYPE_FAILED: _ClassVar[EventType]
    EVENT_TYPE_DELETED: _ClassVar[EventType]
NETWORK_MODE_UNSPECIFIED: NetworkMode
NETWORK_MODE_NONE: NetworkMode
NETWORK_MODE_HOST: NetworkMode
NETWORK_MODE_BRIDGE: NetworkMode
NETWORK_MODE_JOIN: NetworkMode
RUNTIME_CLASS_UNSPECIFIED: RuntimeClass
RUNTIME_CLASS_CONTAINER: RuntimeClass
RUNTIME_CLASS_MICROVM: RuntimeClass
PHASE_UNSPECIFIED: Phase
PHASE_PENDING: Phase
PHASE_QUEUED: Phase
PHASE_CREATED: Phase
PHASE_RUNNING: Phase
PHASE_FROZEN: Phase
PHASE_EXITED: Phase
PHASE_FAILED: Phase
LOG_STREAM_UNSPECIFIED: LogStream
LOG_STREAM_STDOUT: LogStream
LOG_STREAM_STDERR: LogStream
EVENT_TYPE_UNSPECIFIED: EventType
EVENT_TYPE_QUEUED: EventType
EVENT_TYPE_CREATED: EventType
EVENT_TYPE_STARTED: EventType
EVENT_TYPE_FROZEN: EventType
EVENT_TYPE_THAWED: EventType
EVENT_TYPE_EXITED: EventType
EVENT_TYPE_FAILED: EventType
EVENT_TYPE_DELETED: EventType

class ResourceList(_message.Message):
    __slots__ = ("cpu_millicores", "memory_bytes")
    CPU_MILLICORES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    cpu_millicores: int
    memory_bytes: int
    def __init__(self, cpu_millicores: _Optional[int] = ..., memory_bytes: _Optional[int] = ...) -> None: ...

class Resources(_message.Message):
    __slots__ = ("requests", "limits")
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    LIMITS_FIELD_NUMBER: _ClassVar[int]
    requests: ResourceList
    limits: ResourceList
    def __init__(self, requests: _Optional[_Union[ResourceList, _Mapping]] = ..., limits: _Optional[_Union[ResourceList, _Mapping]] = ...) -> None: ...

class Mount(_message.Message):
    __slots__ = ("host_path", "container_path", "read_only")
    HOST_PATH_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_PATH_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    host_path: str
    container_path: str
    read_only: bool
    def __init__(self, host_path: _Optional[str] = ..., container_path: _Optional[str] = ..., read_only: _Optional[bool] = ...) -> None: ...

class SandboxSpec(_message.Message):
    __slots__ = ("image", "command", "args", "env", "working_dir", "resources", "network", "runtime", "mounts", "labels", "stdin", "tty", "priority", "network_namespace", "hostname", "snapshot")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    WORKING_DIR_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FIELD_NUMBER: _ClassVar[int]
    MOUNTS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    TTY_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    NETWORK_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    image: str
    command: _containers.RepeatedScalarFieldContainer[str]
    args: _containers.RepeatedScalarFieldContainer[str]
    env: _containers.ScalarMap[str, str]
    working_dir: str
    resources: Resources
    network: NetworkMode
    runtime: RuntimeClass
    mounts: _containers.RepeatedCompositeFieldContainer[Mount]
    labels: _containers.ScalarMap[str, str]
    stdin: bool
    tty: bool
    priority: int
    network_namespace: str
    hostname: str
    snapshot: str
    def __init__(self, image: _Optional[str] = ..., command: _Optional[_Iterable[str]] = ..., args: _Optional[_Iterable[str]] = ..., env: _Optional[_Mapping[str, str]] = ..., working_dir: _Optional[str] = ..., resources: _Optional[_Union[Resources, _Mapping]] = ..., network: _Optional[_Union[NetworkMode, str]] = ..., runtime: _Optional[_Union[RuntimeClass, str]] = ..., mounts: _Optional[_Iterable[_Union[Mount, _Mapping]]] = ..., labels: _Optional[_Mapping[str, str]] = ..., stdin: _Optional[bool] = ..., tty: _Optional[bool] = ..., priority: _Optional[int] = ..., network_namespace: _Optional[str] = ..., hostname: _Optional[str] = ..., snapshot: _Optional[str] = ...) -> None: ...

class NetworkStatus(_message.Message):
    __slots__ = ("namespace_path", "ip_addresses")
    NAMESPACE_PATH_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESSES_FIELD_NUMBER: _ClassVar[int]
    namespace_path: str
    ip_addresses: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, namespace_path: _Optional[str] = ..., ip_addresses: _Optional[_Iterable[str]] = ...) -> None: ...

class SandboxStatus(_message.Message):
    __slots__ = ("phase", "message", "exit_code", "started_at", "finished_at", "allocated", "policy", "network")
    PHASE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    phase: Phase
    message: str
    exit_code: int
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    allocated: Resources
    policy: str
    network: NetworkStatus
    def __init__(self, phase: _Optional[_Union[Phase, str]] = ..., message: _Optional[str] = ..., exit_code: _Optional[int] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., allocated: _Optional[_Union[Resources, _Mapping]] = ..., policy: _Optional[str] = ..., network: _Optional[_Union[NetworkStatus, _Mapping]] = ...) -> None: ...

class Sandbox(_message.Message):
    __slots__ = ("id", "name", "created_at", "spec", "status")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    created_at: _timestamp_pb2.Timestamp
    spec: SandboxSpec
    status: SandboxStatus
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., spec: _Optional[_Union[SandboxSpec, _Mapping]] = ..., status: _Optional[_Union[SandboxStatus, _Mapping]] = ...) -> None: ...

class CreateSandboxRequest(_message.Message):
    __slots__ = ("name", "spec")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    name: str
    spec: SandboxSpec
    def __init__(self, name: _Optional[str] = ..., spec: _Optional[_Union[SandboxSpec, _Mapping]] = ...) -> None: ...

class CreateSandboxResponse(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: Sandbox
    def __init__(self, sandbox: _Optional[_Union[Sandbox, _Mapping]] = ...) -> None: ...

class StartSandboxRequest(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    def __init__(self, sandbox: _Optional[str] = ...) -> None: ...

class StartSandboxResponse(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: Sandbox
    def __init__(self, sandbox: _Optional[_Union[Sandbox, _Mapping]] = ...) -> None: ...

class GetSandboxRequest(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    def __init__(self, sandbox: _Optional[str] = ...) -> None: ...

class GetSandboxResponse(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: Sandbox
    def __init__(self, sandbox: _Optional[_Union[Sandbox, _Mapping]] = ...) -> None: ...

class ListSandboxesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListSandboxesResponse(_message.Message):
    __slots__ = ("sandboxes",)
    SANDBOXES_FIELD_NUMBER: _ClassVar[int]
    sandboxes: _containers.RepeatedCompositeFieldContainer[Sandbox]
    def __init__(self, sandboxes: _Optional[_Iterable[_Union[Sandbox, _Mapping]]] = ...) -> None: ...

class StopSandboxRequest(_message.Message):
    __slots__ = ("sandbox", "timeout_seconds")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    timeout_seconds: int
    def __init__(self, sandbox: _Optional[str] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class StopSandboxResponse(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: Sandbox
    def __init__(self, sandbox: _Optional[_Union[Sandbox, _Mapping]] = ...) -> None: ...

class DeleteSandboxRequest(_message.Message):
    __slots__ = ("sandbox", "force")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    force: bool
    def __init__(self, sandbox: _Optional[str] = ..., force: _Optional[bool] = ...) -> None: ...

class DeleteSandboxResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ForkSandboxRequest(_message.Message):
    __slots__ = ("sandbox", "count", "names")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    NAMES_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    count: int
    names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, sandbox: _Optional[str] = ..., count: _Optional[int] = ..., names: _Optional[_Iterable[str]] = ...) -> None: ...

class ForkSandboxResponse(_message.Message):
    __slots__ = ("sandboxes",)
    SANDBOXES_FIELD_NUMBER: _ClassVar[int]
    sandboxes: _containers.RepeatedCompositeFieldContainer[Sandbox]
    def __init__(self, sandboxes: _Optional[_Iterable[_Union[Sandbox, _Mapping]]] = ...) -> None: ...

class Snapshot(_message.Message):
    __slots__ = ("id", "name", "sandbox", "sandbox_name", "created_at", "spec", "shared_bytes", "private_bytes", "took_seconds", "ephemeral", "sandboxes")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_NAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    SHARED_BYTES_FIELD_NUMBER: _ClassVar[int]
    PRIVATE_BYTES_FIELD_NUMBER: _ClassVar[int]
    TOOK_SECONDS_FIELD_NUMBER: _ClassVar[int]
    EPHEMERAL_FIELD_NUMBER: _ClassVar[int]
    SANDBOXES_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    sandbox: str
    sandbox_name: str
    created_at: _timestamp_pb2.Timestamp
    spec: SandboxSpec
    shared_bytes: int
    private_bytes: int
    took_seconds: float
    ephemeral: bool
    sandboxes: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., sandbox: _Optional[str] = ..., sandbox_name: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., spec: _Optional[_Union[SandboxSpec, _Mapping]] = ..., shared_bytes: _Optional[int] = ..., private_bytes: _Optional[int] = ..., took_seconds: _Optional[float] = ..., ephemeral: _Optional[bool] = ..., sandboxes: _Optional[int] = ...) -> None: ...

class CreateSnapshotRequest(_message.Message):
    __slots__ = ("sandbox", "name")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    name: str
    def __init__(self, sandbox: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class CreateSnapshotResponse(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: Snapshot
    def __init__(self, snapshot: _Optional[_Union[Snapshot, _Mapping]] = ...) -> None: ...

class GetSnapshotRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: str
    def __init__(self, snapshot: _Optional[str] = ...) -> None: ...

class GetSnapshotResponse(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: Snapshot
    def __init__(self, snapshot: _Optional[_Union[Snapshot, _Mapping]] = ...) -> None: ...

class ListSnapshotsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListSnapshotsResponse(_message.Message):
    __slots__ = ("snapshots",)
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedCompositeFieldContainer[Snapshot]
    def __init__(self, snapshots: _Optional[_Iterable[_Union[Snapshot, _Mapping]]] = ...) -> None: ...

class DeleteSnapshotRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: str
    def __init__(self, snapshot: _Optional[str] = ...) -> None: ...

class DeleteSnapshotResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TerminalSize(_message.Message):
    __slots__ = ("width", "height")
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    width: int
    height: int
    def __init__(self, width: _Optional[int] = ..., height: _Optional[int] = ...) -> None: ...

class ExecStart(_message.Message):
    __slots__ = ("sandbox", "command", "env", "working_dir", "tty", "stdin", "terminal_size")
    class EnvEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    WORKING_DIR_FIELD_NUMBER: _ClassVar[int]
    TTY_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SIZE_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    command: _containers.RepeatedScalarFieldContainer[str]
    env: _containers.ScalarMap[str, str]
    working_dir: str
    tty: bool
    stdin: bool
    terminal_size: TerminalSize
    def __init__(self, sandbox: _Optional[str] = ..., command: _Optional[_Iterable[str]] = ..., env: _Optional[_Mapping[str, str]] = ..., working_dir: _Optional[str] = ..., tty: _Optional[bool] = ..., stdin: _Optional[bool] = ..., terminal_size: _Optional[_Union[TerminalSize, _Mapping]] = ...) -> None: ...

class ExecRequest(_message.Message):
    __slots__ = ("start", "stdin", "resize", "close_stdin")
    START_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    CLOSE_STDIN_FIELD_NUMBER: _ClassVar[int]
    start: ExecStart
    stdin: bytes
    resize: TerminalSize
    close_stdin: bool
    def __init__(self, start: _Optional[_Union[ExecStart, _Mapping]] = ..., stdin: _Optional[bytes] = ..., resize: _Optional[_Union[TerminalSize, _Mapping]] = ..., close_stdin: _Optional[bool] = ...) -> None: ...

class Exit(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: int
    def __init__(self, code: _Optional[int] = ...) -> None: ...

class ExecResponse(_message.Message):
    __slots__ = ("stdout", "stderr", "exit")
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    EXIT_FIELD_NUMBER: _ClassVar[int]
    stdout: bytes
    stderr: bytes
    exit: Exit
    def __init__(self, stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ..., exit: _Optional[_Union[Exit, _Mapping]] = ...) -> None: ...

class AttachStart(_message.Message):
    __slots__ = ("sandbox", "stdin", "terminal_size")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_SIZE_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    stdin: bool
    terminal_size: TerminalSize
    def __init__(self, sandbox: _Optional[str] = ..., stdin: _Optional[bool] = ..., terminal_size: _Optional[_Union[TerminalSize, _Mapping]] = ...) -> None: ...

class AttachRequest(_message.Message):
    __slots__ = ("start", "stdin", "resize", "close_stdin")
    START_FIELD_NUMBER: _ClassVar[int]
    STDIN_FIELD_NUMBER: _ClassVar[int]
    RESIZE_FIELD_NUMBER: _ClassVar[int]
    CLOSE_STDIN_FIELD_NUMBER: _ClassVar[int]
    start: AttachStart
    stdin: bytes
    resize: TerminalSize
    close_stdin: bool
    def __init__(self, start: _Optional[_Union[AttachStart, _Mapping]] = ..., stdin: _Optional[bytes] = ..., resize: _Optional[_Union[TerminalSize, _Mapping]] = ..., close_stdin: _Optional[bool] = ...) -> None: ...

class Attached(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AttachResponse(_message.Message):
    __slots__ = ("stdout", "stderr", "exit", "attached")
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    EXIT_FIELD_NUMBER: _ClassVar[int]
    ATTACHED_FIELD_NUMBER: _ClassVar[int]
    stdout: bytes
    stderr: bytes
    exit: Exit
    attached: Attached
    def __init__(self, stdout: _Optional[bytes] = ..., stderr: _Optional[bytes] = ..., exit: _Optional[_Union[Exit, _Mapping]] = ..., attached: _Optional[_Union[Attached, _Mapping]] = ...) -> None: ...

class LogsRequest(_message.Message):
    __slots__ = ("sandbox", "follow", "tail_lines", "since")
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    TAIL_LINES_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    follow: bool
    tail_lines: int
    since: _timestamp_pb2.Timestamp
    def __init__(self, sandbox: _Optional[str] = ..., follow: _Optional[bool] = ..., tail_lines: _Optional[int] = ..., since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class LogsResponse(_message.Message):
    __slots__ = ("stream", "data", "timestamp")
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    stream: LogStream
    data: bytes
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, stream: _Optional[_Union[LogStream, str]] = ..., data: _Optional[bytes] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetStatsRequest(_message.Message):
    __slots__ = ("sandbox",)
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    sandbox: str
    def __init__(self, sandbox: _Optional[str] = ...) -> None: ...

class Stats(_message.Message):
    __slots__ = ("timestamp", "cpu_usage_nanos", "cpu_throttled_nanos", "memory_usage_bytes", "memory_limit_bytes", "memory_working_set_bytes", "pids", "memory_swap_usage_bytes")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CPU_USAGE_NANOS_FIELD_NUMBER: _ClassVar[int]
    CPU_THROTTLED_NANOS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_USAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_LIMIT_BYTES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_WORKING_SET_BYTES_FIELD_NUMBER: _ClassVar[int]
    PIDS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_SWAP_USAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    cpu_usage_nanos: int
    cpu_throttled_nanos: int
    memory_usage_bytes: int
    memory_limit_bytes: int
    memory_working_set_bytes: int
    pids: int
    memory_swap_usage_bytes: int
    def __init__(self, timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cpu_usage_nanos: _Optional[int] = ..., cpu_throttled_nanos: _Optional[int] = ..., memory_usage_bytes: _Optional[int] = ..., memory_limit_bytes: _Optional[int] = ..., memory_working_set_bytes: _Optional[int] = ..., pids: _Optional[int] = ..., memory_swap_usage_bytes: _Optional[int] = ...) -> None: ...

class GetStatsResponse(_message.Message):
    __slots__ = ("stats",)
    STATS_FIELD_NUMBER: _ClassVar[int]
    stats: Stats
    def __init__(self, stats: _Optional[_Union[Stats, _Mapping]] = ...) -> None: ...

class WatchRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WatchResponse(_message.Message):
    __slots__ = ("type", "timestamp", "sandbox")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_FIELD_NUMBER: _ClassVar[int]
    type: EventType
    timestamp: _timestamp_pb2.Timestamp
    sandbox: Sandbox
    def __init__(self, type: _Optional[_Union[EventType, str]] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., sandbox: _Optional[_Union[Sandbox, _Mapping]] = ...) -> None: ...

class GetNodeRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NodeStatus(_message.Message):
    __slots__ = ("policy", "runtime", "capacity", "allocatable", "allocated", "sandbox_count", "queued_count")
    POLICY_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    ALLOCATABLE_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_FIELD_NUMBER: _ClassVar[int]
    SANDBOX_COUNT_FIELD_NUMBER: _ClassVar[int]
    QUEUED_COUNT_FIELD_NUMBER: _ClassVar[int]
    policy: str
    runtime: str
    capacity: ResourceList
    allocatable: ResourceList
    allocated: ResourceList
    sandbox_count: int
    queued_count: int
    def __init__(self, policy: _Optional[str] = ..., runtime: _Optional[str] = ..., capacity: _Optional[_Union[ResourceList, _Mapping]] = ..., allocatable: _Optional[_Union[ResourceList, _Mapping]] = ..., allocated: _Optional[_Union[ResourceList, _Mapping]] = ..., sandbox_count: _Optional[int] = ..., queued_count: _Optional[int] = ...) -> None: ...

class GetNodeResponse(_message.Message):
    __slots__ = ("node",)
    NODE_FIELD_NUMBER: _ClassVar[int]
    node: NodeStatus
    def __init__(self, node: _Optional[_Union[NodeStatus, _Mapping]] = ...) -> None: ...

class GetVersionRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetVersionResponse(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    def __init__(self, version: _Optional[str] = ...) -> None: ...
