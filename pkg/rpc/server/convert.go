package server

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/TideSand/tidelet/api/tidelet/v1alpha1"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

func toResourceList(s resource.Set) *pb.ResourceList {
	return &pb.ResourceList{CpuMillicores: int64(s.CPU), MemoryBytes: int64(s.Memory)}
}

func fromResourceList(r *pb.ResourceList) resource.Set {
	if r == nil {
		return resource.Set{}
	}
	return resource.Set{CPU: resource.Millicores(r.GetCpuMillicores()), Memory: resource.Bytes(r.GetMemoryBytes())}
}

func toResources(r sandbox.Resources) *pb.Resources {
	return &pb.Resources{Requests: toResourceList(r.Requests), Limits: toResourceList(r.Limits)}
}

func fromResources(r *pb.Resources) sandbox.Resources {
	if r == nil {
		return sandbox.Resources{}
	}
	return sandbox.Resources{Requests: fromResourceList(r.GetRequests()), Limits: fromResourceList(r.GetLimits())}
}

func optTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func fromSpec(s *pb.SandboxSpec) sandbox.Spec {
	if s == nil {
		return sandbox.Spec{}
	}
	out := sandbox.Spec{
		Image:      s.GetImage(),
		Command:    s.GetCommand(),
		Args:       s.GetArgs(),
		Env:        s.GetEnv(),
		WorkingDir: s.GetWorkingDir(),
		Resources:  fromResources(s.GetResources()),
		Labels:     s.GetLabels(),
		Stdin:      s.GetStdin(),
		TTY:        s.GetTty(),
		Priority:   s.GetPriority(),
		Hostname:   s.GetHostname(),
	}
	out.NetworkNamespace = s.GetNetworkNamespace()
	switch s.GetNetwork() {
	case pb.NetworkMode_NETWORK_MODE_HOST:
		out.Network = sandbox.NetworkHost
	case pb.NetworkMode_NETWORK_MODE_BRIDGE:
		out.Network = sandbox.NetworkBridge
	case pb.NetworkMode_NETWORK_MODE_JOIN:
		out.Network = sandbox.NetworkJoin
	case pb.NetworkMode_NETWORK_MODE_NONE, pb.NetworkMode_NETWORK_MODE_UNSPECIFIED:
		out.Network = sandbox.NetworkNone
	default:
		out.Network = sandbox.NetworkMode(s.GetNetwork().String())
	}
	switch s.GetRuntime() {
	case pb.RuntimeClass_RUNTIME_CLASS_MICROVM:
		out.Runtime = sandbox.RuntimeMicroVM
	case pb.RuntimeClass_RUNTIME_CLASS_CONTAINER, pb.RuntimeClass_RUNTIME_CLASS_UNSPECIFIED:
		out.Runtime = sandbox.RuntimeContainer
	default:
		out.Runtime = sandbox.RuntimeClass(s.GetRuntime().String())
	}
	for _, m := range s.GetMounts() {
		out.Mounts = append(out.Mounts, sandbox.Mount{
			HostPath: m.GetHostPath(), ContainerPath: m.GetContainerPath(), ReadOnly: m.GetReadOnly(),
		})
	}
	return out
}

func toSpec(s sandbox.Spec) *pb.SandboxSpec {
	out := &pb.SandboxSpec{
		Image:            s.Image,
		Command:          s.Command,
		Args:             s.Args,
		Env:              s.Env,
		WorkingDir:       s.WorkingDir,
		Resources:        toResources(s.Resources),
		Labels:           s.Labels,
		Stdin:            s.Stdin,
		Tty:              s.TTY,
		Priority:         s.Priority,
		NetworkNamespace: s.NetworkNamespace,
		Hostname:         s.Hostname,
	}
	switch s.Network {
	case sandbox.NetworkHost:
		out.Network = pb.NetworkMode_NETWORK_MODE_HOST
	case sandbox.NetworkBridge:
		out.Network = pb.NetworkMode_NETWORK_MODE_BRIDGE
	case sandbox.NetworkJoin:
		out.Network = pb.NetworkMode_NETWORK_MODE_JOIN
	default:
		out.Network = pb.NetworkMode_NETWORK_MODE_NONE
	}
	switch s.Runtime {
	case sandbox.RuntimeMicroVM:
		out.Runtime = pb.RuntimeClass_RUNTIME_CLASS_MICROVM
	default:
		out.Runtime = pb.RuntimeClass_RUNTIME_CLASS_CONTAINER
	}
	for _, m := range s.Mounts {
		out.Mounts = append(out.Mounts, &pb.Mount{HostPath: m.HostPath, ContainerPath: m.ContainerPath, ReadOnly: m.ReadOnly})
	}
	return out
}

var phaseToProto = map[sandbox.Phase]pb.Phase{
	sandbox.PhasePending: pb.Phase_PHASE_PENDING,
	sandbox.PhaseQueued:  pb.Phase_PHASE_QUEUED,
	sandbox.PhaseCreated: pb.Phase_PHASE_CREATED,
	sandbox.PhaseRunning: pb.Phase_PHASE_RUNNING,
	sandbox.PhaseFrozen:  pb.Phase_PHASE_FROZEN,
	sandbox.PhaseExited:  pb.Phase_PHASE_EXITED,
	sandbox.PhaseFailed:  pb.Phase_PHASE_FAILED,
}

func toSandbox(sb sandbox.Sandbox) *pb.Sandbox {
	st := &pb.SandboxStatus{
		Phase:      phaseToProto[sb.Status.Phase],
		Message:    sb.Status.Message,
		ExitCode:   sb.Status.ExitCode,
		StartedAt:  optTime(sb.Status.StartedAt),
		FinishedAt: optTime(sb.Status.FinishedAt),
		Allocated:  toResources(sb.Status.Allocated),
		Policy:     sb.Status.Policy,
	}
	if n := sb.Status.Network; n != nil {
		st.Network = &pb.NetworkStatus{NamespacePath: n.NamespacePath, IpAddresses: n.IPs}
	}
	return &pb.Sandbox{
		Id:        sb.ID,
		Name:      sb.Name,
		CreatedAt: timestamppb.New(sb.CreatedAt),
		Spec:      toSpec(sb.Spec),
		Status:    st,
	}
}

func toStats(s sandbox.Stats) *pb.Stats {
	return &pb.Stats{
		Timestamp:             timestamppb.New(s.Timestamp),
		CpuUsageNanos:         s.CPUUsageNanos,
		CpuThrottledNanos:     s.CPUThrottledNanos,
		MemoryUsageBytes:      s.MemoryUsageBytes,
		MemoryLimitBytes:      s.MemoryLimitBytes,
		MemoryWorkingSetBytes: s.MemoryWorkingSetBytes,
		MemorySwapUsageBytes:  s.MemorySwapUsageBytes,
		Pids:                  s.Pids,
	}
}

var eventToProto = map[sandbox.EventType]pb.EventType{
	sandbox.EventQueued:  pb.EventType_EVENT_TYPE_QUEUED,
	sandbox.EventCreated: pb.EventType_EVENT_TYPE_CREATED,
	sandbox.EventStarted: pb.EventType_EVENT_TYPE_STARTED,
	sandbox.EventFrozen:  pb.EventType_EVENT_TYPE_FROZEN,
	sandbox.EventThawed:  pb.EventType_EVENT_TYPE_THAWED,
	sandbox.EventExited:  pb.EventType_EVENT_TYPE_EXITED,
	sandbox.EventFailed:  pb.EventType_EVENT_TYPE_FAILED,
	sandbox.EventDeleted: pb.EventType_EVENT_TYPE_DELETED,
}

func toEvent(ev sandbox.Event) *pb.WatchResponse {
	return &pb.WatchResponse{Type: eventToProto[ev.Type], Timestamp: timestamppb.New(ev.Time), Sandbox: toSandbox(ev.Sandbox)}
}

func termSize(t *pb.TerminalSize) *sandbox.TerminalSize {
	if t == nil {
		return nil
	}
	return &sandbox.TerminalSize{Width: t.GetWidth(), Height: t.GetHeight()}
}
