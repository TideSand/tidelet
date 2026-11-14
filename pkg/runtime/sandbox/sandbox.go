// Package sandbox holds the internal model of a sandbox shared by the
// manager, runtimes, policies and the store. The gRPC API in
// api/tidelet/v1 is converted to and from these types at the server
// boundary.
package sandbox

import (
	"time"

	"github.com/TideSand/tidelet/pkg/resource"
)

// NetworkMode selects how a sandbox is attached to the network.
type NetworkMode string

// Network modes.
const (
	// NetworkNone is a private namespace with only loopback.
	NetworkNone NetworkMode = "none"
	// NetworkHost shares the host's namespace.
	NetworkHost NetworkMode = "host"
	// NetworkBridge is a private namespace attached to the CNI bridge.
	NetworkBridge NetworkMode = "bridge"
	// NetworkJoin joins the namespace at Spec.NetworkNamespace.
	NetworkJoin NetworkMode = "join"
)

// NetworkStatus describes a sandbox's network attachment.
type NetworkStatus struct {
	// NamespacePath is the sandbox's own network namespace, if it owns one.
	NamespacePath string `json:"namespacePath,omitempty"`
	// IPs are the CNI-assigned addresses.
	IPs []string `json:"ips,omitempty"`
}

// RuntimeClass selects the isolation technology.
type RuntimeClass string

// Runtime classes.
const (
	RuntimeContainer RuntimeClass = "container"
	RuntimeMicroVM   RuntimeClass = "microvm"
)

// Resources follows Kubernetes semantics: requests are what admission
// accounts, limits are what cgroups enforce.
type Resources struct {
	Requests resource.Set `json:"requests"`
	Limits   resource.Set `json:"limits"`
}

// Mount is a bind mount from the host into the sandbox.
type Mount struct {
	HostPath      string `json:"hostPath"`
	ContainerPath string `json:"containerPath"`
	ReadOnly      bool   `json:"readOnly,omitempty"`
}

// Spec describes a sandbox to create.
type Spec struct {
	Image      string            `json:"image"`
	Command    []string          `json:"command,omitempty"`
	Args       []string          `json:"args,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	WorkingDir string            `json:"workingDir,omitempty"`
	Resources  Resources         `json:"resources"`
	Network    NetworkMode       `json:"network"`
	Runtime    RuntimeClass      `json:"runtime"`
	Mounts     []Mount           `json:"mounts,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	// Stdin keeps the main process's stdin open for Attach.
	Stdin bool `json:"stdin,omitempty"`
	// TTY allocates a pseudo-terminal for the main process.
	TTY bool `json:"tty,omitempty"`
	// Priority is advisory information for the policy.
	Priority int32 `json:"priority,omitempty"`
	// NetworkNamespace is the namespace to join under NetworkJoin.
	NetworkNamespace string `json:"networkNamespace,omitempty"`
	// Hostname inside the sandbox. Defaults to the name or ID.
	Hostname string `json:"hostname,omitempty"`
}

// Hostname returns the effective hostname of the sandbox.
func (s *Sandbox) Hostname() string {
	switch {
	case s.Spec.Hostname != "":
		return s.Spec.Hostname
	case s.Name != "":
		return s.Name
	default:
		return s.ID[:min(12, len(s.ID))]
	}
}

// Phase is the coarse lifecycle state of a sandbox.
type Phase string

// Sandbox phases.
const (
	// PhasePending: admission or runtime creation in progress.
	PhasePending Phase = "Pending"
	// PhaseQueued: accepted by the policy, not yet created, no allocation.
	PhaseQueued Phase = "Queued"
	// PhaseCreated: created in the runtime, not started.
	PhaseCreated Phase = "Created"
	PhaseRunning Phase = "Running"
	// PhaseFrozen: stopped by the cgroup freezer, resumable.
	PhaseFrozen Phase = "Frozen"
	PhaseExited Phase = "Exited"
	PhaseFailed Phase = "Failed"
)

// Terminal reports whether the phase is final.
func (p Phase) Terminal() bool { return p == PhaseExited || p == PhaseFailed }

// HasRuntime reports whether the runtime holds resources for the sandbox.
func (p Phase) HasRuntime() bool {
	return p == PhaseCreated || p == PhaseRunning || p == PhaseFrozen
}

// Status is the observed state of a sandbox.
type Status struct {
	Phase      Phase      `json:"phase"`
	Message    string     `json:"message,omitempty"`
	ExitCode   *int32     `json:"exitCode,omitempty"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	// Allocated is the effective resources after the policy rewrote the
	// spec: what admission accounted and what cgroups enforce. Empty while
	// queued.
	Allocated Resources `json:"allocated"`
	// Policy is the name of the policy that admitted the sandbox.
	Policy string `json:"policy,omitempty"`
	// Network is filled by the runtime at creation.
	Network *NetworkStatus `json:"network,omitempty"`
}

// Sandbox is a sandbox object.
type Sandbox struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	Spec      Spec      `json:"spec"`
	Status    Status    `json:"status"`
}

// Stats is a point-in-time resource usage sample for one sandbox.
type Stats struct {
	Timestamp             time.Time
	CPUUsageNanos         uint64
	CPUThrottledNanos     uint64
	MemoryUsageBytes      uint64
	MemoryLimitBytes      uint64
	MemoryWorkingSetBytes uint64
	// MemorySwapUsageBytes is the swap the sandbox's cgroup holds.
	MemorySwapUsageBytes uint64
	Pids                 uint64
	// Cumulative "some" stall time of the sandbox's cgroup from its
	// cpu.pressure, memory.pressure and io.pressure files, in microseconds:
	// time during which at least one of its tasks was runnable but waited
	// for a CPU, waited for memory, or waited for the disk. Two samples
	// give the share of the interval the sandbox spent stalled, which is
	// the slowdown its work suffered, measured directly. Zero when the
	// kernel or the runtime does not expose it.
	CPUSomeStallUSec    uint64
	MemorySomeStallUSec uint64
	IOSomeStallUSec     uint64
	// BandwidthBytesPerSec is the sandbox's memory traffic over the interval
	// since the previous sample, in bytes per second, as the daemon's
	// traffic meter attributed it (host.Traffic); zero without a meter.
	BandwidthBytesPerSec uint64
}

// NodeStatus describes the node's capacity as seen through the policy.
type NodeStatus struct {
	Policy      string
	Runtime     string
	Capacity    resource.Set
	Allocatable resource.Set
	Allocated   resource.Set
	Sandboxes   int
	Queued      int
}

// TerminalSize is a pseudo-terminal window size.
type TerminalSize struct {
	Width  uint32
	Height uint32
}

// EventType classifies lifecycle events.
type EventType string

// Event types.
const (
	EventQueued  EventType = "queued"
	EventCreated EventType = "created"
	EventStarted EventType = "started"
	EventFrozen  EventType = "frozen"
	EventThawed  EventType = "thawed"
	EventExited  EventType = "exited"
	EventFailed  EventType = "failed"
	EventDeleted EventType = "deleted"
	// EventPrefetched: the sandbox's swapped-out memory was brought back
	// into RAM (a policy's Prefetch action completed).
	EventPrefetched EventType = "prefetched"
)

// Event is a sandbox lifecycle event.
type Event struct {
	Type    EventType
	Time    time.Time
	Sandbox Sandbox
}
