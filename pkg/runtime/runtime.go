// Package runtime defines the interface between the manager and the
// isolation technology that actually runs a sandbox. Runtimes enforce
// what they are told; they never interpret requests or limits.
package runtime

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// ErrNotFound is returned when the runtime has no record of a sandbox.
var ErrNotFound = errors.New("sandbox not found in runtime")

// ErrNotRunning is returned by operations that need a live process.
var ErrNotRunning = errors.New("sandbox is not running")

// ErrNoStdin is returned when attaching with stdin to a sandbox created
// without it.
var ErrNoStdin = errors.New("sandbox was created without stdin")

// Cgroup holds the Linux cgroup v2 settings a runtime applies. Zero means
// "do not set". Policies produce these; runtimes enforce them.
type Cgroup struct {
	// CPUQuotaUS and CPUPeriodUS map to cpu.max. Quota 0 means unlimited.
	CPUQuotaUS  int64  `json:"cpuQuotaUs,omitempty"`
	CPUPeriodUS uint64 `json:"cpuPeriodUs,omitempty"`
	// CPUShares is the cgroup v1 style weight (2..262144); runtimes
	// translate it to cpu.weight. 0 means default.
	CPUShares uint64 `json:"cpuShares,omitempty"`
	// MemoryLimitBytes maps to memory.max. 0 means unlimited.
	MemoryLimitBytes int64 `json:"memoryLimitBytes,omitempty"`
	// MemoryHighBytes maps to memory.high, the soft limit the kernel
	// reclaims against before memory.max bites. 0 means unset.
	MemoryHighBytes int64 `json:"memoryHighBytes,omitempty"`
	// MemorySwapLimitBytes maps to memory.swap.max. 0 means "do not set",
	// a negative value disables swap for the sandbox.
	MemorySwapLimitBytes int64 `json:"memorySwapLimitBytes,omitempty"`
	// MemoryLowBytes maps to memory.low, the amount of the sandbox's memory
	// the kernel reclaims from only when it has no unprotected memory left
	// elsewhere. 0 means "do not set"; MemoryLowMax protects everything the
	// sandbox holds; MemoryLowNone writes an explicit 0 (no protection),
	// which is how a protection set earlier is taken away again.
	MemoryLowBytes int64 `json:"memoryLowBytes,omitempty"`
}

// Sentinels for Cgroup.MemoryLowBytes.
const (
	// MemoryLowMax writes "max" to memory.low: all of the sandbox's memory
	// is protected from reclaim while other cgroups have unprotected memory.
	MemoryLowMax int64 = -1
	// MemoryLowNone writes "0" to memory.low: no protection.
	MemoryLowNone int64 = -2
)

// State is the runtime's view of a sandbox process.
type State string

// Runtime process states.
const (
	StateCreated State = "created"
	StateRunning State = "running"
	StatePaused  State = "paused"
	StateStopped State = "stopped"
	StateUnknown State = "unknown"
)

// Status is the runtime's observed status for a sandbox.
type Status struct {
	State    State
	ExitCode int32
	ExitedAt time.Time
	Pid      uint32
}

// ExitResult is delivered when a sandbox's main process exits.
type ExitResult struct {
	ExitCode int32
	ExitedAt time.Time
	Err      error
}

// ExecOptions parameterise a command run inside a sandbox.
type ExecOptions struct {
	Command    []string
	Env        map[string]string
	WorkingDir string
	// Stdin, when non-nil, is copied to the process until EOF, at which
	// point the process's stdin is closed.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// TTY allocates a pseudo-terminal; output then arrives on Stdout only.
	TTY          bool
	TerminalSize *sandbox.TerminalSize
	// Resize delivers window size changes while the process runs. The
	// caller closes it when done.
	Resize <-chan sandbox.TerminalSize
}

// CheckpointInfo describes a checkpoint just taken.
type CheckpointInfo struct {
	// Entries is the number of filesystem entries in the writable layer.
	Entries int
	Took    time.Duration
}

// RollbackInfo is the outcome of a rollback.
type RollbackInfo struct {
	// Killed is how many processes were killed; 0 means nothing was undone.
	Killed int
	// Restored is how many paths were put back.
	Restored int
	Took     time.Duration
}

// ContainerCheckpointer is implemented by runtimes that checkpoint a sandbox as a whole:
// processes, memory and filesystem. A rollback stops the sandbox entirely (it holds no CPU
// or memory) and leaves it at its latest checkpoint; a restore resumes every process from
// that point. ContainerCheckpoints says whether sandboxes are created checkpointable.
type ContainerCheckpointer interface {
	ContainerCheckpoints() bool
	CheckpointContainer(ctx context.Context, id string) (CheckpointInfo, error)
	RollbackContainer(ctx context.Context, id string) (RollbackInfo, error)
	RestoreContainer(ctx context.Context, id string) (time.Duration, error)
	// DiscardContainerCheckpoint drops the sandbox's whole checkpoint: the call it was taken
	// for is over, and a rollback must never go back further than the call in flight.
	DiscardContainerCheckpoint(id string)
	// AbandonContainer gives up on a sandbox that could not be restored: whatever waits on
	// its execs is told they were killed.
	AbandonContainer(id string)
}

// SnapshotInfo describes a snapshot just taken.
type SnapshotInfo struct {
	// SharedBytes is the memory held in the snapshot's memory files: what every sandbox
	// created from it maps without copying. Files shared with the snapshot it descends
	// from count here too.
	SharedBytes uint64
	// PrivateBytes is the memory held in the snapshot's image itself, which every sandbox
	// created from it gets a copy of.
	PrivateBytes uint64
	// Rebased says the memory files were written anew for this snapshot (always for the
	// first snapshot of a lineage; later when the changes since outgrew a share of them).
	Rebased bool
	Took    time.Duration
}

// Snapshotter is implemented by runtimes that can snapshot a running sandbox (processes,
// memory, filesystem) and create new sandboxes from the snapshot: Create with a spec that
// names a snapshot restores it instead of starting the image's command, and Start resumes
// the restored processes. The snapshotted sandbox keeps running, undisturbed. Sandboxes
// created from one snapshot share its memory copy-on-write.
type Snapshotter interface {
	// Snapshots says whether sandboxes are created snapshottable.
	Snapshots() bool
	// Snapshot takes a snapshot of a running sandbox under the given ID.
	Snapshot(ctx context.Context, id, snapshotID string) (SnapshotInfo, error)
	// HasSnapshot says whether the snapshot's data is still there (it lives in memory and
	// does not survive a reboot).
	HasSnapshot(snapshotID string) bool
	// DeleteSnapshot removes a snapshot. No sandbox created from it may be left.
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// ExecResult is the outcome of an exec.
type ExecResult struct {
	ExitCode int32
}

// AttachOptions parameterise an attachment to the main process.
type AttachOptions struct {
	// Stdin, when non-nil, is copied to the process. A clean EOF closes
	// the process's stdin; any other read error merely detaches.
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	// Attached, if non-nil, is called once output is being forwarded, so a
	// caller can start the process without missing its first bytes.
	Attached     func()
	TerminalSize *sandbox.TerminalSize
	Resize       <-chan sandbox.TerminalSize
}

// Runtime runs sandboxes. Implementations must be safe for concurrent use.
type Runtime interface {
	// Name identifies the runtime in status output.
	Name() string
	// Validate rejects specs this runtime cannot honour (an unavailable
	// runtime class or network mode, for example).
	Validate(spec sandbox.Spec) error
	// Create pulls the image if needed and creates the sandbox with the
	// given cgroup settings, without starting its main process. It fills
	// sb.Status.Network.
	Create(ctx context.Context, sb *sandbox.Sandbox, cg Cgroup) error
	// Start starts the main process of a created sandbox.
	Start(ctx context.Context, id string) error
	// Wait blocks until the sandbox's main process exits or ctx is done.
	Wait(ctx context.Context, id string) (ExitResult, error)
	// Stop sends SIGTERM and, after timeout, SIGKILL. It returns once the
	// process has exited. Stopping a sandbox that is not running is not
	// an error.
	Stop(ctx context.Context, id string, timeout time.Duration) error
	// Delete removes all runtime resources for the sandbox, killing it if
	// necessary. Deleting an unknown sandbox is not an error.
	Delete(ctx context.Context, id string) error
	// Update changes the cgroup settings of a live sandbox.
	Update(ctx context.Context, id string, cg Cgroup) error
	// Freeze stops scheduling the sandbox's processes via the cgroup
	// freezer. Memory stays allocated.
	Freeze(ctx context.Context, id string) error
	// Prefetch brings the sandbox's swapped-out anonymous memory back into
	// RAM, returning how many bytes it asked the kernel to swap in.
	Prefetch(ctx context.Context, id string) (int64, error)
	// Reclaim asks the kernel to reclaim up to the given bytes of the
	// sandbox's memory (cgroup v2 memory.reclaim), swapping out what it can.
	Reclaim(ctx context.Context, id string, bytes int64) error
	// Thaw resumes a frozen sandbox.
	Thaw(ctx context.Context, id string) error
	// Status returns the runtime's view of the sandbox.
	Status(ctx context.Context, id string) (Status, error)
	// Stats samples resource usage. The sandbox must be created.
	Stats(ctx context.Context, id string) (sandbox.Stats, error)
	// Exec runs a command inside a running sandbox and waits for it.
	Exec(ctx context.Context, id string, opts ExecOptions) (ExecResult, error)
	// Checkpoint records the sandbox's writable layer, so that Rollback can
	// return the filesystem to this point. It replaces the sandbox's earlier
	// checkpoint. Quick: proportional to what the sandbox has written, not
	// to its image, and incremental over the earlier checkpoint.
	Checkpoint(ctx context.Context, id string) (CheckpointInfo, error)
	// Rollback kills every process the sandbox started after its latest
	// checkpoint (its tool call in flight) and, when there were any,
	// returns the writable layer to the checkpoint. Processes that were
	// already running at the checkpoint are left alone. It reports how
	// many processes it killed: none means the call had already finished,
	// and nothing was undone.
	Rollback(ctx context.Context, id string) (RollbackInfo, error)
	// Attach forwards the main process's streams. It returns nil when the
	// process exits, or ctx.Err() when the caller gives up.
	Attach(ctx context.Context, id string, opts AttachOptions) error
	// LogPath returns the CRI-format log file for the sandbox.
	LogPath(id string) string
	// List returns the IDs of all sandboxes the runtime knows about that
	// Tidelet created.
	List(ctx context.Context) ([]string, error)
	// Close releases the runtime's connections.
	Close() error
}
