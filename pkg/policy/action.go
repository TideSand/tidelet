package policy

import "time"

// Action is something the policy asks the manager to do. The manager
// executes actions asynchronously and reports the outcome through events.
type Action interface {
	// Target is the ID of the sandbox the action applies to.
	Target() string
}

// Dispatch creates a queued sandbox with the given allocation and starts
// it if a start was requested.
type Dispatch struct {
	ID         string
	Allocation Allocation
}

// Update changes a live sandbox's allocation and cgroup settings.
type Update struct {
	ID         string
	Allocation Allocation
}

// Freeze stops scheduling a running sandbox via the cgroup freezer. The
// sandbox keeps its allocation.
type Freeze struct {
	ID string
}

// Thaw resumes a frozen sandbox.
type Thaw struct {
	ID string
}

// Fail gives up on a queued sandbox.
type Fail struct {
	ID     string
	Reason string
}

// Target implements Action.
func (a Dispatch) Target() string { return a.ID }

// Target implements Action.
func (a Update) Target() string { return a.ID }

// Target implements Action.
func (a Freeze) Target() string { return a.ID }

// Target implements Action.
func (a Thaw) Target() string { return a.ID }

// Target implements Action.
func (a Fail) Target() string { return a.ID }

// Hold keeps back a model response the LLM proxy has just received for a
// sandbox, so the agent does not get it, and so does not start its tool
// calls, until a Release. It is meaningful only among the actions returned
// for the ModelResponseReady event of that call; the proxy forwards the
// response at once otherwise. The proxy forwards a held response on its
// own after its maximum defer time and reports that as a forced delivery.
type Hold struct {
	ID   string
	Call string
}

// Release delivers a held model response.
type Release struct {
	ID   string
	Call string
}

// Prefetch brings a sandbox's swapped-out memory back into RAM, so the
// tool calls it is about to run do not start by faulting it in. The
// manager reports completion with a sandbox.EventPrefetched event.
type Prefetch struct {
	ID string
}

// Reclaim asks the kernel to reclaim up to Bytes of a sandbox's memory
// (cgroup v2 memory.reclaim), swapping out what it can: for a sandbox that
// is waiting on the model and will not touch its memory for a while.
type Reclaim struct {
	ID    string
	Bytes int64
}

// Target implements Action.
func (a Hold) Target() string { return a.ID }

// Rollback undoes the tool call a sandbox is running: its processes are
// killed and its filesystem returned to the checkpoint taken before the
// response was delivered. The outcome comes back as a ModelRolledBack
// event. The policy is expected to Redo the call later, and to Hold the
// agent's next response until then, since the agent goes on with a failed
// result it will never see corrected.
type Rollback struct {
	ID   string
	Call string
}

// Restore resumes a sandbox a whole-sandbox Rollback stopped, from the
// checkpoint taken before the call's response was delivered: every process
// continues from that moment, and the agent, whose model connection is gone,
// sends the call's request again, so the proxy can deliver the response once
// more and the agent runs the call itself. The outcome comes back as a
// ModelRestored event.
type Restore struct {
	ID   string
	Call string
}

// Target implements Action.
func (a Restore) Target() string { return a.ID }

// Redo runs a rolled-back tool call's shell commands again inside the
// sandbox, in order, each with its timeout, and reports a ModelRedone event
// when they are done.
type Redo struct {
	ID         string
	Call       string
	Commands   []RedoCommand
	WorkingDir string
}

// RedoCommand is one shell command of a Redo.
type RedoCommand struct {
	Command string
	Timeout time.Duration
}

// Target implements Action.
func (a Rollback) Target() string { return a.ID }

// Target implements Action.
func (a Redo) Target() string { return a.ID }

// Target implements Action.
func (a Release) Target() string { return a.ID }

// Target implements Action.
func (a Prefetch) Target() string { return a.ID }

// Target implements Action.
func (a Reclaim) Target() string { return a.ID }
