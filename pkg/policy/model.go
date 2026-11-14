package policy

import "time"

// ModelEventKind says what happened to a model call.
type ModelEventKind int

// Model event kinds.
const (
	// ModelRequestStarted: the agent sent a model request. Until the
	// response is delivered the sandbox is waiting, not computing.
	ModelRequestStarted ModelEventKind = iota
	// ModelResponseReady: the proxy holds the complete response. The policy
	// may return a Hold for it; otherwise it is forwarded at once.
	ModelResponseReady
	// ModelResponseDelivered: the response reached the agent, which now
	// runs the tool calls it carries, if any.
	ModelResponseDelivered
	// ModelRolledBack: a Rollback action ran. Killed says how many of the
	// tool call's processes it killed: none means the call had finished
	// before the rollback and nothing was undone.
	ModelRolledBack
	// ModelRedone: a Redo action ran its commands to the end (Status is
	// the last command's exit code, -1 when one could not be run).
	ModelRedone
	// ModelRestored: a Restore action resumed a sandbox a whole-sandbox
	// rollback had stopped (Status 0), or could not (Status -1).
	ModelRestored
)

// String implements fmt.Stringer.
func (k ModelEventKind) String() string {
	switch k {
	case ModelRequestStarted:
		return "request-started"
	case ModelResponseReady:
		return "response-ready"
	case ModelResponseDelivered:
		return "response-delivered"
	}
	return "unknown"
}

// ModelEvent is what the LLM proxy saw of one model call. The proxy never
// changes a request or a response; it only reports them and, when told,
// delays a response.
type ModelEvent struct {
	Kind ModelEventKind
	Time time.Time
	// Sandbox is the ID of the sandbox that made the call.
	Sandbox string
	// Call identifies the model call across its events.
	Call string
	// Status is the upstream HTTP status (response events).
	Status int
	// ToolCalls is the number of tool_use blocks in the response, ToolNames
	// their tool names in order and ToolInputs their inputs as JSON, in the
	// same order (response events).
	ToolCalls  int
	ToolNames  []string
	ToolInputs []string
	// Killed is how many processes a rollback killed (rolled-back events).
	Killed int
	// Whole says the rollback stopped the whole sandbox (rolled-back events):
	// it holds nothing until a Restore, which the policy owes it.
	Whole bool
	// StopReason is the response's stop reason, when the body had one.
	StopReason string
	// Held is how long the response was kept back before delivery, Forced
	// whether the proxy delivered it on its own because the maximum defer
	// time passed (delivered events).
	Held   time.Duration
	Forced bool
	// Body is the hex SHA-256 of the request body (response events), so the
	// call can be matched with the upstream's own record of the request.
	Body string
}

// Checkpointer is implemented by policies that undo tool calls. Before a
// response carrying tool calls is delivered, and after any hold, the
// manager asks CheckpointBefore; when it says yes, the sandbox's filesystem
// is checkpointed first (see runtime.Runtime.Checkpoint), and the time that
// takes counts as held, not as execution. Rollback and Redo actions build
// on the checkpoint.
type Checkpointer interface {
	CheckpointBefore(ev ModelEvent) bool
}

// CheckpointObserver is implemented by checkpointing policies that want to
// know what each checkpoint they asked for cost: the manager reports the
// dump's duration, or the error that ended it, once it is over.
type CheckpointObserver interface {
	CheckpointDone(sandbox, call string, took time.Duration, err error)
}

// ModelAware is implemented by policies that schedule on model calls. The
// manager calls OnModelEvent for every event the LLM proxy reports, under
// the same serialisation as the other policy calls. Policies without it
// never see model events, and every response is forwarded at once.
type ModelAware interface {
	OnModelEvent(ev ModelEvent, view View) []Action
}
