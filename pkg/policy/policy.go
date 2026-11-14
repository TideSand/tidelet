// Package policy defines the overcommit policy contract. A policy is the
// one place in Tidelet that decides how much the node promises, which
// sandboxes run, and how running sandboxes are reshaped under pressure.
//
// The manager owns exactly one Policy. On every call it hands the policy a
// [View], a snapshot of the whole node, and the policy answers with a
// [Decision] (for admission) or a list of [Action]s (for everything
// else). The policy never touches the runtime or the sandbox table; the
// manager executes what it is told and reports outcomes back as events.
//
// Calls into a policy are serialised by the manager, so implementations
// need no locking. A policy may keep state across calls.
//
// To write a policy, implement [Policy], register it in package
// registry, and use only this package plus resource, sandbox and
// runtime. The strict policy is the minimal example.
package policy

import (
	"time"

	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Policy is the overcommit strategy the manager runs.
//
// A policy may use as much or as little of this as it likes. The strict
// policy only implements Admit and Allocatable; smarter policies queue
// sandboxes, react to usage samples, reshape cgroups, and freeze and thaw.
type Policy interface {
	// Name identifies the policy in status output.
	Name() string
	// Allocatable is the capacity the node advertises as schedulable.
	Allocatable(view View) resource.Set
	// Admit decides what to do with a new sandbox.
	Admit(sb sandbox.Sandbox, view View) Decision
	// OnEvent is called after every lifecycle change.
	OnEvent(ev sandbox.Event, view View) []Action
	// OnSample is called with every usage sample.
	OnSample(s Sample, view View) []Action
	// OnTick is called periodically.
	OnTick(now time.Time, view View) []Action
}

// DecisionKind is the outcome of admission.
type DecisionKind int

// Decision kinds.
const (
	// Reject: the node cannot take the sandbox now. The caller gets a
	// resource-exhausted error and no record is kept.
	Reject DecisionKind = iota
	// Invalid: the spec violates the policy's rules (for example requests
	// differ from limits under strict). The caller gets an invalid-argument
	// error.
	Invalid
	// Queue: accepted but not created yet. The sandbox holds no allocation
	// until the policy dispatches it.
	Queue
	// Run: create it now with the given allocation.
	Run
)

// Decision is the result of Admit.
type Decision struct {
	Kind   DecisionKind
	Reason string
	// Allocation is required for Run and ignored otherwise.
	Allocation Allocation
}

// Allocation is what a sandbox is granted: Requests are accounted against
// allocatable, Limits are what it is capped at, Cgroup is the concrete
// enforcement the runtime applies.
type Allocation struct {
	Requests resource.Set
	Limits   resource.Set
	Cgroup   runtime.Cgroup
}
