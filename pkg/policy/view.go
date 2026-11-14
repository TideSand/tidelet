package policy

import (
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// View is everything the manager knows, handed to the policy on every
// call. It is a snapshot; mutating it has no effect.
type View struct {
	Now      time.Time
	Capacity resource.Set
	Reserved resource.Set
	// Allocated is the sum of requests of sandboxes holding an allocation.
	Allocated resource.Set
	// Node is the most recent host-wide sample, zero before the first one.
	Node host.Sample
	// PoolCgroup is the absolute path of the cgroup every sandbox lives
	// under (a sandbox's own cgroup is PoolCgroup/<id>), empty when the
	// daemon runs without a pool cgroup.
	PoolCgroup string
	// Sandboxes are all known sandboxes, oldest first.
	Sandboxes []SandboxView
}

// SandboxView is one sandbox as the policy sees it.
type SandboxView struct {
	Sandbox sandbox.Sandbox
	Cgroup  runtime.Cgroup
	// Accounted is true while the sandbox holds an allocation.
	Accounted bool
	// StartRequested is true when a start was asked for while queued.
	StartRequested bool
	// Stats is the latest usage sample, nil before the first one.
	Stats *sandbox.Stats
}

// Live returns the sandboxes that currently hold an allocation.
func (v View) Live() []SandboxView {
	var out []SandboxView
	for _, s := range v.Sandboxes {
		if s.Accounted {
			out = append(out, s)
		}
	}
	return out
}

// Queued returns the sandboxes waiting to be dispatched.
func (v View) Queued() []SandboxView {
	var out []SandboxView
	for _, s := range v.Sandboxes {
		if s.Sandbox.Status.Phase == sandbox.PhaseQueued {
			out = append(out, s)
		}
	}
	return out
}

// Sample is one round of usage collection.
type Sample struct {
	Time time.Time
	Node host.Sample
	// Sandboxes holds stats for every sandbox the runtime could sample.
	Sandboxes map[string]sandbox.Stats
}
