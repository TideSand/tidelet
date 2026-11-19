// Package overcommit implements the "overcommit" policy: threshold
// overcommit by measured use. A sandbox starts when it arrives
// while the pool's measured CPU and memory use are both under `threshold`
// (80 %) and nothing is booting; otherwise it is rejected, and the workload
// asks again after its own interval. Arrivals are the workload's to pace,
// so each start shows in the measurements before the next decision. Requests are not reserved; what
// counts is what the sandboxes really use. Once a sandbox runs nothing is
// done about it: limits are hard caps, and if the sandboxes together
// outgrow the pool, the kernel swaps.
//
// The CPU figure is smoothed over a few samples and memory use excludes
// file cache (see package meter), so run tideletd with a 1 s
// --sample-interval and --pool-cgroup so the figures are the pool's.
package overcommit

import (
	"fmt"
	"strconv"
	"time"

	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/meter"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Name is the policy's registered name.
const Name = "overcommit"

// Gate is the policy's admission rule, on its own so the policies built
// on it (agentcgroup's optional gate) share it: a
// sandbox that arrives while the load allows one runs, any other is
// rejected for the workload to offer again later.
type Gate struct {
	Threshold float64
	Swap      bool

	Meter meter.Meter
	// Status is the last decision, for status output and tests.
	Status Status
}

// Status is what the gate last saw and did.
type Status struct {
	At       time.Time
	Load     meter.Load
	Open     bool
	Admitted string // the sandbox the last decision started, "" when it rejected
	Rejected int    // arrivals rejected so far
}

// NewGate builds a gate with the policy's defaults.
func NewGate() Gate { return Gate{Threshold: 0.8, Swap: true} }

// Option applies a gate option; ok is false for a key it does not know.
func (g *Gate) Option(k, v string) (ok bool, err error) {
	switch k {
	case "threshold":
		g.Threshold, err = strconv.ParseFloat(v, 64)
		if err == nil && (g.Threshold <= 0 || g.Threshold > 1) {
			err = fmt.Errorf("must be in (0, 1]")
		}
	case "swap":
		g.Swap, err = strconv.ParseBool(v)
	default:
		return false, nil
	}
	return true, err
}

// Admit decides on a sandbox as it arrives: Invalid when its limits could
// never fit the pool, Reject while a sandbox is still booting or the
// pool's measured CPU or memory use is at the threshold, Run otherwise,
// with limits as hard caps and swap as the gate's option says.
func (g *Gate) Admit(sb sandbox.Sandbox, view policy.View) policy.Decision {
	_, limits := meter.Resources(sb)
	if cap := view.Capacity.Sub(view.Reserved); !limits.Fits(cap) {
		return policy.Decision{Kind: policy.Invalid, Reason: fmt.Sprintf("limits %s exceed the pool's capacity %s", limits, cap)}
	}
	load := g.Meter.Load
	g.Status = Status{At: view.Now, Load: load, Rejected: g.Status.Rejected}
	if booting := meter.Booting(view); len(booting) > 0 {
		g.Status.Rejected++
		return policy.Decision{Kind: policy.Reject, Reason: fmt.Sprintf("sandbox %s is still booting", booting[0].Sandbox.ID)}
	}
	if load.CPU >= g.Threshold || load.Memory >= g.Threshold {
		g.Status.Rejected++
		return policy.Decision{Kind: policy.Reject, Reason: fmt.Sprintf("pool at %.0f%% cpu and %.0f%% memory; a start needs both under %.0f%%",
			load.CPU*100, load.Memory*100, g.Threshold*100)}
	}
	g.Status.Open, g.Status.Admitted = true, sb.ID
	return policy.Decision{Kind: policy.Run, Allocation: meter.Shape(sb, g.Swap)}
}

// Sample folds a node sample into the load the next decision reads.
func (g *Gate) Sample(s policy.Sample) meter.Load {
	return g.Meter.Update(s.Node)
}

// Policy is the overcommit policy: the gate and nothing else.
type Policy struct {
	Gate
}

// New builds an overcommit policy. Options: threshold (0..1, default 0.8),
// swap (bool, default true).
func New(opts map[string]string) (*Policy, error) {
	p := &Policy{Gate: NewGate()}
	for k, v := range opts {
		ok, err := p.Option(k, v)
		if !ok {
			return nil, fmt.Errorf("%s: unknown option %q", Name, k)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: option %s=%q: %w", Name, k, v, err)
		}
	}
	return p, nil
}

var _ policy.Policy = (*Policy)(nil)

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Allocatable implements policy.Policy: what the node advertises is its
// capacity minus the reserve; the gate may run more than that.
func (p *Policy) Allocatable(view policy.View) resource.Set {
	return view.Capacity.Sub(view.Reserved)
}

// Admit implements policy.Policy.
func (p *Policy) Admit(sb sandbox.Sandbox, view policy.View) policy.Decision {
	return p.Gate.Admit(sb, view)
}

// OnEvent implements policy.Policy: nothing to do.
func (p *Policy) OnEvent(sandbox.Event, policy.View) []policy.Action { return nil }

// OnSample implements policy.Policy: the load the gate reads.
func (p *Policy) OnSample(s policy.Sample, view policy.View) []policy.Action {
	p.Gate.Sample(s)
	return nil
}

// OnTick implements policy.Policy: nothing to do, decisions are made on arrival.
func (p *Policy) OnTick(time.Time, policy.View) []policy.Action { return nil }
