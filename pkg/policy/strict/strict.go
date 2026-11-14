// Package strict implements the naive, Docker-like policy with no
// overcommit: a sandbox reserves exactly what it is limited to, the node
// never promises more than it physically has, and a sandbox that does not
// fit is rejected outright. It never queues, reshapes, freezes or reacts
// to usage; it uses only Admit and Allocatable from the policy interface.
package strict

import (
	"fmt"
	"strconv"
	"time"

	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Name is the policy's registered name.
const Name = "strict"

// Policy is the strict no-overcommit policy.
type Policy struct {
	// requireLimits rejects sandboxes without a CPU and a memory limit.
	// Off by default to match `docker run`, where an unlimited container
	// is legal and simply unaccounted.
	requireLimits bool
	// swap lets a sandbox swap up to its memory limit, as `docker run
	// --memory` does when --memory-swap is not given. Off by default.
	swap bool
}

// New builds a strict policy from options. Supported: requireLimits=true,
// swap=true.
func New(opts map[string]string) (*Policy, error) {
	p := &Policy{}
	for k, v := range opts {
		switch k {
		case "requireLimits":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("strict: option %s: %w", k, err)
			}
			p.requireLimits = b
		case "swap":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("strict: option %s: %w", k, err)
			}
			p.swap = b
		default:
			return nil, fmt.Errorf("strict: unknown option %q", k)
		}
	}
	return p, nil
}

var _ policy.Policy = (*Policy)(nil)

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Allocatable implements policy.Policy: physical capacity minus reserved.
func (p *Policy) Allocatable(view policy.View) resource.Set {
	return view.Capacity.Sub(view.Reserved)
}

// Admit implements policy.Policy.
//
// Rules:
//   - requests and limits must agree per resource; a missing side is copied
//     from the other, so `--cpus 1` means request 1 and limit 1;
//   - an unset resource is unconstrained and accounts for nothing, unless
//     requireLimits is on;
//   - the sum of accounted requests may not exceed allocatable;
//   - limits become hard caps; swap is disabled, or with the swap option
//     allowed up to the memory limit (swap is not accounted).
func (p *Policy) Admit(sb sandbox.Sandbox, view policy.View) policy.Decision {
	res := sb.Spec.Resources
	cpu, err := p.pick("cpu", int64(res.Requests.CPU), int64(res.Limits.CPU))
	if err != nil {
		return policy.Decision{Kind: policy.Invalid, Reason: err.Error()}
	}
	mem, err := p.pick("memory", int64(res.Requests.Memory), int64(res.Limits.Memory))
	if err != nil {
		return policy.Decision{Kind: policy.Invalid, Reason: err.Error()}
	}
	eff := resource.Set{CPU: resource.Millicores(cpu), Memory: resource.Bytes(mem)}
	allocatable := p.Allocatable(view)
	if !view.Allocated.Add(eff).Fits(allocatable) {
		return policy.Decision{
			Kind: policy.Reject,
			Reason: fmt.Sprintf("needs %s; node has %s allocatable with %s already allocated",
				eff, allocatable, view.Allocated),
		}
	}
	cg := policy.ShapeGuaranteed(eff, eff)
	if p.swap && mem > 0 {
		cg.MemorySwapLimitBytes = mem
	}
	return policy.Decision{
		Kind: policy.Run,
		Allocation: policy.Allocation{
			Requests: eff,
			Limits:   eff,
			Cgroup:   cg,
		},
	}
}

func (p *Policy) pick(name string, request, limit int64) (int64, error) {
	switch {
	case request == 0 && limit == 0:
		if p.requireLimits {
			return 0, fmt.Errorf("%s limit is required under the strict policy", name)
		}
		return 0, nil
	case request == 0:
		return limit, nil
	case limit == 0:
		return request, nil
	case request != limit:
		return 0, fmt.Errorf("strict policy requires %s request (%d) to equal limit (%d)", name, request, limit)
	default:
		return limit, nil
	}
}

// OnEvent implements policy.Policy. Strict does nothing.
func (p *Policy) OnEvent(sandbox.Event, policy.View) []policy.Action { return nil }

// OnSample implements policy.Policy. Strict does nothing.
func (p *Policy) OnSample(policy.Sample, policy.View) []policy.Action { return nil }

// OnTick implements policy.Policy. Strict does nothing.
func (p *Policy) OnTick(_ time.Time, _ policy.View) []policy.Action { return nil }
