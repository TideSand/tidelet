// Package meter holds what the load-based policies (overcommit,
// agentcgroup) share: a once-a-second load figure, the per-sandbox
// observation of CPU rate and stall share from consecutive samples, and the
// shaping of a dispatched sandbox. The figures come from the usage samples
// the manager collects (so run tideletd with a 1 s --sample-interval) and
// from the node sample, which is the pool's cgroup under --pool-cgroup.
package meter

import (
	"math"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Load is the pool's load, once a second: a smoothed share of the CPUs in use, the share of memory in use
// (not counting file cache), and how much of the last 10 s all work was
// stalled on memory.
type Load struct {
	CPU         float64 // share of the pool's CPUs in use, 0..1, smoothed over a few samples
	Memory      float64 // share of the memory capacity in use, not counting file cache, 0..1
	MemoryBytes uint64
	Stalls      float64 // % of the last 10 s in which all work was stalled on memory (PSI full avg10)
	CPUStalls   float64 // % of the last 10 s in which some work waited for a CPU (PSI some avg10)
}

// Meter turns the manager's node samples into Loads. The CPU share is an
// exponential average over samples (0.6 old, 0.4 new).
type Meter struct {
	havePrev bool
	prev     host.Sample
	cpu      float64
	Load     Load // the latest Load, for status output and tests
}

// Update folds a node sample in and returns the load.
func (m *Meter) Update(s host.Sample) Load {
	if m.havePrev {
		if f := host.BusyFraction(m.prev, s); f >= 0 {
			m.cpu = 0.6*m.cpu + 0.4*min(max(f, 0), 1)
		}
	}
	m.prev, m.havePrev = s, true
	used := uint64(0)
	if s.MemTotal > s.MemAvailable {
		used = s.MemTotal - s.MemAvailable
	}
	m.Load = Load{CPU: m.cpu, Memory: s.MemoryUsedFraction(), MemoryBytes: used, Stalls: s.Memory.Full.Avg10, CPUStalls: s.CPU.Some.Avg10}
	return m.Load
}

// Cores is the pool's CPU capacity in cores (1 when unknown).
func Cores(view policy.View) float64 {
	if view.Capacity.CPU <= 0 {
		return 1
	}
	return view.Capacity.CPU.Cores()
}

// MemoryLimit is the pool's memory capacity in bytes.
func MemoryLimit(view policy.View) uint64 {
	if view.Capacity.Memory <= 0 {
		return 1
	}
	return uint64(view.Capacity.Memory)
}

// VCPUs is the CPU limit of a sandbox in cores: the spec's limit before
// dispatch, the allocation after.
func VCPUs(sb sandbox.Sandbox) float64 {
	lim := sb.Status.Allocated.Limits.CPU
	if lim == 0 {
		lim = sb.Spec.Resources.Limits.CPU
	}
	if lim == 0 {
		lim = sb.Spec.Resources.Requests.CPU
	}
	if lim <= 0 {
		return 1
	}
	return lim.Cores()
}

// Resources returns (requests, limits) of a spec with a missing side copied
// from the other, per resource.
func Resources(sb sandbox.Sandbox) (resource.Set, resource.Set) {
	res := sb.Spec.Resources
	requests, limits := res.Requests, res.Limits
	if limits.CPU == 0 {
		limits.CPU = requests.CPU
	}
	if limits.Memory == 0 {
		limits.Memory = requests.Memory
	}
	if requests.CPU == 0 {
		requests.CPU = limits.CPU
	}
	if requests.Memory == 0 {
		requests.Memory = limits.Memory
	}
	return requests, limits
}

// Shape is the allocation of a dispatched sandbox: limits as hard caps
// (cpu.max, memory.max), requests as the default weight, swap allowed or
// not: a sandbox has the CPUs and memory it declared and can be swapped by
// the host when the policy overcommits.
func Shape(sb sandbox.Sandbox, swap bool) policy.Allocation {
	requests, limits := Resources(sb)
	cg := policy.ShapeGuaranteed(requests, limits)
	if swap {
		cg.MemorySwapLimitBytes = 0 // leave memory.swap.max at its default: the sandbox may swap
	}
	return policy.Allocation{Requests: requests, Limits: limits, Cgroup: cg}
}

// Queued is the sandboxes waiting for dispatch, oldest first.
func Queued(view policy.View) []policy.SandboxView { return view.Queued() }

// Booting is the sandboxes dispatched but not yet running, during which the
// load-based policies start nothing else.
func Booting(view policy.View) []policy.SandboxView {
	var out []policy.SandboxView
	for _, s := range view.Sandboxes {
		if s.Sandbox.Status.Phase == sandbox.PhaseCreated {
			out = append(out, s)
		}
	}
	return out
}

// Running is the sandboxes that are running (or frozen: they hold their
// memory), oldest first.
func Running(view policy.View) []policy.SandboxView {
	var out []policy.SandboxView
	for _, s := range view.Sandboxes {
		if p := s.Sandbox.Status.Phase; p == sandbox.PhaseRunning || p == sandbox.PhaseFrozen {
			out = append(out, s)
		}
	}
	return out
}

// Find returns the view of a sandbox by ID.
func Find(view policy.View, id string) (policy.SandboxView, bool) {
	for _, s := range view.Sandboxes {
		if s.Sandbox.ID == id {
			return s, true
		}
	}
	return policy.SandboxView{}, false
}

// Gone says whether an event ends a sandbox's life as far as a policy's
// per-sandbox state goes.
func Gone(ev sandbox.Event) bool {
	switch ev.Type {
	case sandbox.EventExited, sandbox.EventFailed, sandbox.EventDeleted:
		return true
	}
	return false
}

// WeightShares converts a cgroup v2 cpu.weight (1..10000) to the cgroup v1
// style shares the runtime takes, such that the OCI runtime's conversion
// gives that weight back. runc (libcontainer/cgroups) maps shares to weight
// with a quadratic fit of the two ranges, see SharesWeight; the containerd
// cgroups library's older linear fit is not what runs the sandboxes here.
func WeightShares(weight uint64) uint64 {
	if weight <= 1 {
		return 2
	}
	if weight >= 10000 {
		return 262144
	}
	// Invert the quadratic: l^2 + 125 l - 612 (log10(weight) + 7/34) = 0.
	e := math.Log10(float64(weight))
	l := (-125 + math.Sqrt(125*125+4*612*(e+7.0/34.0))) / 2
	shares := uint64(math.Floor(math.Pow(2, l)))
	if shares < 2 {
		shares = 2
	}
	// The runtime rounds its result up; step until it lands on the weight.
	for SharesWeight(shares) < weight && shares < 262144 {
		shares++
	}
	return shares
}

// SharesWeight is the OCI runtime's (runc's) conversion of shares to
// cpu.weight: a quadratic function fitting shares 2..262144 onto weight
// 1..10000.
func SharesWeight(shares uint64) uint64 {
	if shares == 0 {
		return 0
	}
	l := math.Log2(float64(shares))
	exponent := (l*l+125*l)/612.0 - 7.0/34.0
	return uint64(math.Ceil(math.Pow(10, exponent)))
}

// Weighted is an Update giving the sandbox the cpu.weight and memory.low
// asked for (low is a runtime.Cgroup.MemoryLowBytes value), or nothing when
// its cgroup already has them.
func Weighted(sv policy.SandboxView, weight uint64, low int64) (policy.Action, bool) {
	cg := sv.Cgroup
	shares := WeightShares(weight)
	if cg.CPUShares == shares && cg.MemoryLowBytes == low {
		return nil, false
	}
	cg.CPUShares, cg.MemoryLowBytes = shares, low
	return policy.Update{ID: sv.Sandbox.ID, Allocation: policy.Allocation{Requests: sv.Sandbox.Status.Allocated.Requests, Limits: sv.Sandbox.Status.Allocated.Limits, Cgroup: cg}}, true
}

// Stall is the sum of a sandbox's "some" stall totals in microseconds: time
// its work waited for a CPU, for memory or for the disk.
func Stall(st *sandbox.Stats) uint64 {
	return st.CPUSomeStallUSec + st.MemorySomeStallUSec + st.IOSomeStallUSec
}

// Rates is what consecutive samples of one sandbox say: its CPU rate in
// cores and the share of the interval it was stalled. Observe keeps the
// previous sample and returns false until two samples span some time.
type Rates struct {
	have    bool
	at      time.Time
	cpu     uint64 // CPUUsageNanos
	stalled uint64 // stall microseconds
}

// Observe folds a sample in and returns the rate and stall share since the
// previous one, with ok false when there is no previous sample or the
// sample is not newer.
func (r *Rates) Observe(st *sandbox.Stats) (cores, stall float64, ok bool) {
	stalled := Stall(st)
	if r.have && st.Timestamp.After(r.at) && st.CPUUsageNanos >= r.cpu && stalled >= r.stalled {
		dt := st.Timestamp.Sub(r.at).Seconds()
		cores = float64(st.CPUUsageNanos-r.cpu) / 1e9 / dt
		stall = min(1, float64(stalled-r.stalled)/1e6/dt)
		ok = true
	}
	if !r.have || st.Timestamp.After(r.at) {
		r.have, r.at, r.cpu, r.stalled = true, st.Timestamp, st.CPUUsageNanos, stalled
	}
	return cores, stall, ok
}

// Unset is the cgroup memory.low value that leaves the file alone.
const Unset int64 = 0

// Protect and Unprotect are the memory.low settings ("max" and "0") for a
// sandbox in a tool call and a sandbox waiting on the model.
const (
	Protect   = runtime.MemoryLowMax
	Unprotect = runtime.MemoryLowNone
)
