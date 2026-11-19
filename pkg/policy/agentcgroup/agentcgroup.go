// Package agentcgroup implements AgentCgroup (Zheng et al., "AgentCgroup:
// Understanding and Controlling OS Resources of AI Agents", arXiv
// 2602.09345; code github.com/eunomia-bpf/agentcgroup) as a tidelet policy.
// The paper's controller has three mechanisms, each answering one of the
// mismatches it identifies between agent workloads and container-level
// resource control; all three are here, with the substitutions that
// unmodified agents on a stock kernel force, listed below.
//
// 1. Fine-grained resource domains (the granularity mismatch). The paper
// gives every tool call an ephemeral child cgroup of the agent's session
// cgroup (its bash wrapper creates tool_<pid>_<ts>/, moves the command into
// it, sets memory.high from the agent's hint, records duration and
// memory.peak, and removes it on exit), so resource constraints and
// accounting follow tool-call boundaries. Here the tool-call boundaries
// come from the LLM proxy (a response carrying tool calls is delivered: the
// call's domain opens; the agent's next request: it closes), and the domain
// is the sandbox's own cgroup reshaped for the call's duration: memory.high
// from the call's hint on top of the framework baseline the sandbox held
// when the call began, cpu.weight from the call's priority class, both
// lifted when the call ends; the call's duration, peak memory, hint and
// enforcement are recorded as the wrapper's JSONL would. (A child cgroup
// under the container's cgroup is not possible with the OCI runtime's exec
// path, which adds processes to the container cgroup and so forbids
// enabling controllers on its subtree.)
//
// 2. In-kernel enforcement (the responsiveness mismatch). The paper runs
// sched_ext (scx_flatcg: hierarchical cpu.weight scheduling, session_high
// at weight 150 and session_low at 50) and memcg_bpf_ops hooks that, while a
// HIGH cgroup is under memory pressure, treat it as below memory.low
// (protected from reclaim) and give the LOW cgroups custom allocation delays
// above memory.high, escalating to cgroup.freeze rather than termination;
// the protection window is one second, renewed while pressure lasts. Those
// hooks are not in a stock kernel; the paper's own cgroup-v2 fallback
// (memcg_controller.py) does the same with memory.low on HIGH and a reduced
// memory.high on LOW from a user-space loop that detects pressure by three
// signals: memory.events high, PSI stall time growing, usage at 85 % of the
// limit. This policy is that fallback at the daemon's sample cadence:
// priority is by tool call (a heavy call, the paper's "latency-sensitive
// tool call", is HIGH; light calls and sessions waiting on the model are
// LOW), cpu.weight 150/50, and while a protection window is open HIGH
// domains get memory.low = everything they hold and LOW cgroups get
// memory.high at their current use, so the kernel throttles their
// allocations (its own over-high delays); a LOW domain that stays over its
// high through the window is frozen, and thawed when the window closes or
// after a bound. Nothing is ever OOM-killed by the policy.
//
// 3. Intent-driven coordination (the adaptability mismatch). Upward, the
// paper's agent declares AGENT_RESOURCE_HINT (memory:low, memory:medium,
// memory:high, memory:<size>) before a command and the wrapper maps it to
// memory.high (256 MB, 1 GB, max). An unmodified agent does not declare, so the
// hint is derived from the tool call itself the way the paper reports its
// agent classifying them (file reads and git status low, test suites and
// builds high): the wrapper's mapping is then applied unchanged. Downward,
// the wrapper injects natural-language feedback into stderr when a command
// is OOM-killed or throttled beyond recovery; here the same condition is
// detected (a call frozen, or run at its memory limit) and the feedback is
// recorded in the log and the counters, since an unmodified agent never
// sees it.
//
// Admission is not part of AgentCgroup (its sessions are a fixed set), so
// by default every arrival runs at once with its limits as hard caps and
// swap allowed, like overcommit at threshold 1.0 without the gate. The
// `gate` option (a fraction) adds overcommit's admission rule in front: a
// sandbox starts only while the pool's measured CPU and memory are both
// under it, and is rejected for the workload to offer again otherwise.
//
// The paper's downward feedback closes a loop this policy cannot close: a
// call throttled at its hint's memory.high is reported to the agent
// ("[Resource] Command throttled beyond recovery ... run more targeted
// operations ..."), which re-issues it with a higher hint. An unmodified
// agent cannot, and the domain here is the whole sandbox (no child cgroup
// per call), so a light call's cap also throttles a background job an
// earlier heavy call left running, for good: a livelock the paper's
// per-call cgroups never see. The escalate option stands in for the
// agent's response: after that many sample-seconds over its memory.high
// (cumulative over the call) a domain is promoted one hint level,
// memory:low to memory:medium to memory:high (HIGH class), the feedback
// message is logged as the agent would have read it, and the call goes on.
//
// Options: gate (0 = none, else 0..1), weightHigh (150), weightLow (50),
// pressure (0.85: pool memory use that opens a window), psi (10: pool
// memory PSI some avg10, %, that opens a window), window (1s), freezeAfter
// (3s: a LOW domain over its high this long inside windows is frozen),
// maxFreeze (10s), lowHead (256MiB: memory:low allowance over the
// baseline, also the headroom a window leaves a LOW cgroup), mediumHead
// (1GiB), stall (100ms: the growth of a HIGH domain's memory stall over one
// sample that opens a window; the paper's page-fault threshold), release
// (5s: a window's settings are restored only this long after the last
// window, so a pressure that flickers does not reshape every cgroup each
// second), freeze (true), escalate (30s: sample-seconds a domain spends
// throttled at its hint's memory.high before it is promoted one hint
// level, as the agent would re-issue the call on the paper's feedback; 0
// disables).
package agentcgroup

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/meter"
	"github.com/TideSand/tidelet/pkg/policy/overcommit"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Name is the policy's registered name.
const Name = "agentcgroup"

const mib = 1 << 20

// Hint is the agent's declared (here: derived) resource need for a tool
// call, the paper's AGENT_RESOURCE_HINT.
type Hint int

// Hints, in increasing allowance.
const (
	HintLow    Hint = iota // memory:low: 256 MB over the baseline
	HintMedium             // memory:medium: 1 GB over the baseline
	HintHigh               // memory:high: no soft limit (the wrapper's default, max)
)

// String implements fmt.Stringer in the wrapper's spelling.
func (h Hint) String() string {
	switch h {
	case HintLow:
		return "memory:low"
	case HintMedium:
		return "memory:medium"
	}
	return "memory:high"
}

// domain is one tool call's resource domain: the sandbox's cgroup reshaped
// for the call, with the record the wrapper keeps of it.
type domain struct {
	Call     string
	Start    time.Time
	Hint     Hint
	High     bool   // priority class: HIGH (latency-sensitive, heavy) or LOW
	Tools    string // the tool names, for the record
	Baseline uint64 // the sandbox's memory when the call began (the framework)
	Peak     uint64
	HighSet  int64 // the memory.high the hint gave the domain (0: none)
	Over     int   // samples the domain spent over its memory.high
	Promoted int   // hint levels the domain was promoted (escalate)
	OverFor  time.Time
	Frozen   time.Duration
	Feedback bool
}

// box is what the policy keeps per sandbox.
type box struct {
	Domain    *domain
	LastStall uint64 // memory "some" stall, cumulative microseconds
	LastMem   uint64
	FrozenAt  time.Time
	ThawedAt  time.Time
	Squeezed  bool // memory.high lowered to the current use by a window
	Protected bool // memory.low set by a window
}

// Status summarises the policy's decisions.
type Status struct {
	Calls, CallsHigh, CallsLow int // tool-call domains opened, by class
	Windows                    int // protection windows opened
	Protections, Squeezes      int // memory.low grants and memory.high squeezes applied
	Throttled                  int // sample-seconds LOW domains spent over their memory.high
	Freezes, Thaws             int
	FreezesLost                int // freezes that never showed in the phase (refused, or reverted after a timeout)
	Feedback                   int // downward feedback events (frozen or at the limit)
	Rejected                   int // arrivals the gate turned away
	ProtectedNow               bool
	Trigger                    string
	OverBaseline               int
	Escalations                int // domains promoted one hint level after sustained throttling
	Domains                    int
}

// Policy is the AgentCgroup policy.
type Policy struct {
	Gate overcommit.Gate // admission, used when Gate.Threshold > 0

	WeightHigh, WeightLow    uint64
	Pressure, PSI            float64
	Window, FreezeAfter      time.Duration
	MaxFreeze, Cooldown      time.Duration
	LowHead, MediumHead      int64
	Stall                    time.Duration // a HIGH domain's memory-stall growth per sample that opens a window
	Release                  time.Duration // how long after the last window its settings stay before they are restored (no flapping)
	Escalate                 time.Duration // sample-seconds over its memory.high before a domain is promoted one hint level (0: never)
	Freeze                   bool
	Boxes                    map[string]*box
	Status                   Status
	protectedUntil, statusAt time.Time
	lastPoolStall            float64
}

// New builds an AgentCgroup policy with the paper's parameters.
func New(opts map[string]string) (*Policy, error) {
	p := &Policy{Gate: overcommit.NewGate(), WeightHigh: 150, WeightLow: 50, Pressure: 0.85, PSI: 10, Window: time.Second,
		FreezeAfter: 3 * time.Second, MaxFreeze: 10 * time.Second, Cooldown: 5 * time.Second, LowHead: 256 * mib, MediumHead: 1024 * mib,
		Stall: 100 * time.Millisecond, Release: 5 * time.Second, Escalate: 30 * time.Second, Freeze: true, Boxes: map[string]*box{}}
	p.Gate.Threshold = 0 // no admission control unless asked for
	for k, v := range opts {
		var err error
		num := func(dst *float64, lo, hi float64) {
			*dst, err = strconv.ParseFloat(v, 64)
			if err == nil && (*dst < lo || *dst > hi) {
				err = fmt.Errorf("must be in [%g, %g]", lo, hi)
			}
		}
		dur := func(dst *time.Duration) { *dst, err = time.ParseDuration(v) }
		bytes := func(dst *int64) {
			q, e := resource.ParseMemory(v)
			if e != nil {
				err = e
				return
			}
			*dst = int64(q)
		}
		uint := func(dst *uint64) { *dst, err = strconv.ParseUint(v, 10, 64) }
		switch k {
		case "gate":
			num(&p.Gate.Threshold, 0, 1)
		case "weightHigh":
			uint(&p.WeightHigh)
		case "weightLow":
			uint(&p.WeightLow)
		case "pressure":
			num(&p.Pressure, 0, 1)
		case "psi":
			num(&p.PSI, 0, 100)
		case "window":
			dur(&p.Window)
		case "freezeAfter":
			dur(&p.FreezeAfter)
		case "maxFreeze":
			dur(&p.MaxFreeze)
		case "cooldown":
			dur(&p.Cooldown)
		case "stall":
			dur(&p.Stall)
		case "escalate":
			dur(&p.Escalate)
		case "release":
			dur(&p.Release)
		case "lowHead":
			bytes(&p.LowHead)
		case "mediumHead":
			bytes(&p.MediumHead)
		case "freeze":
			p.Freeze, err = strconv.ParseBool(v)
		case "swap":
			p.Gate.Swap, err = strconv.ParseBool(v)
		default:
			return nil, fmt.Errorf("%s: unknown option %q", Name, k)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: option %s=%q: %w", Name, k, v, err)
		}
	}
	return p, nil
}

var (
	_ policy.Policy     = (*Policy)(nil)
	_ policy.ModelAware = (*Policy)(nil)
)

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Allocatable implements policy.Policy: capacity minus the reserve; the
// policy may run more than that.
func (p *Policy) Allocatable(view policy.View) resource.Set {
	return view.Capacity.Sub(view.Reserved)
}

// Admit implements policy.Policy: every arrival runs, with its limits as
// hard caps and swap allowed (the sessions of the paper are a fixed set);
// with a gate, overcommit's rule decides.
func (p *Policy) Admit(sb sandbox.Sandbox, view policy.View) policy.Decision {
	if p.Gate.Threshold > 0 {
		d := p.Gate.Admit(sb, view)
		p.Status.Rejected = p.Gate.Status.Rejected
		return d
	}
	_, limits := meter.Resources(sb)
	if cap := view.Capacity.Sub(view.Reserved); !limits.Fits(cap) {
		return policy.Decision{Kind: policy.Invalid, Reason: fmt.Sprintf("limits %s exceed the pool's capacity %s", limits, cap)}
	}
	return policy.Decision{Kind: policy.Run, Allocation: meter.Shape(sb, p.Gate.Swap)}
}

// OnEvent implements policy.Policy: forget sandboxes that are gone.
func (p *Policy) OnEvent(ev sandbox.Event, view policy.View) []policy.Action {
	if meter.Gone(ev) {
		if b := p.Boxes[ev.Sandbox.ID]; b != nil && b.Domain != nil {
			p.closeDomain(ev.Sandbox.ID, b, ev.Time, "exited")
		}
		delete(p.Boxes, ev.Sandbox.ID)
	}
	return nil
}

// OnTick implements policy.Policy: nothing to do, the sampler drives it.
func (p *Policy) OnTick(time.Time, policy.View) []policy.Action { return nil }

func (p *Policy) box(id string) *box {
	b := p.Boxes[id]
	if b == nil {
		b = &box{}
		p.Boxes[id] = b
	}
	return b
}

// OnModelEvent implements policy.ModelAware: the tool-call boundaries open
// and close the resource domains. No response is ever held.
func (p *Policy) OnModelEvent(ev policy.ModelEvent, view policy.View) []policy.Action {
	sv, ok := meter.Find(view, ev.Sandbox)
	if !ok {
		return nil
	}
	b := p.box(ev.Sandbox)
	switch ev.Kind {
	case policy.ModelResponseDelivered:
		if ev.ToolCalls == 0 {
			return nil
		}
		if b.Domain != nil { // the agent sent no request between two deliveries: the earlier call is over
			p.closeDomain(ev.Sandbox, b, ev.Time, "superseded")
		}
		hint := classify(ev.ToolNames, ev.ToolInputs)
		d := &domain{Call: ev.Call, Start: ev.Time, Hint: hint, High: hint == HintHigh, Tools: strings.Join(ev.ToolNames, ",")}
		if sv.Stats != nil {
			d.Baseline = sv.Stats.MemoryUsageBytes
			d.Peak = d.Baseline
		}
		b.Domain = d
		p.Status.Calls++
		p.Status.Domains++
		if d.High {
			p.Status.CallsHigh++
		} else {
			p.Status.CallsLow++
		}
		cg := p.shape(sv, b, view.Now)
		slog.Debug("agentcgroup domain open", "sandbox", ev.Sandbox, "call", ev.Call, "tools", d.Tools, "hint", hint.String(), "class", class(d.High),
			"baseline", d.Baseline, "memory.high", cg.MemoryHighBytes, "weight", meter.SharesWeight(cg.CPUShares))
		return p.updateIfChanged(sv, cg)
	case policy.ModelRequestStarted:
		if b.Domain == nil {
			return nil
		}
		p.closeDomain(ev.Sandbox, b, ev.Time, "done")
		var out []policy.Action
		if sv.Sandbox.Status.Phase == sandbox.PhaseFrozen { // a frozen domain cannot have sent this; defensive
			out = append(out, policy.Thaw{ID: ev.Sandbox})
			b.FrozenAt, b.ThawedAt = time.Time{}, ev.Time
			p.Status.Thaws++
		}
		return append(out, p.updateIfChanged(sv, p.shape(sv, b, view.Now))...)
	}
	return nil
}

func class(high bool) string {
	if high {
		return "HIGH"
	}
	return "LOW"
}

// closeDomain records the call the wrapper's way and drops the domain.
func (p *Policy) closeDomain(id string, b *box, now time.Time, why string) {
	d := b.Domain
	b.Domain = nil
	p.Status.Domains--
	if !d.Feedback && b.FrozenAt.IsZero() && d.Frozen > 0 {
		d.Feedback = true
	}
	if d.Feedback {
		p.Status.Feedback++
	}
	slog.Info("agentcgroup tool", "sandbox", id, "call", d.Call, "tools", d.Tools, "hint", d.Hint.String(), "class", class(d.High), "end", why,
		"duration", now.Sub(d.Start).Round(time.Millisecond), "baseline", d.Baseline, "peak", d.Peak, "memory.high", d.HighSet,
		"over_high_samples", d.Over, "promoted", d.Promoted, "frozen", d.Frozen.Round(time.Millisecond), "feedback", d.Feedback)
	if d.Feedback {
		// The wrapper's downward message, as the agent would read it on stderr.
		slog.Info("agentcgroup feedback", "sandbox", id, "call", d.Call,
			"message", fmt.Sprintf("[Resource] Command throttled beyond recovery (frozen %s, peak %d MB). Suggestions: run more targeted operations, reduce data size, or split into smaller steps.",
				d.Frozen.Round(time.Second), d.Peak/mib))
	}
}

// shape is the cgroup the sandbox should have now: its hard caps, the
// weight of its class, the domain's memory.high (squeezed to the current
// use for LOW cgroups inside a protection window) and memory.low for HIGH
// domains inside a window.
func (p *Policy) shape(sv policy.SandboxView, b *box, now time.Time) runtime.Cgroup {
	cg := sv.Cgroup
	limit := cg.MemoryLimitBytes
	d := b.Domain
	active := now.Before(p.protectedUntil)
	keep := !active && now.Before(p.protectedUntil.Add(p.Release)) // the last window closed less than Release ago: hold the settings
	high := d != nil && d.High
	// CPU: the class's weight (the paper's session_high 150 / session_low 50).
	if high {
		cg.CPUShares = meter.WeightShares(p.WeightHigh)
	} else {
		cg.CPUShares = meter.WeightShares(p.WeightLow)
	}
	// Memory soft limit: the hint's allowance over the baseline while a domain
	// is open, the hard limit otherwise (memory.high = memory.max means no
	// throttling below the cap; the runtime cannot unset it).
	var want int64
	if limit > 0 {
		want = limit
	}
	if d != nil && limit > 0 {
		switch d.Hint {
		case HintLow:
			want = min(limit, int64(d.Baseline)+p.LowHead)
		case HintMedium:
			want = min(limit, int64(d.Baseline)+p.MediumHead)
		}
		d.HighSet = want
	}
	squeezed := false
	if keep && b.Squeezed && !high && cg.MemoryHighBytes > 0 && cg.MemoryHighBytes < want {
		want, squeezed = cg.MemoryHighBytes, true // between windows: keep the squeeze
	}
	if active && !high && limit > 0 && sv.Stats != nil {
		// A LOW cgroup inside a window: memory.high just above its use (lowHead
		// of headroom, in 64 MiB steps so the value holds still from one sample
		// to the next), so growth beyond that is throttled by the kernel's
		// over-high delay, the paper's memcg hook, while allocation within the
		// headroom costs nothing.
		const step = 64 * mib
		cap := (int64(sv.Stats.MemoryUsageBytes) + p.LowHead + step - 1) / step * step
		if cur := cg.MemoryHighBytes; b.Squeezed && cur > 0 && cur < want && int64(sv.Stats.MemoryUsageBytes)+p.LowHead/2 <= cur && cur <= cap+step {
			cap = cur // keep the current squeeze while it still leaves half the headroom
		}
		if cap < want {
			want = cap
			squeezed = true
		}
	}
	if want > 0 {
		cg.MemoryHighBytes = want
	}
	if squeezed && !b.Squeezed {
		p.Status.Squeezes++
	}
	b.Squeezed = squeezed
	// Memory protection: a HIGH domain inside a window holds on to everything it has.
	if (active || (keep && b.Protected)) && high {
		cg.MemoryLowBytes = runtime.MemoryLowMax
		if !b.Protected {
			p.Status.Protections++
		}
		b.Protected = true
	} else if b.Protected || cg.MemoryLowBytes == runtime.MemoryLowMax {
		cg.MemoryLowBytes = runtime.MemoryLowNone
		b.Protected = false
	}
	return cg
}

// updateIfChanged is the Update applying cg, or nothing when the sandbox's
// cgroup already has these settings.
func (p *Policy) updateIfChanged(sv policy.SandboxView, cg runtime.Cgroup) []policy.Action {
	cur := sv.Cgroup
	if cur.CPUShares == cg.CPUShares && cur.MemoryHighBytes == cg.MemoryHighBytes && cur.MemoryLowBytes == cg.MemoryLowBytes {
		return nil
	}
	if ph := sv.Sandbox.Status.Phase; ph != sandbox.PhaseRunning && ph != sandbox.PhaseFrozen {
		return nil
	}
	return []policy.Action{policy.Update{ID: sv.Sandbox.ID, Allocation: policy.Allocation{Requests: sv.Sandbox.Status.Allocated.Requests, Limits: sv.Sandbox.Status.Allocated.Limits, Cgroup: cg}}}
}

// OnSample implements policy.Policy: the pressure detector and the
// graduated enforcement, once per sample.
func (p *Policy) OnSample(s policy.Sample, view policy.View) []policy.Action {
	load := p.Gate.Sample(s)
	now := s.Time
	var out []policy.Action
	// --- pressure detection: the fallback's three signals, plus the HIGH
	// domains' own stalls (the paper's page-fault trigger on the HIGH cgroup).
	trigger := ""
	if load.Memory >= p.Pressure {
		trigger = fmt.Sprintf("usage(%.0f%%)", 100*load.Memory)
	}
	if psi := s.Node.Memory.Some.Avg10; trigger == "" && psi >= p.PSI {
		trigger = fmt.Sprintf("psi(%.0f%%)", psi)
	}
	running := meter.Running(view)
	for _, sv := range running {
		b := p.Boxes[sv.Sandbox.ID]
		st := s.Sandboxes[sv.Sandbox.ID]
		if b == nil || b.Domain == nil || !b.Domain.High {
			continue
		}
		if grew := time.Duration(st.MemorySomeStallUSec-b.LastStall) * time.Microsecond; b.LastStall > 0 && st.MemorySomeStallUSec > b.LastStall && grew >= p.Stall && trigger == "" {
			trigger = fmt.Sprintf("high-stall(%s +%dms)", sv.Sandbox.ID[:12], grew.Milliseconds())
		}
	}
	if trigger != "" {
		if !now.Before(p.protectedUntil) {
			p.Status.Windows++
			slog.Info("agentcgroup protect", "trigger", trigger, "window", p.Window, "pool_memory", fmt.Sprintf("%.0f%%", 100*load.Memory),
				"psi", fmt.Sprintf("%.0f%%", s.Node.Memory.Some.Avg10), "domains", p.Status.Domains)
		}
		p.protectedUntil = now.Add(p.Window)
		p.Status.Trigger = trigger
	}
	active := now.Before(p.protectedUntil)
	p.Status.ProtectedNow = active
	// --- enforcement per sandbox.
	for _, sv := range running {
		id := sv.Sandbox.ID
		b := p.box(id)
		st, ok := s.Sandboxes[id]
		if !ok {
			continue
		}
		sv.Stats = &st
		if d := b.Domain; d != nil {
			if st.MemoryUsageBytes > d.Peak {
				d.Peak = st.MemoryUsageBytes
			}
			if sv.Cgroup.MemoryHighBytes > 0 && int64(st.MemoryUsageBytes) >= sv.Cgroup.MemoryHighBytes && !d.High {
				d.Over++
				p.Status.Throttled++
				if d.OverFor.IsZero() {
					d.OverFor = now
				}
			} else {
				d.OverFor = time.Time{}
			}
			if d.Baseline > 0 && st.MemoryUsageBytes > d.Baseline {
				p.Status.OverBaseline++
			}
			if sv.Cgroup.MemoryLimitBytes > 0 && int64(st.MemoryUsageBytes) >= sv.Cgroup.MemoryLimitBytes*95/100 && !d.Feedback {
				d.Feedback = true // ran at its hard limit: the wrapper would report the OOM risk
			}
		}
		// Escalation: the agent's response to the throttling feedback, which
		// an unmodified agent cannot give: one hint level up after Escalate
		// sample-seconds over the hint's memory.high, cumulative over the call.
		if d := b.Domain; d != nil && p.Escalate > 0 && !d.High && d.Over >= int(p.Escalate/time.Second)*(d.Promoted+1) {
			from := d.Hint
			if d.Hint == HintLow {
				d.Hint = HintMedium
			} else {
				d.Hint, d.High = HintHigh, true
				p.Status.CallsHigh++
				p.Status.CallsLow--
			}
			d.Promoted++
			d.Feedback = true
			p.Status.Escalations++
			slog.Info("agentcgroup escalate", "sandbox", id, "call", d.Call, "tools", d.Tools, "from", from.String(), "to", d.Hint.String(),
				"over_samples", d.Over, "use", st.MemoryUsageBytes, "memory.high", sv.Cgroup.MemoryHighBytes, "since", now.Sub(d.Start).Round(time.Second))
			slog.Info("agentcgroup feedback", "sandbox", id, "call", d.Call,
				"message", fmt.Sprintf("[Resource] Command throttled at its %s allowance (%d MB, peak %d MB). Suggestions: run more targeted operations, reduce data size, or split into smaller steps; re-issued with %s.",
					from.String(), sv.Cgroup.MemoryHighBytes/mib, d.Peak/mib, d.Hint.String()))
		}
		b.LastStall, b.LastMem = st.MemorySomeStallUSec, st.MemoryUsageBytes
		// Thaw: the window closed, or the bound on a freeze passed. The manager
		// applies actions asynchronously, so a Thaw already issued is given
		// MaxFreeze to show in the phase before it is repeated; a frozen
		// sandbox the policy has no freeze on record for (its Freeze was
		// forgotten below, or predates a restart of the daemon) is thawed too.
		if sv.Sandbox.Status.Phase == sandbox.PhaseFrozen {
			pending := b.FrozenAt.IsZero() && !b.ThawedAt.IsZero() && now.Sub(b.ThawedAt) < p.MaxFreeze
			if !pending && (b.FrozenAt.IsZero() || !active || now.Sub(b.FrozenAt) >= p.MaxFreeze) {
				out = append(out, policy.Thaw{ID: id})
				if b.Domain != nil && !b.FrozenAt.IsZero() {
					b.Domain.Frozen += now.Sub(b.FrozenAt)
				}
				b.FrozenAt, b.ThawedAt = time.Time{}, now
				p.Status.Thaws++
				slog.Info("agentcgroup thaw", "sandbox", id, "window_open", active)
			}
			continue
		}
		// A Freeze issued MaxFreeze ago whose sandbox still runs did not take
		// (the runtime refused it, or the pause timed out and was reverted):
		// forget it, the cooldown runs from now.
		if !b.FrozenAt.IsZero() && now.Sub(b.FrozenAt) >= p.MaxFreeze {
			slog.Warn("agentcgroup freeze not taken", "sandbox", id, "issued", now.Sub(b.FrozenAt).Round(time.Second))
			b.FrozenAt, b.ThawedAt = time.Time{}, now
			p.Status.FreezesLost++
		}
		out = append(out, p.updateIfChanged(sv, p.shape(sv, b, now))...)
		// Freeze: a LOW domain that stays over its memory.high through the
		// window despite the throttling, the paper's last step before
		// termination. Once per episode: while a Freeze is in flight
		// (FrozenAt set, phase not yet Frozen) no second one is issued.
		if d := b.Domain; p.Freeze && active && d != nil && !d.High && !d.OverFor.IsZero() && now.Sub(d.OverFor) >= p.FreezeAfter &&
			sv.Sandbox.Status.Phase == sandbox.PhaseRunning && b.FrozenAt.IsZero() && (b.ThawedAt.IsZero() || now.Sub(b.ThawedAt) >= p.Cooldown) {
			out = append(out, policy.Freeze{ID: id})
			b.FrozenAt = now
			d.Feedback = true
			p.Status.Freezes++
			slog.Info("agentcgroup freeze", "sandbox", id, "call", d.Call, "tools", d.Tools, "use", st.MemoryUsageBytes, "memory.high", sv.Cgroup.MemoryHighBytes,
				"over_for", now.Sub(d.OverFor).Round(time.Second), "trigger", p.Status.Trigger)
		}
	}
	if now.Sub(p.statusAt) >= time.Minute {
		p.statusAt = now
		st := p.Status
		slog.Info("agentcgroup status", "running", len(running), "domains", st.Domains, "calls", st.Calls, "high", st.CallsHigh, "low", st.CallsLow,
			"windows", st.Windows, "protected", st.ProtectedNow, "trigger", st.Trigger, "protections", st.Protections, "squeezes", st.Squeezes,
			"throttled_s", st.Throttled, "freezes", st.Freezes, "thaws", st.Thaws, "freezes_lost", st.FreezesLost, "escalations", st.Escalations, "feedback", st.Feedback, "rejected", st.Rejected,
			"pool_cpu", fmt.Sprintf("%.0f%%", 100*load.CPU), "pool_memory", fmt.Sprintf("%.0f%%", 100*load.Memory))
	}
	return out
}

// --- the upward channel: the hint the agent would declare for a tool call.

var (
	// Light shell commands: the paper's agent classified cat and git as memory:low.
	lightCmd = regexp.MustCompile(`^\s*(cat|head|tail|less|ls|ll|pwd|cd|echo|printf|which|type|env|export|true|false|wc|stat|file|du|df|date|whoami|id|uname|hostname|touch|mkdir|rmdir|rm|cp|mv|ln|chmod|chown|basename|dirname|realpath|readlink|tree|sort|uniq|cut|tr|diff|cmp|md5sum|sha256sum|grep|rg|ag|egrep|fgrep|find|fd|sed|awk|xargs|tee|git\s+(status|diff|log|show|branch|rev-parse|remote|config|blame|ls-files|stash\s+list|describe|tag)|jq|yq|python3?\s+-c\s+['"]?print|sleep|test|\[)\b`)
	// Commands that fetch or unpack: the paper's memory:medium (1 GB).
	mediumCmd = regexp.MustCompile(`\b(pip3?\s+(install|download)|uv\s+(pip|add|sync)|npm\s+(install|ci|i)\b|yarn\s+(install|add)|pnpm\s+(install|add)|apt(-get)?\s+(install|update)|conda\s+(install|create)|mamba\s+install|cargo\s+(fetch|add)|go\s+(get|mod)|git\s+(clone|fetch|pull|checkout|submodule)|wget|curl|tar\s|unzip|gzip|gunzip|zip\s)`)
)

// classify maps a response's tool calls to the hint the agent would have
// declared: the heaviest of the calls. Non-shell tools (Read, Edit, Write,
// Glob, Grep, WebFetch, ...) are memory:low; a shell command is memory:low
// when every command in it is a light one, memory:medium when it fetches
// or unpacks, memory:high (the wrapper's default, no soft limit) otherwise:
// test runners, builds, interpreters running the task's code.
func classify(names, inputs []string) Hint {
	best := HintLow
	for i, name := range names {
		h := HintLow
		switch strings.ToLower(name) {
		case "bash", "shell", "execute", "run", "terminal", "computer", "python", "exec":
			cmd := ""
			if i < len(inputs) {
				var in struct {
					Command string `json:"command"`
					Cmd     string `json:"cmd"`
					Code    string `json:"code"`
				}
				if json.Unmarshal([]byte(inputs[i]), &in) == nil {
					cmd = in.Command
					if cmd == "" {
						cmd = in.Cmd
					}
					if cmd == "" {
						cmd = in.Code
					}
				}
			}
			h = classifyCommand(cmd)
		}
		if h > best {
			best = h
		}
	}
	return best
}

// classifyCommand is the hint for one shell command line.
func classifyCommand(cmd string) Hint {
	if strings.TrimSpace(cmd) == "" {
		return HintHigh // unknown: the wrapper's default, no soft limit
	}
	if mediumCmd.MatchString(cmd) {
		return HintMedium
	}
	// Every pipeline segment light: memory:low.
	parts := regexp.MustCompile(`\s*(\|\||&&|\||;|\n)\s*`).Split(cmd, -1)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !lightCmd.MatchString(part) {
			return HintHigh
		}
	}
	return HintLow
}
