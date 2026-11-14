package agentcgroup

import (
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/meter"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

const gib = 1 << 30

func TestClassify(t *testing.T) {
	cases := []struct {
		names  []string
		inputs []string
		want   Hint
	}{
		{[]string{"Read"}, []string{`{"file_path":"/app/x.py"}`}, HintLow},
		{[]string{"Bash"}, []string{`{"command":"git status && ls -la"}`}, HintLow},
		{[]string{"Bash"}, []string{`{"command":"cat foo.txt | grep bar"}`}, HintLow},
		{[]string{"Bash"}, []string{`{"command":"pip install numpy"}`}, HintMedium},
		{[]string{"Bash"}, []string{`{"command":"python -m pytest tests/ -x"}`}, HintHigh},
		{[]string{"Bash"}, []string{`{"command":"cd /app && python solver.py"}`}, HintHigh},
		{[]string{"Bash"}, []string{`{"command":""}`}, HintHigh},
		{[]string{"Edit", "Bash"}, []string{`{}`, `{"command":"make -j8"}`}, HintHigh},
		{[]string{"Bash"}, []string{`{"command":"git clone https://x/y.git"}`}, HintMedium},
	}
	for _, c := range cases {
		if got := classify(c.names, c.inputs); got != c.want {
			t.Errorf("classify(%v, %v) = %v, want %v", c.names, c.inputs, got, c.want)
		}
	}
}

func TestOptions(t *testing.T) {
	p, err := New(map[string]string{"gate": "0.8", "weightHigh": "200", "lowHead": "512Mi", "freeze": "false", "psi": "20"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Gate.Threshold != 0.8 || p.WeightHigh != 200 || p.LowHead != 512<<20 || p.Freeze || p.PSI != 20 {
		t.Fatalf("options not applied: %+v", p)
	}
	if _, err := New(map[string]string{"bogus": "1"}); err == nil {
		t.Fatal("unknown option accepted")
	}
	if p, _ := New(nil); p.Gate.Threshold != 0 {
		t.Fatal("default must have no gate")
	}
}

func view(now time.Time, sbs ...policy.SandboxView) policy.View {
	return policy.View{Now: now, Capacity: resource.Set{CPU: 56000, Memory: 112 * gib}, Sandboxes: sbs}
}

func running(id string, mem uint64) policy.SandboxView {
	alloc := sandbox.Resources{Requests: resource.Set{CPU: 8000, Memory: 16 * gib}, Limits: resource.Set{CPU: 8000, Memory: 16 * gib}}
	sb := sandbox.Sandbox{ID: id, Status: sandbox.Status{Phase: sandbox.PhaseRunning, Allocated: alloc}}
	cg := policy.ShapeGuaranteed(alloc.Requests, alloc.Limits)
	return policy.SandboxView{Sandbox: sb, Cgroup: cg, Accounted: true, Stats: &sandbox.Stats{MemoryUsageBytes: mem}}
}

func updateOf(t *testing.T, acts []policy.Action) policy.Update {
	t.Helper()
	for _, a := range acts {
		if u, ok := a.(policy.Update); ok {
			return u
		}
	}
	t.Fatalf("no Update in %v", acts)
	return policy.Update{}
}

func TestDomainsAndEnforcement(t *testing.T) {
	p, _ := New(nil)
	t0 := time.Unix(1_700_000_000, 0)
	a, b := running("aaaaaaaaaaaaaaaa", 200<<20), running("bbbbbbbbbbbbbbbb", 300<<20)
	v := view(t0, a, b)

	// Admission without a gate: every arrival runs, swap allowed.
	d := p.Admit(a.Sandbox, v)
	if d.Kind != policy.Run || d.Allocation.Cgroup.MemorySwapLimitBytes != 0 {
		t.Fatalf("admit: %+v", d)
	}

	// A heavy call opens a HIGH domain: weight 150, memory.high = the limit.
	acts := p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Time: t0, Sandbox: a.Sandbox.ID, Call: "c1", ToolCalls: 1,
		ToolNames: []string{"Bash"}, ToolInputs: []string{`{"command":"pytest -q"}`}}, v)
	u := updateOf(t, acts)
	if meter.SharesWeight(u.Allocation.Cgroup.CPUShares) != 150 || u.Allocation.Cgroup.MemoryHighBytes != 16*gib {
		t.Fatalf("HIGH domain cgroup: %+v", u.Allocation.Cgroup)
	}
	a.Cgroup = u.Allocation.Cgroup
	// A light call opens a LOW domain: weight 50, memory.high = baseline + 256 MiB.
	acts = p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Time: t0, Sandbox: b.Sandbox.ID, Call: "c2", ToolCalls: 1,
		ToolNames: []string{"Bash"}, ToolInputs: []string{`{"command":"git status"}`}}, view(t0, a, b))
	u = updateOf(t, acts)
	if meter.SharesWeight(u.Allocation.Cgroup.CPUShares) != 50 || u.Allocation.Cgroup.MemoryHighBytes != 300<<20+256<<20 {
		t.Fatalf("LOW domain cgroup: %+v", u.Allocation.Cgroup)
	}
	b.Cgroup = u.Allocation.Cgroup
	if p.Status.CallsHigh != 1 || p.Status.CallsLow != 1 || p.Status.Domains != 2 {
		t.Fatalf("status: %+v", p.Status)
	}

	// No pressure: a sample changes nothing.
	sample := func(at time.Time, psi float64, memA, memB uint64) policy.Sample {
		return policy.Sample{Time: at, Node: host.Sample{MemTotal: 112 * gib, MemAvailable: 100 * gib, Memory: host.PSI{Some: host.Pressure{Avg10: psi}}},
			Sandboxes: map[string]sandbox.Stats{a.Sandbox.ID: {MemoryUsageBytes: memA}, b.Sandbox.ID: {MemoryUsageBytes: memB}}}
	}
	if acts := p.OnSample(sample(t0.Add(time.Second), 0, 200<<20, 300<<20), view(t0.Add(time.Second), a, b)); len(acts) != 0 {
		t.Fatalf("quiet sample produced %v", acts)
	}
	// Pressure (PSI) opens a window: HIGH protected by memory.low; LOW (at 100 MiB, under its
	// 556 MiB high) squeezed to its use plus 256 MiB of headroom, rounded up to 64 MiB: 384 MiB.
	t1 := t0.Add(2 * time.Second)
	acts = p.OnSample(sample(t1, 40, 200<<20, 100<<20), view(t1, a, b))
	var gotA, gotB bool
	for _, act := range acts {
		if u, ok := act.(policy.Update); ok {
			switch u.ID {
			case a.Sandbox.ID:
				gotA = u.Allocation.Cgroup.MemoryLowBytes == runtime.MemoryLowMax
				a.Cgroup = u.Allocation.Cgroup
			case b.Sandbox.ID:
				gotB = u.Allocation.Cgroup.MemoryHighBytes == 384<<20 && u.Allocation.Cgroup.MemoryLowBytes != runtime.MemoryLowMax
				b.Cgroup = u.Allocation.Cgroup
			}
		}
	}
	if !gotA || !gotB || p.Status.Windows != 1 || !p.Status.ProtectedNow {
		t.Fatalf("window: a=%v b=%v status=%+v acts=%v", gotA, gotB, p.Status, acts)
	}
	// The LOW domain stays over its high for freezeAfter inside renewed windows: frozen.
	var froze bool
	for i := 1; i <= 5 && !froze; i++ {
		at := t1.Add(time.Duration(i) * time.Second)
		for _, act := range p.OnSample(sample(at, 40, 200<<20, 700<<20), view(at, a, b)) {
			if _, ok := act.(policy.Freeze); ok {
				froze = true
			}
			if u, ok := act.(policy.Update); ok && u.ID == b.Sandbox.ID {
				b.Cgroup = u.Allocation.Cgroup
			}
		}
	}
	if !froze || p.Status.Freezes != 1 {
		t.Fatalf("LOW domain over its high was not frozen: %+v", p.Status)
	}
	// The window closes: the frozen sandbox is thawed and HIGH loses its protection.
	b.Sandbox.Status.Phase = sandbox.PhaseFrozen
	t2 := t1.Add(10 * time.Second)
	acts = p.OnSample(sample(t2, 0, 200<<20, 700<<20), view(t2, a, b))
	var thawed, unprotected bool
	for _, act := range acts {
		if _, ok := act.(policy.Thaw); ok {
			thawed = true
		}
		if u, ok := act.(policy.Update); ok && u.ID == a.Sandbox.ID && u.Allocation.Cgroup.MemoryLowBytes == runtime.MemoryLowNone {
			unprotected = true
		}
	}
	if !thawed || !unprotected {
		t.Fatalf("after the window: thawed=%v unprotected=%v acts=%v", thawed, unprotected, acts)
	}
	// The call ends: the domain closes, weight back to LOW, memory.high back to the limit.
	b.Sandbox.Status.Phase = sandbox.PhaseRunning
	acts = p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRequestStarted, Time: t2, Sandbox: b.Sandbox.ID, Call: "c3"}, view(t2, a, b))
	u = updateOf(t, acts)
	if u.Allocation.Cgroup.MemoryHighBytes != 16*gib || meter.SharesWeight(u.Allocation.Cgroup.CPUShares) != 50 || p.Status.Domains != 1 || p.Status.Feedback != 1 {
		t.Fatalf("closed domain: %+v status %+v", u.Allocation.Cgroup, p.Status)
	}
}

func TestGate(t *testing.T) {
	p, _ := New(map[string]string{"gate": "0.8"})
	t0 := time.Unix(1_700_000_000, 0)
	a := running("aaaaaaaaaaaaaaaa", 0)
	// Load under the threshold: run; a sample pushing memory over it: reject.
	p.OnSample(policy.Sample{Time: t0, Node: host.Sample{MemTotal: 112 * gib, MemAvailable: 100 * gib, CPUTotal: 1000, CPUBusy: 100}}, view(t0))
	if d := p.Admit(a.Sandbox, view(t0)); d.Kind != policy.Run {
		t.Fatalf("gate open: %+v", d)
	}
	p.OnSample(policy.Sample{Time: t0.Add(time.Second), Node: host.Sample{MemTotal: 112 * gib, MemAvailable: 5 * gib, CPUTotal: 2000, CPUBusy: 200}}, view(t0))
	if d := p.Admit(a.Sandbox, view(t0)); d.Kind != policy.Reject {
		t.Fatalf("gate closed: %+v", d)
	}
}

// A Freeze is issued once per episode: not again while the phase still says
// Running (the manager applies it later), forgotten after MaxFreeze when it
// never showed, and a Thaw already issued is not repeated within MaxFreeze.
func TestFreezeOnce(t *testing.T) {
	p, _ := New(nil)
	t0 := time.Unix(1_700_000_000, 0)
	a, b := running("aaaaaaaaaaaaaaaa", 200<<20), running("bbbbbbbbbbbbbbbb", 100<<20)
	acts := p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Time: t0, Sandbox: b.Sandbox.ID, Call: "c1", ToolCalls: 1,
		ToolNames: []string{"Bash"}, ToolInputs: []string{`{"command":"git status"}`}}, view(t0, a, b))
	b.Cgroup = updateOf(t, acts).Allocation.Cgroup
	sample := func(at time.Time, psi float64, memB uint64) policy.Sample {
		return policy.Sample{Time: at, Node: host.Sample{MemTotal: 112 * gib, MemAvailable: 100 * gib, Memory: host.PSI{Some: host.Pressure{Avg10: psi}}},
			Sandboxes: map[string]sandbox.Stats{a.Sandbox.ID: {MemoryUsageBytes: 200 << 20}, b.Sandbox.ID: {MemoryUsageBytes: memB}}}
	}
	count := func(acts []policy.Action) (freezes, thaws int) {
		for _, act := range acts {
			switch u := act.(type) {
			case policy.Freeze:
				freezes++
			case policy.Thaw:
				thaws++
			case policy.Update:
				if u.ID == b.Sandbox.ID {
					b.Cgroup = u.Allocation.Cgroup
				}
			}
		}
		return
	}
	// Under pressure, the LOW domain over its high: one Freeze within freezeAfter+1 samples.
	freezes := 0
	for i := 0; i <= 5; i++ {
		at := t0.Add(time.Duration(i+1) * time.Second)
		f, _ := count(p.OnSample(sample(at, 40, 900<<20), view(at, a, b)))
		freezes += f
	}
	if freezes != 1 || p.Status.Freezes != 1 {
		t.Fatalf("freezes issued while the phase stayed Running: %d (status %+v)", freezes, p.Status)
	}
	// Still Running after MaxFreeze: the freeze is forgotten, the cooldown starts, no new one yet.
	at := t0.Add(6*time.Second + p.MaxFreeze)
	if f, _ := count(p.OnSample(sample(at, 40, 900<<20), view(at, a, b))); f != 0 || p.Status.FreezesLost != 1 || !p.Boxes[b.Sandbox.ID].FrozenAt.IsZero() {
		t.Fatalf("lost freeze not forgotten: f=%d status %+v", f, p.Status)
	}
	// After the cooldown and freezeAfter: a second Freeze.
	freezes = 0
	for i := 1; i <= 8; i++ {
		at2 := at.Add(time.Duration(i) * time.Second)
		f, _ := count(p.OnSample(sample(at2, 40, 900<<20), view(at2, a, b)))
		freezes += f
	}
	if freezes != 1 || p.Status.Freezes != 2 {
		t.Fatalf("second episode: %d freezes, status %+v", freezes, p.Status)
	}
	// The phase shows Frozen; the window closes: one Thaw, not repeated while it is in flight,
	// repeated once MaxFreeze passed with the phase still Frozen.
	b.Sandbox.Status.Phase = sandbox.PhaseFrozen
	t3 := at.Add(20 * time.Second)
	thaws := 0
	for i := 0; i < 5; i++ {
		at3 := t3.Add(time.Duration(i) * time.Second)
		_, th := count(p.OnSample(sample(at3, 0, 900<<20), view(at3, a, b)))
		thaws += th
	}
	if thaws != 1 {
		t.Fatalf("thaw repeated while in flight: %d", thaws)
	}
	t4 := t3.Add(p.MaxFreeze + time.Second)
	if _, th := count(p.OnSample(sample(t4, 0, 900<<20), view(t4, a, b))); th != 1 {
		t.Fatalf("thaw not repeated after MaxFreeze: %d", th)
	}
	// A frozen sandbox the policy has no record of is thawed at once.
	c := running("cccccccccccccccc", 100<<20)
	c.Sandbox.Status.Phase = sandbox.PhaseFrozen
	t5 := t4.Add(p.MaxFreeze + time.Second)
	s := sample(t5, 0, 900<<20)
	s.Sandboxes[c.Sandbox.ID] = sandbox.Stats{MemoryUsageBytes: 100 << 20}
	var thawedC bool
	for _, act := range p.OnSample(s, view(t5, a, b, c)) {
		if th, ok := act.(policy.Thaw); ok && th.ID == c.Sandbox.ID {
			thawedC = true
		}
	}
	if !thawedC {
		t.Fatal("unrecorded frozen sandbox not thawed")
	}
}

// A LOW domain throttled at its hint's memory.high for Escalate sample-seconds
// is promoted to memory:medium, then to memory:high (HIGH class, weight 150,
// memory.high = the limit); the window squeeze no longer applies to it.
func TestEscalate(t *testing.T) {
	p, _ := New(map[string]string{"escalate": "5s"})
	t0 := time.Unix(1_700_000_000, 0)
	a := running("aaaaaaaaaaaaaaaa", 300<<20)
	acts := p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Time: t0, Sandbox: a.Sandbox.ID, Call: "c1", ToolCalls: 1,
		ToolNames: []string{"Bash"}, ToolInputs: []string{`{"command":"cat big.txt"}`}}, view(t0, a))
	a.Cgroup = updateOf(t, acts).Allocation.Cgroup
	if a.Cgroup.MemoryHighBytes != 300<<20+256<<20 {
		t.Fatalf("LOW cap: %d", a.Cgroup.MemoryHighBytes)
	}
	sample := func(at time.Time, mem uint64) policy.Sample {
		return policy.Sample{Time: at, Node: host.Sample{MemTotal: 112 * gib, MemAvailable: 100 * gib},
			Sandboxes: map[string]sandbox.Stats{a.Sandbox.ID: {MemoryUsageBytes: mem}}}
	}
	apply := func(acts []policy.Action) {
		for _, act := range acts {
			if u, ok := act.(policy.Update); ok && u.ID == a.Sandbox.ID {
				a.Cgroup = u.Allocation.Cgroup
			}
		}
	}
	// Over the cap (a background job keeps allocating): 5 samples, then medium.
	for i := 1; i <= 5; i++ {
		at := t0.Add(time.Duration(i) * time.Second)
		apply(p.OnSample(sample(at, 600<<20), view(at, a)))
	}
	d := p.Boxes[a.Sandbox.ID].Domain
	if d.Hint != HintMedium || d.Promoted != 1 || a.Cgroup.MemoryHighBytes != 300<<20+1024<<20 || p.Status.Escalations != 1 {
		t.Fatalf("first escalation: hint %v promoted %d high %d status %+v", d.Hint, d.Promoted, a.Cgroup.MemoryHighBytes, p.Status)
	}
	// Still over the new cap for 5 more: high, HIGH class, weight 150, cap = the limit.
	for i := 6; i <= 10; i++ {
		at := t0.Add(time.Duration(i) * time.Second)
		apply(p.OnSample(sample(at, 1400<<20), view(at, a)))
	}
	if d.Hint != HintHigh || !d.High || d.Promoted != 2 || a.Cgroup.MemoryHighBytes != 16*gib || meter.SharesWeight(a.Cgroup.CPUShares) != 150 {
		t.Fatalf("second escalation: hint %v high %v promoted %d memory.high %d weight %d", d.Hint, d.High, d.Promoted, a.Cgroup.MemoryHighBytes, meter.SharesWeight(a.Cgroup.CPUShares))
	}
	if p.Status.CallsHigh != 1 || p.Status.CallsLow != 0 {
		t.Fatalf("class counters: %+v", p.Status)
	}
	// escalate=0 disables it.
	q, _ := New(map[string]string{"escalate": "0"})
	if q.Escalate != 0 {
		t.Fatal("escalate=0 not applied")
	}
}
