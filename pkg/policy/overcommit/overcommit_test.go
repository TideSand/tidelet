package overcommit

import (
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

var t0 = time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC)

func node(at time.Duration, busy, total uint64, mem float64) host.Sample {
	all := uint64(16 << 30)
	return host.Sample{Time: t0.Add(at), CPUBusy: busy, CPUTotal: total, MemTotal: all, MemAvailable: uint64(float64(all) * (1 - mem))}
}

func sb(id string, phase sandbox.Phase) policy.SandboxView {
	s := sandbox.Sandbox{ID: id, CreatedAt: t0, Spec: sandbox.Spec{Resources: sandbox.Resources{Limits: resource.Set{CPU: 2000, Memory: 4 << 30}}}}
	s.Status.Phase = phase
	return policy.SandboxView{Sandbox: s, Accounted: phase != sandbox.PhaseQueued}
}

func sample(at time.Duration, busy, total uint64, mem float64, sbs ...policy.SandboxView) (policy.Sample, policy.View) {
	n := node(at, busy, total, mem)
	return policy.Sample{Time: n.Time, Node: n}, policy.View{Now: n.Time, Capacity: resource.Set{CPU: 8000, Memory: 16 << 30}, Node: n, Sandboxes: sbs}
}

func TestBaseline(t *testing.T) {
	p, err := New(map[string]string{"threshold": "0.5"})
	if err != nil || p.Threshold != 0.5 {
		t.Fatal(err)
	}
	if _, err := New(map[string]string{"threshold": "2"}); err == nil {
		t.Fatal("threshold above 1 accepted")
	}
	if _, err := New(map[string]string{"settle": "3s"}); err == nil {
		t.Fatal("the pacing is the workload's now: settle must not be an option")
	}
	q1, q2 := sb("q1", sandbox.PhasePending), sb("q2", sandbox.PhasePending)
	// Limits the pool could never hold are invalid.
	big := q1.Sandbox
	big.Spec.Resources.Limits.CPU = 16000
	if d := p.Admit(big, policy.View{Capacity: resource.Set{CPU: 8000, Memory: 16 << 30}}); d.Kind != policy.Invalid {
		t.Fatalf("oversized admit = %+v", d)
	}
	// Nothing measured yet, load 0: the first arrival starts, its limits hard caps, swap on.
	_, v := sample(0, 0, 0, 0.2)
	d := p.Admit(q1.Sandbox, v)
	if d.Kind != policy.Run || d.Allocation.Limits.CPU != 2000 || d.Allocation.Cgroup.CPUQuotaUS != 200_000 || d.Allocation.Cgroup.MemorySwapLimitBytes != 0 || p.Status.Admitted != "q1" {
		t.Fatalf("first arrival: %+v (status %+v)", d, p.Status)
	}
	// Two seconds later q1 boots: an arrival is rejected while it does.
	s, v := sample(2*time.Second, 0, 0, 0.2, sb("q1", sandbox.PhaseCreated))
	p.OnSample(s, v)
	if d := p.Admit(q2.Sandbox, v); d.Kind != policy.Reject || p.Status.Rejected != 1 {
		t.Fatalf("started while booting: %+v", d)
	}
	// Running and quiet: q2 starts.
	s, v = sample(4*time.Second, 100, 4000, 0.2, sb("q1", sandbox.PhaseRunning))
	p.OnSample(s, v)
	if d := p.Admit(q2.Sandbox, v); d.Kind != policy.Run || !p.Status.Open {
		t.Fatalf("quiet pool did not start q2: %+v", d)
	}
	// Busy pool (80 % over the interval, smoothed in): shut.
	p2, _ := New(map[string]string{"threshold": "0.5"})
	s, v = sample(0, 0, 1000, 0.2, sb("r", sandbox.PhaseRunning))
	p2.OnSample(s, v)
	for i := 1; i <= 6; i++ { // the smoothed figure needs a few samples to reach 80 %
		s, v = sample(time.Duration(i)*time.Second, uint64(i)*800, 1000+uint64(i)*1000, 0.2, sb("r", sandbox.PhaseRunning))
		p2.OnSample(s, v)
	}
	if d := p2.Admit(q2.Sandbox, v); d.Kind != policy.Reject || p2.Status.Load.CPU < 0.5 {
		t.Fatalf("busy pool started a sandbox: %+v (load %+v)", d, p2.Status.Load)
	}
	// Full memory: shut even with idle CPUs.
	p3, _ := New(nil)
	s, v = sample(0, 0, 0, 0.9, sb("r", sandbox.PhaseRunning))
	p3.OnSample(s, v)
	if d := p3.Admit(q2.Sandbox, v); d.Kind != policy.Reject {
		t.Fatalf("full memory started a sandbox: %+v", d)
	}
}
