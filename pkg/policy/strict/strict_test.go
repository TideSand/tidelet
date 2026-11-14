package strict

import (
	"testing"

	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

func gib(n int64) resource.Bytes { return resource.Bytes(n << 30) }

func view(allocated resource.Set) policy.View {
	return policy.View{
		Capacity:  resource.Set{CPU: 4000, Memory: gib(8)},
		Reserved:  resource.Set{CPU: 500, Memory: gib(1)},
		Allocated: allocated,
	}
}

func sb(req, lim resource.Set) sandbox.Sandbox {
	return sandbox.Sandbox{ID: "x", Spec: sandbox.Spec{Resources: sandbox.Resources{Requests: req, Limits: lim}}}
}

func TestAllocatable(t *testing.T) {
	p, _ := New(nil)
	if got := p.Allocatable(view(resource.Set{})); got != (resource.Set{CPU: 3500, Memory: gib(7)}) {
		t.Fatalf("allocatable = %v", got)
	}
}

func TestAdmitCopiesMissingSide(t *testing.T) {
	p, _ := New(nil)
	d := p.Admit(sb(resource.Set{}, resource.Set{CPU: 1000, Memory: gib(1)}), view(resource.Set{}))
	if d.Kind != policy.Run {
		t.Fatalf("decision = %+v", d)
	}
	a := d.Allocation
	if a.Requests != a.Limits || a.Requests.CPU != 1000 || a.Requests.Memory != gib(1) {
		t.Fatalf("allocation = %+v", a)
	}
	if a.Cgroup.CPUQuotaUS != 100_000 || a.Cgroup.CPUPeriodUS != 100_000 || a.Cgroup.CPUShares != 1024 {
		t.Errorf("cpu cgroup = %+v", a.Cgroup)
	}
	if a.Cgroup.MemoryLimitBytes != int64(gib(1)) || a.Cgroup.MemorySwapLimitBytes != -1 {
		t.Errorf("memory cgroup = %+v", a.Cgroup)
	}
	d = p.Admit(sb(resource.Set{CPU: 500}, resource.Set{}), view(resource.Set{}))
	if d.Kind != policy.Run || d.Allocation.Limits.CPU != 500 || d.Allocation.Cgroup.CPUQuotaUS != 50_000 {
		t.Fatalf("request-only decision = %+v", d)
	}
}

func TestAdmitSwap(t *testing.T) {
	p, err := New(map[string]string{"swap": "true"})
	if err != nil {
		t.Fatal(err)
	}
	d := p.Admit(sb(resource.Set{}, resource.Set{CPU: 1000, Memory: gib(2)}), view(resource.Set{}))
	if d.Kind != policy.Run || d.Allocation.Cgroup.MemoryLimitBytes != int64(gib(2)) || d.Allocation.Cgroup.MemorySwapLimitBytes != int64(gib(2)) {
		t.Fatalf("swap decision = %+v", d)
	}
	if d.Allocation.Requests.Memory != gib(2) {
		t.Errorf("swap must not change what is accounted: %+v", d.Allocation)
	}
	if d := p.Admit(sb(resource.Set{CPU: 500}, resource.Set{}), view(resource.Set{})); d.Allocation.Cgroup.MemorySwapLimitBytes != 0 {
		t.Errorf("no memory limit, no swap setting: %+v", d.Allocation.Cgroup)
	}
	if _, err := New(map[string]string{"swap": "maybe"}); err == nil {
		t.Fatal("bad swap value accepted")
	}
}

func TestAdmitRejectsMismatch(t *testing.T) {
	p, _ := New(nil)
	d := p.Admit(sb(resource.Set{CPU: 500}, resource.Set{CPU: 1000}), view(resource.Set{}))
	if d.Kind != policy.Invalid {
		t.Fatalf("decision = %+v", d)
	}
}

func TestAdmitUnlimited(t *testing.T) {
	p, _ := New(nil)
	d := p.Admit(sb(resource.Set{}, resource.Set{}), view(resource.Set{}))
	if d.Kind != policy.Run || !d.Allocation.Requests.IsZero() || d.Allocation.Cgroup != (policy.ShapeGuaranteed(resource.Set{}, resource.Set{})) {
		t.Fatalf("unlimited decision = %+v", d)
	}
	strictP, err := New(map[string]string{"requireLimits": "true"})
	if err != nil {
		t.Fatal(err)
	}
	if d := strictP.Admit(sb(resource.Set{}, resource.Set{}), view(resource.Set{})); d.Kind != policy.Invalid {
		t.Fatalf("requireLimits decision = %+v", d)
	}
	if _, err := New(map[string]string{"bogus": "1"}); err == nil {
		t.Fatal("unknown option accepted")
	}
}

func TestAdmitNoOvercommit(t *testing.T) {
	p, _ := New(nil)
	used := resource.Set{CPU: 3000, Memory: gib(6)}
	// 3000 + 500 = 3500 fits exactly.
	if d := p.Admit(sb(resource.Set{}, resource.Set{CPU: 500, Memory: gib(1)}), view(used)); d.Kind != policy.Run {
		t.Fatalf("exact fit rejected: %+v", d)
	}
	if d := p.Admit(sb(resource.Set{}, resource.Set{CPU: 501}), view(used)); d.Kind != policy.Reject {
		t.Fatalf("cpu overcommit = %+v", d)
	}
	if d := p.Admit(sb(resource.Set{}, resource.Set{Memory: gib(2)}), view(used)); d.Kind != policy.Reject {
		t.Fatalf("memory overcommit = %+v", d)
	}
}

func TestHooksAreNoops(t *testing.T) {
	p, _ := New(nil)
	if p.OnEvent(sandbox.Event{}, policy.View{}) != nil || p.OnSample(policy.Sample{}, policy.View{}) != nil {
		t.Fatal("strict must not act")
	}
}

func TestQuotaMinimum(t *testing.T) {
	if q := policy.MillicoresToQuota(1, policy.DefaultCPUPeriodUS); q != 1000 {
		t.Fatalf("quota for 1m = %d, want 1000", q)
	}
	if s := policy.MillicoresToShares(1); s != 2 {
		t.Fatalf("shares for 1m = %d, want 2", s)
	}
}
