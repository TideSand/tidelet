package tidesand

import (
	"strings"
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

var t0 = time.Date(2026, 10, 27, 2, 0, 0, 0, time.UTC)

func sb(id string, cores float64) sandbox.Sandbox {
	s := sandbox.Sandbox{ID: id, Spec: sandbox.Spec{Resources: sandbox.Resources{Limits: resource.Set{CPU: resource.Millicores(cores * 1000), Memory: resource.Bytes(16 << 30)}}}}
	s.Status.Phase = sandbox.PhaseRunning
	return s
}

func view(now time.Time, sbs ...sandbox.Sandbox) policy.View {
	v := policy.View{Now: now, Capacity: resource.Set{CPU: 56000, Memory: resource.Bytes(112 << 30)}}
	for _, s := range sbs {
		v.Sandboxes = append(v.Sandboxes, policy.SandboxView{Sandbox: s, Accounted: true})
	}
	return v
}

func ready(id, call string, at time.Time, tools ...string) policy.ModelEvent {
	return policy.ModelEvent{Kind: policy.ModelResponseReady, Sandbox: id, Call: call, ToolCalls: len(tools), ToolNames: tools, Time: at}
}

func request(id, call string, at time.Time) policy.ModelEvent {
	return policy.ModelEvent{Kind: policy.ModelRequestStarted, Sandbox: id, Call: call, Time: at}
}

func sample(at time.Time, available uint64, stats map[string]sandbox.Stats) policy.Sample {
	return policy.Sample{Time: at, Node: host.Sample{Time: at, MemTotal: 112 << 30, MemAvailable: available, SwapTotal: 64 << 30}, Sandboxes: stats}
}

func releases(acts []policy.Action) []string {
	var out []string
	for _, a := range acts {
		if r, ok := a.(policy.Release); ok {
			out = append(out, r.ID)
		}
	}
	return out
}

func kinds(acts []policy.Action) []string {
	var out []string
	for _, a := range acts {
		switch a.(type) {
		case policy.Hold:
			out = append(out, "hold")
		case policy.Release:
			out = append(out, "release")
		case policy.Reclaim:
			out = append(out, "reclaim")
		case policy.Prefetch:
			out = append(out, "prefetch")
		case policy.Rollback:
			out = append(out, "rollback")
		case policy.Restore:
			out = append(out, "restore")
		case policy.Update:
			out = append(out, "update")
		case policy.Dispatch:
			out = append(out, "dispatch")
		}
	}
	return out
}

func noTraffic() map[string]string {
	return map[string]string{"target": "0.25", "bandwidth": "0", "park": "0", "slots": "0"}
}

func TestOptions(t *testing.T) {
	p, err := New(map[string]string{"target": "0.7", "bwTarget": "0.6", "park": "0.9", "pressure": "0.8", "minInsured": "2", "insureEvery": "5s", "slots": "1"})
	if err != nil || p.Target != 0.7 || p.BwTarget != 0.6 || p.Park != 0.9 || p.Pressure != 0.8 || p.MinInsured != 2 || p.InsureEvery != 5*time.Second || p.Slots != 1 {
		t.Fatalf("New = %+v, %v", p, err)
	}
	for _, bad := range []map[string]string{{"target": "0"}, {"ledger": "1"}, {"park": "2"}, {"insure": "2", "busy": "1"}, {"nope": "1"}, {"slots": "-1"}} {
		if _, err := New(bad); err == nil {
			t.Fatalf("New(%v) accepted", bad)
		}
	}
}

// The ceiling: on a budget of 14 cores the first 8-core call runs, the second
// is held behind it, a 2-core call passes, and the head goes when the first
// call ends.
func TestTheCeiling(t *testing.T) {
	p, _ := New(noTraffic())
	a, b, c := sb("a", 8), sb("b", 8), sb("c", 2)
	v := view(t0, a, b, c)
	for _, s := range []sandbox.Sandbox{a, b, c} {
		if d := p.Admit(s, v); d.Kind != policy.Run {
			t.Fatalf("%s: %v", s.ID, d)
		}
	}
	if acts := p.OnModelEvent(ready("a", "a1", t0, "Bash"), v); len(acts) != 0 || p.Status.Released != 1 || p.Boxes["a"].Call.Reserve != 8 {
		t.Fatalf("a must run on its limit: %v %+v", acts, p.Status)
	}
	if acts := p.OnModelEvent(ready("b", "b1", t0, "Bash"), v); len(acts) != 1 || p.Status.Held != 1 {
		t.Fatalf("b must be held: %v", acts)
	}
	if acts := p.OnModelEvent(ready("c", "c1", t0, "Bash"), v); len(releases(acts)) != 1 || p.Status.HeldNow != 1 {
		t.Fatalf("c (2 cores) is held behind b and passes it at once: %v", acts)
	}
	if got := releases(p.OnModelEvent(request("a", "a1", t0.Add(time.Second)), v)); len(got) != 1 || got[0] != "b" || p.Status.HeldNow != 0 {
		t.Fatalf("b must go when a ends: %v", got)
	}
}

// Traffic is a ceiling of its own: a call whose sandbox moved 150 GB/s is
// held while the pool's traffic leaves no room for it.
func TestTraffic(t *testing.T) {
	p, _ := New(map[string]string{"target": "1.0", "bandwidth": "250G", "bwTarget": "0.8", "park": "0", "slots": "0"})
	a := sb("a", 8)
	v := view(t0, a)
	p.Admit(a, v)
	p.Boxes["a"].BwPeaks = []float64{30e9}
	p.bwUsed = 180e9
	if acts := p.OnModelEvent(ready("a", "a1", t0, "Bash"), v); len(acts) != 1 || p.Status.HeldNow != 1 {
		t.Fatalf("180 + 30 > 200 GB/s must hold: %v", acts)
	}
	p.bwUsed = 40e9
	if got := releases(p.schedule(t0.Add(time.Second))); len(got) != 1 || p.Boxes["a"].Call.BwReserve != 30e9 {
		t.Fatalf("40 + 30 <= 200 GB/s: it goes on its traffic reservation: %v", got)
	}
	// A call that moved the whole machine's traffic reserves at most its CPU
	// share of it (8 of 56 cores: 35.7 of 250 GB/s), so it does not wait for an
	// idle pool.
	p.endCall(p.Boxes["a"], t0.Add(5*time.Second))
	p.Boxes["a"].BwPeaks = []float64{250e9}
	if r := p.bwReserve(p.Boxes["a"]); r < 35.6e9 || r > 35.8e9 {
		t.Fatalf("the traffic reservation is capped at the sandbox's share: %.1f GB/s", r/1e9)
	}
	p.bwUsed = 150e9
	if acts := p.OnModelEvent(ready("a", "a2", t0.Add(6*time.Second), "Bash"), v); len(acts) != 0 || p.Boxes["a"].Call == nil || p.Boxes["a"].Call.ID != "a2" || p.Status.HeldNow != 0 {
		t.Fatalf("150 + 35.7 <= 200 GB/s: it goes at once: %v (%+v)", acts, p.Boxes["a"].Call)
	}
}

// Parking: above the park line the idle resident holding most is reclaimed;
// its next response waits for its memory to fit, is prefetched, and goes
// when the memory is back.
func TestParkAndPrefetch(t *testing.T) {
	p, _ := New(map[string]string{"target": "1.0", "bandwidth": "0", "park": "0.9", "parkIdle": "60s", "parkRate": "0", "slots": "0", "idleQuota": "0"})
	a, b := sb("a", 8), sb("b", 8)
	v := view(t0, a, b)
	p.Admit(a, v)
	p.Admit(b, v)
	big, small := uint64(40<<30), uint64(10<<30)
	stats := map[string]sandbox.Stats{"a": {CPUUsageNanos: 0, MemoryWorkingSetBytes: big}, "b": {CPUUsageNanos: 0, MemoryWorkingSetBytes: small}}
	p.OnSample(sample(t0.Add(20*time.Second), 60<<30, stats), v) // 52 of 112 GiB: under the line
	if p.Status.Parked != 0 {
		t.Fatalf("under the park line nothing is parked: %+v", p.Status)
	}
	acts := p.OnSample(sample(t0.Add(70*time.Second), 10<<30, stats), v) // 102 of 112 GiB: 0.91 > 0.85, both idle 70 s
	if k := kinds(acts); len(k) != 2 || k[0] != "reclaim" || acts[0].(policy.Reclaim).ID != "a" || acts[1].(policy.Reclaim).ID != "b" || p.Status.Parked != 2 || p.Boxes["a"].ParkedRSS != float64(big) {
		t.Fatalf("both idle residents are parked, the one holding most first, two per sample: %v (%+v)", acts, p.Status)
	}
	// Its next response (a light one) is held while its memory is prefetched at once.
	acts = p.OnModelEvent(ready("a", "a1", t0.Add(80*time.Second), "Read"), v)
	if k := kinds(acts); len(k) != 2 || k[0] != "hold" || k[1] != "prefetch" || p.Status.LightHeld != 1 || p.Status.Prefetched != 1 || !p.Held[0].Prefetch {
		t.Fatalf("a parked sandbox's response is held and its memory prefetched at once: %v", acts)
	}
	// While its response is held and its memory on its way back, it is not parked again.
	if acts := p.OnSample(sample(t0.Add(82*time.Second), 10<<30, stats), v); len(acts) != 0 || p.Status.Parked != 2 {
		t.Fatalf("a sandbox whose response is held is not parked again: %v (%+v)", acts, p.Status)
	}
	acts = p.OnEvent(sandbox.Event{Type: sandbox.EventPrefetched, Sandbox: a, Time: t0.Add(83 * time.Second)}, v)
	if got := releases(acts); len(got) != 1 || got[0] != "a" || p.Status.HeldNow != 0 {
		t.Fatalf("the response goes once the memory is back: %v", acts)
	}
}

// Insurance is selective: a heavy call on a loaded pool, at most one per
// InsureEvery, never a light one or on a quiet pool; the rollback stops the
// insured call furthest above its reservation when the pool has been
// saturated for Spike, and restores it first in line.
func TestInsuranceAndRollback(t *testing.T) {
	p, _ := New(map[string]string{"target": "0.5", "bandwidth": "0", "park": "0", "insure": "0.5", "busy": "1.5", "minInsured": "4", "insureEvery": "10s", "spike": "3s", "cooldown": "10s", "pressure": "0.85", "idleQuota": "0"})
	a, b, c := sb("a", 8), sb("b", 8), sb("c", 1)
	v := view(t0, a, b, c)
	for _, s := range []sandbox.Sandbox{a, b, c} {
		p.Admit(s, v)
	}
	p.measured = 20 // 20 of the 28-core budget: loaded enough
	if p.CheckpointBefore(request("c", "c1", t0)) || p.Status.SkippedLight != 1 {
		t.Fatalf("a 1-core call is never insured: %+v", p.Status)
	}
	if !p.CheckpointBefore(request("a", "a1", t0)) || p.Status.Insured != 1 || p.dumps != 1 {
		t.Fatalf("a heavy call on a loaded pool is insured: %+v", p.Status)
	}
	if p.CheckpointBefore(request("b", "b1", t0.Add(2*time.Second))) || p.Status.SkippedRate != 1 {
		t.Fatalf("one insured call per InsureEvery: %+v", p.Status)
	}
	p.CheckpointDone("a", "a1", 400*time.Millisecond, nil)
	if p.dumps != 0 || !p.Boxes["a"].InsuredOK || len(p.costs) != 1 {
		t.Fatalf("the dump's cost is learned: %+v", p.Boxes["a"])
	}
	p.measured = 5
	if p.CheckpointBefore(request("b", "b1", t0.Add(20*time.Second))) || p.Status.SkippedQuiet != 1 {
		t.Fatalf("a quiet pool insures nothing: %+v", p.Status)
	}
	// a's call runs insured; b's uninsured.
	p.measured = 5
	p.OnModelEvent(ready("a", "a1", t0.Add(21*time.Second), "Bash"), v)
	p.OnModelEvent(ready("b", "b1", t0.Add(21*time.Second), "Bash"), v)
	if !p.Boxes["a"].Call.Insured || p.Boxes["b"].Call.Insured {
		t.Fatalf("only a is insured: %+v %+v", p.Boxes["a"].Call, p.Boxes["b"].Call)
	}
	// The pool saturates (50 of 56 cores) for 3 s: a, insured and 4 cores above its reservation, is rolled back.
	p.Boxes["a"].Use, p.Boxes["b"].Use = 12, 30
	for i := 0; i <= 3; i++ {
		p.measured = 50
		p.cpuPressureSince = pressureRun(p.cpuPressureSince, t0.Add(time.Duration(30+i)*time.Second), true)
	}
	acts := p.rollback(t0.Add(33 * time.Second))
	if k := kinds(acts); len(k) != 1 || k[0] != "rollback" || acts[0].(policy.Rollback).ID != "a" || p.Status.Rounds != 1 || !p.Boxes["a"].Rolling {
		t.Fatalf("the insured spiker is rolled back: %v (%+v)", acts, p.Status)
	}
	if more := p.rollback(t0.Add(34 * time.Second)); len(more) != 0 {
		t.Fatalf("one round per cooldown: %v", more)
	}
	acts = p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRolledBack, Sandbox: "a", Call: "a1", Time: t0.Add(35 * time.Second), Killed: 3, Whole: true}, v)
	if p.Status.Rollbacks != 1 || !p.Boxes["a"].Stopped || len(p.Held) != 1 || !p.Held[0].Restore {
		t.Fatalf("undone: a restore is queued first in line: %v (%+v)", acts, p.Status)
	}
	// With room again the restore goes; the agent asks again and the call reruns at once, never stopped again.
	p.measured = 5
	acts = p.schedule(t0.Add(40 * time.Second))
	if k := kinds(acts); len(k) != 1 || k[0] != "restore" || p.Boxes["a"].Call == nil || !p.Boxes["a"].Call.Rerun {
		t.Fatalf("the restore goes when the pool has room: %v", acts)
	}
	p.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRestored, Sandbox: "a", Call: "a1", Time: t0.Add(41 * time.Second)}, v)
	if p.Status.Restores != 1 || p.Boxes["a"].Stopped || !p.Boxes["a"].Rerun {
		t.Fatalf("restored: the next heavy response reruns the call: %+v", p.Boxes["a"])
	}
	if acts := p.OnModelEvent(request("a", "a1", t0.Add(42*time.Second)), v); len(acts) != 0 || p.Boxes["a"].Call == nil {
		t.Fatalf("the restored agent's request is not a new call: %v", acts)
	}
	acts = p.OnModelEvent(ready("a", "a1", t0.Add(43*time.Second), "Bash"), v)
	if len(acts) != 0 || p.Boxes["a"].Rerun || p.Boxes["a"].Call == nil || !p.Boxes["a"].Call.Rerun || p.Status.Reruns != 1 {
		t.Fatalf("the rerun goes at once: %v (%+v)", acts, p.Status)
	}
}

func TestAdmission(t *testing.T) {
	p, _ := New(noTraffic())
	a, b, c := sb("a", 8), sb("b", 8), sb("c", 1)
	v := view(t0, a, b)
	p.Admit(a, v)
	p.Admit(b, v)
	p.OnModelEvent(ready("a", "a1", t0, "Bash"), v)
	p.OnModelEvent(ready("b", "b1", t0, "Bash"), v) // held: 2 boots + 8 + 8 > 14
	if d := p.Admit(c, v); d.Kind != policy.Run {
		t.Fatalf("a response held for a moment does not close the gate: %v", d)
	}
	delete(p.Boxes, "c")
	aged := view(t0.Add(31*time.Second), a, b)
	if d := p.Admit(c, aged); d.Kind != policy.Queue || !strings.Contains(d.Reason, "held") {
		t.Fatalf("an arrival must queue once a response has waited MaxPass: %v", d)
	}
	// Memory: the residents' working sets fill the target.
	q, _ := New(map[string]string{"park": "0", "slots": "0"})
	q.Admit(a, v)
	q.OnSample(sample(t0.Add(time.Second), 10<<30, map[string]sandbox.Stats{"a": {CPUUsageNanos: 1e9, MemoryWorkingSetBytes: 100 << 30}}), v)
	if d := q.Admit(b, v); d.Kind != policy.Queue || !strings.Contains(d.Reason, "memory") {
		t.Fatalf("an arrival must queue on memory: %v", d)
	}
}

// The aged head books its reservation: younger responses pass only on what
// the budget has left over, so the pool drains down to the head.
func TestBooking(t *testing.T) {
	p, _ := New(map[string]string{"target": "0.25", "bandwidth": "0", "park": "0", "slots": "0", "idleQuota": "0", "book": "30s", "pressure": "0.3"}) // 14 of 56 cores; booking on; no room on the machine past 16.8
	a, b, c := sb("a", 8), sb("b", 2), sb("c", 2)
	v := view(t0.Add(-20*time.Second), a, b, c) // started 20 s ago: settled, no boot term
	for _, s := range []sandbox.Sandbox{a, b, c} {
		p.Admit(s, v)
	}
	p.measured = 10
	if acts := p.OnModelEvent(ready("a", "a1", t0, "Bash"), v); p.Status.HeldNow != 1 {
		t.Fatalf("10 + 8 > 14: the head is held: %v", acts)
	}
	if acts := p.OnModelEvent(ready("b", "b1", t0.Add(time.Second), "Bash"), v); len(releases(acts)) != 1 || p.Boxes["b"].Call == nil {
		t.Fatalf("10 + 2 <= 14: a younger response passes the head before it has waited MaxPass: %v", acts)
	}
	// 31 s on: the head has waited its turn out and books its 8 cores while b's call runs.
	if acts := p.OnModelEvent(ready("c", "c1", t0.Add(31*time.Second), "Bash"), v); len(releases(acts)) != 0 || p.Status.Booked != 1 || p.Status.HeldNow != 2 {
		t.Fatalf("10 + 2 + the head's 8 > 14: the younger response waits behind the booked head: %v (%+v)", acts, p.Status)
	}
	// The pool drains to 6: the head fits (6 + 8 = 14) and goes first; c waits for a's pending 8.
	p.measured = 6
	if got := releases(p.schedule(t0.Add(32 * time.Second))); len(got) != 1 || got[0] != "a" || p.Status.HeldNow != 1 {
		t.Fatalf("the booked head goes when it fits: %v", got)
	}
}

// Insurance is refused to a sandbox holding more than MaxDump, and to one
// whose dump, expected from its memory at the learned dump rate, would take
// more than Cost of its typical call.
func TestInsuranceMemoryGuard(t *testing.T) {
	p, err := New(map[string]string{"target": "0.5", "bandwidth": "0", "park": "0", "insure": "0", "busy": "4", "minInsured": "4", "insureEvery": "1ms", "maxDump": "2G", "dumpRate": "1G", "idleQuota": "0"})
	if err != nil {
		t.Fatal(err)
	}
	a := sb("a", 8)
	v := view(t0, a)
	p.Admit(a, v)
	b := p.Boxes["a"]
	b.Peaks = []float64{8}
	b.Durations = []float64{10} // 10-s calls: a dump may take 2 s
	b.WorkingSet = 3 * gib
	if p.CheckpointBefore(request("a", "a1", t0)) || p.Status.SkippedBig != 1 {
		t.Fatalf("3 GiB > MaxDump 2 GiB: not insured (%+v)", p.Status)
	}
	b.WorkingSet = 1.5 * gib // 1.5 s at 1 GiB/s: within 2 s
	if !p.CheckpointBefore(request("a", "a2", t0.Add(time.Second))) {
		t.Fatalf("1.5 GiB dumps in 1.5 s of a 10-s call: insured (%+v)", p.Status)
	}
	p.CheckpointDone("a", "a2", 3*time.Second, nil) // the dump ran at 0.5 GiB/s
	if r := median(p.rates); r < 0.49*gib || r > 0.51*gib {
		t.Fatalf("the dump rate is learned: %.2f GiB/s", r/gib)
	}
	if p.CheckpointBefore(request("a", "a3", t0.Add(2*time.Second))) || p.Status.SkippedCost != 1 {
		t.Fatalf("at 0.5 GiB/s the dump takes 3 s > 2 s: not insured (%+v)", p.Status)
	}
}

// The aged head goes past the ceiling when the machine itself has room, so
// the big calls do not starve behind the small ones that pass them.
func TestAgedHeadEscape(t *testing.T) {
	p, _ := New(map[string]string{"target": "0.25", "bandwidth": "0", "park": "0", "slots": "0", "idleQuota": "0", "pressure": "0.85"}) // ceiling 14, machine room 47.6 of 56
	a, b, c := sb("a", 8), sb("b", 2), sb("c", 2)
	v := view(t0.Add(-20*time.Second), a, b, c)
	p.Admit(a, v)
	p.Admit(b, v)
	p.Admit(c, v)
	p.measured = 10
	p.OnModelEvent(ready("b", "b1", t0.Add(-5*time.Second), "Bash"), v) // runs: 10 + 2 <= 14
	if acts := p.OnModelEvent(ready("a", "a1", t0, "Bash"), v); p.Status.HeldNow != 1 {
		t.Fatalf("10 + 8 > 14: the head is held: %v", acts)
	}
	if got := releases(p.schedule(t0.Add(20 * time.Second))); len(got) != 0 {
		t.Fatalf("before MaxPass it waits for the ceiling: %v", got)
	}
	if got := releases(p.schedule(t0.Add(31 * time.Second))); len(got) != 1 || got[0] != "a" || p.Status.Escaped != 1 || !p.Boxes["a"].Call.Escaped {
		t.Fatalf("after MaxPass it goes past the ceiling, the machine having room (10 + 8 <= 47.6): %v (%+v)", got, p.Status)
	}
	// The escaped call runs on the machine's spare room: the pool now measures
	// 16 with a using 6, and a younger small call still fits the ceiling (16 - 6 + 2 <= 14).
	p.measured, p.Boxes["a"].Use = 16, 6
	if acts := p.OnModelEvent(ready("c", "c1", t0.Add(42*time.Second), "Bash"), v); p.Status.HeldNow != 0 || p.Boxes["c"].Call == nil {
		t.Fatalf("the escaped call is not counted against the ceiling: %v (%+v)", acts, p.Status)
	}
	p.OnModelEvent(request("c", "c1", t0.Add(50*time.Second)), v)
	// But nothing is released past the machine's own room: with the pool at 46
	// physical (a using 36 of it, so 10 counted), a 2-core call fits the ceiling
	// and still waits (46 + 2 > 47.6).
	p.measured, p.Boxes["a"].Use = 46, 36
	if acts := p.OnModelEvent(ready("c", "c2", t0.Add(55*time.Second), "Bash"), v); p.Status.HeldNow != 1 {
		t.Fatalf("the machine's room bounds every release: %v (%+v)", acts, p.Status)
	}
	p.measured, p.Boxes["a"].Use = 40, 30
	if got := releases(p.schedule(t0.Add(56 * time.Second))); len(got) != 1 || got[0] != "c" {
		t.Fatalf("and it goes when the machine has room (40 + 2 <= 47.6, 10 + 2 <= 14): %v", got)
	}
	p.OnModelEvent(request("c", "c2", t0.Add(60*time.Second)), v)
	// With the machine itself full an aged head keeps waiting.
	p.OnModelEvent(request("a", "a1", t0.Add(77*time.Second)), v)
	p.measured, p.Boxes["a"].Use = 45, 0
	if acts := p.OnModelEvent(ready("a", "a2", t0.Add(80*time.Second), "Bash"), v); p.Status.HeldNow != 1 {
		t.Fatalf("held: %v", acts)
	}
	if got := releases(p.schedule(t0.Add(120 * time.Second))); len(got) != 0 {
		t.Fatalf("45 + 8 > 47.6: no room on the machine, it waits: %v", got)
	}
}

func TestLearning(t *testing.T) {
	p, _ := New(map[string]string{"bandwidth": "0", "park": "0", "slots": "0", "idleQuota": "0"})
	a := sb("a", 8)
	v := view(t0, a)
	p.Admit(a, v)
	p.OnSample(sample(t0, 100<<30, map[string]sandbox.Stats{"a": {CPUUsageNanos: 0}}), v)
	p.OnModelEvent(ready("a", "a1", t0, "Bash"), v)
	for i := 1; i <= 40; i++ { // 5 cores sustained for 40 s
		p.OnSample(sample(t0.Add(time.Duration(i)*time.Second), 100<<30, map[string]sandbox.Stats{"a": {CPUUsageNanos: uint64(5e9 * i)}}), v)
	}
	p.OnModelEvent(request("a", "a1", t0.Add(41*time.Second)), v)
	if b := p.Boxes["a"]; len(b.Peaks) != 1 || b.Peaks[0] < 4.9 || b.Peaks[0] > 5 || len(b.Durations) != 1 {
		t.Fatalf("the call's sustained demand and duration must be learned: %+v", b)
	}
	if r := p.reserve(p.Boxes["a"]); r != 8 {
		t.Fatalf("reserve = %v, want the limit for a call sustaining half of it", r)
	}
}

// A resident idle for IdleAfter with a background load above the idle quota
// is throttled; its quota comes back, in order, before its next response.
func TestIdleThrottle(t *testing.T) {
	p, _ := New(map[string]string{"idleQuota": "0.5", "idleAfter": "30s", "park": "0", "slots": "0"})
	a := sb("a", 8)
	v := view(t0, a)
	p.Admit(a, v)
	p.Boxes["a"].Standing = 3
	if acts := p.OnSample(sample(t0.Add(10*time.Second), 100<<30, nil), v); len(acts) != 0 {
		t.Fatalf("not idle long enough: %v", acts)
	}
	acts := p.OnSample(sample(t0.Add(31*time.Second), 100<<30, nil), v)
	u, ok := acts[0].(policy.Update)
	if !ok || u.ID != "a" || u.Allocation.Cgroup.CPUQuotaUS != 50_000 || !p.Boxes["a"].Throttled {
		t.Fatalf("a must be throttled to 0.5 cores: %v", acts)
	}
	acts = p.OnModelEvent(ready("a", "a1", t0.Add(40*time.Second), "Read"), v)
	if len(acts) != 3 || acts[1].(policy.Update).Allocation.Cgroup.CPUQuotaUS != 800_000 || p.Boxes["a"].Throttled {
		t.Fatalf("the quota must come back before the response: %v", acts)
	}
}
