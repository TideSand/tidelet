package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/manager/store"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// fakeRuntime records sandboxes in memory and lets tests end them.
type fakeRuntime struct {
	mu         sync.Mutex
	boxes      map[string]*fakeBox
	failErr    error
	cgroups    map[string]runtime.Cgroup
	envs       map[string]map[string]string
	updates    map[string]runtime.Cgroup
	prefetched []string
	reclaimed  []string
	// createGate, when set, holds Create until it is closed.
	createGate chan struct{}
	// reclaimGate, when set, holds Reclaim (after recording it) until it is closed.
	reclaimGate chan struct{}
}

type fakeBox struct {
	started bool
	paused  bool
	done    bool
	code    int32
	exited  chan struct{}
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{boxes: map[string]*fakeBox{}, cgroups: map[string]runtime.Cgroup{}, envs: map[string]map[string]string{}, updates: map[string]runtime.Cgroup{}}
}

func (f *fakeRuntime) Name() string { return "fake" }
func (f *fakeRuntime) Validate(spec sandbox.Spec) error {
	if spec.Runtime == sandbox.RuntimeMicroVM {
		return errors.New("microvm not available")
	}
	return nil
}
func (f *fakeRuntime) Close() error             { return nil }
func (f *fakeRuntime) LogPath(id string) string { return "/nonexistent/" + id }

func (f *fakeRuntime) Create(_ context.Context, sb *sandbox.Sandbox, cg runtime.Cgroup) error {
	if f.createGate != nil {
		<-f.createGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failErr != nil {
		return f.failErr
	}
	f.boxes[sb.ID] = &fakeBox{exited: make(chan struct{})}
	f.cgroups[sb.ID] = cg
	f.envs[sb.ID] = sb.Spec.Env
	return nil
}

func (f *fakeRuntime) env(id string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.envs[id]
}

func (f *fakeRuntime) box(id string) (*fakeBox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.boxes[id]
	if !ok {
		return nil, runtime.ErrNotFound
	}
	return b, nil
}

func (f *fakeRuntime) Start(_ context.Context, id string) error {
	b, err := f.box(id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	b.started = true
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) exit(id string, code int32) {
	b, err := f.box(id)
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !b.done {
		b.done = true
		b.code = code
		close(b.exited)
	}
}

func (f *fakeRuntime) Wait(ctx context.Context, id string) (runtime.ExitResult, error) {
	b, err := f.box(id)
	if err != nil {
		return runtime.ExitResult{}, err
	}
	select {
	case <-ctx.Done():
		return runtime.ExitResult{}, ctx.Err()
	case <-b.exited:
		f.mu.Lock()
		defer f.mu.Unlock()
		return runtime.ExitResult{ExitCode: b.code, ExitedAt: time.Now()}, nil
	}
}

func (f *fakeRuntime) Stop(_ context.Context, id string, _ time.Duration) error {
	b, err := f.box(id)
	if err != nil {
		return nil
	}
	f.mu.Lock()
	started := b.started
	f.mu.Unlock()
	if started {
		f.exit(id, 143)
	}
	return nil
}

func (f *fakeRuntime) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.boxes, id)
	return nil
}

func (f *fakeRuntime) Update(_ context.Context, id string, cg runtime.Cgroup) error {
	if _, err := f.box(id); err != nil {
		return err
	}
	f.mu.Lock()
	f.updates[id] = cg
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Prefetch(_ context.Context, id string) (int64, error) {
	if _, err := f.box(id); err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.prefetched = append(f.prefetched, id)
	f.mu.Unlock()
	return 4096, nil
}

func (f *fakeRuntime) Reclaim(_ context.Context, id string, bytes int64) error {
	if _, err := f.box(id); err != nil {
		return err
	}
	f.mu.Lock()
	f.reclaimed = append(f.reclaimed, id)
	gate := f.reclaimGate
	f.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return nil
}

func (f *fakeRuntime) Freeze(_ context.Context, id string) error {
	b, err := f.box(id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	b.paused = true
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Thaw(_ context.Context, id string) error {
	b, err := f.box(id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	b.paused = false
	f.mu.Unlock()
	return nil
}

func (f *fakeRuntime) Status(_ context.Context, id string) (runtime.Status, error) {
	b, err := f.box(id)
	if err != nil {
		return runtime.Status{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case b.done:
		return runtime.Status{State: runtime.StateStopped, ExitCode: b.code, ExitedAt: time.Now()}, nil
	case b.paused:
		return runtime.Status{State: runtime.StatePaused, Pid: 1}, nil
	case b.started:
		return runtime.Status{State: runtime.StateRunning, Pid: 1}, nil
	default:
		return runtime.Status{State: runtime.StateCreated}, nil
	}
}

func (f *fakeRuntime) Stats(_ context.Context, id string) (sandbox.Stats, error) {
	if _, err := f.box(id); err != nil {
		return sandbox.Stats{}, err
	}
	return sandbox.Stats{Timestamp: time.Now(), MemoryUsageBytes: 42}, nil
}

func (f *fakeRuntime) Exec(context.Context, string, runtime.ExecOptions) (runtime.ExecResult, error) {
	return runtime.ExecResult{}, nil
}

func (f *fakeRuntime) Attach(context.Context, string, runtime.AttachOptions) error { return nil }

func (f *fakeRuntime) Checkpoint(context.Context, string) (runtime.CheckpointInfo, error) {
	return runtime.CheckpointInfo{}, nil
}

func (f *fakeRuntime) Rollback(context.Context, string) (runtime.RollbackInfo, error) {
	return runtime.RollbackInfo{Killed: 1}, nil
}

func (f *fakeRuntime) List(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.boxes))
	for id := range f.boxes {
		ids = append(ids, id)
	}
	return ids, nil
}

// testPolicy is a scriptable policy exercising the full interface.
type testPolicy struct {
	mu      sync.Mutex
	admit   func(sb sandbox.Sandbox, v policy.View) policy.Decision
	onEvent func(ev sandbox.Event, v policy.View) []policy.Action
	samples []policy.Sample
	ticks   int
}

func (p *testPolicy) Name() string                           { return "test" }
func (p *testPolicy) Allocatable(v policy.View) resource.Set { return v.Capacity.Sub(v.Reserved) }
func (p *testPolicy) Admit(sb sandbox.Sandbox, v policy.View) policy.Decision {
	if p.admit != nil {
		return p.admit(sb, v)
	}
	return strictAdmit(sb, v)
}
func (p *testPolicy) OnEvent(ev sandbox.Event, v policy.View) []policy.Action {
	if p.onEvent != nil {
		return p.onEvent(ev, v)
	}
	return nil
}
func (p *testPolicy) OnSample(s policy.Sample, _ policy.View) []policy.Action {
	p.mu.Lock()
	p.samples = append(p.samples, s)
	p.mu.Unlock()
	return nil
}
func (p *testPolicy) OnTick(time.Time, policy.View) []policy.Action {
	p.mu.Lock()
	p.ticks++
	p.mu.Unlock()
	return nil
}

// strictAdmit mirrors the strict policy: requests==limits, reject if full.
func strictAdmit(sb sandbox.Sandbox, v policy.View) policy.Decision {
	eff := sb.Spec.Resources.Limits
	if eff.IsZero() {
		eff = sb.Spec.Resources.Requests
	}
	if !v.Allocated.Add(eff).Fits(v.Capacity.Sub(v.Reserved)) {
		return policy.Decision{Kind: policy.Reject, Reason: "full"}
	}
	return policy.Decision{Kind: policy.Run, Allocation: policy.Allocation{Requests: eff, Limits: eff, Cgroup: policy.ShapeGuaranteed(eff, eff)}}
}

func newTestManager(t *testing.T, rt runtime.Runtime, st store.Store, pol policy.Policy) *Manager {
	t.Helper()
	if pol == nil {
		pol = &testPolicy{}
	}
	m, err := New(Config{
		Policy:   pol,
		Runtime:  rt,
		Store:    st,
		Capacity: resource.Set{CPU: 4000, Memory: 4 << 30},
		Reserved: resource.Set{CPU: 1000, Memory: 1 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func spec(cpu resource.Millicores, mem resource.Bytes) sandbox.Spec {
	return sandbox.Spec{
		Image:     "docker.io/library/alpine:3.20",
		Resources: sandbox.Resources{Limits: resource.Set{CPU: cpu, Memory: mem}},
	}
}

func create(t *testing.T, m *Manager, name string, cpu resource.Millicores, mem resource.Bytes) *sandbox.Sandbox {
	t.Helper()
	sb, err := m.Create(context.Background(), name, spec(cpu, mem))
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return sb
}

func run(t *testing.T, m *Manager, name string, cpu resource.Millicores, mem resource.Bytes) *sandbox.Sandbox {
	t.Helper()
	sb := create(t, m, name, cpu, mem)
	if _, err := m.StartSandbox(context.Background(), sb.ID); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	return sb
}

func waitPhase(t *testing.T, m *Manager, ref string, want sandbox.Phase) sandbox.Sandbox {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sb, err := m.Get(ref)
		if err != nil {
			t.Fatal(err)
		}
		if sb.Status.Phase == want {
			return *sb
		}
		time.Sleep(5 * time.Millisecond)
	}
	sb, _ := m.Get(ref)
	t.Fatalf("sandbox %s phase = %s, want %s", ref, sb.Status.Phase, want)
	return sandbox.Sandbox{}
}

const gib = resource.Bytes(1 << 30)

func TestCreateStartAccountsAndShapes(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)

	sb := create(t, m, "a", 1000, gib)
	if sb.Status.Phase != sandbox.PhaseCreated {
		t.Fatalf("phase after create = %s", sb.Status.Phase)
	}
	if sb.Status.Allocated.Requests.CPU != 1000 || sb.Status.Allocated.Limits.Memory != gib {
		t.Fatalf("allocated = %+v", sb.Status.Allocated)
	}
	cg := rt.cgroups[sb.ID]
	if cg.CPUQuotaUS != 100_000 || cg.MemoryLimitBytes != int64(gib) || cg.MemorySwapLimitBytes != -1 {
		t.Fatalf("cgroup = %+v", cg)
	}
	// Created sandboxes already hold their allocation.
	if n := m.Node(); n.Allocated.CPU != 1000 || n.Allocatable.CPU != 3000 {
		t.Fatalf("node = %+v", n)
	}
	started, err := m.StartSandbox(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get(started.ID); got.Status.Phase != sandbox.PhaseRunning || got.Status.StartedAt == nil {
		t.Fatalf("after start = %+v", got.Status)
	}
	if _, err := m.StartSandbox(context.Background(), "a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("double start err = %v", err)
	}
}

func TestRejectWhenFull(t *testing.T) {
	m := newTestManager(t, newFakeRuntime(), store.NewMemory(), nil)
	run(t, m, "a", 2000, 2*gib)
	run(t, m, "b", 1000, gib) // exactly fills allocatable

	_, err := m.Create(context.Background(), "third", spec(1, 0))
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("expected rejection, got %v", err)
	}
	if _, err := m.Get("third"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected sandbox should not exist: %v", err)
	}
	_, err = m.Create(context.Background(), "", sandbox.Spec{Image: "x", Resources: sandbox.Resources{
		Requests: resource.Set{CPU: 1}, Limits: resource.Set{CPU: 2},
	}})
	_ = err // the test policy does not validate; strict does, see its own tests
}

func TestExitReleasesAllocation(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	events, cancel := m.Subscribe()
	defer cancel()
	sb := run(t, m, "a", 3000, 3*gib)

	rt.exit(sb.ID, 7)
	got := waitPhase(t, m, "a", sandbox.PhaseExited)
	if got.Status.ExitCode == nil || *got.Status.ExitCode != 7 {
		t.Fatalf("exit code = %v", got.Status.ExitCode)
	}
	if n := m.Node(); !n.Allocated.IsZero() {
		t.Fatalf("allocation not released: %+v", n.Allocated)
	}
	run(t, m, "b", 3000, 3*gib) // room again

	var types []sandbox.EventType
	for len(types) < 5 {
		select {
		case ev := <-events:
			types = append(types, ev.Type)
		case <-time.After(time.Second):
			t.Fatalf("events so far: %v", types)
		}
	}
	want := []sandbox.EventType{sandbox.EventCreated, sandbox.EventStarted, sandbox.EventExited, sandbox.EventCreated, sandbox.EventStarted}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
}

func TestStopAndDelete(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	sb := run(t, m, "a", 1000, 0)
	ctx := context.Background()

	if err := m.Delete(ctx, "a", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete running without force: %v", err)
	}
	if _, err := m.Stop(ctx, sb.ID[:8], 0); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "a", sandbox.PhaseExited)
	if err := m.Delete(ctx, "a", false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	// Name is free again; a Created (never started) sandbox can be deleted
	// without force since it is not running.
	create(t, m, "a", 1000, 0)
	if err := m.Delete(ctx, "a", false); err != nil {
		t.Fatal(err)
	}
	run(t, m, "a", 1000, 0)
	if err := m.Delete(ctx, "a", true); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidSpec(t *testing.T) {
	m := newTestManager(t, newFakeRuntime(), store.NewMemory(), nil)
	ctx := context.Background()
	create(t, m, "a", 0, 0)
	if _, err := m.Create(ctx, "a", sandbox.Spec{Image: "x"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("dup name: %v", err)
	}
	if _, err := m.Create(ctx, "", sandbox.Spec{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing image: %v", err)
	}
	if _, err := m.Create(ctx, "", sandbox.Spec{Image: "x", Runtime: sandbox.RuntimeMicroVM}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("microvm: %v", err)
	}
	if _, err := m.Create(ctx, "bad name!", sandbox.Spec{Image: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad name: %v", err)
	}
	pol := &testPolicy{admit: func(sandbox.Sandbox, policy.View) policy.Decision {
		return policy.Decision{Kind: policy.Invalid, Reason: "nope"}
	}}
	m2 := newTestManager(t, newFakeRuntime(), store.NewMemory(), pol)
	if _, err := m2.Create(ctx, "", sandbox.Spec{Image: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("policy invalid: %v", err)
	}
}

func TestRuntimeFailureReleasesAllocation(t *testing.T) {
	rt := newFakeRuntime()
	rt.failErr = fmt.Errorf("boom")
	m := newTestManager(t, rt, store.NewMemory(), nil)
	_, err := m.Create(context.Background(), "a", spec(3000, 0))
	if err == nil {
		t.Fatal("expected error")
	}
	sb, err := m.Get("a")
	if err != nil || sb.Status.Phase != sandbox.PhaseFailed {
		t.Fatalf("failed sandbox should be kept for inspection: %v %+v", err, sb)
	}
	if n := m.Node(); !n.Allocated.IsZero() {
		t.Fatalf("allocation leaked: %+v", n.Allocated)
	}
}

func TestQueueDispatchAndStartRequested(t *testing.T) {
	rt := newFakeRuntime()
	// Queue everything on admit; dispatch the oldest queued sandbox
	// whenever something exits.
	pol := &testPolicy{
		admit: func(sandbox.Sandbox, policy.View) policy.Decision {
			return policy.Decision{Kind: policy.Queue, Reason: "waiting"}
		},
		onEvent: func(ev sandbox.Event, v policy.View) []policy.Action {
			if ev.Type != sandbox.EventExited {
				return nil
			}
			q := v.Queued()
			if len(q) == 0 {
				return nil
			}
			eff := q[0].Sandbox.Spec.Resources.Limits
			return []policy.Action{policy.Dispatch{ID: q[0].Sandbox.ID, Allocation: policy.Allocation{Requests: eff, Limits: eff}}}
		},
	}
	m := newTestManager(t, rt, store.NewMemory(), pol)
	ctx := context.Background()

	q := create(t, m, "q", 1000, gib)
	if q.Status.Phase != sandbox.PhaseQueued {
		t.Fatalf("phase = %s", q.Status.Phase)
	}
	if n := m.Node(); !n.Allocated.IsZero() || n.Queued != 1 {
		t.Fatalf("queued sandbox must not hold allocation: %+v", n)
	}
	if _, err := m.Stats(ctx, "q"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stats on queued: %v", err)
	}
	// Asking to start a queued sandbox records the wish.
	if got, err := m.StartSandbox(ctx, "q"); err != nil || got.Status.Phase != sandbox.PhaseQueued {
		t.Fatalf("start queued = %+v, %v", got, err)
	}

	// Simulate something exiting: inject a running sandbox directly via a
	// Run decision, then exit it.
	pol.admit = strictAdmit
	r := run(t, m, "r", 1000, 0)
	rt.exit(r.ID, 0)
	waitPhase(t, m, "r", sandbox.PhaseExited)

	got := waitPhase(t, m, "q", sandbox.PhaseRunning)
	if got.Status.Allocated.Requests.CPU != 1000 {
		t.Fatalf("dispatched allocation = %+v", got.Status.Allocated)
	}
	if n := m.Node(); n.Allocated.CPU != 1000 || n.Queued != 0 {
		t.Fatalf("node after dispatch = %+v", n)
	}
}

// `tidelet run` creates and then starts; a queueing policy may dispatch the
// sandbox in between, so the start can arrive while it is being created.
func TestStartWhileDispatchCreates(t *testing.T) {
	rt := newFakeRuntime()
	rt.createGate = make(chan struct{})
	pol := &testPolicy{admit: func(sandbox.Sandbox, policy.View) policy.Decision {
		return policy.Decision{Kind: policy.Queue, Reason: "waiting"}
	}}
	m := newTestManager(t, rt, store.NewMemory(), pol)
	ctx := context.Background()

	q := create(t, m, "q", 1000, gib)
	eff := q.Spec.Resources.Limits
	done := make(chan error, 1)
	go func() {
		done <- m.dispatch(ctx, policy.Dispatch{ID: q.ID, Allocation: policy.Allocation{Requests: eff, Limits: eff}})
	}()
	waitPhase(t, m, "q", sandbox.PhasePending)
	if got, err := m.StartSandbox(ctx, "q"); err != nil || got.Status.Phase != sandbox.PhasePending {
		t.Fatalf("start while pending = %+v, %v", got, err)
	}
	close(rt.createGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "q", sandbox.PhaseRunning)
}

// A queued sandbox may be deleted while its dispatch is creating it (the
// replay harness's cutoff deletes the queue as the emptied pool dispatches
// it): what the runtime created is removed again, not left running untracked.
func TestDeleteWhileDispatchCreates(t *testing.T) {
	rt := newFakeRuntime()
	rt.createGate = make(chan struct{})
	pol := &testPolicy{admit: func(sandbox.Sandbox, policy.View) policy.Decision {
		return policy.Decision{Kind: policy.Queue, Reason: "waiting"}
	}}
	m := newTestManager(t, rt, store.NewMemory(), pol)
	ctx := context.Background()

	q := create(t, m, "q", 1000, gib)
	eff := q.Spec.Resources.Limits
	done := make(chan error, 1)
	go func() {
		done <- m.dispatch(ctx, policy.Dispatch{ID: q.ID, Allocation: policy.Allocation{Requests: eff, Limits: eff}})
	}()
	waitPhase(t, m, "q", sandbox.PhasePending)
	if err := m.Delete(ctx, "q", true); err != nil {
		t.Fatal(err)
	}
	close(rt.createGate)
	if err := <-done; !errors.Is(err, ErrConflict) {
		t.Fatalf("dispatch of a deleted sandbox = %v, want a conflict", err)
	}
	if _, err := rt.box(q.ID); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatal("the sandbox created after its delete must be removed from the runtime")
	}
	if _, err := m.Get("q"); err == nil {
		t.Fatal("the deleted sandbox must be gone")
	}
	// A dispatch that starts after the delete began is refused outright.
	r := create(t, m, "r", 1000, gib)
	m.mu.Lock()
	m.entries[r.ID].deleting = true
	m.mu.Unlock()
	if err := m.dispatch(ctx, policy.Dispatch{ID: r.ID, Allocation: policy.Allocation{Requests: eff, Limits: eff}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("dispatch during delete = %v, want a conflict", err)
	}
}

func TestFreezeThawUpdateActions(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	ctx := context.Background()
	sb := run(t, m, "a", 1000, gib)

	if err := m.apply(ctx, policy.Freeze{ID: sb.ID}); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get("a"); got.Status.Phase != sandbox.PhaseFrozen {
		t.Fatalf("phase = %s", got.Status.Phase)
	}
	if _, err := m.Exec(ctx, "a", runtime.ExecOptions{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("exec on frozen: %v", err)
	}
	if n := m.Node(); n.Allocated.CPU != 1000 {
		t.Fatalf("frozen must keep allocation: %+v", n.Allocated)
	}
	if err := m.apply(ctx, policy.Freeze{ID: sb.ID}); !errors.Is(err, ErrConflict) {
		t.Fatalf("double freeze: %v", err)
	}
	if err := m.apply(ctx, policy.Thaw{ID: sb.ID}); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get("a"); got.Status.Phase != sandbox.PhaseRunning {
		t.Fatalf("phase = %s", got.Status.Phase)
	}

	newAlloc := policy.Allocation{Requests: resource.Set{CPU: 500, Memory: gib}, Limits: resource.Set{CPU: 2000, Memory: gib},
		Cgroup: runtime.Cgroup{CPUQuotaUS: 200_000, CPUPeriodUS: 100_000, MemoryHighBytes: 1 << 29}}
	if err := m.apply(ctx, policy.Update{ID: sb.ID, Allocation: newAlloc}); err != nil {
		t.Fatal(err)
	}
	if rt.updates[sb.ID] != newAlloc.Cgroup {
		t.Fatalf("runtime update = %+v", rt.updates[sb.ID])
	}
	if n := m.Node(); n.Allocated.CPU != 500 {
		t.Fatalf("accounting after update = %+v", n.Allocated)
	}

	// Stopping a frozen sandbox works and releases it.
	if err := m.apply(ctx, policy.Freeze{ID: sb.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Stop(ctx, "a", 0); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m, "a", sandbox.PhaseExited)
	if n := m.Node(); !n.Allocated.IsZero() {
		t.Fatalf("allocation after stop = %+v", n.Allocated)
	}
}

func TestSamplerFeedsPolicy(t *testing.T) {
	rt := newFakeRuntime()
	pol := &testPolicy{}
	m, err := New(Config{
		Policy: pol, Runtime: rt, Store: store.NewMemory(),
		Capacity:       resource.Set{CPU: 4000, Memory: 4 << 30},
		SampleInterval: 10 * time.Millisecond, TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	sb := run(t, m, "a", 1000, 0)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pol.mu.Lock()
		n, ticks := len(pol.samples), pol.ticks
		var last policy.Sample
		if n > 0 {
			last = pol.samples[n-1]
		}
		pol.mu.Unlock()
		if n > 0 && ticks > 0 && last.Sandboxes[sb.ID].MemoryUsageBytes == 42 && last.Node.MemTotal > 0 {
			m.mu.Lock()
			v := m.viewLocked()
			m.mu.Unlock()
			if v.Sandboxes[0].Stats == nil || v.Node.MemTotal == 0 {
				t.Fatalf("view missing stats: %+v", v.Sandboxes[0])
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("policy never received a usable sample and tick")
}

func TestRestartReconciles(t *testing.T) {
	rt := newFakeRuntime()
	st := store.NewMemory()
	m := newTestManager(t, rt, st, nil)
	ctx := context.Background()
	a := run(t, m, "a", 1000, gib)
	b := run(t, m, "b", 1000, gib)
	c := create(t, m, "c", 500, 0) // created, never started
	rt.exit(b.ID, 0)
	waitPhase(t, m, "b", sandbox.PhaseExited)
	if err := m.apply(ctx, policy.Freeze{ID: a.ID}); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()

	// A sandbox the runtime lost, plus an orphan the store never saw.
	lost := create(t, m, "lost", 500, 0)
	rt.mu.Lock()
	delete(rt.boxes, lost.ID)
	rt.boxes["zzz"] = &fakeBox{exited: make(chan struct{})}
	rt.mu.Unlock()

	m2 := newTestManager(t, rt, st, nil)
	phases := map[string]sandbox.Phase{}
	for _, name := range []string{"a", "b", "c", "lost"} {
		got, err := m2.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		phases[name] = got.Status.Phase
	}
	want := map[string]sandbox.Phase{"a": sandbox.PhaseFrozen, "b": sandbox.PhaseExited, "c": sandbox.PhaseCreated, "lost": sandbox.PhaseFailed}
	if fmt.Sprint(phases) != fmt.Sprint(want) {
		t.Fatalf("phases after restart = %v, want %v", phases, want)
	}
	if n := m2.Node(); n.Allocated.CPU != 1500 || n.Sandboxes != 4 {
		t.Fatalf("node after restart = %+v", n)
	}
	ids, _ := rt.List(ctx)
	sort.Strings(ids)
	wantIDs := []string{a.ID, b.ID, c.ID}
	sort.Strings(wantIDs)
	if fmt.Sprint(ids) != fmt.Sprint(wantIDs) {
		t.Fatalf("runtime sandboxes after restart = %v, want %v", ids, wantIDs)
	}
	// Exit watcher re-attached to the frozen sandbox.
	rt.exit(a.ID, 0)
	waitPhase(t, m2, "a", sandbox.PhaseExited)
	// Created sandbox can still be started after restart.
	if _, err := m2.StartSandbox(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, m2, "c", sandbox.PhaseRunning)
}

// modelPolicy is a test policy that schedules on model events: it holds
// every tool-calling response, asks for a prefetch and a reclaim, and
// releases on the prefetched event.
type modelPolicy struct {
	testPolicy
	events []policy.ModelEvent
}

func (p *modelPolicy) OnModelEvent(ev policy.ModelEvent, v policy.View) []policy.Action {
	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
	if ev.Kind == policy.ModelResponseReady && ev.ToolCalls > 0 {
		return []policy.Action{policy.Hold{ID: ev.Sandbox, Call: ev.Call}, policy.Prefetch{ID: ev.Sandbox}, policy.Reclaim{ID: ev.Sandbox, Bytes: 1 << 20}}
	}
	return nil
}

func (p *modelPolicy) OnEvent(ev sandbox.Event, v policy.View) []policy.Action {
	if ev.Type == sandbox.EventPrefetched {
		return []policy.Action{policy.Release{ID: ev.Sandbox.ID, Call: ev.Sandbox.ID + "-1"}}
	}
	return nil
}

type fakeGate struct {
	mu       sync.Mutex
	released []string
}

func (g *fakeGate) Release(call string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.released = append(g.released, call)
	return true
}

func TestModelEventsHoldPrefetchRelease(t *testing.T) {
	rt := newFakeRuntime()
	pol := &modelPolicy{}
	m := newTestManager(t, rt, store.NewMemory(), pol)
	gate := &fakeGate{}
	m.SetModelGate(gate)
	sb := run(t, m, "a", 1000, gib)

	// The sandbox is found by its bridge address, with the label's upstream.
	m.mu.Lock()
	e := m.entries[sb.ID]
	e.sb.Status.Network = &sandbox.NetworkStatus{IPs: []string{"10.88.0.9"}}
	e.sb.Spec.Labels = map[string]string{UpstreamLabel: "http://10.88.0.1:18201"}
	m.mu.Unlock()
	id, up, ok := m.LookupSandbox("10.88.0.9")
	if !ok || id != sb.ID || up == nil || up.Host != "10.88.0.1:18201" {
		t.Fatalf("LookupSandbox = %q, %v, %v", id, up, ok)
	}
	if _, _, ok := m.LookupSandbox("10.88.0.10"); ok {
		t.Fatal("unknown address resolved")
	}

	// A text response: not held. A tool-calling one: held, prefetched, reclaimed.
	if m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRequestStarted, Sandbox: sb.ID, Call: sb.ID + "-0"}) {
		t.Fatal("request-started held")
	}
	if m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseReady, Sandbox: sb.ID, Call: sb.ID + "-0"}) {
		t.Fatal("text response held")
	}
	if !m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseReady, Sandbox: sb.ID, Call: sb.ID + "-1", ToolCalls: 1}) {
		t.Fatal("tool-calling response not held")
	}
	if st := m.Model(); st.Calls != 1 || st.Held != 1 || st.Open != 1 {
		t.Fatalf("stats = %+v", st)
	}
	// The Prefetch and Reclaim actions run asynchronously, and the Release
	// follows the prefetched event; the sampling loops hold the wait group,
	// so poll for the outcome instead of waiting on it.
	var prefetched, reclaimed, released []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		prefetched, reclaimed = append([]string(nil), rt.prefetched...), append([]string(nil), rt.reclaimed...)
		rt.mu.Unlock()
		gate.mu.Lock()
		released = append([]string(nil), gate.released...)
		gate.mu.Unlock()
		if len(prefetched) > 0 && len(reclaimed) > 0 && len(released) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(prefetched) != 1 || prefetched[0] != sb.ID || len(reclaimed) != 1 {
		t.Fatalf("runtime saw prefetch %v, reclaim %v", prefetched, reclaimed)
	}
	if len(released) != 1 || released[0] != sb.ID+"-1" {
		t.Fatalf("gate released %v", released)
	}
	// Delivery closes the hold and the counters.
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Sandbox: sb.ID, Call: sb.ID + "-1", Held: 300 * time.Millisecond})
	if st := m.Model(); st.Open != 0 || st.HeldTotal != 300*time.Millisecond || st.Forced != 0 {
		t.Fatalf("stats after delivery = %+v", st)
	}
	// A forced delivery is counted as such.
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseReady, Sandbox: sb.ID, Call: sb.ID + "-2", ToolCalls: 1})
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseDelivered, Sandbox: sb.ID, Call: sb.ID + "-2", Held: time.Minute, Forced: true})
	if st := m.Model(); st.Forced != 1 || st.Held != 2 {
		t.Fatalf("stats after forced = %+v", st)
	}
	// Without a model-aware policy nothing is held.
	plain := newTestManager(t, newFakeRuntime(), store.NewMemory(), nil)
	if plain.OnModelEvent(policy.ModelEvent{Kind: policy.ModelResponseReady, Sandbox: "x", Call: "x-1", ToolCalls: 3}) {
		t.Fatal("plain policy held")
	}
}

func TestThreadCountFollowsTheCPULimit(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	ctx := context.Background()

	a := create(t, m, "a", 1500, gib) // rounds up to two threads
	for _, k := range ThreadEnv {
		if got := rt.env(a.ID)[k]; got != "2" {
			t.Errorf("%s = %q, want 2", k, got)
		}
	}
	// The stored spec is the one submitted.
	if got, _ := m.Get("a"); len(got.Spec.Env) != 0 {
		t.Fatalf("stored env = %v, want none", got.Spec.Env)
	}

	// A value the spec sets wins; the rest still follow the limit.
	s := spec(1000, gib)
	s.Env = map[string]string{"OMP_NUM_THREADS": "4", "OTHER": "x"}
	b, err := m.Create(ctx, "b", s)
	if err != nil {
		t.Fatal(err)
	}
	if env := rt.env(b.ID); env["OMP_NUM_THREADS"] != "4" || env["OPENBLAS_NUM_THREADS"] != "1" || env["OTHER"] != "x" {
		t.Fatalf("env = %v", env)
	}

	// Without a CPU limit nothing is set.
	c := create(t, m, "c", 0, gib)
	for _, k := range ThreadEnv {
		if v, ok := rt.env(c.ID)[k]; ok {
			t.Errorf("%s = %q without a CPU limit", k, v)
		}
	}
}

func TestReclaimRunsOneAtATimePerSandbox(t *testing.T) {
	rt := newFakeRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	ctx := context.Background()
	sb := run(t, m, "a", 1000, gib)
	rt.reclaimGate = make(chan struct{})
	reclaims := func() int {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return len(rt.reclaimed)
	}
	first := make(chan error, 1)
	go func() { first <- m.apply(ctx, policy.Reclaim{ID: sb.ID, Bytes: 1 << 20}) }()
	for deadline := time.Now().Add(5 * time.Second); reclaims() == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the first reclaim never reached the runtime")
		}
	}
	// The kernel is still at work on the first: a second ask is dropped, not queued behind it.
	if err := m.apply(ctx, policy.Reclaim{ID: sb.ID, Bytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if n := reclaims(); n != 1 {
		t.Fatalf("a reclaim while one is in flight must be dropped: %d reached the runtime", n)
	}
	close(rt.reclaimGate)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := m.apply(ctx, policy.Reclaim{ID: sb.ID, Bytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if n := reclaims(); n != 2 {
		t.Fatalf("after the first returned, the next must run: %d reached the runtime", n)
	}
}

// TestSwapToLimit: with SwapToLimit, a sandbox whose policy leaves its swap
// unlimited gets memory.swap.max = its memory limit, at admission and on an
// update; a policy's own setting (none, or a limit) is kept.
func TestSwapToLimit(t *testing.T) {
	rt := newFakeRuntime()
	swapless := func(sb sandbox.Sandbox, v policy.View) policy.Decision {
		dec := strictAdmit(sb, v)
		if dec.Kind == policy.Run {
			dec.Allocation.Cgroup.MemorySwapLimitBytes = 0 // the kernel's default: unlimited
		}
		return dec
	}
	m, err := New(Config{
		Policy:      &testPolicy{admit: swapless},
		Runtime:     rt,
		Store:       store.NewMemory(),
		Capacity:    resource.Set{CPU: 4000, Memory: 4 << 30},
		Reserved:    resource.Set{CPU: 1000, Memory: 1 << 30},
		SwapToLimit: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })

	sb := run(t, m, "a", 1000, gib)
	if cg := rt.cgroups[sb.ID]; cg.MemoryLimitBytes != int64(gib) || cg.MemorySwapLimitBytes != int64(gib) {
		t.Fatalf("cgroup at admission = %+v", cg)
	}
	// An update that leaves the swap unlimited is capped the same way.
	upd := policy.Allocation{Requests: resource.Set{CPU: 500, Memory: gib}, Limits: resource.Set{CPU: 2000, Memory: gib},
		Cgroup: runtime.Cgroup{CPUQuotaUS: 200_000, CPUPeriodUS: 100_000, MemoryLimitBytes: int64(gib)}}
	if err := m.apply(context.Background(), policy.Update{ID: sb.ID, Allocation: upd}); err != nil {
		t.Fatal(err)
	}
	if cg := rt.updates[sb.ID]; cg.MemorySwapLimitBytes != int64(gib) || cg.CPUQuotaUS != 200_000 {
		t.Fatalf("runtime update = %+v", cg)
	}
	// A policy that forbids swap keeps it forbidden.
	upd.Cgroup.MemorySwapLimitBytes = -1
	if err := m.apply(context.Background(), policy.Update{ID: sb.ID, Allocation: upd}); err != nil {
		t.Fatal(err)
	}
	if cg := rt.updates[sb.ID]; cg.MemorySwapLimitBytes != -1 {
		t.Fatalf("runtime update = %+v", cg)
	}
	// Without a memory limit there is nothing to cap at.
	m2 := newTestManager(t, newFakeRuntime(), store.NewMemory(), &testPolicy{admit: swapless})
	m2.cfg.SwapToLimit = true
	if cg := m2.shape(runtime.Cgroup{CPUQuotaUS: 100_000}); cg.MemorySwapLimitBytes != 0 {
		t.Fatalf("shape without a limit = %+v", cg)
	}
}
