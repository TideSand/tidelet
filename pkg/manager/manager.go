// Package manager owns the sandbox table. It runs admission through the
// policy, executes the policy's actions against the runtime, persists
// state, samples usage, and tracks sandbox exits so that allocations are
// released promptly. It is the only component that touches the runtime.
package manager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/log/crilog"
	"github.com/TideSand/tidelet/pkg/manager/store"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Sentinel errors. The API server maps them to status codes.
var (
	ErrNotFound  = errors.New("sandbox not found")
	ErrConflict  = errors.New("conflict")
	ErrInvalid   = errors.New("invalid request")
	ErrAmbiguous = errors.New("ambiguous sandbox reference")
	// ErrRejected wraps a policy's refusal to take a sandbox now.
	ErrRejected = errors.New("rejected by policy")
)

// Config configures a Manager.
type Config struct {
	Policy   policy.Policy
	Runtime  runtime.Runtime
	Store    store.Store
	Capacity resource.Set
	Reserved resource.Set
	// StopTimeout is the default grace period between SIGTERM and SIGKILL.
	StopTimeout time.Duration
	// SampleInterval is how often usage is collected and handed to the
	// policy. Zero disables sampling.
	SampleInterval time.Duration
	// TickInterval is how often the policy's OnTick runs. Zero disables it.
	TickInterval time.Duration
	// Collect reads the node sample handed to the policy. Nil means the
	// whole host (host.Collect); a host.Scope's Collect confines it to the
	// pool's cgroup.
	Collect func() (host.Sample, error)
	// PoolCgroup is the absolute path of the pool's cgroup (the
	// host.Scope's). A policy.Signaled policy gets the pool's pressure
	// triggers installed there.
	PoolCgroup string
	// Pressure are the triggers installed for a Signaled policy; nil means
	// 100 ms of stall within 1 s, for both CPU and memory.
	Pressure []host.PressureTrigger
	// SwapToLimit caps the swap of a sandbox whose policy leaves it
	// unlimited at the sandbox's memory limit, as `docker run --memory`
	// does without --memory-swap: the sandbox is OOM-killed where a
	// container run that way would have been, whatever the policy. A
	// policy's own swap setting (none, or a limit) is kept.
	SwapToLimit bool
	// SnapshotDir is where snapshot records are kept. Empty keeps them in memory only.
	SnapshotDir string
	Logger      *slog.Logger
}

// ModelGate is the LLM proxy as the manager drives it: it delivers a held
// model response. Set with SetModelGate once the proxy exists.
type ModelGate interface {
	Release(call string) bool
}

// UpstreamLabel is the sandbox label naming the model API base the LLM
// proxy forwards that sandbox's calls to (default: the proxy's upstream).
const UpstreamLabel = "tidelet.io/llm-upstream"

// ModelStats counts what the LLM proxy reported.
type ModelStats struct {
	Calls     uint64        // model calls seen
	Held      uint64        // responses the policy held
	Forced    uint64        // held responses delivered by the deadline, not a Release
	Open      int           // responses held right now
	HeldTotal time.Duration // time responses spent held, summed
	// Checkpoints, Rollbacks (those that found a call to undo), Redos: the
	// undo machinery's counters, and the time checkpoints took, summed.
	Checkpoints     uint64
	CheckpointTotal time.Duration
	Rollbacks       uint64
	Redos           uint64
	Restores        uint64
}

type entry struct {
	sb             sandbox.Sandbox
	cgroup         runtime.Cgroup
	accounted      bool
	startRequested bool
	stats          *sandbox.Stats
	cancel         context.CancelFunc
	// suspended: a whole-sandbox rollback stopped it; its task's exit is not the
	// sandbox's, and a Restore brings it back.
	suspended bool
	// deleting: a Delete has begun; a dispatch must not create it, and one
	// that is creating it already removes what it created.
	deleting bool
	// lastCall is the sandbox's latest model call: a rollback issued for an earlier one
	// comes too late, the call is over.
	lastCall string
	// updating: a policy Update of this sandbox is being applied; updPending is the one
	// that arrived meanwhile and replaces any older pending one. Updates of one sandbox
	// run one at a time, newest settings last: two runc updates writing the same cgroup
	// at once interleave per file, so the kernel could end with one update's memory.high
	// and the other's weight while the record said otherwise, and nothing repaired it.
	updating   bool
	updPending *policy.Update
	// updSeq: the batch sequence of the newest Update seen for this sandbox; an older one
	// arriving later (a slow batch) is stale and dropped.
	updSeq uint64
}

// Manager is the sandbox table and lifecycle driver.
type Manager struct {
	cfg Config
	log *slog.Logger

	mu       sync.Mutex
	tmu      sync.Mutex
	timers   map[string]*time.Timer
	entries  map[string]*entry
	byName   map[string]string
	snaps    map[string]*sandbox.Snapshot
	lastNode host.Sample
	subs     map[chan sandbox.Event]struct{}
	gate     ModelGate
	model    ModelStats
	heldOpen map[string]string // held call -> sandbox
	// reclaiming holds the sandboxes with a Reclaim in flight: memory.reclaim is a
	// synchronous write that can take the kernel a while, and a policy that asks
	// again before it returns would pile up threads doing the same work.
	reclaiming map[string]bool
	// traffic is the memory traffic meter, opened for a BandwidthTriggered policy.
	traffic  *host.Traffic
	bwPolicy policy.BandwidthTriggered

	wg  sync.WaitGroup
	ctx context.Context
	// actSeq numbers the policy's action batches (atomic: runActions runs with or without
	// m.mu held); updSem bounds the cgroup updates in flight.
	actSeq  atomic.Uint64
	updSem  chan struct{}
	updOnce sync.Once
	cancel  context.CancelFunc
}

// New creates a manager. Call Start to load persisted state and reconcile
// with the runtime.
func New(cfg Config) (*Manager, error) {
	if cfg.Policy == nil || cfg.Runtime == nil || cfg.Store == nil {
		return nil, errors.New("manager: Policy, Runtime and Store are required")
	}
	if cfg.StopTimeout == 0 {
		cfg.StopTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:        cfg,
		log:        cfg.Logger.With("component", "manager"),
		entries:    map[string]*entry{},
		timers:     map[string]*time.Timer{},
		byName:     map[string]string{},
		snaps:      map[string]*sandbox.Snapshot{},
		subs:       map[chan sandbox.Event]struct{}{},
		heldOpen:   map[string]string{},
		reclaiming: map[string]bool{},
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// SetModelGate connects the LLM proxy, so Release actions reach it.
func (m *Manager) SetModelGate(g ModelGate) {
	m.mu.Lock()
	m.gate = g
	m.mu.Unlock()
}

// LookupSandbox implements the proxy's resolver: the sandbox whose bridge
// address is ip, and the upstream its label names, if any.
func (m *Manager) LookupSandbox(ip string) (string, *url.URL, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.entries {
		if e.sb.Status.Network == nil || !e.sb.Status.Phase.HasRuntime() {
			continue
		}
		for _, have := range e.sb.Status.Network.IPs {
			if have == ip {
				var upstream *url.URL
				if raw := e.sb.Spec.Labels[UpstreamLabel]; raw != "" {
					if u, err := url.Parse(raw); err == nil && u.Host != "" {
						upstream = u
					} else {
						m.log.Warn("ignoring malformed upstream label", "id", e.sb.ID, "value", raw)
					}
				}
				return e.sb.ID, upstream, true
			}
		}
	}
	return "", nil, false
}

// OnModelEvent implements the proxy's resolver: the event goes to the
// policy when it is model-aware, under the manager's serialisation, and
// the answer says whether a ResponseReady response is to be held. Hold
// actions are consumed here; every other action runs as usual.
func (m *Manager) OnModelEvent(ev policy.ModelEvent) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch ev.Kind {
	case policy.ModelRequestStarted:
		m.model.Calls++
		if e, ok := m.entries[ev.Sandbox]; ok {
			e.lastCall = ev.Call
		}
		// The agent asks the model again: the call its whole checkpoint was taken for is
		// over (or was never delivered), so the checkpoint can only rewind too far now.
		if cc, whole := m.wholeCheckpoints(); whole {
			if e, ok := m.entries[ev.Sandbox]; !ok || !e.suspended {
				cc.DiscardContainerCheckpoint(ev.Sandbox)
			}
		}
	case policy.ModelResponseDelivered:
		if _, held := m.heldOpen[ev.Call]; held {
			delete(m.heldOpen, ev.Call)
			m.model.Open = len(m.heldOpen)
			m.model.HeldTotal += ev.Held
			if ev.Forced {
				m.model.Forced++
			}
		}
	}
	aware, ok := m.cfg.Policy.(policy.ModelAware)
	if !ok {
		return false
	}
	actions := aware.OnModelEvent(ev, m.viewLocked())
	hold, released := false, false
	rest := actions[:0]
	for _, a := range actions {
		switch x := a.(type) {
		case policy.Hold:
			if ev.Kind == policy.ModelResponseReady && x.Call == ev.Call {
				hold = true
			} else {
				m.log.Warn("ignoring Hold outside its response-ready event", "id", x.ID, "call", x.Call)
			}
			continue
		case policy.Release:
			if ev.Kind == policy.ModelResponseReady && x.Call == ev.Call {
				released = true
				continue
			}
		}
		rest = append(rest, a)
	}
	if hold && released {
		// Held and released in the same event (the policy scheduled it at once): the
		// response is not held at all. A Release run before the proxy had registered the
		// hold found nothing to release, and the response then stayed held for good.
		hold = false
	}
	if hold {
		m.heldOpen[ev.Call] = ev.Sandbox
		m.model.Held++
		m.model.Open = len(m.heldOpen)
	}
	m.runActions(rest)
	return hold
}

// BeforeDelivery implements the proxy's resolver: a checkpointing policy
// gets the sandbox's filesystem checkpointed before the agent sees a
// response with tool calls.
func (m *Manager) BeforeDelivery(ev policy.ModelEvent) time.Duration {
	cp, ok := m.cfg.Policy.(policy.Checkpointer)
	if !ok || !cp.CheckpointBefore(ev) {
		return 0
	}
	started := time.Now()
	e, err := m.withRuntime(ev.Sandbox, sandbox.PhaseRunning)
	if err != nil {
		m.log.Warn("checkpoint skipped", "id", ev.Sandbox, "call", ev.Call, "err", err)
		m.checkpointDone(ev, time.Since(started), err)
		return time.Since(started)
	}
	ctx, cancel := context.WithTimeout(m.ctx, 2*time.Minute)
	defer cancel()
	var info runtime.CheckpointInfo
	if cc, whole := m.wholeCheckpoints(); whole {
		info, err = cc.CheckpointContainer(ctx, e.sb.ID)
	} else {
		info, err = m.cfg.Runtime.Checkpoint(ctx, e.sb.ID)
	}
	m.checkpointDone(ev, info.Took, err)
	if err != nil {
		m.log.Warn("checkpoint failed", "id", ev.Sandbox, "call", ev.Call, "err", err)
		return time.Since(started)
	}
	m.mu.Lock()
	m.model.Checkpoints++
	m.model.CheckpointTotal += info.Took
	m.mu.Unlock()
	m.log.Debug("checkpoint", "id", ev.Sandbox, "call", ev.Call, "entries", info.Entries, "tookMs", info.Took.Milliseconds())
	return time.Since(started)
}

// checkpointDone tells a policy that observes its checkpoints how this one ended.
func (m *Manager) checkpointDone(ev policy.ModelEvent, took time.Duration, err error) {
	if o, ok := m.cfg.Policy.(policy.CheckpointObserver); ok {
		o.CheckpointDone(ev.Sandbox, ev.Call, took, err)
	}
}

// BeforeRequest implements the proxy's resolver: with whole-sandbox
// checkpoints and a policy that asks for one at the request, the sandbox is
// checkpointed while its agent waits on the model; the proxy waits for it
// before the response's first byte. The checkpoint overlaps the model's
// own latency, so it costs the agent nothing it can measure.
func (m *Manager) BeforeRequest(ev policy.ModelEvent) func() {
	cc, whole := m.wholeCheckpoints()
	cp, ok := m.cfg.Policy.(policy.Checkpointer)
	if !whole || !ok || !cp.CheckpointBefore(ev) {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		e, err := m.withRuntime(ev.Sandbox, sandbox.PhaseRunning)
		if err != nil {
			m.log.Debug("checkpoint skipped", "id", ev.Sandbox, "call", ev.Call, "err", err)
			m.checkpointDone(ev, 0, err)
			return
		}
		ctx, cancel := context.WithTimeout(m.ctx, 2*time.Minute)
		defer cancel()
		info, err := cc.CheckpointContainer(ctx, e.sb.ID)
		m.checkpointDone(ev, info.Took, err)
		if err != nil {
			m.log.Warn("checkpoint failed", "id", ev.Sandbox, "call", ev.Call, "err", err)
			return
		}
		m.mu.Lock()
		m.model.Checkpoints++
		m.model.CheckpointTotal += info.Took
		m.mu.Unlock()
		m.log.Debug("checkpoint", "id", ev.Sandbox, "call", ev.Call, "at", "request", "entries", info.Entries, "tookMs", info.Took.Milliseconds())
	}()
	return func() { <-done }
}

// Model reports the LLM proxy counters.
func (m *Manager) Model() ModelStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.model
}

// Start loads persisted sandboxes, reconciles them with the runtime, and
// starts the sampling and tick loops.
func (m *Manager) Start(ctx context.Context) error {
	recs, err := m.cfg.Store.List()
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	known := map[string]bool{}
	for _, rec := range recs {
		known[rec.Sandbox.ID] = true
		if err := m.reconcile(ctx, rec); err != nil {
			m.log.Warn("reconcile sandbox", "id", rec.Sandbox.ID, "err", err)
		}
	}
	m.loadSnapshots(ctx)
	ids, err := m.cfg.Runtime.List(ctx)
	if err != nil {
		return fmt.Errorf("list runtime sandboxes: %w", err)
	}
	for _, id := range ids {
		if known[id] {
			continue
		}
		m.log.Warn("removing orphaned runtime sandbox", "id", id)
		if err := m.cfg.Runtime.Delete(ctx, id); err != nil {
			m.log.Warn("delete orphan", "id", id, "err", err)
		}
	}
	if m.cfg.SampleInterval > 0 {
		m.wg.Add(1)
		go m.sampleLoop()
	}
	if m.cfg.TickInterval > 0 {
		m.wg.Add(1)
		go m.tickLoop()
	}
	m.meterTraffic()
	m.watchPressure()
	m.log.Info("manager started", "policy", m.cfg.Policy.Name(), "runtime", m.cfg.Runtime.Name(),
		"capacity", m.cfg.Capacity.String(), "sandboxes", len(m.entries))
	return nil
}

func (m *Manager) reconcile(ctx context.Context, rec store.Record) error {
	e := &entry{sb: rec.Sandbox, startRequested: rec.StartRequested}
	if len(rec.Cgroup) > 0 {
		if err := json.Unmarshal(rec.Cgroup, &e.cgroup); err != nil {
			return fmt.Errorf("decode cgroup: %w", err)
		}
	}
	now := time.Now()
	fail := func(msg string) {
		e.sb.Status.Phase = sandbox.PhaseFailed
		e.sb.Status.Message = msg
		e.sb.Status.FinishedAt = &now
		e.accounted = false
	}
	switch phase := e.sb.Status.Phase; {
	case phase == sandbox.PhaseQueued, phase.Terminal():
		// Nothing in the runtime to check.
	default:
		st, err := m.cfg.Runtime.Status(ctx, e.sb.ID)
		switch {
		case errors.Is(err, runtime.ErrNotFound):
			fail("sandbox disappeared from runtime while tideletd was down")
		case err != nil:
			return err
		case st.State == runtime.StateCreated:
			e.sb.Status.Phase = sandbox.PhaseCreated
			e.accounted = true
		case st.State == runtime.StateRunning:
			e.sb.Status.Phase = sandbox.PhaseRunning
			e.accounted = true
		case st.State == runtime.StatePaused:
			e.sb.Status.Phase = sandbox.PhaseFrozen
			e.accounted = true
		case st.State == runtime.StateStopped:
			e.sb.Status.Phase = sandbox.PhaseExited
			code := st.ExitCode
			e.sb.Status.ExitCode = &code
			t := st.ExitedAt
			e.sb.Status.FinishedAt = &t
			e.accounted = false
		default:
			fail(fmt.Sprintf("unexpected runtime state %q after restart", st.State))
		}
	}
	m.mu.Lock()
	m.entries[e.sb.ID] = e
	if e.sb.Name != "" {
		m.byName[e.sb.Name] = e.sb.ID
	}
	if e.sb.Status.Phase == sandbox.PhaseRunning || e.sb.Status.Phase == sandbox.PhaseFrozen {
		m.watchLocked(e)
	}
	m.mu.Unlock()
	return m.persist(e)
}

// Close stops the loops and exit watchers. Sandboxes keep running in the
// runtime.
func (m *Manager) Close() error {
	m.cancel()
	m.wg.Wait()
	if m.traffic != nil {
		m.traffic.Close()
		m.traffic = nil
	}
	m.mu.Lock()
	for ch := range m.subs {
		close(ch)
		delete(m.subs, ch)
	}
	m.mu.Unlock()
	return nil
}

// Subscribe returns a channel of lifecycle events. Call the returned
// function to unsubscribe. Slow subscribers lose events rather than
// blocking the manager.
func (m *Manager) Subscribe() (<-chan sandbox.Event, func()) {
	ch := make(chan sandbox.Event, 256)
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
	return ch, func() {
		m.mu.Lock()
		if _, ok := m.subs[ch]; ok {
			delete(m.subs, ch)
			close(ch)
		}
		m.mu.Unlock()
	}
}

// emitLocked publishes an event, hands it to the policy, and schedules the
// policy's actions. Caller holds m.mu.
func (m *Manager) emitLocked(typ sandbox.EventType, e *entry) {
	ev := sandbox.Event{Type: typ, Time: time.Now(), Sandbox: e.sb}
	for ch := range m.subs {
		select {
		case ch <- ev:
		default:
			m.log.Warn("dropping event for slow subscriber", "type", typ, "id", e.sb.ID)
		}
	}
	m.runActions(m.cfg.Policy.OnEvent(ev, m.viewLocked()))
}

// runActions executes policy actions outside the lock.
func (m *Manager) runActions(actions []policy.Action) {
	if len(actions) == 0 {
		return
	}
	// A prefetch or a reclaim moves a sandbox's memory and can take seconds: each runs
	// on its own, so the releases behind it in the batch are not delayed by it. A freeze
	// or a thaw runs on its own too: a batch of cgroup updates ahead of it (one per
	// sandbox under a loaded host) would land it seconds after the policy meant it.
	// Cgroup updates fan out, each on its own goroutine under a bound: applied one after
	// another, a batch of one update per sandbox took minutes on a loaded host, so the
	// cgroups trailed the policy's decisions by minutes and the next batches piled up
	// behind. Each batch is numbered, and an update from an older batch that reaches a
	// sandbox after a newer one is stale and dropped.
	var slow, quick []policy.Action
	var ups []policy.Update
	for _, a := range actions {
		switch a := a.(type) {
		case policy.Prefetch, policy.Reclaim, policy.Freeze, policy.Thaw:
			slow = append(slow, a)
		case policy.Update:
			ups = append(ups, a)
		default:
			quick = append(quick, a)
		}
	}
	for _, a := range slow {
		m.runActions1(a)
	}
	if len(ups) > 0 {
		// No m.mu here: emitLocked calls runActions with the lock held.
		seq := m.actSeq.Add(1)
		m.updOnce.Do(func() { m.updSem = make(chan struct{}, 32) })
		sem := m.updSem
		for _, u := range ups {
			m.wg.Add(1)
			go func(u policy.Update) {
				defer m.wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := m.update(m.ctx, u, seq); err != nil {
					if errors.Is(err, ErrNotFound) || errors.Is(err, runtime.ErrNotFound) || errors.Is(err, runtime.ErrNotRunning) {
						m.log.Debug("policy action dropped, sandbox gone", "action", fmt.Sprintf("%T", u), "id", u.Target(), "err", err)
						return
					}
					m.log.Warn("policy action failed", "action", fmt.Sprintf("%T", u), "id", u.Target(), "err", err)
				}
			}(u)
		}
	}
	if len(quick) == 0 {
		return
	}
	actions = quick
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for _, a := range actions {
			if err := m.apply(m.ctx, a); err != nil {
				if errors.Is(err, ErrNotFound) || errors.Is(err, runtime.ErrNotFound) || errors.Is(err, runtime.ErrNotRunning) {
					// The sandbox went away under the policy's decision: an ordinary race.
					m.log.Debug("policy action dropped, sandbox gone", "action", fmt.Sprintf("%T", a), "id", a.Target(), "err", err)
					continue
				}
				m.log.Warn("policy action failed", "action", fmt.Sprintf("%T", a), "id", a.Target(), "err", err)
			}
		}
	}()
}

// runActions1 applies one action on its own goroutine.
func (m *Manager) runActions1(a policy.Action) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.apply(m.ctx, a); err != nil {
			if errors.Is(err, ErrNotFound) || errors.Is(err, runtime.ErrNotFound) || errors.Is(err, runtime.ErrNotRunning) {
				m.log.Debug("policy action dropped, sandbox gone", "action", fmt.Sprintf("%T", a), "id", a.Target(), "err", err)
				return
			}
			m.log.Warn("policy action failed", "action", fmt.Sprintf("%T", a), "id", a.Target(), "err", err)
		}
	}()
}

func (m *Manager) sampleLoop() {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.SampleInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			m.sample(m.ctx)
		}
	}
}

func (m *Manager) tickLoop() {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-t.C:
			m.mu.Lock()
			actions := m.cfg.Policy.OnTick(now, m.viewLocked())
			m.runActions(actions)
			m.mu.Unlock()
		}
	}
}

// sample collects node and per-sandbox usage and hands it to the policy.
func (m *Manager) sample(ctx context.Context) {
	s := policy.Sample{Time: time.Now(), Sandboxes: map[string]sandbox.Stats{}}
	collect := m.cfg.Collect
	if collect == nil {
		collect = host.Collect
	}
	ns, err := collect()
	if err != nil {
		m.log.Warn("collect node sample", "err", err)
	}
	s.Node = ns

	m.mu.Lock()
	var ids []string
	for id, e := range m.entries {
		if e.sb.Status.Phase.HasRuntime() {
			ids = append(ids, id)
		}
	}
	m.mu.Unlock()
	for _, id := range ids {
		st, err := m.cfg.Runtime.Stats(ctx, id)
		if err != nil {
			continue // likely exited meanwhile
		}
		s.Sandboxes[id] = st
	}
	if m.traffic != nil {
		total, by := m.traffic.Read(s.Time)
		ns.Bandwidth, s.Node.Bandwidth = total, total
		for id, st := range s.Sandboxes {
			if ino, ok := host.CgroupID(filepath.Join(m.cfg.PoolCgroup, id)); ok {
				st.BandwidthBytesPerSec = by[ino]
				s.Sandboxes[id] = st
			}
		}
	}

	m.mu.Lock()
	m.lastNode = ns
	for id, st := range s.Sandboxes {
		if e, ok := m.entries[id]; ok {
			stc := st
			e.stats = &stc
		}
	}
	actions := m.cfg.Policy.OnSample(s, m.viewLocked())
	m.runActions(actions)
	if m.bwPolicy != nil {
		if trig := m.bwPolicy.BandwidthTrigger(); trig > 0 && ns.Bandwidth >= trig {
			if sp, ok := m.cfg.Policy.(policy.Signaled); ok {
				m.runActions(sp.OnSignal(policy.Signal{Kind: policy.SignalBandwidthPressure, Time: s.Time}, m.viewLocked()))
			}
		}
	}
	m.mu.Unlock()
}

// meterTraffic opens the memory traffic meter on the pool's CPUs for a
// policy that budgets bandwidth. Without one (no counters, no permission)
// the policy sees no traffic and never holds for it.
func (m *Manager) meterTraffic() {
	bt, ok := m.cfg.Policy.(policy.BandwidthTriggered)
	if !ok || m.cfg.SampleInterval <= 0 {
		return
	}
	var cpus []int
	if m.cfg.PoolCgroup != "" {
		cpus = host.PoolCPUs(m.cfg.PoolCgroup)
	}
	t, err := host.NewTraffic(cpus, 0)
	if err != nil {
		m.log.Warn("memory traffic meter unavailable; the policy will not see bandwidth", "err", err)
		return
	}
	m.traffic, m.bwPolicy = t, bt
	m.log.Info("memory traffic meter installed", "event", t.Event, "bytesPerEvent", t.BytesPerEvent, "cpus", len(t.CPUs()), "trigger", bt.BandwidthTrigger())
}

// viewLocked builds the policy's view. Caller holds m.mu.
func (m *Manager) viewLocked() policy.View {
	v := policy.View{
		Now:        time.Now(),
		Capacity:   m.cfg.Capacity,
		Reserved:   m.cfg.Reserved,
		Node:       m.lastNode,
		PoolCgroup: m.cfg.PoolCgroup,
	}
	for _, e := range m.entries {
		sv := policy.SandboxView{Sandbox: e.sb, Cgroup: e.cgroup, Accounted: e.accounted, StartRequested: e.startRequested}
		if e.stats != nil {
			st := *e.stats
			sv.Stats = &st
		}
		if e.accounted {
			v.Allocated = v.Allocated.Add(e.sb.Status.Allocated.Requests)
		}
		v.Sandboxes = append(v.Sandboxes, sv)
	}
	sort.Slice(v.Sandboxes, func(i, j int) bool {
		return v.Sandboxes[i].Sandbox.CreatedAt.Before(v.Sandboxes[j].Sandbox.CreatedAt)
	})
	return v
}

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func validateSpec(spec *sandbox.Spec) error {
	if spec.Image == "" {
		return fmt.Errorf("%w: image is required", ErrInvalid)
	}
	switch spec.Runtime {
	case "", sandbox.RuntimeContainer:
		spec.Runtime = sandbox.RuntimeContainer
	case sandbox.RuntimeMicroVM:
	default:
		return fmt.Errorf("%w: unknown runtime %q", ErrInvalid, spec.Runtime)
	}
	switch spec.Network {
	case "":
		spec.Network = sandbox.NetworkNone
	case sandbox.NetworkNone, sandbox.NetworkHost, sandbox.NetworkBridge:
	case sandbox.NetworkJoin:
		if spec.NetworkNamespace == "" {
			return fmt.Errorf("%w: network mode join requires networkNamespace", ErrInvalid)
		}
		if _, err := os.Stat(spec.NetworkNamespace); err != nil {
			return fmt.Errorf("%w: networkNamespace %s: %w", ErrInvalid, spec.NetworkNamespace, err)
		}
	default:
		return fmt.Errorf("%w: unknown network mode %q", ErrInvalid, spec.Network)
	}
	for _, r := range []resource.Set{spec.Resources.Requests, spec.Resources.Limits} {
		if r.CPU < 0 || r.Memory < 0 {
			return fmt.Errorf("%w: resources must not be negative", ErrInvalid)
		}
	}
	for _, mnt := range spec.Mounts {
		if mnt.HostPath == "" || mnt.ContainerPath == "" {
			return fmt.Errorf("%w: mount hostPath and containerPath are required", ErrInvalid)
		}
		if _, err := os.Stat(mnt.HostPath); err != nil {
			return fmt.Errorf("%w: mount hostPath %s: %w", ErrInvalid, mnt.HostPath, err)
		}
	}
	return nil
}

// Create submits a sandbox to the policy. Depending on the decision the
// returned sandbox is Created (in the runtime, not started) or Queued.
func (m *Manager) Create(ctx context.Context, name string, spec sandbox.Spec) (*sandbox.Sandbox, error) {
	if spec.Snapshot != "" {
		var err error
		if spec, err = m.fromSnapshot(spec); err != nil {
			return nil, err
		}
	}
	if err := validateSpec(&spec); err != nil {
		return nil, err
	}
	if err := m.cfg.Runtime.Validate(spec); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if name != "" && !nameRE.MatchString(name) {
		return nil, fmt.Errorf("%w: invalid name %q", ErrInvalid, name)
	}
	sb := sandbox.Sandbox{
		ID:        newID(),
		Name:      name,
		CreatedAt: time.Now(),
		Spec:      spec,
		Status:    sandbox.Status{Phase: sandbox.PhasePending, Policy: m.cfg.Policy.Name()},
	}

	m.mu.Lock()
	if name != "" {
		if _, exists := m.byName[name]; exists {
			m.mu.Unlock()
			return nil, fmt.Errorf("%w: a sandbox named %q already exists", ErrConflict, name)
		}
	}
	dec := m.cfg.Policy.Admit(sb, m.viewLocked())
	e := &entry{sb: sb}
	switch dec.Kind {
	case policy.Reject:
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrRejected, dec.Reason)
	case policy.Invalid:
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrInvalid, dec.Reason)
	case policy.Queue:
		e.sb.Status.Phase = sandbox.PhaseQueued
		e.sb.Status.Message = dec.Reason
		m.addLocked(e)
		m.emitLocked(sandbox.EventQueued, e)
		m.mu.Unlock()
		if err := m.persist(e); err != nil {
			return nil, err
		}
		m.log.Info("sandbox queued", "id", sb.ID, "name", name, "reason", dec.Reason)
		return m.snapshot(e), nil
	case policy.Run:
		m.grantLocked(e, dec.Allocation)
		m.addLocked(e)
		m.mu.Unlock()
	default:
		m.mu.Unlock()
		return nil, fmt.Errorf("policy %s returned unknown decision %d", m.cfg.Policy.Name(), dec.Kind)
	}
	if err := m.persist(e); err != nil {
		m.remove(e.sb.ID)
		return nil, err
	}
	if err := m.createInRuntime(ctx, e); err != nil {
		return nil, err
	}
	if m.startRequested(e) {
		if err := m.start(ctx, e); err != nil {
			return nil, err
		}
	}
	return m.snapshot(e), nil
}

// startRequested reports whether a start was asked for before the sandbox
// was Created. Caller must not hold m.mu.
func (m *Manager) startRequested(e *entry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return e.startRequested
}

// addLocked inserts an entry into the table. Caller holds m.mu.
func (m *Manager) addLocked(e *entry) {
	m.entries[e.sb.ID] = e
	if e.sb.Name != "" {
		m.byName[e.sb.Name] = e.sb.ID
	}
}

// shape applies the daemon's sandbox defaults to a policy's cgroup settings:
// with SwapToLimit, a sandbox the policy lets swap without bound may swap up
// to its memory limit instead.
func (m *Manager) shape(cg runtime.Cgroup) runtime.Cgroup {
	if m.cfg.SwapToLimit && cg.MemoryLimitBytes > 0 && cg.MemorySwapLimitBytes == 0 {
		cg.MemorySwapLimitBytes = cg.MemoryLimitBytes
	}
	return cg
}

// grantLocked records an allocation and marks the entry as accounted.
// Caller holds m.mu.
func (m *Manager) grantLocked(e *entry, a policy.Allocation) {
	e.sb.Status.Allocated = sandbox.Resources{Requests: a.Requests, Limits: a.Limits}
	e.cgroup = m.shape(a.Cgroup)
	e.accounted = true
	e.sb.Status.Phase = sandbox.PhasePending
	e.sb.Status.Message = ""
}

// ThreadEnv names the environment variables that size the thread pools of
// the common math and inference libraries: OpenMP, OpenBLAS, MKL, BLIS,
// numba, ONNX Runtime and Rayon. A sandbox sees every CPU of the node and is
// held to its CPU limit by cpu.max alone, so a library sizing its pool from
// the CPUs it sees would start one thread per host CPU and have them
// throttled. Every sandbox with a CPU limit gets each of these set to the
// limit rounded up, unless its spec sets it.
var ThreadEnv = []string{"OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "BLIS_NUM_THREADS",
	"NUMBA_NUM_THREADS", "ORT_INTRA_OP_NUM_THREADS", "RAYON_NUM_THREADS"}

// threadEnv is a sandbox's environment with the ThreadEnv variables set to
// its CPU limit rounded up, the spec's own values kept; env itself is left
// unchanged, and without a CPU limit it is returned as it is.
func threadEnv(env map[string]string, limit resource.Millicores) map[string]string {
	if limit <= 0 {
		return env
	}
	n := strconv.FormatInt(int64((limit+999)/1000), 10)
	out := make(map[string]string, len(env)+len(ThreadEnv))
	for _, k := range ThreadEnv {
		out[k] = n
	}
	maps.Copy(out, env)
	return out
}

// createInRuntime creates an allocated entry in the runtime and moves it
// to Created, or fails it. Caller must not hold m.mu.
func (m *Manager) createInRuntime(ctx context.Context, e *entry) error {
	m.mu.Lock()
	sbCopy := e.sb
	cg := e.cgroup
	m.mu.Unlock()
	// The runtime gets the thread counts its allocation implies; the stored spec stays as asked.
	sbCopy.Spec.Env = threadEnv(sbCopy.Spec.Env, sbCopy.Status.Allocated.Limits.CPU)
	m.log.Info("creating sandbox", "id", sbCopy.ID, "name", sbCopy.Name, "image", sbCopy.Spec.Image,
		"requests", sbCopy.Status.Allocated.Requests.String(), "limits", sbCopy.Status.Allocated.Limits.String())
	if err := m.cfg.Runtime.Create(ctx, &sbCopy, cg); err != nil {
		m.fail(e, err.Error())
		return fmt.Errorf("create sandbox %s: %w", sbCopy.ID, err)
	}
	m.mu.Lock()
	if _, alive := m.entries[sbCopy.ID]; !alive || e.deleting {
		// Deleted while it was being created (a queued sandbox the policy dispatched
		// just as its owner deleted it): the runtime now holds a sandbox
		// nothing tracks, so it goes again.
		m.mu.Unlock()
		m.log.Warn("sandbox deleted while being created; removing it from the runtime", "id", sbCopy.ID)
		if err := m.cfg.Runtime.Delete(ctx, sbCopy.ID); err != nil && !errors.Is(err, runtime.ErrNotFound) {
			m.log.Warn("remove sandbox created after its delete", "id", sbCopy.ID, "err", err)
		}
		return fmt.Errorf("%w: sandbox %s was deleted while being created", ErrConflict, sbCopy.ID)
	}
	e.sb.Status.Phase = sandbox.PhaseCreated
	e.sb.Status.Network = sbCopy.Status.Network
	m.emitLocked(sandbox.EventCreated, e)
	m.mu.Unlock()
	if err := m.persist(e); err != nil {
		m.log.Warn("persist sandbox", "id", sbCopy.ID, "err", err)
	}
	return nil
}

// fail moves an entry to Failed and releases its allocation.
func (m *Manager) fail(e *entry, msg string) {
	m.mu.Lock()
	if e.sb.Status.Phase.Terminal() {
		m.mu.Unlock()
		return
	}
	if e.cancel != nil {
		e.cancel()
	}
	now := time.Now()
	e.accounted = false
	e.sb.Status.Phase = sandbox.PhaseFailed
	e.sb.Status.Message = msg
	e.sb.Status.FinishedAt = &now
	m.emitLocked(sandbox.EventFailed, e)
	m.mu.Unlock()
	m.log.Warn("sandbox failed", "id", e.sb.ID, "reason", msg)
	if err := m.persist(e); err != nil {
		m.log.Warn("persist sandbox", "id", e.sb.ID, "err", err)
	}
}

// StartSandbox starts a created sandbox. On a queued sandbox, or one still
// being created (Pending), it records that a start is wanted: a client that
// creates and then starts can find its sandbox dispatched in between.
func (m *Manager) StartSandbox(ctx context.Context, ref string) (*sandbox.Sandbox, error) {
	e, err := m.resolve(ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	switch e.sb.Status.Phase {
	case sandbox.PhaseQueued, sandbox.PhasePending:
		e.startRequested = true
		m.mu.Unlock()
		if err := m.persist(e); err != nil {
			return nil, err
		}
		return m.snapshot(e), nil
	case sandbox.PhaseCreated:
		m.mu.Unlock()
	default:
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: sandbox %s is %s, not Created", ErrConflict, e.sb.ID, phase)
	}
	return m.snapshot(e), m.start(ctx, e)
}

func (m *Manager) start(ctx context.Context, e *entry) error {
	if err := m.cfg.Runtime.Start(ctx, e.sb.ID); err != nil {
		m.fail(e, "start: "+err.Error())
		return fmt.Errorf("start sandbox %s: %w", e.sb.ID, err)
	}
	m.mu.Lock()
	now := time.Now()
	e.sb.Status.Phase = sandbox.PhaseRunning
	e.sb.Status.StartedAt = &now
	e.startRequested = false
	m.watchLocked(e)
	m.emitLocked(sandbox.EventStarted, e)
	m.mu.Unlock()
	if err := m.persist(e); err != nil {
		m.log.Warn("persist sandbox", "id", e.sb.ID, "err", err)
	}
	return nil
}

// watchLocked starts the exit watcher. Caller holds m.mu.
func (m *Manager) watchLocked(e *entry) {
	if e.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	e.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		res, err := m.cfg.Runtime.Wait(ctx, e.sb.ID)
		m.mu.Lock()
		if e.suspended {
			// Stopped by a whole-sandbox rollback: the restore watches the new task.
			if e.cancel != nil {
				e.cancel()
				e.cancel = nil
			}
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		if err != nil {
			if ctx.Err() == nil {
				m.log.Warn("exit watcher", "id", e.sb.ID, "err", err)
			}
			return
		}
		m.finish(e, res.ExitCode, res.ExitedAt, res.Err)
	}()
}

// finish records that a sandbox's main process has exited and releases
// its allocation. It is idempotent.
func (m *Manager) finish(e *entry, code int32, at time.Time, cause error) {
	m.mu.Lock()
	if e.sb.Status.Phase != sandbox.PhaseRunning && e.sb.Status.Phase != sandbox.PhaseFrozen {
		m.mu.Unlock()
		return
	}
	e.accounted = false
	if e.cancel != nil {
		e.cancel()
		e.cancel = nil
	}
	if at.IsZero() {
		at = time.Now()
	}
	e.sb.Status.ExitCode = &code
	e.sb.Status.FinishedAt = &at
	if cause != nil {
		e.sb.Status.Phase = sandbox.PhaseFailed
		e.sb.Status.Message = cause.Error()
		m.emitLocked(sandbox.EventFailed, e)
	} else {
		e.sb.Status.Phase = sandbox.PhaseExited
		m.emitLocked(sandbox.EventExited, e)
	}
	m.mu.Unlock()
	m.log.Info("sandbox exited", "id", e.sb.ID, "code", code)
	if err := m.persist(e); err != nil {
		m.log.Warn("persist sandbox", "id", e.sb.ID, "err", err)
	}
}

func (m *Manager) persist(e *entry) error {
	m.mu.Lock()
	rec := store.Record{Sandbox: e.sb, StartRequested: e.startRequested}
	if e.cgroup != (runtime.Cgroup{}) {
		rec.Cgroup, _ = json.Marshal(e.cgroup)
	}
	m.mu.Unlock()
	if err := m.cfg.Store.Save(rec); err != nil {
		return fmt.Errorf("persist sandbox %s: %w", e.sb.ID, err)
	}
	return nil
}

func (m *Manager) remove(id string) {
	m.mu.Lock()
	from := ""
	if e, ok := m.entries[id]; ok {
		if e.cancel != nil {
			e.cancel()
		}
		if e.sb.Name != "" && m.byName[e.sb.Name] == id {
			delete(m.byName, e.sb.Name)
		}
		delete(m.entries, id)
		from = e.sb.Spec.Snapshot
	}
	m.mu.Unlock()
	m.releaseSnapshot(from)
}

func (m *Manager) snapshot(e *entry) *sandbox.Sandbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	sb := e.sb
	return &sb
}

// resolve finds a sandbox by exact ID, exact name, or unique ID prefix.
func (m *Manager) resolve(ref string) (*entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[ref]; ok {
		return e, nil
	}
	if id, ok := m.byName[ref]; ok {
		return m.entries[id], nil
	}
	var match *entry
	for id, e := range m.entries {
		if strings.HasPrefix(id, ref) {
			if match != nil {
				return nil, fmt.Errorf("%w: %q matches multiple sandboxes", ErrAmbiguous, ref)
			}
			match = e
		}
	}
	if match == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, ref)
	}
	return match, nil
}

// Get returns a sandbox by ID, name, or ID prefix.
func (m *Manager) Get(ref string) (*sandbox.Sandbox, error) {
	e, err := m.resolve(ref)
	if err != nil {
		return nil, err
	}
	return m.snapshot(e), nil
}

// List returns all sandboxes, newest first.
func (m *Manager) List() []sandbox.Sandbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sandbox.Sandbox, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e.sb)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

// Stop terminates a running or frozen sandbox. Stopping any other phase is
// a no-op.
func (m *Manager) Stop(ctx context.Context, ref string, timeout time.Duration) (*sandbox.Sandbox, error) {
	e, err := m.resolve(ref)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = m.cfg.StopTimeout
	}
	if err := m.cfg.Runtime.Stop(ctx, e.sb.ID, timeout); err != nil && !errors.Is(err, runtime.ErrNotFound) {
		return nil, fmt.Errorf("stop sandbox %s: %w", e.sb.ID, err)
	}
	st, err := m.cfg.Runtime.Status(ctx, e.sb.ID)
	if err == nil && st.State == runtime.StateStopped {
		m.finish(e, st.ExitCode, st.ExitedAt, nil)
	}
	return m.snapshot(e), nil
}

// Delete removes a sandbox. A live sandbox is rejected unless force is
// set, in which case it is stopped first.
func (m *Manager) Delete(ctx context.Context, ref string, force bool) error {
	e, err := m.resolve(ref)
	if err != nil {
		return err
	}
	m.mu.Lock()
	phase := e.sb.Status.Phase
	e.deleting = true
	m.mu.Unlock()
	if phase == sandbox.PhaseRunning || phase == sandbox.PhaseFrozen || phase == sandbox.PhasePending {
		if !force {
			return fmt.Errorf("%w: sandbox %s is %s; stop it or use force", ErrConflict, e.sb.ID, phase)
		}
		if _, err := m.Stop(ctx, e.sb.ID, 0); err != nil {
			return err
		}
	}
	if err := m.cfg.Runtime.Delete(ctx, e.sb.ID); err != nil {
		return fmt.Errorf("delete sandbox %s: %w", e.sb.ID, err)
	}
	if err := m.cfg.Store.Delete(e.sb.ID); err != nil {
		return fmt.Errorf("delete sandbox %s state: %w", e.sb.ID, err)
	}
	m.mu.Lock()
	e.accounted = false
	m.emitLocked(sandbox.EventDeleted, e)
	m.mu.Unlock()
	m.remove(e.sb.ID)
	m.log.Info("sandbox deleted", "id", e.sb.ID)
	return nil
}

// apply executes one policy action.
func (m *Manager) apply(ctx context.Context, a policy.Action) error {
	switch a := a.(type) {
	case policy.Dispatch:
		return m.dispatch(ctx, a)
	case policy.Update:
		return m.update(ctx, a, 0)
	case policy.Freeze:
		return m.freeze(ctx, a.ID)
	case policy.Thaw:
		return m.thaw(ctx, a.ID)
	case policy.Fail:
		e, err := m.resolve(a.ID)
		if err != nil {
			return err
		}
		m.mu.Lock()
		queued := e.sb.Status.Phase == sandbox.PhaseQueued
		m.mu.Unlock()
		if !queued {
			return fmt.Errorf("%w: sandbox %s is not queued", ErrConflict, a.ID)
		}
		m.fail(e, a.Reason)
		return nil
	case policy.Release:
		m.mu.Lock()
		gate := m.gate
		m.mu.Unlock()
		if gate == nil {
			return errors.New("no llm proxy to release through")
		}
		if !gate.Release(a.Call) {
			m.log.Debug("release found no held response", "id", a.ID, "call", a.Call)
		}
		return nil
	case policy.Prefetch:
		return m.prefetch(ctx, a.ID)
	case policy.Reclaim:
		e, err := m.withRuntime(a.ID, sandbox.PhaseRunning)
		if err != nil {
			return err
		}
		m.mu.Lock()
		busy := m.reclaiming[a.ID]
		if !busy {
			m.reclaiming[a.ID] = true
		}
		m.mu.Unlock()
		if busy {
			m.log.Debug("reclaim already in flight", "id", a.ID)
			return nil
		}
		defer func() {
			m.mu.Lock()
			delete(m.reclaiming, a.ID)
			m.mu.Unlock()
		}()
		return m.cfg.Runtime.Reclaim(ctx, e.sb.ID, a.Bytes)
	case policy.Hold:
		return errors.New("Hold is only meaningful in the actions for a response-ready event")
	case policy.Rollback:
		return m.rollback(ctx, a)
	case policy.Redo:
		return m.redo(ctx, a)
	case policy.Restore:
		return m.restore(ctx, a)
	case policy.Wake:
		m.wake(a.Key, a.After)
		return nil
	default:
		return fmt.Errorf("unknown action %T", a)
	}
}

// wholeCheckpoints reports whether checkpoints and rollbacks take the whole sandbox.
func (m *Manager) wholeCheckpoints() (runtime.ContainerCheckpointer, bool) {
	cc, ok := m.cfg.Runtime.(runtime.ContainerCheckpointer)
	return cc, ok && cc.ContainerCheckpoints()
}

// rollbackWhole stops the sandbox at its latest whole checkpoint: its processes are killed,
// its writable layer put back, and it holds nothing until a Restore.
func (m *Manager) rollbackWhole(ctx context.Context, cc runtime.ContainerCheckpointer, e *entry, a policy.Rollback) error {
	m.mu.Lock()
	if a.Call != "" && e.lastCall != "" && e.lastCall != a.Call {
		// The agent has moved on to its next call: there is nothing to undo any more.
		m.mu.Unlock()
		m.log.Info("rollback skipped: the call is over", "id", a.ID, "call", a.Call, "current", e.lastCall)
		m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRolledBack, Time: time.Now(), Sandbox: a.ID, Call: a.Call})
		return nil
	}
	e.suspended = true
	m.mu.Unlock()
	info, err := cc.RollbackContainer(ctx, e.sb.ID)
	if err != nil {
		m.log.Warn("rollback failed", "id", a.ID, "call", a.Call, "killed", info.Killed, "err", err)
		if info.Killed == 0 {
			// Nothing was stopped: the sandbox runs on as it was.
			m.mu.Lock()
			e.suspended = false
			m.mu.Unlock()
		}
	}
	if info.Killed > 0 {
		m.mu.Lock()
		m.model.Rollbacks++
		m.mu.Unlock()
	}
	m.log.Info("rollback", "id", a.ID, "call", a.Call, "whole", true, "killed", info.Killed, "restored", info.Restored, "tookMs", info.Took.Milliseconds())
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRolledBack, Time: time.Now(), Sandbox: a.ID, Call: a.Call, Killed: info.Killed, Whole: info.Killed > 0})
	return err
}

// restore resumes a sandbox a whole rollback stopped, puts its current cgroup settings back
// (the task comes back with those it was created with), and watches the new task.
func (m *Manager) restore(ctx context.Context, a policy.Restore) error {
	cc, whole := m.wholeCheckpoints()
	e, err := m.resolve(a.ID)
	if err != nil || !whole {
		if err == nil {
			err = errors.New("restore: whole-sandbox checkpoints are off")
		}
		m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRestored, Time: time.Now(), Sandbox: a.ID, Call: a.Call, Status: -1})
		return err
	}
	took, err := cc.RestoreContainer(ctx, e.sb.ID)
	for _, wait := range []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second} {
		if err == nil || ctx.Err() != nil {
			break
		}
		m.log.Warn("restore failed; trying again", "id", a.ID, "call", a.Call, "after", wait, "err", err)
		time.Sleep(wait)
		took, err = cc.RestoreContainer(ctx, e.sb.ID)
	}
	status := 0
	if err != nil {
		status = -1
		m.log.Error("restore failed; the sandbox's execs are given up on", "id", a.ID, "call", a.Call, "err", err)
		cc.AbandonContainer(e.sb.ID)
	} else {
		m.mu.Lock()
		e.suspended = false
		if e.cancel != nil {
			e.cancel()
			e.cancel = nil
		}
		m.watchLocked(e)
		cg := e.cgroup
		m.mu.Unlock()
		if cg != (runtime.Cgroup{}) {
			if uerr := m.cfg.Runtime.Update(ctx, e.sb.ID, cg); uerr != nil {
				m.log.Warn("restore: cgroup settings", "id", a.ID, "err", uerr)
			}
		}
		m.mu.Lock()
		m.model.Restores++
		m.mu.Unlock()
	}
	m.log.Info("restore", "id", a.ID, "call", a.Call, "status", status, "tookMs", took.Milliseconds())
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRestored, Time: time.Now(), Sandbox: a.ID, Call: a.Call, Status: status})
	return err
}

// rollback undoes the sandbox's tool call in flight and tells the policy
// what happened.
func (m *Manager) rollback(ctx context.Context, a policy.Rollback) error {
	e, err := m.withRuntime(a.ID, sandbox.PhaseRunning)
	if err != nil {
		return err
	}
	if cc, whole := m.wholeCheckpoints(); whole {
		return m.rollbackWhole(ctx, cc, e, a)
	}
	info, err := m.cfg.Runtime.Rollback(ctx, e.sb.ID)
	if err != nil {
		m.log.Warn("rollback failed", "id", a.ID, "call", a.Call, "killed", info.Killed, "err", err)
	}
	if info.Killed > 0 {
		m.mu.Lock()
		m.model.Rollbacks++
		m.mu.Unlock()
	}
	m.log.Info("rollback", "id", a.ID, "call", a.Call, "killed", info.Killed, "restored", info.Restored, "tookMs", info.Took.Milliseconds())
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRolledBack, Time: time.Now(), Sandbox: a.ID, Call: a.Call, Killed: info.Killed})
	return err
}

// redo runs a rolled-back call's commands again, in order, and tells the
// policy when they are done.
func (m *Manager) redo(ctx context.Context, a policy.Redo) error {
	e, err := m.withRuntime(a.ID, sandbox.PhaseRunning)
	if err != nil {
		return err
	}
	status := 0
	started := time.Now()
	for _, c := range a.Commands {
		cctx, cancel := context.WithTimeout(ctx, max(c.Timeout, time.Second))
		res, err := m.cfg.Runtime.Exec(cctx, e.sb.ID, runtime.ExecOptions{Command: []string{"/bin/bash", "-c", c.Command}, WorkingDir: a.WorkingDir})
		cancel()
		status = int(res.ExitCode)
		if err != nil {
			m.log.Warn("redo command", "id", a.ID, "call", a.Call, "exit", res.ExitCode, "err", err)
			if res.ExitCode == 0 {
				status = -1
			}
		}
	}
	m.mu.Lock()
	m.model.Redos++
	m.mu.Unlock()
	m.log.Info("redo done", "id", a.ID, "call", a.Call, "commands", len(a.Commands), "status", status, "tookMs", time.Since(started).Milliseconds())
	m.OnModelEvent(policy.ModelEvent{Kind: policy.ModelRedone, Time: time.Now(), Sandbox: a.ID, Call: a.Call, Status: status})
	return nil
}

func (m *Manager) dispatch(ctx context.Context, a policy.Dispatch) error {
	e, err := m.resolve(a.ID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if e.sb.Status.Phase != sandbox.PhaseQueued || e.deleting {
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		if e.deleting {
			return fmt.Errorf("%w: sandbox %s is being deleted", ErrConflict, a.ID)
		}
		return fmt.Errorf("%w: sandbox %s is %s, not Queued", ErrConflict, a.ID, phase)
	}
	m.grantLocked(e, a.Allocation)
	m.mu.Unlock()
	if err := m.persist(e); err != nil {
		return err
	}
	if err := m.createInRuntime(ctx, e); err != nil {
		return err
	}
	// Read after creation: the start may have been asked for while it ran.
	if m.startRequested(e) {
		return m.start(ctx, e)
	}
	return nil
}

func (m *Manager) update(ctx context.Context, a policy.Update, seq uint64) error {
	e, err := m.resolve(a.ID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if seq > 0 {
		if seq < e.updSeq {
			// From an older batch than one already seen for this sandbox: stale.
			m.mu.Unlock()
			return nil
		}
		e.updSeq = seq
	}
	if e.updating {
		// Applied after the one in flight; an older pending update is superseded, the
		// policy's newest settings are the ones that count.
		e.updPending = &a
		m.mu.Unlock()
		return nil
	}
	e.updating = true
	m.mu.Unlock()
	first := m.applyUpdate(ctx, e, a)
	for {
		m.mu.Lock()
		next := e.updPending
		e.updPending = nil
		if next == nil {
			e.updating = false
			m.mu.Unlock()
			return first
		}
		m.mu.Unlock()
		if err := m.applyUpdate(ctx, e, *next); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, runtime.ErrNotFound) && !errors.Is(err, runtime.ErrNotRunning) {
			m.log.Warn("policy action failed", "action", fmt.Sprintf("%T", *next), "id", next.ID, "err", err)
		}
	}
}

// applyUpdate puts one Update's settings on the sandbox's cgroup and records them.
func (m *Manager) applyUpdate(ctx context.Context, e *entry, a policy.Update) error {
	m.mu.Lock()
	if !e.sb.Status.Phase.HasRuntime() {
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		return fmt.Errorf("%w: sandbox %s is %s", ErrConflict, a.ID, phase)
	}
	m.mu.Unlock()
	cg := m.shape(a.Allocation.Cgroup)
	if err := m.cfg.Runtime.Update(ctx, e.sb.ID, cg); err != nil {
		return err
	}
	m.mu.Lock()
	alloc := sandbox.Resources{Requests: a.Allocation.Requests, Limits: a.Allocation.Limits}
	changed := e.sb.Status.Allocated != alloc
	e.sb.Status.Allocated = alloc
	e.cgroup = cg
	m.mu.Unlock()
	m.log.Info("sandbox updated", "id", e.sb.ID, "requests", a.Allocation.Requests.String(), "limits", a.Allocation.Limits.String())
	if !changed {
		// Only the cgroup's soft settings moved: nothing a restart needs back, and the
		// record's write + fsync + rename waited 6-600 ms on the journal of a loaded host,
		// holding the sandbox's update slot for that long.
		return nil
	}
	return m.persist(e)
}

// prefetch swaps the sandbox's memory back in and tells the policy when
// that is done.
func (m *Manager) prefetch(ctx context.Context, id string) error {
	e, err := m.withRuntime(id, sandbox.PhaseRunning)
	if err != nil {
		return err
	}
	start := time.Now()
	n, err := m.cfg.Runtime.Prefetch(ctx, e.sb.ID)
	if err != nil {
		return err
	}
	m.log.Debug("sandbox prefetched", "id", e.sb.ID, "bytes", n, "took", time.Since(start))
	m.mu.Lock()
	m.emitLocked(sandbox.EventPrefetched, e)
	m.mu.Unlock()
	return nil
}

func (m *Manager) freeze(ctx context.Context, id string) error {
	e, err := m.resolve(id)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if e.sb.Status.Phase != sandbox.PhaseRunning {
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		return fmt.Errorf("%w: sandbox %s is %s, not Running", ErrConflict, id, phase)
	}
	m.mu.Unlock()
	if err := m.cfg.Runtime.Freeze(ctx, e.sb.ID); err != nil {
		return err
	}
	m.mu.Lock()
	e.sb.Status.Phase = sandbox.PhaseFrozen
	m.emitLocked(sandbox.EventFrozen, e)
	m.mu.Unlock()
	m.log.Info("sandbox frozen", "id", e.sb.ID)
	return m.persist(e)
}

func (m *Manager) thaw(ctx context.Context, id string) error {
	e, err := m.resolve(id)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if e.sb.Status.Phase != sandbox.PhaseFrozen {
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		return fmt.Errorf("%w: sandbox %s is %s, not Frozen", ErrConflict, id, phase)
	}
	m.mu.Unlock()
	if err := m.cfg.Runtime.Thaw(ctx, e.sb.ID); err != nil {
		return err
	}
	m.mu.Lock()
	e.sb.Status.Phase = sandbox.PhaseRunning
	m.emitLocked(sandbox.EventThawed, e)
	m.mu.Unlock()
	m.log.Info("sandbox thawed", "id", e.sb.ID)
	return m.persist(e)
}

func (m *Manager) withRuntime(ref string, phases ...sandbox.Phase) (*entry, error) {
	e, err := m.resolve(ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	phase := e.sb.Status.Phase
	m.mu.Unlock()
	for _, p := range phases {
		if phase == p {
			return e, nil
		}
	}
	return nil, fmt.Errorf("%w: sandbox %s is %s", ErrConflict, e.sb.ID, phase)
}

// Exec runs a command inside a running sandbox.
func (m *Manager) Exec(ctx context.Context, ref string, opts runtime.ExecOptions) (runtime.ExecResult, error) {
	e, err := m.withRuntime(ref, sandbox.PhaseRunning)
	if err != nil {
		return runtime.ExecResult{}, err
	}
	return m.cfg.Runtime.Exec(ctx, e.sb.ID, opts)
}

// Attach forwards the main process's streams of a created, running or
// frozen sandbox.
func (m *Manager) Attach(ctx context.Context, ref string, opts runtime.AttachOptions) error {
	e, err := m.withRuntime(ref, sandbox.PhaseCreated, sandbox.PhaseRunning, sandbox.PhaseFrozen)
	if err != nil {
		return err
	}
	return m.cfg.Runtime.Attach(ctx, e.sb.ID, opts)
}

// Stats samples a live sandbox's resource usage.
func (m *Manager) Stats(ctx context.Context, ref string) (sandbox.Stats, error) {
	e, err := m.withRuntime(ref, sandbox.PhaseCreated, sandbox.PhaseRunning, sandbox.PhaseFrozen)
	if err != nil {
		return sandbox.Stats{}, err
	}
	return m.cfg.Runtime.Stats(ctx, e.sb.ID)
}

// LogsOptions control Logs.
type LogsOptions struct {
	Follow    bool
	TailLines int64
	Since     time.Time
}

// Logs streams recorded output to fn. With Follow it keeps going until
// the sandbox leaves the runtime.
func (m *Manager) Logs(ctx context.Context, ref string, opts LogsOptions, fn func(crilog.Entry) error) error {
	e, err := m.resolve(ref)
	if err != nil {
		return err
	}
	done := func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return !e.sb.Status.Phase.HasRuntime() && e.sb.Status.Phase != sandbox.PhasePending
	}
	return crilog.Read(ctx, m.cfg.Runtime.LogPath(e.sb.ID), crilog.ReadOptions{
		Follow:    opts.Follow,
		Done:      done,
		TailLines: opts.TailLines,
		Since:     opts.Since,
	}, fn)
}

// Snapshot returns the policy's view of the node, for metrics and
// diagnostics.
func (m *Manager) Snapshot() policy.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.viewLocked()
}

// Node reports capacity through the active policy.
func (m *Manager) Node() sandbox.NodeStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.viewLocked()
	return sandbox.NodeStatus{
		Policy:      m.cfg.Policy.Name(),
		Runtime:     m.cfg.Runtime.Name(),
		Capacity:    m.cfg.Capacity,
		Allocatable: m.cfg.Policy.Allocatable(v),
		Allocated:   v.Allocated,
		Sandboxes:   len(v.Sandboxes),
		Queued:      len(v.Queued()),
	}
}

// DebugWhole runs a whole-sandbox checkpoint, rollback or restore by hand, for testing the
// mechanism outside a policy.
func (m *Manager) DebugWhole(ctx context.Context, op, ref string) (string, error) {
	cc, whole := m.wholeCheckpoints()
	if !whole {
		return "", errors.New("whole-sandbox checkpoints are off")
	}
	e, err := m.resolve(ref)
	if err != nil {
		return "", err
	}
	switch op {
	case "checkpoint":
		info, err := cc.CheckpointContainer(ctx, e.sb.ID)
		return fmt.Sprintf("entries=%d tookMs=%d", info.Entries, info.Took.Milliseconds()), err
	case "rollback":
		return "rolled back", m.rollbackWhole(ctx, cc, e, policy.Rollback{ID: e.sb.ID, Call: "debug"})
	case "restore":
		return "restored", m.restore(ctx, policy.Restore{ID: e.sb.ID, Call: "debug"})
	}
	return "", fmt.Errorf("unknown op %q", op)
}

// OnSignal delivers a signal (a pressure trigger, a timer) to a Signaled
// policy, serialised with every other policy call.
func (m *Manager) OnSignal(sig policy.Signal) {
	sp, ok := m.cfg.Policy.(policy.Signaled)
	if !ok {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runActions(sp.OnSignal(sig, m.viewLocked()))
}

// wake arms (or replaces, or with after 0 cancels) the policy's timer key.
func (m *Manager) wake(key string, after time.Duration) {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	if t, ok := m.timers[key]; ok {
		t.Stop()
		delete(m.timers, key)
	}
	if after <= 0 {
		return
	}
	m.timers[key] = time.AfterFunc(after, func() {
		m.tmu.Lock()
		delete(m.timers, key)
		m.tmu.Unlock()
		if m.ctx.Err() == nil {
			m.OnSignal(policy.Signal{Kind: policy.SignalTimer, Time: time.Now(), Key: key})
		}
	})
}

// watchPressure installs the pool's pressure triggers for a Signaled policy.
func (m *Manager) watchPressure() {
	if _, ok := m.cfg.Policy.(policy.Signaled); !ok || m.cfg.PoolCgroup == "" {
		return
	}
	triggers := m.cfg.Pressure
	if triggers == nil {
		triggers = []host.PressureTrigger{
			{Resource: "cpu", Threshold: 100 * time.Millisecond, Window: time.Second},
			{Resource: "memory", Threshold: 100 * time.Millisecond, Window: time.Second},
		}
	}
	err := host.WatchPressure(m.ctx, m.cfg.PoolCgroup, triggers, func(resource string, at time.Time) {
		kind := policy.SignalCPUPressure
		if resource == "memory" {
			kind = policy.SignalMemoryPressure
		}
		m.OnSignal(policy.Signal{Kind: kind, Time: at})
	})
	if err != nil {
		m.log.Error("pressure triggers not installed; the policy will not see contention", "cgroup", m.cfg.PoolCgroup, "err", err)
		return
	}
	m.log.Info("pressure triggers installed", "cgroup", m.cfg.PoolCgroup, "triggers", len(triggers))
	// The policy's own timers start with a first signal, so it can arm them.
	m.OnSignal(policy.Signal{Kind: policy.SignalTimer, Time: time.Now(), Key: "start"})
}
