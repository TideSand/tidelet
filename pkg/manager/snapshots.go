package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Snapshots.
//
// A snapshot is a running sandbox at one instant (processes, memory, filesystem), taken
// without stopping it. Creating a sandbox with Spec.Snapshot set makes a copy of the
// snapshotted sandbox instead of starting an image's command, and that is all forking is:
// the copy goes through the policy's admission like any other sandbox, has its own
// cgroup, network identity and writable filesystem layer, and shares the snapshot's
// memory with every other copy until it writes to it.

func (m *Manager) snapshotter() (runtime.Snapshotter, error) {
	s, ok := m.cfg.Runtime.(runtime.Snapshotter)
	if !ok || !s.Snapshots() {
		return nil, fmt.Errorf("%w: the daemon runs without snapshot support (start tideletd with --container-checkpoints)", ErrConflict)
	}
	return s, nil
}

// loadSnapshots reads the persisted snapshots, dropping those whose data is gone
// (snapshots live in memory and do not survive a reboot).
func (m *Manager) loadSnapshots(ctx context.Context) {
	if m.cfg.SnapshotDir == "" {
		return
	}
	s, _ := m.cfg.Runtime.(runtime.Snapshotter)
	files, _ := filepath.Glob(filepath.Join(m.cfg.SnapshotDir, "*.json"))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var snap sandbox.Snapshot
		if err := json.Unmarshal(data, &snap); err != nil || snap.ID == "" {
			m.log.Warn("unreadable snapshot record", "file", f, "err", err)
			continue
		}
		if s == nil || !s.Snapshots() || !s.HasSnapshot(snap.ID) {
			m.log.Warn("snapshot is gone; dropping its record", "id", snap.ID, "name", snap.Name)
			_ = os.Remove(f)
			if s != nil {
				_ = s.DeleteSnapshot(ctx, snap.ID)
			}
			continue
		}
		m.snaps[snap.ID] = &snap
	}
}

func (m *Manager) persistSnapshot(snap *sandbox.Snapshot) error {
	if m.cfg.SnapshotDir == "" {
		return nil
	}
	if err := os.MkdirAll(m.cfg.SnapshotDir, 0o750); err != nil {
		return err
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(m.cfg.SnapshotDir, snap.ID+".json.tmp")
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, strings.TrimSuffix(tmp, ".tmp"))
}

// resolveSnapshotLocked finds a snapshot by exact ID, name, or unique ID prefix. Caller holds m.mu.
func (m *Manager) resolveSnapshotLocked(ref string) (*sandbox.Snapshot, error) {
	if s, ok := m.snaps[ref]; ok {
		return s, nil
	}
	var match *sandbox.Snapshot
	for id, s := range m.snaps {
		if s.Name != "" && s.Name == ref {
			return s, nil
		}
		if ref != "" && strings.HasPrefix(id, ref) {
			if match != nil {
				return nil, fmt.Errorf("%w: %q matches multiple snapshots", ErrAmbiguous, ref)
			}
			match = s
		}
	}
	if match == nil {
		return nil, fmt.Errorf("%w: snapshot %q", ErrNotFound, ref)
	}
	return match, nil
}

// usersLocked counts the sandboxes created from a snapshot. Caller holds m.mu.
func (m *Manager) usersLocked(id string) int {
	n := 0
	for _, e := range m.entries {
		if e.sb.Spec.Snapshot == id {
			n++
		}
	}
	return n
}

func (m *Manager) snapshotViewLocked(s *sandbox.Snapshot) *sandbox.Snapshot {
	out := *s
	out.Sandboxes = m.usersLocked(s.ID)
	return &out
}

// CreateSnapshot snapshots a running sandbox, which keeps running. An ephemeral snapshot
// is deleted with the last sandbox created from it.
func (m *Manager) CreateSnapshot(ctx context.Context, ref, name string, ephemeral bool) (*sandbox.Snapshot, error) {
	rt, err := m.snapshotter()
	if err != nil {
		return nil, err
	}
	if name != "" && !nameRE.MatchString(name) {
		return nil, fmt.Errorf("%w: invalid name %q", ErrInvalid, name)
	}
	e, err := m.resolve(ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if e.sb.Status.Phase != sandbox.PhaseRunning || e.suspended {
		phase := e.sb.Status.Phase
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: sandbox %s is %s, not Running", ErrConflict, e.sb.ID, phase)
	}
	if e.sb.Spec.Runtime == sandbox.RuntimeMicroVM || e.sb.Spec.TTY || e.sb.Spec.Stdin {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: microVM sandboxes and sandboxes with a terminal or an open stdin cannot be snapshotted", ErrInvalid)
	}
	if name != "" {
		for _, s := range m.snaps {
			if s.Name == name {
				m.mu.Unlock()
				return nil, fmt.Errorf("%w: a snapshot named %q already exists", ErrConflict, name)
			}
		}
	}
	snap := &sandbox.Snapshot{ID: newID(), Name: name, Sandbox: e.sb.ID, SandboxName: e.sb.Name, CreatedAt: time.Now(),
		Spec: e.sb.Spec, Ephemeral: ephemeral}
	snap.Spec.Labels = maps.Clone(e.sb.Spec.Labels)
	snap.Spec.Env = maps.Clone(e.sb.Spec.Env)
	// The resources a copy gets by default are what the sandbox was granted.
	if a := e.sb.Status.Allocated; !a.Requests.IsZero() || !a.Limits.IsZero() {
		snap.Spec.Resources = a
	}
	m.mu.Unlock()

	info, err := rt.Snapshot(ctx, snap.Sandbox, snap.ID)
	if err != nil {
		return nil, fmt.Errorf("snapshot sandbox %s: %w", snap.Sandbox, err)
	}
	snap.SharedBytes, snap.PrivateBytes, snap.Rebased, snap.Took = info.SharedBytes, info.PrivateBytes, info.Rebased, info.Took
	if err := m.persistSnapshot(snap); err != nil {
		_ = rt.DeleteSnapshot(context.WithoutCancel(ctx), snap.ID)
		return nil, fmt.Errorf("persist snapshot: %w", err)
	}
	m.mu.Lock()
	m.snaps[snap.ID] = snap
	out := m.snapshotViewLocked(snap)
	m.mu.Unlock()
	m.log.Info("snapshot taken", "id", snap.ID, "name", name, "sandbox", snap.Sandbox, "shared", resource.Bytes(info.SharedBytes).String(),
		"private", resource.Bytes(info.PrivateBytes).String(), "took", info.Took.Round(time.Millisecond))
	return out, nil
}

// GetSnapshot returns a snapshot by ID, name, or ID prefix.
func (m *Manager) GetSnapshot(ref string) (*sandbox.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := m.resolveSnapshotLocked(ref)
	if err != nil {
		return nil, err
	}
	return m.snapshotViewLocked(s), nil
}

// ListSnapshots returns all snapshots, newest first.
func (m *Manager) ListSnapshots() []sandbox.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sandbox.Snapshot, 0, len(m.snaps))
	for _, s := range m.snaps {
		out = append(out, *m.snapshotViewLocked(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

// DeleteSnapshot removes a snapshot. It is refused while sandboxes created from it exist.
func (m *Manager) DeleteSnapshot(ctx context.Context, ref string) error {
	m.mu.Lock()
	s, err := m.resolveSnapshotLocked(ref)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if n := m.usersLocked(s.ID); n > 0 {
		m.mu.Unlock()
		return fmt.Errorf("%w: snapshot %s has %d sandboxes; delete them first", ErrConflict, s.ID, n)
	}
	delete(m.snaps, s.ID)
	m.mu.Unlock()
	return m.deleteSnapshotData(ctx, s)
}

func (m *Manager) deleteSnapshotData(ctx context.Context, s *sandbox.Snapshot) error {
	if m.cfg.SnapshotDir != "" {
		_ = os.Remove(filepath.Join(m.cfg.SnapshotDir, s.ID+".json"))
	}
	if rt, ok := m.cfg.Runtime.(runtime.Snapshotter); ok {
		if err := rt.DeleteSnapshot(ctx, s.ID); err != nil {
			return fmt.Errorf("delete snapshot %s: %w", s.ID, err)
		}
	}
	m.log.Info("snapshot deleted", "id", s.ID, "name", s.Name)
	return nil
}

// releaseSnapshot deletes an ephemeral snapshot once no sandbox created from it is left.
func (m *Manager) releaseSnapshot(id string) {
	if id == "" {
		return
	}
	m.mu.Lock()
	s := m.snaps[id]
	if s == nil || !s.Ephemeral || m.usersLocked(id) > 0 {
		m.mu.Unlock()
		return
	}
	delete(m.snaps, id)
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := m.deleteSnapshotData(ctx, s); err != nil {
		m.log.Warn("delete ephemeral snapshot", "id", id, "err", err)
	}
}

// fromSnapshot turns a request to create a sandbox from a snapshot into the spec the
// sandbox runs with: the snapshotted sandbox's, since its processes are already running,
// with the request's resources, labels, hostname and priority where it gives them.
func (m *Manager) fromSnapshot(req sandbox.Spec) (sandbox.Spec, error) {
	if _, err := m.snapshotter(); err != nil {
		return req, err
	}
	m.mu.Lock()
	snap, err := m.resolveSnapshotLocked(req.Snapshot)
	if err != nil {
		m.mu.Unlock()
		return req, err
	}
	spec := snap.Spec
	spec.Labels = maps.Clone(snap.Spec.Labels)
	spec.Env = maps.Clone(snap.Spec.Env)
	id := snap.ID
	m.mu.Unlock()

	switch {
	case req.Image != "" && req.Image != spec.Image:
		return req, fmt.Errorf("%w: a sandbox created from a snapshot runs the snapshot's image (%s)", ErrInvalid, spec.Image)
	case len(req.Command) > 0 || len(req.Args) > 0 || len(req.Env) > 0 || req.WorkingDir != "" || len(req.Mounts) > 0:
		return req, fmt.Errorf("%w: a sandbox created from a snapshot continues the snapshot's processes; command, args, env, working directory and mounts cannot be set", ErrInvalid)
	case req.Network != "" && req.Network != spec.Network:
		return req, fmt.Errorf("%w: a sandbox created from a snapshot has the snapshot's network mode (%s)", ErrInvalid, spec.Network)
	case req.Stdin || req.TTY:
		return req, fmt.Errorf("%w: a sandbox created from a snapshot has no stdin or terminal of its own", ErrInvalid)
	}
	spec.Snapshot = id
	spec.Stdin, spec.TTY = false, false
	spec.Hostname = req.Hostname
	if !req.Resources.Requests.IsZero() || !req.Resources.Limits.IsZero() {
		spec.Resources = req.Resources
	}
	if req.Priority != 0 {
		spec.Priority = req.Priority
	}
	if len(req.Labels) > 0 {
		if spec.Labels == nil {
			spec.Labels = map[string]string{}
		}
		maps.Copy(spec.Labels, req.Labels)
	}
	return spec, nil
}

// Fork makes running copies of a running sandbox: one snapshot, deleted with the last
// copy, and a sandbox created from it and started for each name (an empty name is an
// unnamed copy). The copies that the policy queued start when it dispatches them. If a
// copy cannot be created the ones already made are removed.
func (m *Manager) Fork(ctx context.Context, ref string, names []string) ([]*sandbox.Sandbox, error) {
	if len(names) == 0 {
		names = []string{""}
	}
	seen := map[string]bool{}
	for _, n := range names {
		if n != "" && seen[n] {
			return nil, fmt.Errorf("%w: duplicate name %q", ErrInvalid, n)
		}
		seen[n] = true
	}
	snap, serr := m.CreateSnapshot(ctx, ref, "", true)
	if serr != nil {
		return nil, serr
	}
	// The copies are created side by side: each is a restore of its own.
	out := make([]*sandbox.Sandbox, len(names))
	errs := make([]error, len(names))
	sem := make(chan struct{}, forkParallel)
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sb, err := m.Create(ctx, name, sandbox.Spec{Snapshot: snap.ID})
			if err == nil {
				out[i] = sb
				_, err = m.StartSandbox(ctx, sb.ID)
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	failed := 0
	var err error
	for _, e := range errs {
		if e != nil {
			failed++
			if err == nil {
				err = e
			}
		}
	}
	if err != nil {
		if failed > 1 {
			err = fmt.Errorf("%w (and %d more copies failed)", err, failed-1)
		}
		cctx := context.WithoutCancel(ctx)
		for _, sb := range out {
			if sb == nil {
				continue
			}
			if derr := m.Delete(cctx, sb.ID, true); derr != nil && !errors.Is(derr, ErrNotFound) {
				m.log.Warn("remove copy of a failed fork", "id", sb.ID, "err", derr)
			}
		}
		m.releaseSnapshot(snap.ID)
		return nil, err
	}
	for i, sb := range out {
		if cur, err := m.Get(sb.ID); err == nil {
			out[i] = cur
		}
	}
	return out, nil
}

// forkParallel is how many copies of one fork are created at a time.
const forkParallel = 16
