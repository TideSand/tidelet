package manager

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/TideSand/tidelet/pkg/manager/store"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// snapRuntime is a fakeRuntime that can take snapshots.
type snapRuntime struct {
	*fakeRuntime
	smu     sync.Mutex
	snaps   map[string]string // snapshot -> sandbox
	created map[string]string // sandbox -> the snapshot it was created from
	deleted []string
}

func newSnapRuntime() *snapRuntime {
	return &snapRuntime{fakeRuntime: newFakeRuntime(), snaps: map[string]string{}, created: map[string]string{}}
}

func (s *snapRuntime) Snapshots() bool { return true }

func (s *snapRuntime) Snapshot(_ context.Context, id, snapID string) (runtime.SnapshotInfo, error) {
	s.smu.Lock()
	defer s.smu.Unlock()
	s.snaps[snapID] = id
	return runtime.SnapshotInfo{SharedBytes: 100 << 20, PrivateBytes: 1 << 20, Rebased: true}, nil
}

func (s *snapRuntime) HasSnapshot(id string) bool {
	s.smu.Lock()
	defer s.smu.Unlock()
	_, ok := s.snaps[id]
	return ok
}

func (s *snapRuntime) DeleteSnapshot(_ context.Context, id string) error {
	s.smu.Lock()
	defer s.smu.Unlock()
	delete(s.snaps, id)
	s.deleted = append(s.deleted, id)
	return nil
}

func (s *snapRuntime) Create(ctx context.Context, sb *sandbox.Sandbox, cg runtime.Cgroup) error {
	if sb.Spec.Snapshot != "" {
		s.smu.Lock()
		_, ok := s.snaps[sb.Spec.Snapshot]
		s.created[sb.ID] = sb.Spec.Snapshot
		s.smu.Unlock()
		if !ok {
			return errors.New("no such snapshot")
		}
	}
	return s.fakeRuntime.Create(ctx, sb, cg)
}

func TestSnapshotAndCreateFromIt(t *testing.T) {
	rt := newSnapRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	ctx := context.Background()
	src := run(t, m, "src", 1000, 1<<30)

	if _, err := m.CreateSnapshot(ctx, "nope", "", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot of unknown sandbox: %v", err)
	}
	snap, err := m.CreateSnapshot(ctx, "src", "warm", false)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Sandbox != src.ID || snap.SandboxName != "src" || snap.SharedBytes != 100<<20 || snap.Spec.Image != src.Spec.Image {
		t.Fatalf("snapshot = %+v", snap)
	}
	if _, err := m.CreateSnapshot(ctx, "src", "warm", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate snapshot name: %v", err)
	}

	// A copy inherits the snapshot's spec and the resources its sandbox was granted.
	c, err := m.Create(ctx, "copy", sandbox.Spec{Snapshot: "warm"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec.Snapshot != snap.ID || c.Spec.Image != src.Spec.Image || c.Status.Allocated.Limits.CPU != 1000 || c.Status.Allocated.Limits.Memory != 1<<30 {
		t.Fatalf("copy = %+v", c)
	}
	if rt.created[c.ID] != snap.ID {
		t.Fatalf("runtime created the copy from %q", rt.created[c.ID])
	}
	// Its own resources and labels, where the request gives them.
	req := sandbox.Spec{Snapshot: snap.ID[:8], Labels: map[string]string{"role": "worker"}}
	req.Resources.Requests = resource.Set{CPU: 500, Memory: 256 << 20}
	req.Resources.Limits = req.Resources.Requests
	c2, err := m.Create(ctx, "", req)
	if err != nil {
		t.Fatal(err)
	}
	if c2.Status.Allocated.Limits.CPU != 500 || c2.Spec.Labels["role"] != "worker" {
		t.Fatalf("copy with its own resources = %+v", c2)
	}
	// What a copy cannot change.
	for _, bad := range []sandbox.Spec{
		{Snapshot: "warm", Image: "other:latest"},
		{Snapshot: "warm", Command: []string{"sh"}},
		{Snapshot: "warm", Env: map[string]string{"A": "b"}},
		{Snapshot: "warm", Network: sandbox.NetworkHost},
		{Snapshot: "warm", Stdin: true},
	} {
		if _, err := m.Create(ctx, "", bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("create %+v: %v, want invalid", bad, err)
		}
	}
	if _, err := m.Create(ctx, "", sandbox.Spec{Snapshot: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("create from unknown snapshot: %v", err)
	}

	if got, _ := m.GetSnapshot("warm"); got == nil || got.Sandboxes != 2 {
		t.Fatalf("snapshot users = %+v", got)
	}
	if err := m.DeleteSnapshot(ctx, "warm"); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete snapshot in use: %v", err)
	}
	for _, id := range []string{c.ID, c2.ID} {
		if err := m.Delete(ctx, id, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.ListSnapshots()) != 1 {
		t.Fatal("a named snapshot must outlive its sandboxes")
	}
	if err := m.DeleteSnapshot(ctx, "warm"); err != nil {
		t.Fatal(err)
	}
	if rt.HasSnapshot(snap.ID) || len(m.ListSnapshots()) != 0 {
		t.Fatal("snapshot not deleted")
	}
}

func TestForkIsEphemeral(t *testing.T) {
	rt := newSnapRuntime()
	m := newTestManager(t, rt, store.NewMemory(), nil)
	ctx := context.Background()
	run(t, m, "src", 500, 1<<28)

	copies, err := m.Fork(ctx, "src", []string{"a", "", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if len(copies) != 3 || copies[0].Name != "a" || copies[2].Name != "c" {
		t.Fatalf("copies = %+v", copies)
	}
	snaps := m.ListSnapshots()
	if len(snaps) != 1 || !snaps[0].Ephemeral || snaps[0].Sandboxes != 3 {
		t.Fatalf("snapshots = %+v", snaps)
	}
	for _, c := range copies {
		if got := waitPhase(t, m, c.ID, sandbox.PhaseRunning); got.Spec.Snapshot != snaps[0].ID {
			t.Fatalf("copy %s from %q", c.ID, got.Spec.Snapshot)
		}
	}
	for i, c := range copies {
		if err := m.Delete(ctx, c.ID, true); err != nil {
			t.Fatal(err)
		}
		if left := len(m.ListSnapshots()); (i < 2) != (left == 1) {
			t.Fatalf("after deleting %d copies: %d snapshots", i+1, left)
		}
	}
	if rt.HasSnapshot(snaps[0].ID) {
		t.Fatal("ephemeral snapshot not deleted with its last copy")
	}

	// A fork that cannot make all its copies leaves nothing behind.
	if _, err := m.Fork(ctx, "src", []string{"x", "src"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("fork with a taken name: %v", err)
	}
	if len(m.ListSnapshots()) != 0 || len(m.List()) != 1 {
		t.Fatalf("failed fork left %d snapshots, %d sandboxes", len(m.ListSnapshots()), len(m.List()))
	}
	if _, err := m.Fork(ctx, "src", []string{"d", "d"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("fork with duplicate names: %v", err)
	}
}

func TestSnapshotNeedsSupportAndARunningSandbox(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, newFakeRuntime(), store.NewMemory(), nil)
	run(t, m, "src", 500, 1<<28)
	if _, err := m.CreateSnapshot(ctx, "src", "", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("snapshot without runtime support: %v", err)
	}

	m = newTestManager(t, newSnapRuntime(), store.NewMemory(), nil)
	create(t, m, "idle", 500, 1<<28)
	if _, err := m.CreateSnapshot(ctx, "idle", "", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("snapshot of a sandbox that is not running: %v", err)
	}
}
