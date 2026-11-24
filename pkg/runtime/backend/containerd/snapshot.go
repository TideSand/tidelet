package containerd

// Snapshots and the sandboxes created from them.
//
// A snapshot is a running sandbox at one instant: a CRIU dump of its processes, a
// committed copy of its writable filesystem layer, and its memory. The sandbox is paused
// for the dump and then runs on, with its connections and its execs untouched.
//
// What makes a sandbox created from a snapshot cheap is where the memory goes. The dump is
// rewritten (package criuimg) so that the processes' private memory lives in sparse files
// on a tmpfs instead of in the image, and every memory area becomes a private mapping of
// its file. Creating a sandbox from the snapshot is then a CRIU restore that maps those
// files: no memory is copied, pages the sandbox only reads are the page-cache pages every
// other sandbox of the snapshot reads, and the kernel copies a page when a sandbox first
// writes it. A sandbox of a 1 GiB snapshot starts with about a megabyte of its own.
//
// The filesystem is shared the same way: the snapshot's layer is a committed containerd
// snapshot, and each sandbox created from it gets a fresh writable layer on top.
//
// Sandboxes of one lineage (a sandbox, the sandboxes created from its snapshots, and
// theirs) share one directory of memory files, their family's, mounted read-only at
// /.tidelet/mem in each. A snapshot of a sandbox that itself came from a snapshot holds
// only the pages that sandbox has written since; once those outgrow rebaseRatio of the
// shared pages, the memory files are written anew for it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/TideSand/tidelet/pkg/log/crilog"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/criuimg"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

const (
	memMount = criuimg.MemMount
	// labelFamily names the memory-file directory a sandbox mounts; labelSnapshot the
	// snapshot it was created from.
	labelFamily   = "io.tidelet.family"
	labelSnapshot = "io.tidelet.snapshot"
	layerPrefix   = "tidelet-snapshot-"
	// rebaseRatio: a snapshot whose own pages exceed this share of the pages in the
	// memory files it maps gets memory files of its own.
	rebaseRatio = 0.25
	gcRootLabel = "containerd.io/gc.root"
)

// ctlEntry is a file of a sandbox's exec control directory that a running exec holds open.
type ctlEntry struct {
	Name string `json:"name"`
	FIFO bool   `json:"fifo,omitempty"`
}

// snapshotMeta is what the runtime keeps about a snapshot, next to its CRIU image.
type snapshotMeta struct {
	ID      string `json:"id"`
	Sandbox string `json:"sandbox"`
	Family  string `json:"family"`
	// Layer is the committed containerd snapshot holding the sandbox's filesystem.
	Layer string `json:"layer"`
	Image string `json:"image"`
	// Spec is the snapshotted sandbox's OCI spec: a sandbox created from the snapshot runs
	// with it, apart from what belongs to the sandbox itself (cgroup, network, control dir).
	Spec     *specs.Spec          `json:"spec"`
	MemFiles []string             `json:"memFiles"`
	Ctl      []ctlEntry           `json:"ctl,omitempty"`
	Info     runtime.SnapshotInfo `json:"info"`
}

var _ runtime.Snapshotter = (*Runtime)(nil)

// Snapshots implements runtime.Snapshotter.
func (r *Runtime) Snapshots() bool { return r.cfg.InitBinary != "" && r.cfg.SnapshotDir != "" }

func (r *Runtime) snapshotImageDir(id string) string {
	return filepath.Join(r.cfg.SnapshotDir, "images", id)
}

func (r *Runtime) memDir(family string) string {
	return filepath.Join(r.cfg.SnapshotDir, "mem", family)
}

func (r *Runtime) snapshotterName() string {
	if r.cfg.Snapshotter != "" {
		return r.cfg.Snapshotter
	}
	return defaults.DefaultSnapshotter
}

func gcRoot() snapshots.Opt {
	return snapshots.WithLabels(map[string]string{gcRootLabel: time.Now().UTC().Format(time.RFC3339)})
}

// ensureMemDir creates a family's memory-file directory with the file sandboxes bind
// over /proc/sys/vm/overcommit_memory (see tidelet-init).
func (r *Runtime) ensureMemDir(family string) (string, error) {
	dir := r.memDir(family)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	oc := filepath.Join(dir, ".overcommit")
	if _, err := os.Stat(oc); err != nil {
		if err := os.WriteFile(oc, []byte("2\n"), 0o444); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func memMountSpec(dir string) specs.Mount {
	return specs.Mount{Type: "bind", Source: dir, Destination: memMount, Options: []string{"rbind", "ro"}}
}

// bindOvercommit binds the family's overcommit file over /proc/sys/vm/overcommit_memory in
// the mount namespace of a sandbox that is created but not started. runc refuses such a
// mount in the spec, and CRIU carries it into every sandbox restored from a snapshot.
func (r *Runtime) bindOvercommit(ctx context.Context, pid uint32) error {
	out, err := exec.CommandContext(ctx, "nsenter", "--mount=/proc/"+strconv.Itoa(int(pid))+"/ns/mnt", initMount, "--bind-overcommit").CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (r *Runtime) loadSnapshotMeta(id string) (*snapshotMeta, error) {
	data, err := os.ReadFile(filepath.Join(r.snapshotImageDir(id), "snapshot.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("snapshot %s: %w", id, runtime.ErrNotFound)
		}
		return nil, err
	}
	var m snapshotMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", id, err)
	}
	return &m, nil
}

// HasSnapshot implements runtime.Snapshotter.
func (r *Runtime) HasSnapshot(id string) bool {
	_, err := os.Stat(filepath.Join(r.snapshotImageDir(id), "snapshot.json"))
	return err == nil
}

// Snapshot implements runtime.Snapshotter.
func (r *Runtime) Snapshot(ctx context.Context, id, snapID string) (info runtime.SnapshotInfo, retErr error) {
	start := time.Now()
	if !r.Snapshots() {
		return info, errors.New("the daemon runs without snapshot support")
	}
	t, err := r.load(ctx, id)
	if err != nil {
		return info, err
	}
	ctr, err := r.client.LoadContainer(ctx, id)
	if err != nil {
		return info, fmt.Errorf("load container: %w", err)
	}
	cinfo, err := ctr.Info(ctx)
	if err != nil {
		return info, fmt.Errorf("container info: %w", err)
	}
	family := cinfo.Labels[labelFamily]
	if family == "" {
		return info, fmt.Errorf("sandbox %s was created without snapshot support", id)
	}
	spec, err := ctr.Spec(ctx)
	if err != nil {
		return info, fmt.Errorf("container spec: %w", err)
	}
	upper, _, err := r.overlay(ctx, id)
	if err != nil {
		return info, err
	}
	sn := r.client.SnapshotService(r.snapshotterName())
	active, err := sn.Stat(ctx, cinfo.SnapshotKey)
	if err != nil {
		return info, fmt.Errorf("stat rootfs snapshot: %w", err)
	}
	imgDir := r.snapshotImageDir(snapID)
	if err := os.MkdirAll(imgDir, 0o700); err != nil {
		return info, err
	}
	meta := &snapshotMeta{ID: snapID, Sandbox: id, Family: family, Layer: layerPrefix + snapID, Image: cinfo.Image, Spec: spec}
	tmpKey := meta.Layer + "-tmp"
	defer func() {
		if retErr != nil {
			cctx := context.WithoutCancel(ctx)
			_ = os.RemoveAll(imgDir)
			_ = sn.Remove(cctx, tmpKey)
			_ = sn.Remove(cctx, meta.Layer)
		}
	}()
	mounts, err := sn.Prepare(ctx, tmpKey, active.Parent, gcRoot())
	if err != nil {
		return info, fmt.Errorf("prepare layer: %w", err)
	}
	layerDir := ""
	for _, m := range mounts {
		for _, o := range m.Options {
			if v, ok := strings.CutPrefix(o, "upperdir="); ok {
				layerDir = v
			}
		}
	}
	if layerDir == "" {
		return info, fmt.Errorf("snapshotter %s gave the layer no writable directory", r.snapshotterName())
	}

	// Paused, the filesystem and the processes are copied at the same instant.
	var perr error
	for try := 0; try < 20; try++ {
		if perr = t.task.Pause(ctx); perr == nil || !strings.Contains(perr.Error(), "paused") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if perr != nil {
		r.thawAfterFailedPause(context.WithoutCancel(ctx), id, t)
		return info, fmt.Errorf("pause: %w", perr)
	}
	_, err = copyRsync(ctx, upper, layerDir, nil)
	if err == nil {
		meta.Ctl = ctlEntries(r.ctlDir(id))
		err = saveStdin(r.ctlDir(id), filepath.Join(imgDir, stdinDir), meta.Ctl)
	}
	if err == nil {
		if _, err = t.task.Checkpoint(ctx, client.WithCheckpointImagePath(imgDir), withOpenTCP); err != nil {
			err = fmt.Errorf("criu dump: %w", err)
		}
	}
	if rerr := r.thaw(context.WithoutCancel(ctx), t); rerr != nil && !r.thawIfFrozen(id) {
		r.log.Warn("resume after snapshot", "id", id, "err", rerr)
		if err == nil {
			err = fmt.Errorf("resume: %w", rerr)
		}
	}
	if err != nil {
		return info, err
	}
	if err := sn.Commit(ctx, meta.Layer, tmpKey, gcRoot()); err != nil {
		return info, fmt.Errorf("commit layer: %w", err)
	}

	// The memory moves from the image into the family's memory files.
	r.snapMu.Lock()
	defer r.snapMu.Unlock()
	memDir, err := r.ensureMemDir(family)
	if err != nil {
		return info, err
	}
	set, err := criuimg.Open(imgDir, memDir)
	if err != nil {
		return info, err
	}
	survey, err := set.Survey()
	if err != nil {
		return info, err
	}
	if len(survey.MemFiles) == 0 || float64(survey.EagerPages) > rebaseRatio*float64(survey.SharedPages) {
		if survey, err = set.Linearize(snapID); err != nil {
			return info, fmt.Errorf("move memory into files: %w", err)
		}
		info.Rebased = true
	}
	meta.MemFiles = survey.MemFiles
	info.SharedBytes = set.MemBytes(survey.MemFiles)
	info.PrivateBytes = set.PagesBytes()
	info.Took = time.Since(start)
	meta.Info = info
	data, err := json.Marshal(meta)
	if err != nil {
		return info, err
	}
	if err := os.WriteFile(filepath.Join(imgDir, "snapshot.json"), data, 0o600); err != nil {
		return info, err
	}
	return info, nil
}

// stdinDir is where a snapshot keeps the stdin its sandbox's execs had not finished reading.
const stdinDir = "stdin"

// saveStdin copies the stdin parts among entries from a control directory to dir.
func saveStdin(from, dir string, entries []ctlEntry) error {
	for _, e := range entries {
		if e.FIFO {
			continue
		}
		data, err := os.ReadFile(filepath.Join(from, e.Name))
		if err != nil {
			continue // its exec ended meanwhile
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// ctlEntries lists what the execs running in a sandbox have in its control directory: their
// stdin parts and their output pipes.
func ctlEntries(dir string) []ctlEntry {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []ctlEntry
	for _, e := range entries {
		switch ext := filepath.Ext(e.Name()); {
		case ext == ".out" || ext == ".err" || ext == ".done":
			out = append(out, ctlEntry{Name: e.Name(), FIFO: true})
		case strings.Contains(e.Name(), stdinExt+"."):
			out = append(out, ctlEntry{Name: e.Name()})
		}
	}
	return out
}

// createFromSnapshot creates a sandbox as a copy of a snapshot: a container on a fresh
// writable layer over the snapshot's, and a task that restores the snapshot's processes
// when it is started.
func (r *Runtime) createFromSnapshot(ctx context.Context, sb *sandbox.Sandbox, cg runtime.Cgroup) (retErr error) {
	if !r.Snapshots() {
		return errors.New("the daemon runs without snapshot support")
	}
	meta, err := r.loadSnapshotMeta(sb.Spec.Snapshot)
	if err != nil {
		return err
	}
	id := sb.ID
	t0 := time.Now()
	var tNet, tCtr time.Duration
	spec, err := cloneSpec(meta.Spec)
	if err != nil {
		return err
	}
	labels := map[string]string{labelManaged: "true", labelName: sb.Name, labelRuntime: string(sandbox.RuntimeContainer),
		labelFamily: meta.Family, labelSnapshot: meta.ID}
	for k, v := range sb.Spec.Labels {
		labels[k] = v
	}

	// What belongs to the new sandbox: its control directory, cgroup, hostname and network.
	ctl := r.ctlDir(id)
	if err := prepareCtl(filepath.Join(r.snapshotImageDir(meta.ID), stdinDir), ctl, meta.Ctl); err != nil {
		return fmt.Errorf("control directory: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(ctl)
		}
	}()
	replace := map[string]string{ctlMount: ctl}
	var netStatus *sandbox.NetworkStatus
	if sb.Spec.Network == sandbox.NetworkBridge {
		if r.cfg.Network == nil {
			return errors.New("bridge networking is not configured")
		}
		att, err := r.cfg.Network.Setup(ctx, id, sb.Hostname())
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = r.cfg.Network.Teardown(cctx, id, att.NamespacePath)
			}
		}()
		netStatus = &sandbox.NetworkStatus{NamespacePath: att.NamespacePath, IPs: att.IPs}
		labels[labelNetNS] = att.NamespacePath
		labels[labelIPs] = strings.Join(att.IPs, ",")
		replace["/etc/hosts"], replace["/etc/resolv.conf"] = att.HostsFile, att.ResolvConf
		for i, ns := range spec.Linux.Namespaces {
			if ns.Type == specs.NetworkNamespace {
				spec.Linux.Namespaces[i].Path = att.NamespacePath
			}
		}
	}
	tNet = time.Since(t0)
	for i, m := range spec.Mounts {
		if src, ok := replace[m.Destination]; ok {
			spec.Mounts[i].Source = src
		}
	}
	spec.Hostname = sb.Hostname()
	if spec.Linux.CgroupsPath != "" {
		spec.Linux.CgroupsPath = path.Join(path.Dir(spec.Linux.CgroupsPath), id)
	}
	spec.Linux.Resources = linuxResources(cg)

	sn := r.client.SnapshotService(r.snapshotterName())
	if _, err := sn.Prepare(ctx, id, meta.Layer, gcRoot()); err != nil {
		return fmt.Errorf("prepare rootfs: %w", err)
	}
	ctrOpts := []client.NewContainerOpts{
		client.WithImageName(meta.Image),
		client.WithContainerLabels(labels),
		client.WithSnapshotter(r.snapshotterName()),
		client.WithSnapshot(id),
		client.WithSpec(spec),
	}
	if r.cfg.RuntimeHandler != "" {
		ctrOpts = append(ctrOpts, client.WithRuntime(r.cfg.RuntimeHandler, nil))
	}
	ctr, err := r.client.NewContainer(ctx, id, ctrOpts...)
	if err != nil {
		_ = sn.Remove(context.WithoutCancel(ctx), id)
		return fmt.Errorf("create container: %w", err)
	}
	defer func() {
		if retErr != nil {
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := ctr.Delete(cctx, client.WithSnapshotCleanup); err != nil {
				r.log.Warn("cleanup after failed create", "id", id, "err", err)
			}
		}
	}()
	tCtr = time.Since(t0) - tNet
	hub, err := newIOHub(id, r.LogPath(id), false, false)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	stdin, stdout, stderr := hub.streams()
	work := r.restoreWorkDir(id)
	if err := os.MkdirAll(work, 0o700); err != nil {
		hub.close()
		return err
	}
	task, err := ctr.NewTask(ctx, cio.NewCreator(cio.WithStreams(stdin, stdout, stderr), cio.WithFIFODir(r.cfg.FIFODir)),
		client.WithRestoreImagePath(r.snapshotImageDir(meta.ID)), client.WithRestoreWorkPath(work))
	if err != nil {
		hub.close()
		r.keepRestoreLog(id, work)
		_ = os.RemoveAll(work)
		return fmt.Errorf("create task: %w", err)
	}
	r.mu.Lock()
	r.tasks[id] = &taskIO{task: task, hub: hub}
	if r.lastCg == nil {
		r.lastCg = map[string]runtime.Cgroup{}
	}
	r.lastCg[id] = cg
	r.restoring[id] = meta
	r.mu.Unlock()
	sb.Status.Network = netStatus
	r.log.Debug("sandbox created from snapshot", "id", id, "snapshot", meta.ID, "network", tNet.Round(time.Millisecond),
		"container", tCtr.Round(time.Millisecond), "task", (time.Since(t0) - tNet - tCtr).Round(time.Millisecond))
	return nil
}

func (r *Runtime) restoreWorkDir(id string) string {
	return filepath.Join(r.cfg.SnapshotDir, "work", id)
}

func cloneSpec(s *specs.Spec) (*oci.Spec, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var out oci.Spec
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if out.Linux == nil {
		return nil, errors.New("snapshot has no Linux spec")
	}
	return &out, nil
}

// prepareCtl makes a new sandbox's control directory look like the snapshotted one's did
// to the execs that were running: CRIU reopens their output pipes by path, and init goes on
// reading their stdin parts (from holds the snapshot's). Their stdin ends with those parts:
// what the snapshotted sandbox's client sends later is not for the copy.
func prepareCtl(from, dir string, entries []ctlEntry) error {
	if err := os.MkdirAll(filepath.Join(dir, "req"), 0o777); err != nil {
		return err
	}
	_ = os.Chmod(dir, 0o777)
	_ = os.Chmod(filepath.Join(dir, "req"), 0o777)
	if err := makeWakePipe(dir); err != nil {
		return err
	}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name)
		if e.FIFO {
			if err := syscall.Mkfifo(p, 0o666); err != nil && !os.IsExist(err) {
				return err
			}
			_ = os.Chmod(p, 0o666)
			continue
		}
		data, _ := os.ReadFile(filepath.Join(from, e.Name))
		if err := os.WriteFile(p, data, 0o644); err != nil {
			return err
		}
		eid, _, _ := strings.Cut(e.Name, stdinExt+".")
		if err := os.WriteFile(filepath.Join(dir, eid+stdinExt+".eof"), nil, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// started finishes a sandbox created from a snapshot once its processes run: the restore
// brought the snapshotted sandbox's hostname back, and the execs it inherited write to
// pipes nobody reads yet.
func (r *Runtime) started(ctx context.Context, id string, t *taskIO, meta *snapshotMeta) {
	_ = os.RemoveAll(r.restoreWorkDir(id))
	if spec, err := t.task.Spec(ctx); err == nil && spec.Hostname != "" {
		if out, err := exec.CommandContext(ctx, "nsenter", "--uts=/proc/"+strconv.Itoa(int(t.task.Pid()))+"/ns/uts", "hostname", spec.Hostname).CombinedOutput(); err != nil {
			r.log.Debug("set hostname of restored sandbox", "id", id, "err", err, "out", strings.TrimSpace(string(out)))
		}
	}
	dir := r.ctlDir(id)
	_ = os.WriteFile(filepath.Join(dir, "id"), []byte(id+"\n"), 0o644)
	poke(filepath.Join(dir, "wake")) // inherited execs' stdin now has its end
	stdout, stderr := &hubWriter{h: t.hub, stream: crilog.Stdout}, &hubWriter{h: t.hub, stream: crilog.Stderr}
	execs := map[string]*inherited{}
	for _, e := range meta.Ctl {
		ext := filepath.Ext(e.Name)
		if !e.FIFO || (ext != ".out" && ext != ".err") {
			continue
		}
		f, err := os.OpenFile(filepath.Join(dir, e.Name), os.O_RDWR, 0)
		if err != nil {
			continue
		}
		w := stdout
		if ext == ".err" {
			w = stderr
		}
		eid := strings.TrimSuffix(e.Name, ext)
		x := execs[eid]
		if x == nil {
			x = &inherited{}
			execs[eid] = x
		}
		x.files = append(x.files, f)
		x.copies.Add(1)
		go func() { defer x.copies.Done(); copyPipe(f, w) }()
	}
	for eid, x := range execs {
		go x.wait(dir, eid)
	}
}

// inherited is an exec a sandbox took over from the snapshotted one: its output pipes,
// copied to the sandbox's log.
type inherited struct {
	files  []*os.File
	copies sync.WaitGroup
}

// wait waits for the exec to end (init pokes its done pipe, as does deleting the sandbox),
// then ends the copies of its output.
func (x *inherited) wait(dir, eid string) {
	if done, err := openPipe(filepath.Join(dir, eid+".done")); err == nil {
		ended := poked(done)
		// An exec that ended before the pipe was open left its status and poked nobody.
		if _, err := os.Stat(filepath.Join(dir, eid+".status")); err != nil {
			<-ended
		}
		done.Close()
	}
	for _, f := range x.files {
		endPipe(f)
	}
	x.copies.Wait()
	for _, f := range x.files {
		f.Close()
	}
	for _, ext := range []string{".out", ".err", ".done", ".status"} {
		_ = os.Remove(filepath.Join(dir, eid+ext))
	}
	removeStdin(filepath.Join(dir, eid+stdinExt))
}

// DeleteSnapshot implements runtime.Snapshotter.
func (r *Runtime) DeleteSnapshot(ctx context.Context, id string) error {
	meta, err := r.loadSnapshotMeta(id)
	if err != nil {
		if errors.Is(err, runtime.ErrNotFound) {
			return nil
		}
		return err
	}
	using, err := r.client.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelSnapshot, id))
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	if len(using) > 0 {
		return fmt.Errorf("snapshot %s still has %d sandboxes", id, len(using))
	}
	if err := os.RemoveAll(r.snapshotImageDir(id)); err != nil {
		return err
	}
	r.removeLayers(ctx, meta.Layer)
	r.collectFamily(ctx, meta.Family)
	return nil
}

// removeLayers removes a snapshot's filesystem layer, and with it every layer of deleted
// snapshots that only it kept alive: a layer stays while a snapshot taken of one of its
// sandboxes builds on it.
func (r *Runtime) removeLayers(ctx context.Context, layer string) {
	sn := r.client.SnapshotService(r.snapshotterName())
	for layer != "" && strings.HasPrefix(layer, layerPrefix) {
		if r.HasSnapshot(strings.TrimPrefix(layer, layerPrefix)) {
			return
		}
		info, err := sn.Stat(ctx, layer)
		if err != nil {
			return
		}
		if err := sn.Remove(ctx, layer); err != nil {
			if !errdefs.IsFailedPrecondition(err) {
				r.log.Warn("remove snapshot layer", "layer", layer, "err", err)
			}
			return
		}
		layer = info.Parent
	}
}

// collectFamily removes the memory files of a family that no snapshot maps any more, and
// the family's directory once no snapshot and no sandbox is left of it.
func (r *Runtime) collectFamily(ctx context.Context, family string) {
	if family == "" || !r.Snapshots() {
		return
	}
	r.snapMu.Lock()
	defer r.snapMu.Unlock()
	dir := r.memDir(family)
	files, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	used := map[string]bool{".overcommit": true}
	snaps := 0
	metas, _ := os.ReadDir(filepath.Join(r.cfg.SnapshotDir, "images"))
	for _, e := range metas {
		if m, err := r.loadSnapshotMeta(e.Name()); err == nil && m.Family == family {
			snaps++
			for _, f := range m.MemFiles {
				used[f] = true
			}
		}
	}
	for _, f := range files {
		if !used[f.Name()] {
			_ = os.Remove(filepath.Join(dir, f.Name()))
		}
	}
	if snaps > 0 {
		return
	}
	if ctrs, err := r.client.Containers(ctx, fmt.Sprintf("labels.%q==%s", labelFamily, family)); err == nil && len(ctrs) == 0 {
		_ = os.RemoveAll(dir)
	}
}
