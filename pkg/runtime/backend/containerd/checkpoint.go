package containerd

// Checkpoints and rollbacks of a container sandbox's filesystem, for policies
// that undo a tool call and run it again later.
//
// A sandbox's rootfs is an overlay: the image's layers below, one writable
// "upper" directory above, where everything the sandbox has written lives
// (changed files as copies, deleted ones as whiteout device nodes, replaced
// directories marked opaque). A checkpoint is a copy of that upper directory.
// When the checkpoint directory is on the same reflink-capable filesystem as
// the upper directories (XFS with reflink, btrfs), the copy clones extents
// (cp --reflink=always) and costs the entries alone, whatever the files'
// sizes. Otherwise it is taken by rsync, hard-linked against the sandbox's
// previous checkpoint, and costs the entries plus every byte changed since
// the last checkpoint, never the image. It is taken while the agent waits on
// the model, so the upper directory is quiet.
//
// A rollback first kills every process the sandbox started after the
// checkpoint (the tool call in flight; the agent itself and whatever ran
// before are older and stay). If nothing was running the call had finished
// and the sandbox is left as it is. Otherwise the checkpoint is mounted as a
// read-only overlay over the same image layers, the view the sandbox had at
// the checkpoint, and every path whose upper entry differs from the
// checkpoint's is put back from that view through the live rootfs mount:
// removed, then copied when the view has it. Acting through the mount keeps
// the overlay consistent; the kernel owns the upper directory.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/defaults"

	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/backend/swapin"
)

// DefaultCheckpointDir is where checkpoints go when Config.CheckpointDir is empty.
const DefaultCheckpointDir = "/var/lib/tidelet/checkpoints"

// defaultRollbackExclude are the rootfs paths a rollback leaves alone: the
// kernel's pseudo filesystems, the runtime's mounts, and the agent's own
// state (its session transcript and shell snapshots), which the agent keeps
// open and which describe the conversation, not the task's files.
var defaultRollbackExclude = []string{"proc", "sys", "dev", "run", "root/.claude", "tmp/claude*", "logs"}

// defaultKeepProcesses are process names a rollback never kills: the agent.
var defaultKeepProcesses = []string{"claude", "tidebox-exec"}

// checkpoint is one sandbox's latest checkpoint.
type checkpoint struct {
	dir   string // the copy of the upper directory
	seq   int
	ticks uint64 // /proc/uptime in clock ticks when it was taken; processes started later are the tool call's
	at    time.Time
}

func (r *Runtime) checkpointDir() string {
	if r.cfg.CheckpointDir != "" {
		return r.cfg.CheckpointDir
	}
	return DefaultCheckpointDir
}

func (r *Runtime) rollbackExclude() []string {
	if r.cfg.RollbackExclude != nil {
		return r.cfg.RollbackExclude
	}
	return defaultRollbackExclude
}

func (r *Runtime) keepProcesses() []string {
	if r.cfg.KeepProcesses != nil {
		return r.cfg.KeepProcesses
	}
	return defaultKeepProcesses
}

// overlay returns the upper and lower directories of the sandbox's rootfs.
func (r *Runtime) overlay(ctx context.Context, id string) (upper, lower string, err error) {
	name := r.cfg.Snapshotter
	if name == "" {
		name = defaults.DefaultSnapshotter
	}
	mounts, err := r.client.SnapshotService(name).Mounts(ctx, id)
	if err != nil {
		return "", "", fmt.Errorf("snapshot mounts: %w", err)
	}
	for _, m := range mounts {
		if m.Type != "overlay" {
			continue
		}
		for _, o := range m.Options {
			if v, ok := strings.CutPrefix(o, "upperdir="); ok {
				upper = v
			}
			if v, ok := strings.CutPrefix(o, "lowerdir="); ok {
				lower = v
			}
		}
	}
	if upper == "" || lower == "" {
		return "", "", fmt.Errorf("sandbox %s: rootfs is not an overlay with an upper directory (%d mounts)", id, len(mounts))
	}
	return upper, lower, nil
}

// rootfs is where the shim mounted the sandbox's root in the host's mount
// namespace: the bundle of containerd's v2 task runtime.
func (r *Runtime) rootfs(id string) string {
	return filepath.Join("/run/containerd/io.containerd.runtime.v2.task", r.cfg.Namespace, id, "rootfs")
}

// uptimeTicks is the host's uptime in clock ticks, the unit of a process's
// start time in /proc/<pid>/stat.
func uptimeTicks() (uint64, error) {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0, err
	}
	f := strings.Fields(string(data))
	if len(f) == 0 {
		return 0, fmt.Errorf("/proc/uptime: empty")
	}
	secs, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return 0, err
	}
	return uint64(secs * 100), nil // CLK_TCK is 100 on Linux
}

// Checkpoint implements runtime.Runtime.
func (r *Runtime) Checkpoint(ctx context.Context, id string) (runtime.CheckpointInfo, error) {
	start := time.Now()
	upper, _, err := r.overlay(ctx, id)
	if err != nil {
		return runtime.CheckpointInfo{}, err
	}
	base := filepath.Join(r.checkpointDir(), id)
	if err := os.MkdirAll(base, 0o700); err != nil {
		return runtime.CheckpointInfo{}, err
	}
	r.mu.Lock()
	prev := r.ckpts[id]
	r.mu.Unlock()
	seq := 1
	if prev != nil {
		seq = prev.seq + 1
	}
	dir := filepath.Join(base, strconv.Itoa(seq))
	ticks, err := uptimeTicks()
	if err != nil {
		return runtime.CheckpointInfo{}, err
	}
	var entries int
	if r.reflinkOK() {
		entries, err = r.copyReflink(ctx, upper, dir)
		if errors.Is(err, errNoReflink) {
			r.mu.Lock()
			r.noReflink = true
			r.mu.Unlock()
			r.log.Warn("checkpoints fall back to rsync: the checkpoint directory cannot share extents with the sandboxes' writable layers (put it on the same XFS or btrfs filesystem)",
				"checkpointDir", r.checkpointDir(), "upper", upper, "err", err)
		}
	} else {
		err = errNoReflink
	}
	if errors.Is(err, errNoReflink) {
		_ = os.RemoveAll(dir)
		entries, err = copyRsync(ctx, upper, dir, prev)
	}
	if err != nil {
		_ = os.RemoveAll(dir)
		return runtime.CheckpointInfo{}, err
	}
	ck := &checkpoint{dir: dir, seq: seq, ticks: ticks, at: start}
	r.mu.Lock()
	r.ckpts[id] = ck
	r.mu.Unlock()
	if prev != nil {
		go func() { _ = os.RemoveAll(prev.dir) }()
	}
	return runtime.CheckpointInfo{Entries: entries, Took: time.Since(start)}, nil
}

// errNoReflink: the filesystem cannot clone the writable layer's extents into the checkpoint.
var errNoReflink = errors.New("reflink not supported")

func (r *Runtime) reflinkOK() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.noReflink
}

// copyReflink copies the upper directory with cp --reflink=always: every file's data is shared
// with the original until either is written, so the cost is the directory entries, not the bytes.
// It needs the checkpoint directory on the same reflink-capable filesystem (XFS with reflink=1,
// btrfs) as the upper directory. Files that vanish while copying (the agent's temporary files)
// are not an error.
func (r *Runtime) copyReflink(ctx context.Context, upper, dir string) (int, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, err
	}
	out, err := exec.CommandContext(ctx, "cp", "-a", "--reflink=always", upper+"/.", dir).CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		for _, line := range strings.Split(msg, "\n") {
			switch {
			case strings.Contains(line, "Operation not supported"), strings.Contains(line, "Invalid cross-device link"),
				strings.Contains(line, "failed to clone"):
				return 0, fmt.Errorf("%w: %s", errNoReflink, line)
			case line == "", strings.Contains(line, "No such file or directory"):
			default:
				return 0, fmt.Errorf("cp --reflink: %w: %s", err, msg)
			}
		}
	}
	entries := 0
	_ = filepath.WalkDir(dir, func(_ string, _ fs.DirEntry, err error) error {
		if err == nil {
			entries++
		}
		return nil
	})
	return max(0, entries-1), nil
}

// copyRsync copies the upper directory with rsync, hard-linking the files unchanged since the
// previous checkpoint: the cost is the entries plus every byte changed since.
func copyRsync(ctx context.Context, upper, dir string, prev *checkpoint) (int, error) {
	args := []string{"-aAXH", "--numeric-ids", "--stats"}
	if prev != nil {
		args = append(args, "--link-dest="+prev.dir)
	}
	args = append(args, upper+"/", dir+"/")
	out, err := exec.CommandContext(ctx, "rsync", args...).CombinedOutput()
	if err != nil {
		// 24: files vanished while copying (the agent's own temp files); 23: some entries
		// could not be copied. Both leave a usable checkpoint of everything else.
		if ee, ok := err.(*exec.ExitError); !ok || (ee.ExitCode() != 23 && ee.ExitCode() != 24) {
			return 0, fmt.Errorf("rsync: %w: %s", err, strings.TrimSpace(string(out)))
		}
	}
	entries := 0
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "Number of files: "); ok {
			n, _, _ := strings.Cut(v, " ")
			entries, _ = strconv.Atoi(strings.ReplaceAll(n, ",", ""))
		}
	}
	return entries, nil
}

// forgetCheckpoints drops the sandbox's checkpoints.
func (r *Runtime) forgetCheckpoints(id string) {
	r.mu.Lock()
	delete(r.ckpts, id)
	r.mu.Unlock()
	base := filepath.Join(r.checkpointDir(), id)
	if _, err := os.Stat(base); err == nil {
		go func() {
			_ = syscall.Unmount(filepath.Join(base, "view"), 0)
			_ = os.RemoveAll(base)
		}()
	}
}

// proc is one process of the sandbox's cgroup.
type proc struct {
	pid   int
	comm  string
	start uint64 // clock ticks after boot
}

// procs lists the processes in the cgroup and its descendants.
func procs(cgroup string) ([]proc, error) {
	var out []proc
	err := filepath.WalkDir(cgroup, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "cgroup.procs" {
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		for _, f := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(f)
			if err != nil {
				continue
			}
			stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if err != nil {
				continue
			}
			s := string(stat)
			open, close := strings.Index(s, "("), strings.LastIndex(s, ")")
			if open < 0 || close < open {
				continue
			}
			fields := strings.Fields(s[close+1:])
			if len(fields) < 20 {
				continue
			}
			st, _ := strconv.ParseUint(fields[19], 10, 64) // field 22 of the line: starttime
			out = append(out, proc{pid: pid, comm: s[open+1 : close], start: st})
		}
		return nil
	})
	return out, err
}

// young lists the processes started after the checkpoint, the agent's own excluded.
func (r *Runtime) young(cgroup string, taskPid int, ck *checkpoint) ([]proc, error) {
	all, err := procs(cgroup)
	if err != nil {
		return nil, err
	}
	keep := map[string]bool{}
	for _, k := range r.keepProcesses() {
		keep[k] = true
	}
	var out []proc
	for _, p := range all {
		if p.pid == taskPid || keep[p.comm] || p.start <= ck.ticks {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// Rollback implements runtime.Runtime.
func (r *Runtime) Rollback(ctx context.Context, id string) (runtime.RollbackInfo, error) {
	start := time.Now()
	r.mu.Lock()
	ck := r.ckpts[id]
	r.mu.Unlock()
	if ck == nil {
		return runtime.RollbackInfo{}, fmt.Errorf("sandbox %s has no checkpoint", id)
	}
	t, err := r.load(ctx, id)
	if err != nil {
		return runtime.RollbackInfo{}, err
	}
	taskPid := int(t.task.Pid())
	cg, err := swapin.CgroupOf(taskPid)
	if err != nil {
		return runtime.RollbackInfo{}, err
	}
	victims, err := r.young(cg, taskPid, ck)
	if err != nil {
		return runtime.RollbackInfo{}, err
	}
	if len(victims) == 0 {
		return runtime.RollbackInfo{Took: time.Since(start)}, nil
	}
	for _, p := range victims {
		_ = syscall.Kill(p.pid, syscall.SIGKILL)
	}
	// Wait for them to go, so nothing writes while the files are put back.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		left, _ := r.young(cg, taskPid, ck)
		if len(left) == 0 {
			break
		}
		for _, p := range left {
			_ = syscall.Kill(p.pid, syscall.SIGKILL)
		}
		time.Sleep(50 * time.Millisecond)
	}
	restored, err := r.restore(ctx, id, ck)
	info := runtime.RollbackInfo{Killed: len(victims), Restored: restored, Took: time.Since(start)}
	if err != nil {
		return info, err
	}
	r.log.Info("sandbox rolled back", "id", id, "killed", len(victims), "restored", restored, "took", info.Took.Round(time.Millisecond))
	return info, nil
}

// entry is one path of an upper directory, as compared between a checkpoint
// and the live upper.
type entry struct {
	mode     fs.FileMode
	size     int64
	mtime    int64 // nanoseconds; rsync keeps them
	uid, gid uint32
	rdev     uint64
	link     string
	opaque   bool
}

func (e entry) same(o entry) bool {
	if e.mode.Type() != o.mode.Type() {
		return false
	}
	switch {
	case e.mode.IsDir():
		return e.mode == o.mode && e.uid == o.uid && e.gid == o.gid && e.opaque == o.opaque
	case e.mode&fs.ModeSymlink != 0:
		return e.link == o.link && e.uid == o.uid && e.gid == o.gid
	case e.mode&fs.ModeCharDevice != 0:
		return e.mode == o.mode && e.rdev == o.rdev // a whiteout; rsync does not keep its time
	default:
		return e.mode == o.mode && e.size == o.size && e.mtime == o.mtime && e.uid == o.uid && e.gid == o.gid && e.rdev == o.rdev
	}
}

// scan lists an upper directory's entries by relative path.
func scan(root string) (map[string]entry, error) {
	out := map[string]entry{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == root {
				return err
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil || rel == "." {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		e := entry{mode: fi.Mode(), size: fi.Size(), mtime: fi.ModTime().UnixNano()}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			e.uid, e.gid, e.rdev = st.Uid, st.Gid, uint64(st.Rdev)
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			e.link, _ = os.Readlink(p)
		}
		if fi.IsDir() {
			var buf [8]byte
			if n, err := syscall.Getxattr(p, "trusted.overlay.opaque", buf[:]); err == nil && n > 0 && buf[0] == 'y' {
				e.opaque = true
			}
		}
		out[rel] = e
		return nil
	})
	return out, err
}

// excluded says whether a rollback leaves the path alone.
func excluded(rel string, patterns []string) bool {
	for _, pat := range patterns {
		if strings.HasSuffix(pat, "*") {
			if strings.HasPrefix(rel, strings.TrimSuffix(pat, "*")) {
				return true
			}
			continue
		}
		if rel == pat || strings.HasPrefix(rel, pat+"/") {
			return true
		}
	}
	return false
}

// changed lists the paths whose entries differ between the checkpoint and
// the live upper, shallowest first, a changed directory standing for its
// subtree.
func changed(ck, live map[string]entry, exclude []string) []string {
	keys := make([]string, 0, len(ck)+len(live))
	seen := map[string]bool{}
	for k := range ck {
		keys, seen[k] = append(keys, k), true
	}
	for k := range live {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []string
	last := ""
	for _, k := range keys {
		if last != "" && strings.HasPrefix(k, last+"/") {
			continue
		}
		if excluded(k, exclude) {
			continue
		}
		c, cok := ck[k]
		l, lok := live[k]
		if cok && lok && c.same(l) {
			continue
		}
		out = append(out, k)
		last = k
	}
	return out
}

// restore puts the changed paths back from the checkpoint's view.
func (r *Runtime) restore(ctx context.Context, id string, ck *checkpoint) (int, error) {
	upper, lower, err := r.overlay(ctx, id)
	if err != nil {
		return 0, err
	}
	snap, err := scan(ck.dir)
	if err != nil {
		return 0, fmt.Errorf("scan checkpoint: %w", err)
	}
	live, err := scan(upper)
	if err != nil {
		return 0, fmt.Errorf("scan upper: %w", err)
	}
	paths := changed(snap, live, r.rollbackExclude())
	r.log.Debug("rollback: changed paths", "id", id, "paths", paths)
	if len(paths) == 0 {
		return 0, nil
	}
	base := filepath.Dir(ck.dir)
	view, work := filepath.Join(base, "view"), filepath.Join(base, "work")
	_ = os.RemoveAll(work)
	for _, d := range []string{view, work} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return 0, err
		}
	}
	opts := "lowerdir=" + lower + ",upperdir=" + ck.dir + ",workdir=" + work
	if err := syscall.Mount("overlay", view, "overlay", syscall.MS_RDONLY, opts); err != nil {
		return 0, fmt.Errorf("mount checkpoint view: %w", err)
	}
	defer func() {
		_ = syscall.Unmount(view, 0)
		_ = os.RemoveAll(work)
	}()
	rootfs := r.rootfs(id)
	if _, err := os.Stat(rootfs); err != nil {
		return 0, fmt.Errorf("rootfs: %w", err)
	}
	restored := 0
	for _, p := range paths {
		dst, src := filepath.Join(rootfs, p), filepath.Join(view, p)
		if err := os.RemoveAll(dst); err != nil {
			r.log.Warn("rollback: remove", "id", id, "path", p, "err", err)
			continue
		}
		if _, err := os.Lstat(src); err != nil {
			restored++
			continue // the checkpoint had nothing there
		}
		if out, err := exec.CommandContext(ctx, "cp", "-a", src, dst).CombinedOutput(); err != nil {
			r.log.Warn("rollback: copy", "id", id, "path", p, "err", err, "out", strings.TrimSpace(string(out)))
			continue
		}
		restored++
	}
	return restored, nil
}
