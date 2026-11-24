package containerd

// Whole-sandbox checkpoints: processes, memory and filesystem, for policies
// that stop a sandbox in the middle of a tool call and later resume it from
// the moment before the call began.
//
// CRIU checkpoints a container as the process tree under its init, and a
// process started with `runc exec` is outside that tree. A checkpointable
// sandbox (Config.InitBinary set) therefore runs tidelet-init as its init,
// and its execs are started by that init (execInTree) instead of the shim:
// the agent and its tool calls are descendants of init, and CRIU, through
// runc, takes the whole sandbox.
//
// A checkpoint is taken with the sandbox paused: the writable layer is copied
// (as for the filesystem checkpoints, see checkpoint.go) and CRIU dumps the
// processes to an image directory, leaving them running, with open TCP
// connections allowed. A rollback kills the task, deletes it (containerd
// unmounts the rootfs), and puts the writable layer back to the copy; the
// sandbox then holds no CPU and no memory. A restore creates the task again
// from the CRIU image: every process continues from the checkpoint, with its
// TCP connections closed (CRIU's tcp-close, see /etc/criu/runc.conf), so an
// agent that was waiting on its model call sees the connection drop and sends
// the request again.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/containerd/api/types/runc/options"
	client "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/backend/swapin"
	"slices"
	"sort"
)

const (
	initMount = "/.tidelet/init"
	ctlMount  = "/.tidelet/ctl"
	// criuConfig is the CRIU configuration runc reads for every checkpoint and restore.
	criuConfig = "/etc/criu/runc.conf"
)

// criuOptions are the CRIU options a whole-sandbox checkpoint needs beyond the ones runc
// passes itself (tcp-established and file-locks come from the checkpoint request): connections
// half-open at the dump are skipped, no iptables round trip locks the network (the sandbox is
// paused anyway), and established connections come back closed on restore, so the agent
// reconnects. runc handles the bind mounts. (CRIU 4.1 failed to start with these plus
// tcp-established and file-locks in the file; the request's flags carry those two.)
//
// The agent watches its config directory and its working directory with inotify; on overlayfs
// those watches have no usable file handle, so CRIU is told to find their paths by scanning
// (force-irmap) the places agents watch: these, plus every checkpointable sandbox's working
// directory as it is created (ensureCRIUConfig's extra paths; a watch on an unscanned
// directory fails the whole dump with "fsnotify: Can't dump that handle").
var criuOptions = []string{"tcp-close", "skip-in-flight", "force-irmap", "irmap-scan-path /root", "irmap-scan-path /app",
	"irmap-scan-path /tmp", "irmap-scan-path /home"}

// wholeCkpt is a sandbox's latest whole checkpoint.
type wholeCkpt struct {
	fs, img string
	seq     int
	at      time.Time
}

// ContainerCheckpoints implements runtime.ContainerCheckpointer.
func (r *Runtime) ContainerCheckpoints() bool { return r.cfg.InitBinary != "" }

func (r *Runtime) ctlDir(id string) string {
	base := r.cfg.CtlDir
	if base == "" {
		base = "/run/tidelet/ctl"
	}
	return filepath.Join(base, id)
}

func (r *Runtime) criuDir() string {
	if r.cfg.CRIUDir != "" {
		return r.cfg.CRIUDir
	}
	return filepath.Join(r.checkpointDir(), "criu")
}

// ensureCRIUConfig writes the CRIU options runc passes to every dump and restore, with an
// irmap scan path for each of `scan` (the sandboxes' working directories), and lifts the
// per-user inotify instance limit: every agent holds one, so do the container shims, and
// CRIU needs one of its own; at the default 128 a pool of agents leaves CRIU none.
func ensureCRIUConfig(scan ...string) error {
	if b, err := os.ReadFile("/proc/sys/fs/inotify/max_user_instances"); err == nil {
		if n, _ := strconv.Atoi(strings.TrimSpace(string(b))); n < 8192 {
			_ = os.WriteFile("/proc/sys/fs/inotify/max_user_instances", []byte("8192"), 0o644)
		}
	}
	opts := append([]string(nil), criuOptions...)
	sort.Strings(scan)
	for _, p := range scan {
		if o := "irmap-scan-path " + p; p != "" && p != "/" && !slices.Contains(opts, o) {
			opts = append(opts, o)
		}
	}
	want := strings.Join(opts, "\n") + "\n"
	if have, err := os.ReadFile(criuConfig); err == nil && string(have) == want {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(criuConfig), 0o755); err != nil {
		return err
	}
	return os.WriteFile(criuConfig, []byte(want), 0o644)
}

// addScanPath adds a sandbox's working directory to the CRIU scan paths and rewrites the
// configuration when it is new.
func (r *Runtime) addScanPath(dir string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scanPaths[dir] {
		return
	}
	r.scanPaths[dir] = true
	paths := make([]string, 0, len(r.scanPaths))
	for p := range r.scanPaths {
		paths = append(paths, p)
	}
	if err := ensureCRIUConfig(paths...); err != nil {
		slog.Warn("could not add the working directory to the CRIU scan paths", "dir", dir, "err", err)
	}
}

// makeWakePipe creates the pipe a sandbox's init waits on for exec and kill requests.
func makeWakePipe(dir string) error {
	p := filepath.Join(dir, "wake")
	if err := syscall.Mkfifo(p, 0o666); err != nil && !errors.Is(err, syscall.EEXIST) {
		return fmt.Errorf("mkfifo: %w", err)
	}
	return os.Chmod(p, 0o666)
}

// inTreeSpecOpts makes the sandbox checkpointable: tidelet-init as its init, ahead of the
// sandbox's own command, and the control directory it takes exec requests from.
func (r *Runtime) inTreeSpecOpts(id string) ([]oci.SpecOpts, error) {
	dir := r.ctlDir(id)
	if err := os.MkdirAll(filepath.Join(dir, "req"), 0o777); err != nil {
		return nil, err
	}
	_ = os.Chmod(dir, 0o777)
	_ = os.Chmod(filepath.Join(dir, "req"), 0o777)
	if err := makeWakePipe(dir); err != nil {
		return nil, err
	}
	return []oci.SpecOpts{
		oci.WithMounts([]specs.Mount{
			{Type: "bind", Source: r.cfg.InitBinary, Destination: initMount, Options: []string{"rbind", "ro"}},
			{Type: "bind", Source: dir, Destination: ctlMount, Options: []string{"rbind", "rw"}},
		}),
		func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
			if s.Process != nil {
				s.Process.Args = append([]string{initMount, "--"}, s.Process.Args...)
			}
			return nil
		},
	}, nil
}

type initRequest struct {
	Args   []string `json:"args"`
	Env    []string `json:"env"`
	Cwd    string   `json:"cwd"`
	Stdin  string   `json:"stdin"`
	Stdout string   `json:"stdout"`
	Stderr string   `json:"stderr"`
}

// openPipe creates a named pipe if it is not there and opens it for reading. It is held
// read-write, so it never reports end-of-file and stays usable across a restore.
func openPipe(p string) (*os.File, error) {
	if err := syscall.Mkfifo(p, 0o666); err != nil && !errors.Is(err, syscall.EEXIST) {
		return nil, fmt.Errorf("mkfifo: %w", err)
	}
	_ = os.Chmod(p, 0o666)
	return os.OpenFile(p, os.O_RDWR, 0)
}

// poke writes a byte to a named pipe, waking whoever reads it; without a reader it does nothing.
func poke(p string) {
	if fd, err := syscall.Open(p, syscall.O_WRONLY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0); err == nil {
		_, _ = syscall.Write(fd, []byte{1})
		_ = syscall.Close(fd)
	}
}

// pokeAll pokes every pipe in dir whose name ends in suffix.
func pokeAll(dir, suffix string) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), suffix) {
			poke(filepath.Join(dir, e.Name()))
		}
	}
}

// poked returns a channel closed when a byte arrives on f, or f is closed.
func poked(f *os.File) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		var b [1]byte
		_, _ = f.Read(b[:])
		close(ch)
	}()
	return ch
}

// copyPipe copies an output pipe to w until endPipe is called on it, and then what the
// pipe still holds: once the writer has exited that is all it wrote.
func copyPipe(f *os.File, w io.Writer) {
	buf := make([]byte, 32<<10)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return
		}
		_ = f.SetReadDeadline(time.Time{})
		rc, err := f.SyscallConn()
		if err != nil {
			return
		}
		_ = rc.Read(func(fd uintptr) bool {
			for {
				n, err := syscall.Read(int(fd), buf)
				if n > 0 {
					if _, werr := w.Write(buf[:n]); werr != nil {
						return true
					}
					continue
				}
				if err != syscall.EINTR {
					return true // empty (EAGAIN): done
				}
			}
		})
		return
	}
}

// endPipe makes the pipe's copyPipe finish with what the pipe holds.
func endPipe(f *os.File) { _ = f.SetReadDeadline(time.Now()) }

// execInTree runs a command as a child of the sandbox's init (see tidelet-init). Stdin is
// passed on as it arrives, in numbered part files init relays; stdout and stderr are named
// pipes this side holds open, and init reports the end of the command by writing its status
// file and a byte to the done pipe. While the sandbox is rolled back the exec simply waits:
// its process is in the checkpoint and comes back with the restore.
func (r *Runtime) execInTree(ctx context.Context, id string, t *taskIO, opts runtime.ExecOptions) (runtime.ExecResult, error) {
	spec, err := t.task.Spec(ctx)
	if err != nil {
		return runtime.ExecResult{}, fmt.Errorf("task spec: %w", err)
	}
	env := append([]string(nil), spec.Process.Env...)
	env = append(env, sortedEnv(opts.Env)...)
	cwd := spec.Process.Cwd
	if opts.WorkingDir != "" {
		cwd = opts.WorkingDir
	}
	dir := r.ctlDir(id)
	eid := "x" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var stdin *stdinParts
	defer func() {
		for _, s := range []string{".out", ".err", ".status", ".kill", ".done"} {
			_ = os.Remove(filepath.Join(dir, eid+s))
		}
		if stdin != nil {
			stdin.stop()
		}
	}()
	req := initRequest{Args: opts.Command, Env: env, Cwd: cwd}
	if opts.Stdin != nil {
		stdin = &stdinParts{prefix: filepath.Join(dir, eid+stdinExt), wake: filepath.Join(dir, "wake")}
		go stdin.copy(opts.Stdin)
		req.Stdin = ctlMount + "/" + eid + stdinExt
	}
	var readers []*os.File
	defer func() {
		for _, f := range readers {
			f.Close()
		}
	}()
	pump := func(suffix string, w io.Writer) (string, chan struct{}, error) {
		f, err := openPipe(filepath.Join(dir, eid+suffix))
		if err != nil {
			return "", nil, err
		}
		readers = append(readers, f)
		done := make(chan struct{})
		if w == nil {
			w = io.Discard
		}
		go func() { copyPipe(f, w); close(done) }()
		return ctlMount + "/" + eid + suffix, done, nil
	}
	outPath, outDone, err := pump(".out", opts.Stdout)
	if err != nil {
		return runtime.ExecResult{}, err
	}
	errPath, errDone, err := pump(".err", opts.Stderr)
	if err != nil {
		return runtime.ExecResult{}, err
	}
	doneFile, err := openPipe(filepath.Join(dir, eid+".done"))
	if err != nil {
		return runtime.ExecResult{}, err
	}
	defer doneFile.Close()
	ended := poked(doneFile)
	req.Stdout, req.Stderr = outPath, errPath
	data, _ := json.Marshal(req)
	tmp := filepath.Join(dir, "req", eid+".json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return runtime.ExecResult{}, err
	}
	if err := os.Rename(tmp, strings.TrimSuffix(tmp, ".tmp")); err != nil {
		return runtime.ExecResult{}, err
	}
	wake := filepath.Join(dir, "wake")
	poke(wake)

	code := -1
	var waitErr error
	select {
	case <-ended:
	case <-ctx.Done():
		waitErr = ctx.Err()
		_ = os.WriteFile(filepath.Join(dir, eid+".kill"), nil, 0o644)
		poke(wake)
		select {
		case <-ended:
		case <-time.After(30 * time.Second):
			return runtime.ExecResult{ExitCode: -1}, fmt.Errorf("exec: process did not end after kill: %w", ctx.Err())
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, eid+".status")); err == nil {
		code, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	} else {
		code = 137 // the sandbox was deleted under the exec: as if killed
	}
	for _, f := range readers {
		endPipe(f)
	}
	<-outDone
	<-errDone
	return runtime.ExecResult{ExitCode: int32(code)}, waitErr
}

// stdinExt follows an exec's ID in the names of its stdin files.
const stdinExt = ".in"

// stdinParts writes an exec's stdin where the sandbox's init finds it: one file per read, named
// <prefix>.0, .1, ..., each renamed into place whole, and <prefix>.eof after the last. Init is
// woken after each.
type stdinParts struct {
	prefix, wake string
	mu           sync.Mutex
	n            int
	stopped      bool
}

func (s *stdinParts) put(name string, data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false
	}
	tmp := s.prefix + "-tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false
	}
	if err := os.Rename(tmp, s.prefix+"."+name); err != nil {
		return false
	}
	poke(s.wake)
	return true
}

// copy runs until the reader ends or the exec does. Only a clean end of the reader ends the
// command's stdin: a client that went away leaves it open, and the exec is cancelled instead.
func (s *stdinParts) copy(r io.Reader) {
	// An empty first part: a snapshot taken before any input arrived still shows the stdin.
	if !s.put("0", nil) {
		return
	}
	s.n = 1
	buf := make([]byte, 256<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if !s.put(strconv.Itoa(s.n), buf[:n]) {
				return
			}
			s.n++
		}
		if err == io.EOF {
			s.put("eof", nil)
		}
		if err != nil {
			return
		}
	}
}

// stop ends the copy and removes the parts.
func (s *stdinParts) stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	removeStdin(s.prefix)
}

// removeStdin removes an exec's stdin files: the parts, the end marker, a part being written.
func removeStdin(prefix string) {
	dir, base := filepath.Split(prefix)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if rest, ok := strings.CutPrefix(e.Name(), base); ok && (strings.HasPrefix(rest, ".") || rest == "-tmp") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// withOpenTCP lets the dump take established TCP connections.
func withOpenTCP(ci *client.CheckpointTaskInfo) error {
	if ci.Options == nil {
		ci.Options = &options.CheckpointOptions{}
	}
	o, ok := ci.Options.(*options.CheckpointOptions)
	if !ok {
		return errors.New("unexpected checkpoint options type")
	}
	o.OpenTcp = true
	o.FileLocks = true
	return nil
}

// CheckpointContainer implements runtime.ContainerCheckpointer: the writable layer and the
// processes, with the sandbox paused in between so the two agree. It replaces the sandbox's
// earlier whole checkpoint.
func (r *Runtime) CheckpointContainer(ctx context.Context, id string) (runtime.CheckpointInfo, error) {
	start := time.Now()
	t, err := r.load(ctx, id)
	if err != nil {
		return runtime.CheckpointInfo{}, err
	}
	upper, _, err := r.overlay(ctx, id)
	if err != nil {
		return runtime.CheckpointInfo{}, err
	}
	r.mu.Lock()
	prev := r.whole[id]
	base := r.wholeBase[id]
	r.mu.Unlock()
	seq := 1
	if prev != nil {
		seq = prev.seq + 1
	} else if base != nil {
		seq = base.seq + 1
	}
	fsDir := filepath.Join(r.checkpointDir(), id, "whole-"+strconv.Itoa(seq))
	imgDir := filepath.Join(r.criuDir(), id, strconv.Itoa(seq))
	for _, d := range []string{filepath.Dir(fsDir), imgDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return runtime.CheckpointInfo{}, err
		}
	}
	r.mu.Lock()
	rolling := r.rolling[id]
	r.mu.Unlock()
	if rolling {
		return runtime.CheckpointInfo{}, fmt.Errorf("sandbox %s is being rolled back", id)
	}
	// The images go to a tmpfs: a sandbox with many forked processes dumps several times
	// its memory (12 GiB became 78 GB once), so there must be room for four times it.
	if st, serr := r.Stats(ctx, id); serr == nil && st.MemoryUsageBytes > 0 {
		var fs unix.Statfs_t
		if unix.Statfs(imgDir, &fs) == nil {
			if free := uint64(fs.Bavail) * uint64(fs.Bsize); free < 4*st.MemoryUsageBytes {
				return runtime.CheckpointInfo{}, fmt.Errorf("criu images of sandbox %s would not fit: %d MB free in %s, the sandbox holds %d MB", id, free>>20, r.criuDir(), st.MemoryUsageBytes>>20)
			}
		}
	}
	// The container may still be paused by a checkpoint that is just finishing (the agent
	// sent two requests close together): wait for it rather than skip this call's checkpoint.
	var perr error
	for try := 0; try < 20; try++ {
		if perr = t.task.Pause(ctx); perr == nil || !strings.Contains(perr.Error(), "paused") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if perr != nil {
		// A failed pause can still leave the sandbox stopped. Still "paused" after a second
		// is a shim/freezer disagreement (the task stays PAUSED for the shim while runc says it
		// is not). And runc's wait for the freezer can time out ("timeout of 10s reached waiting
		// for the cgroup to freeze", with tens of workers in the sandbox) while the kernel
		// finishes the freeze a moment later. Left
		// alone the sandbox never runs again: thawed now, and watched for a while after.
		r.thawAfterFailedPause(context.WithoutCancel(ctx), id, t)
		return runtime.CheckpointInfo{}, fmt.Errorf("pause: %w", perr)
	}
	// Files unchanged since the latest copy (of this call or the one before) are hard-linked.
	var prevFS *checkpoint
	switch {
	case prev != nil:
		prevFS = &checkpoint{dir: prev.fs}
	case base != nil:
		prevFS = &checkpoint{dir: base.fs}
	}
	entries, err := copyRsync(ctx, upper, fsDir, prevFS)
	if err == nil {
		_, err = t.task.Checkpoint(ctx, client.WithCheckpointImagePath(imgDir), withOpenTCP)
		if err != nil {
			err = fmt.Errorf("criu dump: %w", err)
		}
	}
	late := err != nil && ctx.Err() != nil && dumpRunning(imgDir)
	if late {
		// The dump outlived its deadline. It goes on inside the shim, with the sandbox's
		// processes seized, and when it ends CRIU puts the freezer back as it found it:
		// frozen. A resume now is undone by that, so the sandbox is resumed once the dump
		// has ended (finishLateDump); until then it is paused, as it would be during the dump.
		r.log.Warn("checkpoint dump outlived its deadline; the sandbox stays paused until it ends", "id", id)
		go r.finishLateDump(id, imgDir)
	} else if rerr := r.thaw(context.WithoutCancel(ctx), t); rerr != nil {
		if err == nil {
			err = fmt.Errorf("resume: %w", rerr)
		} else {
			r.log.Warn("resume after a failed checkpoint", "id", id, "err", rerr)
		}
	}
	if err != nil {
		// A dump cut off by its deadline can leave the cgroup frozen by CRIU's own freezer,
		// which the task's Resume does not undo: the sandbox would never run again. Not
		// while the dump still runs: finishLateDump thaws the sandbox once it has ended.
		if !late && r.thawIfFrozen(id) {
			err = fmt.Errorf("%w (its cgroup was left frozen; thawed)", err)
		}
		_ = os.RemoveAll(fsDir)
		if !late {
			_ = os.RemoveAll(imgDir)
		}
		// No checkpoint of this call: the earlier one must not stand in for it, unless a
		// rollback in flight (which is what killed this dump) is about to restore it.
		r.mu.Lock()
		rolling := r.rolling[id]
		r.mu.Unlock()
		if !rolling {
			r.DiscardContainerCheckpoint(id)
		}
		return runtime.CheckpointInfo{}, err
	}
	r.mu.Lock()
	r.whole[id] = &wholeCkpt{fs: fsDir, img: imgDir, seq: seq, at: start}
	delete(r.wholeBase, id)
	r.mu.Unlock()
	for _, old := range []*wholeCkpt{prev, base} {
		if old != nil {
			go func() { _ = os.RemoveAll(old.fs); _ = os.RemoveAll(old.img) }()
		}
	}
	return runtime.CheckpointInfo{Entries: entries, Took: time.Since(start)}, nil
}

// thawAfterFailedPause makes sure a sandbox whose pause failed runs: the freezer and the shim
// are reconciled now, and since the freeze a timed-out pause started can complete after the
// failure, the cgroup is watched for half a minute and thawed the moment it freezes.
func (r *Runtime) thawAfterFailedPause(ctx context.Context, id string, t *taskIO) {
	reconcile := func() bool {
		thawed := r.thawIfFrozen(id)
		if st, err := t.task.Status(ctx); err == nil && (st.Status == client.Paused || st.Status == client.Pausing) {
			if terr := r.thaw(ctx, t); terr != nil {
				r.log.Warn("sandbox stuck paused after a failed pause; thaw failed", "id", id, "err", terr)
			} else {
				thawed = true
			}
		}
		return thawed
	}
	if reconcile() {
		r.log.Warn("sandbox was left paused by a failed pause; thawed, this call is not checkpointed", "id", id)
	}
	go func() {
		for i := 0; i < 30; i++ {
			time.Sleep(time.Second)
			if reconcile() {
				r.log.Warn("sandbox froze after its pause had failed; thawed, this call is not checkpointed", "id", id)
				return
			}
		}
	}()
}

// thawIfFrozen thaws a sandbox's cgroup that is still frozen, and says whether it was.
func (r *Runtime) thawIfFrozen(id string) bool {
	if r.cfg.CgroupParent == "" {
		return false
	}
	freeze := filepath.Join("/sys/fs/cgroup", r.cfg.CgroupParent, id, "cgroup.freeze")
	if b, err := os.ReadFile(freeze); err != nil || strings.TrimSpace(string(b)) != "1" {
		return false
	}
	return os.WriteFile(freeze, []byte("0"), 0o644) == nil
}

// thaw makes sure a paused sandbox runs again after a checkpoint, and says so when it does
// not. The shim and the cgroup can disagree: a dump that ended after an earlier resume has
// put the freezer back, so the shim counts the task as running while its cgroup is frozen,
// and the task's status (runc reads the freezer) says paused. Pausing and resuming through
// the shim then brings the two back together.
func (r *Runtime) thaw(ctx context.Context, t *taskIO) error {
	var last error
	for try := 0; try < 20; try++ {
		st, err := t.task.Status(ctx)
		if err != nil {
			return fmt.Errorf("status: %w", err)
		}
		if st.Status != client.Paused && st.Status != client.Pausing {
			return nil
		}
		if err := t.task.Resume(ctx); err != nil {
			// The shim says paused while runc (the freezer) says running / not paused: a pause
			// through the shim then a resume brings the two back together.
			if strings.Contains(err.Error(), "running") || strings.Contains(err.Error(), "not paused") {
				_ = t.task.Pause(ctx)
				err = t.task.Resume(ctx)
			}
			last = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if last == nil {
		last = errors.New("still paused")
	}
	return last
}

// finishLateDump waits for a dump that outlived its deadline to end, however long it takes (a
// sandbox whose pages are mostly in swap can take hours), then resumes the sandbox and removes
// the image, which is of a checkpoint that does not count. It says so every hour: giving up
// instead left the sandbox frozen for good once the dump ended with nobody waiting.
func (r *Runtime) finishLateDump(id, imgDir string) {
	start := time.Now()
	for hours := 1; dumpRunning(imgDir); {
		if time.Since(start) > time.Duration(hours)*time.Hour {
			r.log.Error("checkpoint dump still running; the sandbox stays paused until it ends", "id", id, "for", time.Since(start).Round(time.Minute))
			hours++
		}
		time.Sleep(time.Second)
	}
	_ = os.RemoveAll(imgDir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t, err := r.load(ctx, id)
	if err != nil {
		r.log.Debug("late checkpoint dump ended; sandbox gone", "id", id, "err", err)
		return
	}
	if err := r.thaw(ctx, t); err != nil && !r.thawIfFrozen(id) {
		r.log.Error("sandbox left paused after a late checkpoint dump", "id", id, "err", err)
		return
	}
	r.log.Warn("late checkpoint dump ended; sandbox resumed", "id", id, "after", time.Since(start).Round(time.Second))
}

// dumpRunning says whether a process is still dumping into (or out of) the image directory:
// the OCI runtime's checkpoint command names it in its arguments, and CRIU holds it open.
func dumpRunning(imgDir string) bool {
	procs, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	needle := []byte(imgDir)
	for _, p := range procs {
		if !p.IsDir() || p.Name()[0] < '0' || p.Name()[0] > '9' {
			continue
		}
		dir := filepath.Join("/proc", p.Name())
		if args, err := os.ReadFile(filepath.Join(dir, "cmdline")); err == nil && bytesContains(args, needle) {
			return true
		}
		if comm, err := os.ReadFile(filepath.Join(dir, "comm")); err != nil || strings.TrimSpace(string(comm)) != "criu" {
			continue
		}
		fds, err := os.ReadDir(filepath.Join(dir, "fd"))
		if err != nil {
			continue
		}
		for _, fd := range fds {
			if target, err := os.Readlink(filepath.Join(dir, "fd", fd.Name())); err == nil && (target == imgDir || strings.HasPrefix(target, imgDir+"/")) {
				return true
			}
		}
	}
	return false
}

func bytesContains(b, sub []byte) bool { return strings.Contains(string(b), string(sub)) }

// RollbackContainer implements runtime.ContainerCheckpointer: the sandbox's processes are
// killed, its task deleted, and its writable layer put back to the latest whole checkpoint.
// It holds no CPU and no memory until RestoreContainer.
func (r *Runtime) RollbackContainer(ctx context.Context, id string) (runtime.RollbackInfo, error) {
	start := time.Now()
	r.mu.Lock()
	ck := r.whole[id]
	if ck != nil {
		r.rolling[id] = true
	}
	r.mu.Unlock()
	if ck == nil {
		return runtime.RollbackInfo{}, fmt.Errorf("sandbox %s has no whole checkpoint", id)
	}
	defer func() {
		r.mu.Lock()
		delete(r.rolling, id)
		r.mu.Unlock()
	}()
	t, err := r.load(ctx, id)
	if err != nil {
		return runtime.RollbackInfo{}, err
	}
	upper, _, err := r.overlay(ctx, id)
	if err != nil {
		return runtime.RollbackInfo{}, err
	}
	killed := 0
	if pids, err := t.task.Pids(ctx); err == nil {
		killed = len(pids)
	}
	// The task's cgroup goes with it: read the CPU it used so far now, and add it to the
	// sandbox's base once the task is deleted, for monotonic counters (added before, a stats
	// read while the old cgroup still lived would have counted its usage twice).
	var used [2]uint64
	if cg, err := swapin.CgroupOf(int(t.task.Pid())); err == nil {
		if s, err := cgroupStats(cg); err == nil {
			used = [2]uint64{s.CPUUsageNanos, s.CPUThrottledNanos}
		}
	}
	exitCh, err := t.task.Wait(ctx)
	if err != nil {
		return runtime.RollbackInfo{}, fmt.Errorf("wait: %w", err)
	}
	if err := t.task.Kill(ctx, syscall.SIGKILL, client.WithKillAll); err != nil {
		return runtime.RollbackInfo{Killed: killed}, fmt.Errorf("kill: %w", err)
	}
	select {
	case <-exitCh:
	case <-time.After(30 * time.Second):
		return runtime.RollbackInfo{Killed: killed}, errors.New("task did not exit after SIGKILL")
	}
	if _, err := t.task.Delete(ctx); err != nil {
		return runtime.RollbackInfo{Killed: killed}, fmt.Errorf("delete task: %w", err)
	}
	r.forget(id)
	// The sandbox is stopped at this checkpoint: it belongs to the restore from now on, out of
	// reach of a discard (the agent's next request can still reach the proxy as it is killed).
	r.mu.Lock()
	b := r.cpuBase[id]
	r.cpuBase[id] = [2]uint64{b[0] + used[0], b[1] + used[1]}
	if r.whole[id] == ck {
		delete(r.whole, id)
	}
	r.rolled[id] = ck
	r.mu.Unlock()
	out, err := exec.CommandContext(ctx, "rsync", "-aAXH", "--numeric-ids", "--delete", "--stats", ck.fs+"/", upper+"/").CombinedOutput()
	if err != nil {
		return runtime.RollbackInfo{Killed: killed}, fmt.Errorf("restore writable layer: %w: %s", err, strings.TrimSpace(string(out)))
	}
	restored := 0
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "Number of regular files transferred: "); ok {
			restored, _ = strconv.Atoi(strings.ReplaceAll(v, ",", ""))
		}
	}
	return runtime.RollbackInfo{Killed: killed, Restored: restored, Took: time.Since(start)}, nil
}

// RestoreContainer implements runtime.ContainerCheckpointer: the task is created again from
// the latest whole checkpoint's CRIU image and every process continues from it.
func (r *Runtime) RestoreContainer(ctx context.Context, id string) (time.Duration, error) {
	start := time.Now()
	r.mu.Lock()
	ck := r.rolled[id]
	if ck == nil {
		ck = r.whole[id]
	}
	r.mu.Unlock()
	if ck == nil {
		return 0, fmt.Errorf("sandbox %s has no whole checkpoint", id)
	}
	ctr, err := r.client.LoadContainer(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("load container: %w", err)
	}
	// The rollback's task must be gone before its successor is created: containerd must
	// have dropped it and its cgroup must be empty (the killed processes exit after the kill
	// returns). A restore right after a rollback otherwise fails inside CRIU.
	cg := ""
	if r.cfg.CgroupParent != "" {
		cg = filepath.Join("/sys/fs/cgroup", r.cfg.CgroupParent, id)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if _, err := ctr.Task(ctx, nil); err == nil {
			continue
		}
		if cg != "" {
			if procs, err := os.ReadFile(filepath.Join(cg, "cgroup.procs")); err == nil && strings.TrimSpace(string(procs)) != "" {
				continue
			}
		}
		break
	}
	hub, err := newIOHub(id, r.LogPath(id), false, false)
	if err != nil {
		return 0, fmt.Errorf("open log: %w", err)
	}
	stdin, stdout, stderr := hub.streams()
	work := ck.img + "-restore"
	_ = os.MkdirAll(work, 0o700)
	task, err := ctr.NewTask(ctx, cio.NewCreator(cio.WithStreams(stdin, stdout, stderr), cio.WithFIFODir(r.cfg.FIFODir)),
		client.WithRestoreImagePath(ck.img), client.WithRestoreWorkPath(work))
	if err != nil {
		hub.close()
		r.keepRestoreLog(id, work)
		return 0, fmt.Errorf("restore task: %w", err)
	}
	if err := task.Start(ctx); err != nil {
		hub.close()
		_, _ = task.Delete(context.WithoutCancel(ctx), client.WithProcessKill)
		r.keepRestoreLog(id, work)
		return 0, fmt.Errorf("start restored task: %w", err)
	}
	poke(filepath.Join(r.ctlDir(id), "wake")) // stdin that arrived after the checkpoint is passed on again
	r.mu.Lock()
	r.tasks[id] = &taskIO{task: task, hub: hub}
	if r.rolled[id] == ck {
		delete(r.rolled, id)
		// Restored: the image is spent, the writable-layer copy stays as the next base.
		if old := r.wholeBase[id]; old != nil && old.fs != ck.fs {
			go func() { _ = os.RemoveAll(old.fs) }()
		}
		r.wholeBase[id] = &wholeCkpt{fs: ck.fs, seq: ck.seq}
		go func() { _ = os.RemoveAll(ck.img); _ = os.RemoveAll(ck.img + "-restore") }()
	}
	r.mu.Unlock()
	return time.Since(start), nil
}

// keepRestoreLog copies a failed restore's CRIU log to a directory of its own next
// to the checkpoint directory, where it survives the sandbox's deletion, for diagnosis.
func (r *Runtime) keepRestoreLog(id, work string) {
	data, err := os.ReadFile(filepath.Join(work, "restore.log"))
	if err != nil {
		return
	}
	dir := filepath.Join(filepath.Dir(r.checkpointDir()), "restore-failures")
	_ = os.MkdirAll(dir, 0o700)
	name := filepath.Join(dir, id+"-"+time.Now().UTC().Format("20060102-150405")+".log")
	if os.WriteFile(name, data, 0o600) == nil {
		r.log.Warn("restore failed; CRIU log kept", "id", id, "log", name)
	}
}

// DiscardContainerCheckpoint implements runtime.ContainerCheckpointer. The CRIU image (the
// sandbox's memory) goes at once; the copy of the writable layer is kept as the base the next
// checkpoint hard-links unchanged files against, but can no longer be rolled back to.
func (r *Runtime) DiscardContainerCheckpoint(id string) {
	r.mu.Lock()
	ck := r.whole[id]
	delete(r.whole, id)
	old := r.wholeBase[id]
	if ck != nil {
		r.wholeBase[id] = &wholeCkpt{fs: ck.fs, seq: ck.seq}
	}
	r.mu.Unlock()
	if ck != nil {
		go func() {
			_ = os.RemoveAll(ck.img)
			_ = os.RemoveAll(ck.img + "-restore")
			if old != nil {
				_ = os.RemoveAll(old.fs)
			}
		}()
	}
}

// AbandonContainer ends the execs a sandbox that could not be restored was running: each gets
// exit status 137, as if killed, so whoever waits on them stops waiting.
func (r *Runtime) AbandonContainer(id string) {
	dir := r.ctlDir(id)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if eid, ok := strings.CutSuffix(e.Name(), ".out"); ok {
			if _, err := os.Stat(filepath.Join(dir, eid+".status")); err != nil {
				_ = os.WriteFile(filepath.Join(dir, eid+".status"), []byte("137"), 0o644)
				poke(filepath.Join(dir, eid+".done"))
			}
		}
	}
}

// forgetWhole drops the sandbox's whole checkpoint and its control directory.
func (r *Runtime) forgetWhole(id string) {
	r.mu.Lock()
	ck := r.whole[id]
	rolled := r.rolled[id]
	delete(r.whole, id)
	delete(r.wholeBase, id)
	delete(r.rolled, id)
	delete(r.cpuBase, id)
	r.mu.Unlock()
	go func() {
		for _, c := range []*wholeCkpt{ck, rolled} {
			if c != nil {
				_ = os.RemoveAll(c.img)
				_ = os.RemoveAll(c.img + "-restore")
			}
		}
		_ = os.RemoveAll(filepath.Join(r.criuDir(), id))
		pokeAll(r.ctlDir(id), ".done") // execs waiting on the sandbox stop waiting
		_ = os.RemoveAll(r.ctlDir(id))
	}()
}
