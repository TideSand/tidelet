// Package containerd implements the Tidelet runtime on top of containerd
// and an OCI runtime such as runc. Each sandbox is one containerd
// container with one task, created in a dedicated containerd namespace
// and labelled so Tidelet can find its own containers after a restart.
//
// The daemon owns the task's standard streams: containerd FIFOs are read
// by an ioHub that writes the CRI log file and feeds attached clients.
// After a daemon restart the runtime re-attaches to the existing FIFOs.
package containerd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/cgroups/v3/cgroup1/stats"
	v2stats "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/TideSand/tidelet/pkg/log/crilog"
	"github.com/TideSand/tidelet/pkg/runtime"
	"github.com/TideSand/tidelet/pkg/runtime/backend/swapin"
	"github.com/TideSand/tidelet/pkg/runtime/network"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

const (
	// Name is the runtime's registered name.
	Name = "containerd"

	labelManaged = "io.tidelet.managed"
	labelName    = "io.tidelet.name"
	labelNetNS   = "io.tidelet.netns"
	labelIPs     = "io.tidelet.ips"
	labelRuntime = "io.tidelet.runtime"

	// DefaultMicroVMHandler is the containerd runtime handler for Kata
	// Containers.
	DefaultMicroVMHandler = "io.containerd.kata.v2"

	// DefaultAddress is the containerd socket on most distributions.
	DefaultAddress = "/run/containerd/containerd.sock"
	// DefaultNamespace keeps Tidelet's containers apart from everything
	// else on the host.
	DefaultNamespace = "tidelet"

	// defaultCPUPeriodUS is used when a quota is given without a period.
	defaultCPUPeriodUS = 100_000
)

// Config configures the containerd runtime.
type Config struct {
	Address   string
	Namespace string
	// LogDir is where per-sandbox CRI log files are written.
	LogDir string
	// FIFODir is where the task FIFOs live. It must survive daemon
	// restarts (but need not survive reboots).
	FIFODir string
	// Snapshotter overrides containerd's default snapshotter.
	Snapshotter string
	// RuntimeHandler overrides the OCI runtime for container sandboxes.
	// Empty means containerd's default (runc).
	RuntimeHandler string
	// MicroVMHandler is the runtime handler used for microVM sandboxes,
	// typically DefaultMicroVMHandler. Empty disables the microVM runtime
	// class. The matching shim binary must be on containerd's PATH.
	MicroVMHandler string
	// Network attaches bridged sandboxes. Nil disables bridge and join.
	Network *network.Manager
	// CgroupParent is the cgroup every container sandbox is created under,
	// as an OCI cgroups path (for the cgroup v2 fs driver, a path below
	// /sys/fs/cgroup). Empty means containerd's default, "/<namespace>", so
	// the sandboxes of a daemon sit together under /sys/fs/cgroup/<namespace>
	// either way; see host.Scope for what that allows.
	CgroupParent string
	// CheckpointDir holds the sandboxes' filesystem checkpoints (default
	// DefaultCheckpointDir).
	CheckpointDir string
	// RollbackExclude lists rootfs paths (relative, a trailing * for a
	// prefix) a rollback leaves alone; nil means the default set.
	RollbackExclude []string
	// KeepProcesses names processes a rollback never kills; nil means the
	// default set (the agent).
	KeepProcesses []string
	// InitBinary, when set, makes every container sandbox checkpointable as a whole: this
	// host path (a static tidelet-init) runs as the sandbox's init, and execs are started by
	// it, so CRIU can take the agent with its tool calls (see wholeckpt.go).
	InitBinary string
	// CtlDir holds each checkpointable sandbox's exec control directory (default
	// /run/tidelet/ctl); CRIUDir the CRIU images (default <CheckpointDir>/criu; a tmpfs
	// makes dumps and restores quick).
	CtlDir  string
	CRIUDir string
	Logger  *slog.Logger
}

// taskIO is a loaded task together with the hub that owns its streams.
type taskIO struct {
	task client.Task
	hub  *ioHub
}

// Runtime is the containerd-backed runtime.
type Runtime struct {
	cfg    Config
	client *client.Client
	log    *slog.Logger

	mu    sync.Mutex
	tasks map[string]*taskIO
	ckpts map[string]*checkpoint
	whole map[string]*wholeCkpt
	// wholeBase: the writable-layer copy of a discarded whole checkpoint, kept only as the
	// base the next one hard-links against.
	wholeBase map[string]*wholeCkpt
	// rolled: the checkpoint a stopped sandbox will be restored from.
	rolled map[string]*wholeCkpt
	// pausing: sandboxes with a Freeze in flight. A cgroup found frozen with no pause in
	// flight and no Paused task (a pause that timed out after its revert, a dump's leftover)
	// is thawed by the sampler, since nothing else would ever see it.
	pausing map[string]bool
	// lastCg: the cgroup settings last applied by Update, per sandbox. An Update that
	// changes only the soft settings (cpu.weight, memory.high, memory.low) is written to the
	// cgroup files directly: a runc update forks a process and takes a second or more on a
	// saturated host, and a policy reshaping hundreds of sandboxes a second fell minutes behind.
	lastCg map[string]runtime.Cgroup
	// rolling: sandboxes with a rollback in flight, whose latest checkpoint must survive it.
	rolling map[string]bool
	// scanPaths: the working directories of the checkpointable sandboxes created so far, each
	// an irmap scan path in the CRIU configuration.
	scanPaths map[string]bool
	// cpuBase: CPU usage and throttling (ns) of a sandbox's tasks that rollbacks deleted.
	cpuBase map[string][2]uint64
	// noReflink is set once a reflink copy failed: checkpoints use rsync from then on.
	noReflink bool
}

var _ runtime.Runtime = (*Runtime)(nil)

// New connects to containerd.
func New(ctx context.Context, cfg Config) (*Runtime, error) {
	if cfg.Address == "" {
		cfg.Address = DefaultAddress
	}
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
	if cfg.LogDir == "" || cfg.FIFODir == "" {
		return nil, errors.New("containerd runtime: LogDir and FIFODir are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	for _, dir := range []string{cfg.LogDir, cfg.FIFODir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	c, err := client.New(cfg.Address,
		client.WithDefaultNamespace(cfg.Namespace),
		client.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to containerd at %s: %w", cfg.Address, err)
	}
	if _, err := c.Version(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("containerd at %s not serving: %w", cfg.Address, err)
	}
	if cfg.InitBinary != "" {
		if err := ensureCRIUConfig(); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("write %s: %w", criuConfig, err)
		}
	}
	r := &Runtime{
		cfg:       cfg,
		client:    c,
		log:       cfg.Logger.With("runtime", Name),
		tasks:     map[string]*taskIO{},
		ckpts:     map[string]*checkpoint{},
		whole:     map[string]*wholeCkpt{},
		wholeBase: map[string]*wholeCkpt{},
		scanPaths: map[string]bool{},
		rolled:    map[string]*wholeCkpt{}, rolling: map[string]bool{},
		cpuBase: map[string][2]uint64{},
	}
	if cfg.MicroVMHandler != "" {
		if bin, err := shimBinary(cfg.MicroVMHandler); err != nil {
			r.log.Warn("microVM runtime class disabled: shim not found", "handler", cfg.MicroVMHandler, "binary", bin)
			r.cfg.MicroVMHandler = ""
		} else {
			r.log.Info("microVM runtime class enabled", "handler", cfg.MicroVMHandler)
		}
	}
	return r, nil
}

// shimBinary checks that the shim for a runtime v2 handler such as
// io.containerd.kata.v2 (binary containerd-shim-kata-v2) is installed.
func shimBinary(handler string) (string, error) {
	parts := strings.Split(handler, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid runtime handler %q", handler)
	}
	bin := "containerd-shim-" + parts[len(parts)-2] + "-" + parts[len(parts)-1]
	_, err := exec.LookPath(bin)
	return bin, err
}

// Name implements runtime.Runtime.
func (r *Runtime) Name() string { return Name }

// Validate implements runtime.Runtime.
func (r *Runtime) Validate(spec sandbox.Spec) error {
	if spec.Runtime == sandbox.RuntimeMicroVM {
		if r.cfg.MicroVMHandler == "" {
			return errors.New("microVM runtime class is not available on this node")
		}
		if spec.Network == sandbox.NetworkHost {
			return errors.New("microVM sandboxes cannot use host networking")
		}
	}
	if (spec.Network == sandbox.NetworkBridge || spec.Network == sandbox.NetworkJoin) && r.cfg.Network == nil {
		return fmt.Errorf("network mode %s requires CNI, which is not configured", spec.Network)
	}
	return nil
}

// Close implements runtime.Runtime.
func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, t := range r.tasks {
		// Stop copying from the FIFOs without closing the sandbox's
		// stdin; the next daemon re-attaches.
		t.task.IO().Cancel()
		t.hub.close()
		delete(r.tasks, id)
	}
	return r.client.Close()
}

// LogPath implements runtime.Runtime.
func (r *Runtime) LogPath(id string) string {
	return filepath.Join(r.cfg.LogDir, id+".log")
}

func (r *Runtime) ensureImage(ctx context.Context, ref string) (client.Image, error) {
	img, err := r.client.GetImage(ctx, ref)
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("get image %s: %w", ref, err)
	}
	if err != nil {
		r.log.Info("pulling image", "ref", ref)
		pullOpts := []client.RemoteOpt{client.WithPullUnpack}
		if r.cfg.Snapshotter != "" {
			pullOpts = append(pullOpts, client.WithPullSnapshotter(r.cfg.Snapshotter))
		}
		img, err = r.client.Pull(ctx, ref, pullOpts...)
		if err != nil {
			return nil, fmt.Errorf("pull image %s: %w", ref, err)
		}
	}
	unpacked, err := img.IsUnpacked(ctx, r.cfg.Snapshotter)
	if err != nil {
		return nil, fmt.Errorf("check image %s unpacked: %w", ref, err)
	}
	if !unpacked {
		if err := img.Unpack(ctx, r.cfg.Snapshotter); err != nil {
			return nil, fmt.Errorf("unpack image %s: %w", ref, err)
		}
	}
	return img, nil
}

// Create implements runtime.Runtime.
func (r *Runtime) Create(ctx context.Context, sb *sandbox.Sandbox, cg runtime.Cgroup) (retErr error) {
	img, err := r.ensureImage(ctx, sb.Spec.Image)
	if err != nil {
		return err
	}
	specOpts := buildSpecOpts(sb, img, cg, r.cfg.CgroupParent)
	if wd := sb.Spec.WorkingDir; wd != "" && r.ContainerCheckpoints() && sb.Spec.Runtime != sandbox.RuntimeMicroVM {
		r.addScanPath(wd) // CRIU must be able to find the agent's inotify watches on its working directory
	}
	if r.ContainerCheckpoints() && sb.Spec.Runtime != sandbox.RuntimeMicroVM {
		extra, err := r.inTreeSpecOpts(sb.ID)
		if err != nil {
			return fmt.Errorf("checkpointable sandbox: %w", err)
		}
		specOpts = append(specOpts, extra...)
	}
	labels := map[string]string{labelManaged: "true", labelName: sb.Name, labelRuntime: string(sb.Spec.Runtime)}
	for k, v := range sb.Spec.Labels {
		labels[k] = v
	}

	netOpts, netStatus, err := r.setupNetwork(ctx, sb)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil && netStatus != nil && netStatus.NamespacePath != "" {
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := r.cfg.Network.Teardown(cctx, sb.ID, netStatus.NamespacePath); err != nil {
				r.log.Warn("network cleanup after failed create", "id", sb.ID, "err", err)
			}
		}
	}()
	specOpts = append(specOpts, netOpts...)
	if netStatus != nil {
		labels[labelNetNS] = netStatus.NamespacePath
		labels[labelIPs] = strings.Join(netStatus.IPs, ",")
	}

	// Order matters: the snapshot must exist before WithNewSpec runs, since
	// WithImageConfig resolves the image's user against the rootfs.
	ctrOpts := []client.NewContainerOpts{
		client.WithImage(img),
		client.WithContainerLabels(labels),
	}
	if r.cfg.Snapshotter != "" {
		ctrOpts = append(ctrOpts, client.WithSnapshotter(r.cfg.Snapshotter))
	}
	ctrOpts = append(ctrOpts,
		client.WithNewSnapshot(sb.ID, img),
		client.WithNewSpec(specOpts...),
	)
	switch {
	case sb.Spec.Runtime == sandbox.RuntimeMicroVM:
		ctrOpts = append(ctrOpts, client.WithRuntime(r.cfg.MicroVMHandler, nil))
	case r.cfg.RuntimeHandler != "":
		ctrOpts = append(ctrOpts, client.WithRuntime(r.cfg.RuntimeHandler, nil))
	}

	ctr, err := r.client.NewContainer(ctx, sb.ID, ctrOpts...)
	if err != nil {
		return fmt.Errorf("create container: %w", err)
	}
	defer func() {
		if retErr != nil {
			// Use a fresh context: the caller's may already be cancelled.
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := ctr.Delete(cctx, client.WithSnapshotCleanup); err != nil {
				r.log.Warn("cleanup after failed create", "id", sb.ID, "err", err)
			}
		}
	}()

	hub, err := newIOHub(sb.ID, r.LogPath(sb.ID), sb.Spec.TTY, sb.Spec.Stdin)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	stdin, stdout, stderr := hub.streams()
	ioOpts := []cio.Opt{cio.WithStreams(stdin, stdout, stderr), cio.WithFIFODir(r.cfg.FIFODir)}
	if sb.Spec.TTY {
		ioOpts = append(ioOpts, cio.WithTerminal)
	}
	task, err := ctr.NewTask(ctx, cio.NewCreator(ioOpts...))
	if err != nil {
		hub.close()
		return fmt.Errorf("create task: %w", err)
	}
	r.mu.Lock()
	r.tasks[sb.ID] = &taskIO{task: task, hub: hub}
	r.mu.Unlock()
	sb.Status.Network = netStatus
	r.mu.Lock()
	if r.lastCg == nil {
		r.lastCg = map[string]runtime.Cgroup{}
	}
	r.lastCg[sb.ID] = cg // the settings the container was created with: a later soft-only Update takes the direct path
	r.mu.Unlock()
	return nil
}

// setupNetwork prepares the network namespace and the OCI options that
// attach the sandbox to it.
func (r *Runtime) setupNetwork(ctx context.Context, sb *sandbox.Sandbox) ([]oci.SpecOpts, *sandbox.NetworkStatus, error) {
	hostname := sb.Hostname()
	switch sb.Spec.Network {
	case "", sandbox.NetworkNone:
		// containerd's default spec creates a fresh network namespace.
		return nil, nil, nil
	case sandbox.NetworkHost:
		return []oci.SpecOpts{
			oci.WithHostNamespace(specs.NetworkNamespace),
			oci.WithHostHostsFile,
			oci.WithHostResolvconf,
		}, nil, nil
	case sandbox.NetworkBridge:
		if r.cfg.Network == nil {
			return nil, nil, errors.New("bridge networking is not configured")
		}
		att, err := r.cfg.Network.Setup(ctx, sb.ID, hostname)
		if err != nil {
			return nil, nil, err
		}
		return joinNamespaceOpts(att.NamespacePath, att.HostsFile, att.ResolvConf),
			&sandbox.NetworkStatus{NamespacePath: att.NamespacePath, IPs: att.IPs}, nil
	case sandbox.NetworkJoin:
		if r.cfg.Network == nil {
			return nil, nil, errors.New("join networking is not configured")
		}
		hosts, resolv, err := r.cfg.Network.Files(sb.Spec.NetworkNamespace)
		if err != nil {
			return nil, nil, err
		}
		return joinNamespaceOpts(sb.Spec.NetworkNamespace, hosts, resolv), nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported network mode %q", sb.Spec.Network)
	}
}

func joinNamespaceOpts(nsPath, hostsFile, resolvConf string) []oci.SpecOpts {
	return []oci.SpecOpts{
		oci.WithLinuxNamespace(specs.LinuxNamespace{Type: specs.NetworkNamespace, Path: nsPath}),
		oci.WithMounts([]specs.Mount{
			{Type: "bind", Source: hostsFile, Destination: "/etc/hosts", Options: []string{"rbind", "ro"}},
			{Type: "bind", Source: resolvConf, Destination: "/etc/resolv.conf", Options: []string{"rbind", "ro"}},
		}),
	}
}

// withOOMScoreAdj gives the container's processes the OOM score adjustment
// Docker gives them (0). Without it they inherit the shim's -998, every
// process's badness is clamped alike, and a memory-cgroup OOM may kill the
// agent (claude) instead of the tool that grew: a recorded task that ran on
// after its tool was killed then dies in the replay. The exec'd processes
// copy the container's process spec, so they get it too.
func withOOMScoreAdj(adj int) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		if s.Process == nil {
			s.Process = &specs.Process{}
		}
		s.Process.OOMScoreAdj = &adj
		return nil
	}
}

func buildSpecOpts(sb *sandbox.Sandbox, img client.Image, cg runtime.Cgroup, cgroupParent string) []oci.SpecOpts {
	spec := sb.Spec
	opts := []oci.SpecOpts{oci.WithDefaultUnixDevices, withOOMScoreAdj(0)}
	if cgroupParent != "" && spec.Runtime != sandbox.RuntimeMicroVM {
		opts = append(opts, oci.WithCgroup(path.Join(cgroupParent, sb.ID)))
	}
	switch {
	case len(spec.Command) > 0:
		opts = append(opts, oci.WithImageConfig(img), oci.WithProcessArgs(append(spec.Command, spec.Args...)...))
	case len(spec.Args) > 0:
		opts = append(opts, oci.WithImageConfigArgs(img, spec.Args))
	default:
		opts = append(opts, oci.WithImageConfig(img))
	}
	if len(spec.Env) > 0 {
		opts = append(opts, oci.WithEnv(sortedEnv(spec.Env)))
	}
	if spec.TTY {
		opts = append(opts, oci.WithTTY)
		if _, ok := spec.Env["TERM"]; !ok {
			opts = append(opts, oci.WithEnv([]string{"TERM=xterm"}))
		}
	}
	if spec.WorkingDir != "" {
		opts = append(opts, oci.WithProcessCwd(spec.WorkingDir))
	}
	opts = append(opts, oci.WithHostname(sb.Hostname()))
	if spec.Runtime != sandbox.RuntimeMicroVM {
		// Private cgroup namespace plus a read-only cgroupfs mount so the
		// sandbox sees its own limits at /sys/fs/cgroup, as Docker does on
		// cgroup v2 hosts. containerd's default spec has neither. Inside a
		// microVM the guest kernel provides its own view.
		opts = append(opts,
			oci.WithLinuxNamespace(specs.LinuxNamespace{Type: specs.CgroupNamespace}),
			oci.WithMounts([]specs.Mount{{
				Type:        "cgroup",
				Source:      "cgroup",
				Destination: "/sys/fs/cgroup",
				Options:     []string{"nosuid", "noexec", "nodev", "relatime", "ro"},
			}}),
		)
	}

	if len(spec.Mounts) > 0 {
		mounts := make([]specs.Mount, 0, len(spec.Mounts))
		for _, m := range spec.Mounts {
			mode := "rw"
			if m.ReadOnly {
				mode = "ro"
			}
			mounts = append(mounts, specs.Mount{
				Type:        "bind",
				Source:      m.HostPath,
				Destination: m.ContainerPath,
				Options:     []string{"rbind", mode},
			})
		}
		opts = append(opts, oci.WithMounts(mounts))
	}
	opts = append(opts, func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		if s.Linux == nil {
			s.Linux = &specs.Linux{}
		}
		s.Linux.Resources = linuxResources(cg)
		return nil
	})
	return opts
}

func sortedEnv(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}
	return out
}

// linuxResources translates runtime.Cgroup into OCI Linux resources. runc
// maps these onto cgroup v2 (cpu.max, cpu.weight, memory.max,
// memory.high, memory.swap.max).
func linuxResources(cg runtime.Cgroup) *specs.LinuxResources {
	res := &specs.LinuxResources{}
	if cg.CPUQuotaUS > 0 || cg.CPUShares > 0 {
		res.CPU = &specs.LinuxCPU{}
		if cg.CPUQuotaUS > 0 {
			quota := cg.CPUQuotaUS
			period := cg.CPUPeriodUS
			if period == 0 {
				period = defaultCPUPeriodUS
			}
			res.CPU.Quota = &quota
			res.CPU.Period = &period
		}
		if cg.CPUShares > 0 {
			shares := cg.CPUShares
			res.CPU.Shares = &shares
		}
	}
	if cg.MemoryLimitBytes > 0 || cg.MemoryHighBytes > 0 || cg.MemorySwapLimitBytes != 0 {
		res.Memory = &specs.LinuxMemory{}
		if cg.MemoryLimitBytes > 0 {
			limit := cg.MemoryLimitBytes
			res.Memory.Limit = &limit
			switch {
			case cg.MemorySwapLimitBytes < 0:
				// OCI "swap" is memory+swap; equal to the limit means no swap.
				res.Memory.Swap = &limit
			case cg.MemorySwapLimitBytes > 0:
				swap := limit + cg.MemorySwapLimitBytes
				res.Memory.Swap = &swap
			}
		}
		if cg.MemoryHighBytes > 0 {
			res.Unified = map[string]string{"memory.high": strconv.FormatInt(cg.MemoryHighBytes, 10)}
		}
	}
	if low := memoryLow(cg.MemoryLowBytes); low != "" {
		if res.Unified == nil {
			res.Unified = map[string]string{}
		}
		res.Unified["memory.low"] = low
	}
	return res
}

// memoryLow is the memory.low value for a Cgroup.MemoryLowBytes, "" when
// the file is to be left alone.
func memoryLow(v int64) string {
	switch {
	case v == runtime.MemoryLowMax:
		return "max"
	case v == runtime.MemoryLowNone:
		return "0"
	case v > 0:
		return strconv.FormatInt(v, 10)
	}
	return ""
}

// errNoTask means the container exists but has no task (never created or
// already deleted).
var errNoTask = errors.New("container has no task")

// load returns the task and hub for id, re-attaching to the FIFOs of a
// task created by a previous daemon if needed.
func (r *Runtime) load(ctx context.Context, id string) (*taskIO, error) {
	r.mu.Lock()
	if t, ok := r.tasks[id]; ok {
		r.mu.Unlock()
		return t, nil
	}
	if _, stopped := r.rolled[id]; stopped {
		// Stopped by a whole-sandbox rollback, possibly being restored right now: there is no
		// task to attach to (attaching to the one a restore is creating races the restore).
		r.mu.Unlock()
		return nil, errNoTask
	}
	r.mu.Unlock()

	ctr, err := r.client.LoadContainer(ctx, id)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, runtime.ErrNotFound
		}
		return nil, fmt.Errorf("load container: %w", err)
	}
	var hub *ioHub
	task, err := ctr.Task(ctx, func(fifos *cio.FIFOSet) (cio.IO, error) {
		var herr error
		hub, herr = newIOHub(id, r.LogPath(id), fifos.Terminal, fifos.Stdin != "")
		if herr != nil {
			return nil, herr
		}
		stdin, stdout, stderr := hub.streams()
		return cio.NewAttach(cio.WithStreams(stdin, stdout, stderr))(fifos)
	})
	if err != nil {
		if hub != nil {
			hub.close()
		}
		if errdefs.IsNotFound(err) {
			return nil, errNoTask
		}
		return nil, fmt.Errorf("load task: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.tasks[id]; ok {
		// Lost a race with a concurrent load; keep the first attachment.
		task.IO().Cancel()
		hub.close()
		return existing, nil
	}
	t := &taskIO{task: task, hub: hub}
	r.tasks[id] = t
	r.log.Info("re-attached to task", "id", id)
	return t, nil
}

func (r *Runtime) forget(id string) {
	r.mu.Lock()
	delete(r.lastCg, id)
	defer r.mu.Unlock()
	if t, ok := r.tasks[id]; ok {
		t.hub.close()
		delete(r.tasks, id)
	}
}

// Start implements runtime.Runtime.
func (r *Runtime) Start(ctx context.Context, id string) error {
	t, err := r.load(ctx, id)
	if err != nil {
		return err
	}
	if err := t.task.Start(ctx); err != nil {
		return fmt.Errorf("start task: %w", err)
	}
	return nil
}

// Wait implements runtime.Runtime.
func (r *Runtime) Wait(ctx context.Context, id string) (runtime.ExitResult, error) {
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, errNoTask) {
			return runtime.ExitResult{}, runtime.ErrNotRunning
		}
		return runtime.ExitResult{}, err
	}
	exitCh, err := t.task.Wait(ctx)
	if err != nil {
		return runtime.ExitResult{}, fmt.Errorf("wait: %w", err)
	}
	select {
	case <-ctx.Done():
		return runtime.ExitResult{}, ctx.Err()
	case st := <-exitCh:
		code, at, err := st.Result()
		// Let the output copiers drain the FIFOs, then release attached
		// clients and flush the log.
		t.task.IO().Wait()
		t.hub.close()
		return runtime.ExitResult{ExitCode: int32(code), ExitedAt: at, Err: err}, nil
	}
}

// Status implements runtime.Runtime.
func (r *Runtime) Status(ctx context.Context, id string) (runtime.Status, error) {
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, errNoTask) {
			return runtime.Status{State: runtime.StateUnknown}, nil
		}
		return runtime.Status{}, err
	}
	st, err := t.task.Status(ctx)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return runtime.Status{State: runtime.StateUnknown}, nil
		}
		return runtime.Status{}, fmt.Errorf("task status: %w", err)
	}
	out := runtime.Status{Pid: t.task.Pid(), ExitCode: int32(st.ExitStatus), ExitedAt: st.ExitTime}
	switch st.Status {
	case client.Created:
		out.State = runtime.StateCreated
	case client.Running:
		out.State = runtime.StateRunning
	case client.Pausing, client.Paused:
		out.State = runtime.StatePaused
	case client.Stopped:
		out.State = runtime.StateStopped
	default:
		out.State = runtime.StateUnknown
	}
	return out, nil
}

// Stop implements runtime.Runtime.
func (r *Runtime) Stop(ctx context.Context, id string, timeout time.Duration) error {
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, runtime.ErrNotFound) || errors.Is(err, errNoTask) {
			return nil
		}
		return err
	}
	st, err := t.task.Status(ctx)
	if err != nil {
		return fmt.Errorf("task status: %w", err)
	}
	switch st.Status {
	case client.Stopped, client.Created:
		return nil
	case client.Paused, client.Pausing:
		// Signals are not delivered to frozen processes.
		if err := t.task.Resume(ctx); err != nil && !errdefs.IsNotFound(err) {
			if !strings.Contains(err.Error(), "not paused") { // the shim alone thought it paused: signals will arrive
				return fmt.Errorf("thaw before stop: %w", err)
			}
		}
	}
	exitCh, err := t.task.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	if err := t.task.Kill(ctx, syscall.SIGTERM); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("send SIGTERM: %w", err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-exitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	r.log.Info("grace period expired, killing", "id", id)
	if err := t.task.Kill(ctx, syscall.SIGKILL, client.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("send SIGKILL: %w", err)
	}
	select {
	case <-exitCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Delete implements runtime.Runtime.
func (r *Runtime) Delete(ctx context.Context, id string) error {
	r.forgetCheckpoints(id)
	r.AbandonContainer(id) // execs still waiting on the sandbox end as killed
	r.forgetWhole(id)
	t, err := r.load(ctx, id)
	switch {
	case errors.Is(err, runtime.ErrNotFound):
		return r.removeLog(id)
	case errors.Is(err, errNoTask):
	case err != nil:
		return err
	default:
		// A frozen cgroup stays frozen after the kill (runc kills through cgroup.kill and
		// does not thaw), and runc refuses to delete what its freezer calls paused, even
		// with no process left. Thaw first; the kill follows at once.
		if r.thawIfFrozen(id) {
			r.log.Debug("thawed frozen cgroup before delete", "id", id)
		}
		if _, err := t.task.Delete(ctx, client.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("delete task: %w", err)
		}
		r.forget(id)
	}
	ctr, err := r.client.LoadContainer(ctx, id)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("load container: %w", err)
	}
	if err == nil {
		labels, _ := ctr.Labels(ctx)
		if err := ctr.Delete(ctx, client.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("delete container: %w", err)
		}
		if nsPath := labels[labelNetNS]; nsPath != "" && r.cfg.Network != nil {
			if err := r.cfg.Network.Teardown(ctx, id, nsPath); err != nil {
				return fmt.Errorf("network teardown: %w", err)
			}
		}
	}
	return r.removeLog(id)
}

func (r *Runtime) removeLog(id string) error {
	if err := os.Remove(r.LogPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove log: %w", err)
	}
	return nil
}

// Update implements runtime.Runtime.
func (r *Runtime) Update(ctx context.Context, id string, cg runtime.Cgroup) error {
	t, err := r.load(ctx, id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	last, known := r.lastCg[id]
	r.mu.Unlock()
	if known && r.cfg.CgroupParent != "" && softOnly(last, cg) {
		if err := r.writeSoft(id, cg); err != nil {
			return err
		}
	} else if err := t.task.Update(ctx, client.WithResources(linuxResources(cg))); err != nil {
		return fmt.Errorf("update resources: %w", err)
	}
	r.mu.Lock()
	if r.lastCg == nil {
		r.lastCg = map[string]runtime.Cgroup{}
	}
	r.lastCg[id] = cg
	r.mu.Unlock()
	return nil
}

// softOnly reports whether b differs from a only in the settings written by writeSoft.
func softOnly(a, b runtime.Cgroup) bool {
	return a.CPUQuotaUS == b.CPUQuotaUS && a.CPUPeriodUS == b.CPUPeriodUS &&
		a.MemoryLimitBytes == b.MemoryLimitBytes && a.MemorySwapLimitBytes == b.MemorySwapLimitBytes
}

// writeSoft puts cpu.weight, memory.low and memory.high on the sandbox's cgroup files,
// the values runc would write for cg, in that order and with one exception: memory.high is
// never written below the cgroup's current use. The kernel makes the writer reclaim the
// excess synchronously, and with the pages going out to a saturated swap device that write
// took 30 s to minutes (a runc update caught in D state in the block layer's write
// throttling), holding every later update of the sandbox behind it. At the current use the
// write returns at once and growth beyond it is throttled as the policy intends; the
// cgroup's own allocations reclaim it down from there.
func (r *Runtime) writeSoft(id string, cg runtime.Cgroup) error {
	dir := filepath.Join("/sys/fs/cgroup", r.cfg.CgroupParent, id)
	write := func(name, value string) error {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o644); err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENODEV) {
				return fmt.Errorf("%w: %s: %v", runtime.ErrNotRunning, name, err)
			}
			return fmt.Errorf("update resources: %s: %w", name, err)
		}
		return nil
	}
	if cg.CPUShares > 0 {
		if err := write("cpu.weight", strconv.FormatUint(sharesToWeight(cg.CPUShares), 10)); err != nil {
			return err
		}
	}
	if low := memoryLow(cg.MemoryLowBytes); low != "" {
		if err := write("memory.low", low); err != nil {
			return err
		}
	}
	if cg.MemoryHighBytes > 0 {
		high := cg.MemoryHighBytes
		if b, err := os.ReadFile(filepath.Join(dir, "memory.current")); err == nil {
			if cur, perr := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); perr == nil && cur > high {
				high = cur
			}
		}
		if err := write("memory.high", strconv.FormatInt(high, 10)); err != nil {
			return err
		}
	}
	return nil
}

// sharesToWeight is runc's conversion of cgroup v1 cpu.shares to cgroup v2 cpu.weight
// (the same curve as the meter's SharesWeight).
func sharesToWeight(shares uint64) uint64 {
	if shares == 0 {
		return 0
	}
	if shares <= 2 {
		return 1
	}
	if shares >= 262144 {
		return 10000
	}
	l := math.Log2(float64(shares))
	exponent := (l*l+125*l)/612.0 - 7.0/34.0
	w := uint64(math.Ceil(math.Pow(10, exponent)))
	if w < 1 {
		w = 1
	}
	if w > 10000 {
		w = 10000
	}
	return w
}

// Prefetch implements runtime.Runtime: every process of the sandbox's task
// has its swapped pages read back (see package swapin).
func (r *Runtime) Prefetch(ctx context.Context, id string) (int64, error) {
	t, err := r.load(ctx, id)
	if err != nil {
		return 0, err
	}
	procs, err := t.task.Pids(ctx)
	if err != nil {
		return 0, fmt.Errorf("list pids: %w", err)
	}
	var total int64
	for _, p := range procs {
		n, err := swapin.Process(int(p.Pid))
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// Reclaim implements runtime.Runtime through the task's cgroup v2
// memory.reclaim file.
func (r *Runtime) Reclaim(ctx context.Context, id string, bytes int64) error {
	t, err := r.load(ctx, id)
	if err != nil {
		return err
	}
	cg, err := swapin.CgroupOf(int(t.task.Pid()))
	if err != nil {
		return err
	}
	if err := swapin.CgroupReclaim(cg, bytes); err != nil {
		if errors.Is(err, syscall.ENODEV) || errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %v", runtime.ErrNotRunning, err) // the cgroup is being removed
		}
		return err
	}
	return nil
}

// Freeze implements runtime.Runtime.
func (r *Runtime) Freeze(ctx context.Context, id string) error {
	t, err := r.load(ctx, id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.pausing == nil {
		r.pausing = map[string]bool{}
	}
	r.pausing[id] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pausing, id)
		r.mu.Unlock()
	}()
	if err := t.task.Pause(ctx); err != nil {
		// runc gives the cgroup 10 s to freeze; tasks in uninterruptible sleep (swapping,
		// reclaim) can take longer. It then reports the failure but leaves cgroup.freeze set,
		// so the shim counts the task as running while runc, which reads the freezer, says
		// paused: nothing can pause, resume or delete it through the shim any more, and the
		// cgroup freezes for good once the tasks get there. Put the freezer back so the
		// states agree, and let the caller try again later. The shim may still say Pausing
		// for an instant after runc's failure: runc has returned, so that pause is over too.
		if strings.Contains(err.Error(), "waiting for the cgroup to freeze") {
			if st, serr := t.task.Status(ctx); serr == nil && st.Status != client.Paused && r.thawIfFrozen(id) {
				return fmt.Errorf("pause: %w (freezer reverted)", err)
			}
		}
		return fmt.Errorf("pause: %w", err)
	}
	return nil
}

// reconcileFreezer thaws a sandbox's cgroup found frozen while no Freeze is in flight
// and the shim does not count the task as paused: the leftover of a pause that froze
// the cgroup after its timeout was reverted. Called from Stats, once a second.
func (r *Runtime) reconcileFreezer(ctx context.Context, t *taskIO, id string) {
	if r.cfg.CgroupParent == "" {
		return
	}
	b, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", r.cfg.CgroupParent, id, "cgroup.freeze"))
	if err != nil || strings.TrimSpace(string(b)) != "1" {
		return
	}
	r.mu.Lock()
	inflight := r.pausing[id]
	r.mu.Unlock()
	if inflight {
		return
	}
	st, err := t.task.Status(ctx)
	if err != nil || st.Status == client.Paused || st.Status == client.Pausing {
		return
	}
	if r.thawIfFrozen(id) {
		r.log.Warn("frozen cgroup with no pause in flight thawed", "id", id, "task", string(st.Status))
	}
}

// Thaw implements runtime.Runtime.
func (r *Runtime) Thaw(ctx context.Context, id string) error {
	t, err := r.load(ctx, id)
	if err != nil {
		return err
	}
	// thaw resumes through the shim and reconciles the shim's state with the freezer's
	// when they disagree; a cgroup left frozen while the shim counts the task as running
	// (a pause that timed out) is thawed directly.
	if err := r.thaw(ctx, t); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	r.thawIfFrozen(id)
	return nil
}

// Stats implements runtime.Runtime.
func (r *Runtime) Stats(ctx context.Context, id string) (sandbox.Stats, error) {
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, errNoTask) {
			return sandbox.Stats{}, runtime.ErrNotRunning
		}
		return sandbox.Stats{}, err
	}
	r.reconcileFreezer(ctx, t, id)
	// The sandbox's cgroup files first: containerd's Metrics RPC goes through the shim, which
	// answers in tens of seconds on a saturated host (the sampler then ran minutes behind
	// and so did every decision taken on its samples). The files cost microseconds.
	if r.cfg.CgroupParent != "" {
		cgdir := filepath.Join("/sys/fs/cgroup", r.cfg.CgroupParent, id)
		if out, cerr := cgroupStats(cgdir); cerr == nil {
			if b, perr := os.ReadFile(filepath.Join(cgdir, "pids.current")); perr == nil {
				if v, verr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); verr == nil {
					out.Pids = v
				}
			}
			out.CPUSomeStallUSec = stallTotal(cgdir + "/cpu.pressure")
			out.MemorySomeStallUSec = stallTotal(cgdir + "/memory.pressure")
			out.IOSomeStallUSec = stallTotal(cgdir + "/io.pressure")
			return r.withCPUBase(id, out), nil
		}
	}
	m, err := t.task.Metrics(ctx)
	if err != nil {
		// A task restored from a checkpoint has no metrics from the shim ("no metrics
		// received"): read its cgroup directly.
		cg, cerr := swapin.CgroupOf(int(t.task.Pid()))
		if cerr != nil {
			return sandbox.Stats{}, fmt.Errorf("metrics: %w", err)
		}
		out, cerr := cgroupStats(cg)
		if cerr != nil {
			return sandbox.Stats{}, fmt.Errorf("metrics: %w (cgroup: %v)", err, cerr)
		}
		out.CPUSomeStallUSec = stallTotal(cg + "/cpu.pressure")
		out.MemorySomeStallUSec = stallTotal(cg + "/memory.pressure")
		out.IOSomeStallUSec = stallTotal(cg + "/io.pressure")
		return r.withCPUBase(id, out), nil
	}
	data, err := typeurl.UnmarshalAny(m.Data)
	if err != nil {
		return sandbox.Stats{}, fmt.Errorf("decode metrics: %w", err)
	}
	out := sandbox.Stats{Timestamp: m.Timestamp.AsTime()}
	switch s := data.(type) {
	case *v2stats.Metrics:
		if c := s.GetCPU(); c != nil {
			out.CPUUsageNanos = c.GetUsageUsec() * 1000
			out.CPUThrottledNanos = c.GetThrottledUsec() * 1000
		}
		if mem := s.GetMemory(); mem != nil {
			out.MemoryUsageBytes = mem.GetUsage()
			if l := mem.GetUsageLimit(); l != 0 && l < 1<<62 {
				out.MemoryLimitBytes = l
			}
			out.MemoryWorkingSetBytes = mem.GetUsage() - min(mem.GetInactiveFile(), mem.GetUsage())
			out.MemorySwapUsageBytes = mem.GetSwapUsage() // memory.swap.current
		}
		if p := s.GetPids(); p != nil {
			out.Pids = p.GetCurrent()
		}
	case *stats.Metrics:
		if c := s.GetCPU(); c != nil && c.GetUsage() != nil {
			out.CPUUsageNanos = c.GetUsage().GetTotal()
		}
		if c := s.GetCPU(); c != nil && c.GetThrottling() != nil {
			out.CPUThrottledNanos = c.GetThrottling().GetThrottledTime()
		}
		if mem := s.GetMemory(); mem != nil && mem.GetUsage() != nil {
			out.MemoryUsageBytes = mem.GetUsage().GetUsage()
			out.MemoryLimitBytes = mem.GetUsage().GetLimit()
			out.MemoryWorkingSetBytes = mem.GetUsage().GetUsage() - min(mem.GetTotalInactiveFile(), mem.GetUsage().GetUsage())
			// cgroup v1 counts memory+swap together in memsw; swap is the difference.
			if sw := mem.GetSwap().GetUsage(); sw > mem.GetUsage().GetUsage() {
				out.MemorySwapUsageBytes = sw - mem.GetUsage().GetUsage()
			}
		}
		if p := s.GetPids(); p != nil {
			out.Pids = p.GetCurrent()
		}
	default:
		return sandbox.Stats{}, fmt.Errorf("unexpected metrics type %T", data)
	}
	// Pressure stall totals are not in containerd's metrics; they come from the
	// task's own cgroup. A microVM task's cgroup holds the hypervisor, whose
	// stalls still say how the guest fares.
	if cg, err := swapin.CgroupOf(int(t.task.Pid())); err == nil {
		out.CPUSomeStallUSec = stallTotal(cg + "/cpu.pressure")
		out.MemorySomeStallUSec = stallTotal(cg + "/memory.pressure")
		out.IOSomeStallUSec = stallTotal(cg + "/io.pressure")
	}
	return r.withCPUBase(id, out), nil
}

// withCPUBase adds the CPU a sandbox used in its earlier incarnations: a whole-sandbox
// rollback deletes its task, and with it the cgroup whose counters start again at zero on the
// restore; the sandbox's CPU counters must not go backwards.
func (r *Runtime) withCPUBase(id string, s sandbox.Stats) sandbox.Stats {
	r.mu.Lock()
	b := r.cpuBase[id]
	r.mu.Unlock()
	s.CPUUsageNanos += b[0]
	s.CPUThrottledNanos += b[1]
	return s
}

// cgroupStats reads a cgroup v2 directory's counters into Stats.
func cgroupStats(cg string) (sandbox.Stats, error) {
	out := sandbox.Stats{Timestamp: time.Now()}
	kv := func(file string) map[string]uint64 {
		m := map[string]uint64{}
		data, err := os.ReadFile(filepath.Join(cg, file))
		if err != nil {
			return m
		}
		for _, line := range strings.Split(string(data), "\n") {
			if f := strings.Fields(line); len(f) == 2 {
				if v, err := strconv.ParseUint(f[1], 10, 64); err == nil {
					m[f[0]] = v
				}
			}
		}
		return m
	}
	one := func(file string) (uint64, bool) {
		data, err := os.ReadFile(filepath.Join(cg, file))
		if err != nil {
			return 0, false
		}
		v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		return v, err == nil
	}
	cpu := kv("cpu.stat")
	if _, ok := cpu["usage_usec"]; !ok {
		return out, fmt.Errorf("no cpu.stat in %s", cg)
	}
	out.CPUUsageNanos = cpu["usage_usec"] * 1000
	out.CPUThrottledNanos = cpu["throttled_usec"] * 1000
	if v, ok := one("memory.current"); ok {
		out.MemoryUsageBytes = v
		out.MemoryWorkingSetBytes = v - min(kv("memory.stat")["inactive_file"], v)
	}
	if v, ok := one("memory.max"); ok && v < 1<<62 {
		out.MemoryLimitBytes = v
	}
	if v, ok := one("memory.swap.current"); ok {
		out.MemorySwapUsageBytes = v
	}
	if v, ok := one("pids.current"); ok {
		out.Pids = v
	}
	return out, nil
}

// stallTotal is the cumulative "some" stall time in microseconds of a
// cgroup v2 pressure file, 0 when it cannot be read.
func stallTotal(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "some ") {
			continue
		}
		for _, kv := range strings.Fields(line)[1:] {
			if v, ok := strings.CutPrefix(kv, "total="); ok {
				n, _ := strconv.ParseUint(v, 10, 64)
				return n
			}
		}
	}
	return 0
}

// Exec implements runtime.Runtime.
func (r *Runtime) Exec(ctx context.Context, id string, opts runtime.ExecOptions) (runtime.ExecResult, error) {
	if len(opts.Command) == 0 {
		return runtime.ExecResult{}, errors.New("exec: empty command")
	}
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, errNoTask) {
			return runtime.ExecResult{}, runtime.ErrNotRunning
		}
		return runtime.ExecResult{}, err
	}
	if r.ContainerCheckpoints() && !opts.TTY {
		return r.execInTree(ctx, id, t, opts)
	}
	spec, err := t.task.Spec(ctx)
	if err != nil {
		return runtime.ExecResult{}, fmt.Errorf("task spec: %w", err)
	}
	pspec := *spec.Process
	pspec.Terminal = opts.TTY
	pspec.Args = opts.Command
	if opts.WorkingDir != "" {
		pspec.Cwd = opts.WorkingDir
	}
	env := append([]string(nil), pspec.Env...)
	if opts.TTY && !hasEnv(env, "TERM") && opts.Env["TERM"] == "" {
		env = append(env, "TERM=xterm")
	}
	env = append(env, sortedEnv(opts.Env)...)
	pspec.Env = env

	stdout, stderr := opts.Stdout, opts.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	execID := "exec-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	// Detach the exec lifecycle from ctx so that we can still clean up the
	// process when ctx expires or is cancelled. The lifecycle context has no
	// deadline: the process may run for hours, and containerd's Wait is an
	// RPC on this context, so a deadline here would lose the exit status of
	// any exec outliving it. Cleanup calls get their own short deadlines.
	bg, cancelBG := context.WithCancel(context.Background())
	defer cancelBG()
	cleanupCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 60*time.Second)
	}

	var procRef procHolder
	var stdin io.Reader
	if opts.Stdin != nil {
		// The shim keeps the process's stdin open until CloseIO is called,
		// so signal EOF explicitly once the caller's reader is drained.
		stdin = &eofNotifier{Reader: opts.Stdin, onEOF: func() {
			if p := procRef.get(); p != nil {
				cctx, cancel := cleanupCtx()
				defer cancel()
				_ = p.CloseIO(cctx, client.WithStdinCloser)
			}
		}}
	}
	ioOpts := []cio.Opt{cio.WithStreams(stdin, stdout, stderr), cio.WithFIFODir(r.cfg.FIFODir)}
	if opts.TTY {
		ioOpts = append(ioOpts, cio.WithTerminal)
	}
	proc, err := t.task.Exec(bg, execID, &pspec, cio.NewCreator(ioOpts...))
	if err != nil {
		return runtime.ExecResult{}, fmt.Errorf("exec create: %w", err)
	}
	procRef.set(proc)
	defer func() {
		cctx, cancel := cleanupCtx()
		defer cancel()
		if _, err := proc.Delete(cctx); err != nil && !errdefs.IsNotFound(err) {
			r.log.Warn("delete exec process", "id", id, "exec", execID, "err", err)
		}
	}()
	exitCh, err := proc.Wait(bg)
	if err != nil {
		return runtime.ExecResult{}, fmt.Errorf("exec wait: %w", err)
	}
	if err := proc.Start(bg); err != nil {
		return runtime.ExecResult{}, fmt.Errorf("exec start: %w", err)
	}
	// The policy's soft cgroup settings again, after the exec: a runc exec applies the
	// container's recorded resources on entry, which can undo the values written to the
	// cgroup files directly (microseconds; the recorded hard limits are untouched).
	r.mu.Lock()
	last, known := r.lastCg[id]
	r.mu.Unlock()
	if known && r.cfg.CgroupParent != "" {
		_ = r.writeSoft(id, last)
	}
	if opts.TTY {
		if opts.TerminalSize != nil {
			_ = proc.Resize(bg, opts.TerminalSize.Width, opts.TerminalSize.Height)
		}
		if opts.Resize != nil {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case sz, ok := <-opts.Resize:
						if !ok {
							return
						}
						_ = proc.Resize(bg, sz.Width, sz.Height)
					}
				}
			}()
		}
	}
	var st client.ExitStatus
	select {
	case st = <-exitCh:
	case <-ctx.Done():
		cctx, cancel := cleanupCtx()
		defer cancel()
		_ = proc.Kill(cctx, syscall.SIGKILL)
		select {
		case st = <-exitCh:
		case <-cctx.Done():
			return runtime.ExecResult{}, fmt.Errorf("exec: process did not die after kill: %w", ctx.Err())
		}
		proc.IO().Wait()
		code, _, _ := st.Result()
		return runtime.ExecResult{ExitCode: int32(code)}, ctx.Err()
	}
	// Flush remaining output before returning.
	proc.IO().Wait()
	code, _, err := st.Result()
	if err != nil {
		return runtime.ExecResult{}, fmt.Errorf("exec: %w", err)
	}
	return runtime.ExecResult{ExitCode: int32(code)}, nil
}

func hasEnv(env []string, key string) bool {
	for _, kv := range env {
		if len(kv) > len(key) && kv[:len(key)] == key && kv[len(key)] == '=' {
			return true
		}
	}
	return false
}

// Attach implements runtime.Runtime.
func (r *Runtime) Attach(ctx context.Context, id string, opts runtime.AttachOptions) error {
	t, err := r.load(ctx, id)
	if err != nil {
		if errors.Is(err, errNoTask) {
			return runtime.ErrNotRunning
		}
		return err
	}
	if opts.Stdin != nil && !t.hub.hasStdin() {
		return runtime.ErrNoStdin
	}
	sub := t.hub.subscribe()
	defer t.hub.unsubscribe(sub)
	if opts.Attached != nil {
		opts.Attached()
	}
	if opts.TerminalSize != nil && t.hub.tty {
		// Fails harmlessly if the process has not started yet; the client
		// sends the size again after StartSandbox.
		_ = t.task.Resize(ctx, opts.TerminalSize.Width, opts.TerminalSize.Height)
	}
	if opts.Resize != nil && t.hub.tty {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sz, ok := <-opts.Resize:
					if !ok {
						return
					}
					_ = t.task.Resize(ctx, sz.Width, sz.Height)
				}
			}
		}()
	}
	if opts.Stdin != nil {
		go func() {
			_, err := io.Copy(stdinWriter{t.hub}, opts.Stdin)
			if err == nil {
				// Clean EOF from the client: close the process's stdin.
				if t.hub.closeStdin() {
					cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_ = t.task.CloseIO(cctx, client.WithStdinCloser)
				}
			}
		}()
	}
	stdout, stderr := opts.Stdout, opts.Stderr
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-sub.ch:
			if !ok {
				if sub.dropped {
					return errSlowSubscriber
				}
				return nil
			}
			w := stdout
			if f.stream == crilog.Stderr {
				w = stderr
			}
			if _, err := w.Write(f.data); err != nil {
				return err
			}
		}
	}
}

type stdinWriter struct{ h *ioHub }

func (w stdinWriter) Write(p []byte) (int, error) { return w.h.writeStdin(p) }

// List implements runtime.Runtime.
func (r *Runtime) List(ctx context.Context) ([]string, error) {
	ctrs, err := r.client.Containers(ctx, fmt.Sprintf("labels.%q==true", labelManaged))
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	ids := make([]string, 0, len(ctrs))
	for _, c := range ctrs {
		ids = append(ids, c.ID())
	}
	sort.Strings(ids)
	return ids, nil
}

// procHolder lets the stdin EOF hook reach the exec process once created.
type procHolder struct {
	mu sync.Mutex
	p  client.Process
}

func (h *procHolder) set(p client.Process) { h.mu.Lock(); h.p = p; h.mu.Unlock() }
func (h *procHolder) get() client.Process  { h.mu.Lock(); defer h.mu.Unlock(); return h.p }

// eofNotifier calls onEOF once when the wrapped reader is exhausted or
// fails.
type eofNotifier struct {
	io.Reader
	onEOF func()
	done  bool
}

func (e *eofNotifier) Read(p []byte) (int, error) {
	n, err := e.Reader.Read(p)
	if err != nil && !e.done {
		e.done = true
		if e.onEOF != nil {
			e.onEOF()
		}
	}
	return n, err
}
