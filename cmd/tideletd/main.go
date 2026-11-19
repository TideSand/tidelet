// Command tideletd is the Tidelet daemon.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"net/url"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/llmproxy"
	"github.com/TideSand/tidelet/pkg/manager"
	"github.com/TideSand/tidelet/pkg/manager/store"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/registry"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/rpc/server"
	"github.com/TideSand/tidelet/pkg/runtime/backend/containerd"
	"github.com/TideSand/tidelet/pkg/runtime/network"
	"github.com/TideSand/tidelet/pkg/version"
)

type kvFlag map[string]string

func (f kvFlag) String() string { return fmt.Sprint(map[string]string(f)) }
func (f kvFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("expected key=value, got %q", s)
	}
	f[k] = v
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tideletd:", err)
		os.Exit(1)
	}
}

func run() error {
	policyOpts := kvFlag{}
	var (
		listen         = flag.String("listen", server.DefaultSocket, "gRPC unix socket path or tcp://host:port")
		httpListen     = flag.String("http-listen", server.DefaultHTTPAddress, "REST gateway host:port (empty disables)")
		stateDir       = flag.String("state-dir", "/var/lib/tidelet", "directory for persistent state and logs")
		wholeCkpts     = flag.Bool("container-checkpoints", false, "make sandboxes checkpointable as a whole (processes, memory, filesystem) with CRIU: tidelet-init, next to tideletd, runs as each sandbox's init and starts its execs; whole-sandbox rollbacks stop a sandbox and restores resume it")
		holdWhole      = flag.Bool("hold-whole", false, "the LLM proxy keeps a model response's whole body back until the policy's decision (for policies that checkpoint before delivery); off, responses stream up to their first tool call")
		criuDir        = flag.String("criu-dir", "", "with --container-checkpoints: where CRIU images go (default <run-dir>/criu, a tmpfs mounted there when it is not one already)")
		snapshotDir    = flag.String("snapshot-dir", "", "with --container-checkpoints: where snapshots keep their CRIU images and the memory their sandboxes share (default <run-dir>/snapshots, a tmpfs mounted there when it is not one already; it must be a tmpfs)")
		checkpointDir  = flag.String("checkpoint-dir", "", "directory for filesystem checkpoints (default <state-dir>/checkpoints); on the snapshotter's own XFS or btrfs filesystem, checkpoints are reflink copies")
		runDir         = flag.String("run-dir", "/run/tidelet", "directory for task FIFOs")
		ctrAddr        = flag.String("containerd-address", containerd.DefaultAddress, "containerd socket")
		ctrNS          = flag.String("containerd-namespace", containerd.DefaultNamespace, "containerd namespace to use")
		snapshotter    = flag.String("snapshotter", "", "containerd snapshotter (default: containerd's default)")
		runtimeHandler = flag.String("runtime-handler", "", "containerd runtime handler for containers (default: runc)")
		microvmHandler = flag.String("microvm-runtime-handler", containerd.DefaultMicroVMHandler, "containerd runtime handler for microVM sandboxes (empty disables)")
		cniConfDir     = flag.String("cni-conf-dir", network.DefaultConfDir, "CNI network configuration directory")
		cniPluginDir   = flag.String("cni-plugin-dir", network.DefaultPluginDir, "CNI plugin binary directory")
		cniDisable     = flag.Bool("cni-disable", false, "disable bridge networking")
		policyName     = flag.String("policy", "strict", "overcommit policy: "+strings.Join(registry.Names(), ", "))
		sandboxSwap    = flag.String("sandbox-swap", "unlimited", "how much a sandbox whose policy leaves its swap unlimited may swap: \"unlimited\" (the kernel's default) or \"limit\" (up to its memory limit, as docker run --memory without --memory-swap: the sandbox is OOM-killed where a container run that way would have been); a policy's own swap setting is kept")
		reservedCPU    = flag.String("system-reserved-cpu", "0", "CPU reserved for the host, e.g. 500m")
		reservedMem    = flag.String("system-reserved-memory", "0", "memory reserved for the host, e.g. 1Gi")
		capacityCPU    = flag.String("capacity-cpu", "", "override detected CPU capacity")
		capacityMem    = flag.String("capacity-memory", "", "override detected memory capacity")
		poolCgroup     = flag.String("pool-cgroup", "", "confine the pool to this cgroup v2 directory (\"auto\": /sys/fs/cgroup/<containerd namespace>, where containerd puts the sandboxes anyway): capacity comes from its cpuset and memory.max, and the policy's node sample from its own counters, not the whole host's")
		poolCpuset     = flag.String("pool-cpuset", "", "with --pool-cgroup: pin the pool's cgroup to these CPUs, e.g. 0-7")
		poolMemory     = flag.String("pool-memory", "", "with --pool-cgroup: cap the pool's cgroup at this much memory, e.g. 16Gi")
		stopTimeout    = flag.Duration("stop-timeout", 10*time.Second, "default grace period before SIGKILL")
		sampleInterval = flag.Duration("sample-interval", 5*time.Second, "usage sampling interval fed to the policy (0 disables)")
		tickInterval   = flag.Duration("tick-interval", time.Second, "policy tick interval (0 disables)")
		llmListen      = flag.String("llm-proxy-listen", "", "host:port for the pass-through model API proxy sandboxes call, e.g. 10.88.0.1:18800 (empty disables; the defer and speculate policies need it)")
		llmUpstream    = flag.String("llm-upstream", "https://api.anthropic.com", "model API base the proxy forwards to unless a sandbox's "+manager.UpstreamLabel+" label names another")
		llmMaxDefer    = flag.Duration("llm-max-defer", 60*time.Second, "longest the proxy keeps a model response back for the policy")
		logLevel       = flag.String("log-level", "info", "debug, info, warn or error")
		showVersion    = flag.Bool("version", false, "print version and exit")
	)
	flag.Var(policyOpts, "policy-opt", "policy option key=value (repeatable), e.g. requireLimits=true")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Version)
		return nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	capacity, err := host.Detect()
	if err != nil {
		return fmt.Errorf("detect capacity: %w", err)
	}
	var scope *host.Scope
	if *poolCgroup != "" {
		dir := *poolCgroup
		if dir == "auto" {
			dir = "/" + *ctrNS
		}
		if scope, err = host.NewScope(dir); err != nil {
			return fmt.Errorf("pool cgroup: %w", err)
		}
		var poolMem resource.Bytes
		if *poolMemory != "" {
			if poolMem, err = resource.ParseMemory(*poolMemory); err != nil {
				return err
			}
		}
		if err := scope.Apply(*poolCpuset, poolMem); err != nil {
			return fmt.Errorf("pool cgroup: %w", err)
		}
		if capacity, err = scope.Capacity(); err != nil {
			return fmt.Errorf("pool capacity: %w", err)
		}
	} else if *poolCpuset != "" || *poolMemory != "" {
		return errors.New("--pool-cpuset and --pool-memory need --pool-cgroup")
	}
	if *capacityCPU != "" {
		if capacity.CPU, err = resource.ParseCPU(*capacityCPU); err != nil {
			return err
		}
	}
	if *capacityMem != "" {
		if capacity.Memory, err = resource.ParseMemory(*capacityMem); err != nil {
			return err
		}
	}
	var reserved resource.Set
	if reserved.CPU, err = resource.ParseCPU(*reservedCPU); err != nil {
		return err
	}
	if reserved.Memory, err = resource.ParseMemory(*reservedMem); err != nil {
		return err
	}

	pol, err := registry.New(*policyName, policyOpts)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var net *network.Manager
	if !*cniDisable {
		net, err = network.New(network.Config{
			ConfDir:          *cniConfDir,
			PluginDirs:       []string{*cniPluginDir},
			NetNSDir:         filepath.Join(*runDir, "netns"),
			FilesDir:         filepath.Join(*stateDir, "net"),
			WriteDefaultConf: true,
			Logger:           logger,
		})
		if err != nil {
			logger.Warn("bridge networking disabled", "err", err)
			net = nil
		}
	}
	ctrCfg := containerd.Config{
		Address:        *ctrAddr,
		Namespace:      *ctrNS,
		LogDir:         filepath.Join(*stateDir, "logs"),
		FIFODir:        filepath.Join(*runDir, "fifo"),
		Snapshotter:    *snapshotter,
		CheckpointDir:  checkpointPath(*stateDir, *checkpointDir),
		RuntimeHandler: *runtimeHandler,
		MicroVMHandler: *microvmHandler,
		Network:        net,
		Logger:         logger,
	}
	if scope != nil {
		ctrCfg.CgroupParent = scope.CgroupPath()
	}
	if *wholeCkpts {
		self, err := os.Executable()
		if err != nil {
			return err
		}
		ctrCfg.InitBinary = filepath.Join(filepath.Dir(self), "tidelet-init")
		if _, err := os.Stat(ctrCfg.InitBinary); err != nil {
			return fmt.Errorf("--container-checkpoints needs tidelet-init next to tideletd: %w", err)
		}
		ctrCfg.CtlDir = filepath.Join(*runDir, "ctl")
		ctrCfg.CRIUDir = *criuDir
		if ctrCfg.CRIUDir == "" {
			ctrCfg.CRIUDir = filepath.Join(*runDir, "criu")
			if err := ensureTmpfs(ctrCfg.CRIUDir); err != nil {
				logger.Warn("CRIU images go to disk", "dir", ctrCfg.CRIUDir, "err", err)
			}
		}
		ctrCfg.SnapshotDir = *snapshotDir
		if ctrCfg.SnapshotDir == "" {
			ctrCfg.SnapshotDir = filepath.Join(*runDir, "snapshots")
			if err := ensureTmpfs(ctrCfg.SnapshotDir); err != nil {
				return fmt.Errorf("snapshot directory %s: %w", ctrCfg.SnapshotDir, err)
			}
		}
	}
	rt, err := containerd.New(ctx, ctrCfg)
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	st, err := store.NewDir(filepath.Join(*stateDir, "sandboxes"))
	if err != nil {
		return err
	}
	swapToLimit := false
	switch *sandboxSwap {
	case "unlimited":
	case "limit":
		swapToLimit = true
	default:
		return fmt.Errorf("invalid --sandbox-swap %q: unlimited or limit", *sandboxSwap)
	}
	mgrCfg := manager.Config{
		Policy:         pol,
		Runtime:        rt,
		Store:          st,
		Capacity:       capacity,
		Reserved:       reserved,
		StopTimeout:    *stopTimeout,
		SampleInterval: *sampleInterval,
		TickInterval:   *tickInterval,
		SwapToLimit:    swapToLimit,
		SnapshotDir:    filepath.Join(*stateDir, "snapshots"),
		Logger:         logger,
	}
	if scope != nil {
		mgrCfg.Collect = scope.Collect
		mgrCfg.PoolCgroup = scope.Path
	}
	mgr, err := manager.New(mgrCfg)
	if err != nil {
		return err
	}
	if err := mgr.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()

	if *llmListen != "" {
		up, err := url.Parse(*llmUpstream)
		if err != nil || up.Host == "" {
			return fmt.Errorf("invalid --llm-upstream %q", *llmUpstream)
		}
		px, err := llmproxy.New(llmproxy.Config{Upstream: up, MaxDefer: *llmMaxDefer, Resolver: mgr, Logger: logger, HoldWhole: *holdWhole})
		if err != nil {
			return err
		}
		mgr.SetModelGate(px)
		go func() {
			if err := px.Serve(ctx, *llmListen); err != nil {
				logger.Error("llm proxy stopped", "err", err)
			}
		}()
	} else if _, aware := pol.(policy.ModelAware); aware {
		logger.Warn("policy schedules on model calls but --llm-proxy-listen is empty: no model events will arrive", "policy", pol.Name())
	}

	if scope != nil {
		logger.Info("pool confined to a cgroup", "cgroup", scope.Path, "cpus", scope.CPUs(), "cpuset", *poolCpuset, "memory", *poolMemory)
	}
	logger.Info("tideletd starting", "version", version.Version, "policy", pol.Name(),
		"capacity", capacity.String(), "reserved", reserved.String())
	return server.New(mgr, logger).Serve(ctx, *listen, *httpListen)
}

// checkpointPath is --checkpoint-dir, or <state-dir>/checkpoints.
func checkpointPath(stateDir, flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return filepath.Join(stateDir, "checkpoints")
}

// ensureTmpfs mounts a tmpfs at dir unless one is there already: CRIU images are written and
// read back within minutes, and memory is the quickest place for them.
func ensureTmpfs(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err == nil && st.Type == 0x01021994 { // TMPFS_MAGIC
		return nil
	}
	return syscall.Mount("tmpfs", dir, "tmpfs", 0, "mode=0700,size=50%")
}
