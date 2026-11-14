// Command tidelet is the Tidelet CLI.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/TideSand/tidelet/api/tidelet/v1alpha1"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/rpc/client"
	"github.com/TideSand/tidelet/pkg/rpc/server"
	"github.com/TideSand/tidelet/pkg/version"
)

var (
	socket  string
	jsonOut bool
	api     *client.Client
)

// exitError carries a process exit code to main.
type exitError struct{ code int }

func (e exitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }

func main() {
	root := &cobra.Command{
		Use:           "tidelet",
		Short:         "Manage Tidelet sandboxes",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			var err error
			api, err = client.New(socket)
			return err
		},
		PersistentPostRun: func(*cobra.Command, []string) {
			if api != nil {
				_ = api.Close()
			}
		},
	}
	root.PersistentFlags().StringVar(&socket, "socket", envOr("TIDELET_SOCKET", server.DefaultSocket), "tideletd socket path or tcp://host:port")
	root.PersistentFlags().BoolVar(&jsonOut, "json", false, "output JSON")
	root.AddCommand(runCmd(), startCmd(), lsCmd(), inspectCmd(), stopCmd(), rmCmd(), execCmd(), attachCmd(),
		logsCmd(), statsCmd(), nodeCmd(), eventsCmd(), versionCmd())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := root.ExecuteContext(ctx)
	stop()
	var ee exitError
	switch {
	case errors.As(err, &ee):
		os.Exit(ee.code)
	case err != nil:
		if st, ok := status.FromError(err); ok {
			// The code names the kind of failure for scripts (a rejection by the policy is
			// ResourceExhausted; a retry may well succeed later).
			fmt.Fprintf(os.Stderr, "error: %s: %s\n", st.Code(), st.Message())
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func printJSON(m proto.Message) error {
	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func parseKV(items []string) map[string]string {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]string, len(items))
	for _, it := range items {
		k, v, _ := strings.Cut(it, "=")
		out[k] = v
	}
	return out
}

func parseResources(cpuReq, memReq, cpuLim, memLim string) (*pb.Resources, error) {
	parse := func(cpu, mem string) (*pb.ResourceList, error) {
		c, err := resource.ParseCPU(cpu)
		if err != nil {
			return nil, err
		}
		m, err := resource.ParseMemory(mem)
		if err != nil {
			return nil, err
		}
		return &pb.ResourceList{CpuMillicores: int64(c), MemoryBytes: int64(m)}, nil
	}
	req, err := parse(cpuReq, memReq)
	if err != nil {
		return nil, err
	}
	lim, err := parse(cpuLim, memLim)
	if err != nil {
		return nil, err
	}
	return &pb.Resources{Requests: req, Limits: lim}, nil
}

// terminal wraps the local TTY for interactive sessions.
type terminal struct {
	fd      int
	state   *term.State
	resize  chan *pb.TerminalSize
	stopSig func()
}

func (t *terminal) size() *pb.TerminalSize {
	w, h, err := term.GetSize(t.fd)
	if err != nil {
		return nil
	}
	return &pb.TerminalSize{Width: uint32(w), Height: uint32(h)}
}

// setupTerminal puts stdin in raw mode when tty is requested and stdin is
// a terminal, and watches for window size changes.
func setupTerminal(tty bool) (*terminal, error) {
	t := &terminal{fd: int(os.Stdin.Fd()), resize: make(chan *pb.TerminalSize, 4)}
	if !tty || !term.IsTerminal(t.fd) {
		return t, nil
	}
	st, err := term.MakeRaw(t.fd)
	if err != nil {
		return nil, err
	}
	t.state = st
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGWINCH)
	done := make(chan struct{})
	t.stopSig = func() { signal.Stop(sig); close(done) }
	go func() {
		for {
			select {
			case <-done:
				return
			case <-sig:
				if sz := t.size(); sz != nil {
					select {
					case t.resize <- sz:
					default:
					}
				}
			}
		}
	}()
	return t, nil
}

func (t *terminal) restore() {
	if t.stopSig != nil {
		t.stopSig()
	}
	if t.state != nil {
		_ = term.Restore(t.fd, t.state)
	}
}

func runCmd() *cobra.Command {
	var (
		name, image, workdir, network   string
		cpus, memory, cpuReq, memReq    string
		env, mounts, labels             []string
		stdin, tty, detach, hostNetwork bool
		priority                        int32
		netns, runtimeClass, hostname   string
	)
	cmd := &cobra.Command{
		Use:   "run [flags] -- [command...]",
		Short: "Create and start a sandbox",
		Long: `Create and start a sandbox.

--cpus and --memory set hard limits, exactly like docker run. Under the
strict policy they are also what the sandbox reserves on the node.
--cpu-request and --memory-request set Kubernetes-style requests separately;
the strict policy rejects requests that differ from limits.

Without --detach the command attaches to the sandbox and exits with its
exit code, like docker run.`,
		Example: `  tidelet run --name hello --cpus 1 --memory 512Mi --image docker.io/library/alpine:3.20 -- sleep 300
  tidelet run -it --image docker.io/library/alpine:3.20 -- sh`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			res, err := parseResources(cpuReq, memReq, cpus, memory)
			if err != nil {
				return err
			}
			spec := &pb.SandboxSpec{
				Image:      image,
				Command:    args,
				Env:        parseKV(env),
				WorkingDir: workdir,
				Resources:  res,
				Labels:     parseKV(labels),
				Stdin:      stdin,
				Tty:        tty,
				Priority:   priority,
				Hostname:   hostname,
			}
			if hostNetwork {
				network = "host"
			}
			if netns != "" {
				network = "join"
				spec.NetworkNamespace = netns
			}
			switch network {
			case "none":
				spec.Network = pb.NetworkMode_NETWORK_MODE_NONE
			case "host":
				spec.Network = pb.NetworkMode_NETWORK_MODE_HOST
			case "bridge":
				spec.Network = pb.NetworkMode_NETWORK_MODE_BRIDGE
			case "join":
				spec.Network = pb.NetworkMode_NETWORK_MODE_JOIN
			default:
				return fmt.Errorf("unknown network mode %q", network)
			}
			switch runtimeClass {
			case "container":
				spec.Runtime = pb.RuntimeClass_RUNTIME_CLASS_CONTAINER
			case "microvm":
				spec.Runtime = pb.RuntimeClass_RUNTIME_CLASS_MICROVM
			default:
				return fmt.Errorf("unknown runtime class %q", runtimeClass)
			}
			for _, m := range mounts {
				parts := strings.Split(m, ":")
				if len(parts) < 2 || len(parts) > 3 {
					return fmt.Errorf("invalid mount %q, want host:container[:ro]", m)
				}
				spec.Mounts = append(spec.Mounts, &pb.Mount{
					HostPath: parts[0], ContainerPath: parts[1], ReadOnly: len(parts) == 3 && parts[2] == "ro",
				})
			}
			created, err := api.Sandboxes.CreateSandbox(ctx, &pb.CreateSandboxRequest{Name: name, Spec: spec})
			if err != nil {
				return err
			}
			sb := created.GetSandbox()
			if sb.GetStatus().GetPhase() == pb.Phase_PHASE_QUEUED {
				fmt.Fprintf(os.Stderr, "sandbox %s queued by policy; it will start when dispatched\n", sb.GetId())
				_, err := api.Sandboxes.StartSandbox(ctx, &pb.StartSandboxRequest{Sandbox: sb.GetId()})
				if err != nil {
					return err
				}
				fmt.Println(sb.GetId())
				return nil
			}
			if detach {
				if _, err := api.Sandboxes.StartSandbox(ctx, &pb.StartSandboxRequest{Sandbox: sb.GetId()}); err != nil {
					return err
				}
				fmt.Println(sb.GetId())
				return nil
			}
			return runAttached(ctx, sb.GetId(), stdin, tty, true)
		},
	}
	f := cmd.Flags()
	f.StringVar(&name, "name", "", "sandbox name (unique)")
	f.StringVar(&image, "image", "", "OCI image reference (required)")
	f.StringVar(&cpus, "cpus", "", "CPU limit, e.g. 1 or 500m")
	f.StringVar(&memory, "memory", "", "memory limit, e.g. 512Mi")
	f.StringVar(&cpuReq, "cpu-request", "", "CPU request (defaults to limit)")
	f.StringVar(&memReq, "memory-request", "", "memory request (defaults to limit)")
	f.StringArrayVarP(&env, "env", "e", nil, "environment variable KEY=VALUE")
	f.StringArrayVarP(&mounts, "mount", "v", nil, "bind mount host:container[:ro]")
	f.StringArrayVarP(&labels, "label", "l", nil, "label KEY=VALUE")
	f.StringVarP(&workdir, "workdir", "w", "", "working directory")
	f.StringVar(&network, "network", "bridge", "network mode: bridge, none, host or join")
	f.StringVar(&netns, "netns", "", "network namespace path to join (implies --network join)")
	f.BoolVar(&hostNetwork, "host-network", false, "shorthand for --network host")
	f.StringVar(&runtimeClass, "runtime", "container", "runtime class: container or microvm")
	f.StringVar(&hostname, "hostname", "", "hostname inside the sandbox (default: name or ID)")
	f.BoolVarP(&stdin, "interactive", "i", false, "keep stdin open")
	f.BoolVarP(&tty, "tty", "t", false, "allocate a pseudo-terminal")
	f.BoolVarP(&detach, "detach", "d", false, "start in the background and print the ID")
	f.Int32Var(&priority, "priority", 0, "advisory priority for the policy")
	_ = cmd.MarkFlagRequired("image")
	return cmd
}

// runAttached attaches to a sandbox, optionally starting it once the
// attachment is confirmed, and exits with the sandbox's exit code.
func runAttached(ctx context.Context, id string, stdin, tty, start bool) error {
	t, err := setupTerminal(tty)
	if err != nil {
		return err
	}
	defer t.restore()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var in *os.File
	if stdin {
		in = os.Stdin
	}
	startErr := make(chan error, 1)
	opts := client.AttachOptions{
		Sandbox:      id,
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		TerminalSize: t.size(),
		Resize:       t.resize,
		Attached: func() {
			if !start {
				return
			}
			go func() {
				_, err := api.Sandboxes.StartSandbox(ctx, &pb.StartSandboxRequest{Sandbox: id})
				if err == nil && tty {
					if sz := t.size(); sz != nil {
						select {
						case t.resize <- sz:
						default:
						}
					}
				}
				startErr <- err
			}()
		},
	}
	if in != nil {
		opts.Stdin = in
	}
	code, err := api.Attach(ctx, opts)
	t.restore()
	if err != nil {
		return err
	}
	if start {
		select {
		case serr := <-startErr:
			if serr != nil {
				return serr
			}
		default:
		}
	}
	if code == nil {
		// Stream ended without an exit: look the status up.
		resp, gerr := api.Sandboxes.GetSandbox(context.Background(), &pb.GetSandboxRequest{Sandbox: id})
		if gerr != nil || resp.GetSandbox().GetStatus().ExitCode == nil {
			return nil
		}
		c := resp.GetSandbox().GetStatus().GetExitCode()
		code = &c
	}
	if *code != 0 {
		return exitError{int(*code)}
	}
	return nil
}

func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start SANDBOX...",
		Short: "Start created sandboxes",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, ref := range args {
				resp, err := api.Sandboxes.StartSandbox(cmd.Context(), &pb.StartSandboxRequest{Sandbox: ref})
				if err != nil {
					return err
				}
				fmt.Println(resp.GetSandbox().GetId())
			}
			return nil
		},
	}
}

func lsCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list", "ps"},
		Short:   "List sandboxes",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := api.Sandboxes.ListSandboxes(cmd.Context(), &pb.ListSandboxesRequest{})
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(resp)
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tNAME\tIMAGE\tRUNTIME\tPHASE\tCPU\tMEMORY\tIP\tCREATED")
			for _, sb := range resp.GetSandboxes() {
				lim := sb.GetStatus().GetAllocated().GetLimits()
				ip := "-"
				if ips := sb.GetStatus().GetNetwork().GetIpAddresses(); len(ips) > 0 {
					ip, _, _ = strings.Cut(ips[0], "/")
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					sb.GetId()[:12], sb.GetName(), sb.GetSpec().GetImage(),
					strings.ToLower(strings.TrimPrefix(sb.GetSpec().GetRuntime().String(), "RUNTIME_CLASS_")), phaseString(sb),
					orDash(resource.Millicores(lim.GetCpuMillicores())), orDash(resource.Bytes(lim.GetMemoryBytes())),
					ip, ago(sb.GetCreatedAt().AsTime()))
			}
			return tw.Flush()
		},
	}
}

func phaseName(p pb.Phase) string {
	return strings.Title(strings.ToLower(strings.TrimPrefix(p.String(), "PHASE_"))) //nolint:staticcheck // ASCII only
}

func phaseString(sb *pb.Sandbox) string {
	st := sb.GetStatus()
	if st.ExitCode != nil && st.GetPhase() != pb.Phase_PHASE_RUNNING {
		return fmt.Sprintf("%s(%d)", phaseName(st.GetPhase()), st.GetExitCode())
	}
	return phaseName(st.GetPhase())
}

func orDash(s fmt.Stringer) string {
	if v := s.String(); v != "0" {
		return v
	}
	return "-"
}

func ago(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func inspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect SANDBOX",
		Short: "Show a sandbox as JSON",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := api.Sandboxes.GetSandbox(cmd.Context(), &pb.GetSandboxRequest{Sandbox: args[0]})
			if err != nil {
				return err
			}
			return printJSON(resp.GetSandbox())
		},
	}
}

func stopCmd() *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "stop SANDBOX...",
		Short: "Stop running sandboxes",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, ref := range args {
				resp, err := api.Sandboxes.StopSandbox(cmd.Context(), &pb.StopSandboxRequest{
					Sandbox: ref, TimeoutSeconds: int64(timeout / time.Second),
				})
				if err != nil {
					return err
				}
				fmt.Println(resp.GetSandbox().GetId())
			}
			return nil
		},
	}
	cmd.Flags().DurationVarP(&timeout, "timeout", "t", 0, "grace period before SIGKILL (default: daemon setting)")
	return cmd
}

func rmCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:     "rm SANDBOX...",
		Aliases: []string{"delete", "remove"},
		Short:   "Delete sandboxes",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, ref := range args {
				if _, err := api.Sandboxes.DeleteSandbox(cmd.Context(), &pb.DeleteSandboxRequest{Sandbox: ref, Force: force}); err != nil {
					return err
				}
				fmt.Println(ref)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "stop the sandbox first if it is running")
	return cmd
}

func execCmd() *cobra.Command {
	var (
		env     []string
		workdir string
		stdin   bool
		tty     bool
	)
	cmd := &cobra.Command{
		Use:   "exec [flags] SANDBOX -- command...",
		Short: "Run a command inside a running sandbox",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			t, err := setupTerminal(tty)
			if err != nil {
				return err
			}
			defer t.restore()
			opts := client.ExecOptions{
				Sandbox:      args[0],
				Command:      args[1:],
				Env:          parseKV(env),
				WorkingDir:   workdir,
				TTY:          tty,
				Stdout:       os.Stdout,
				Stderr:       os.Stderr,
				TerminalSize: t.size(),
				Resize:       t.resize,
			}
			if stdin {
				opts.Stdin = os.Stdin
			}
			code, err := api.Exec(cmd.Context(), opts)
			t.restore()
			if err != nil {
				return err
			}
			if code != 0 {
				return exitError{int(code)}
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVarP(&env, "env", "e", nil, "environment variable KEY=VALUE")
	cmd.Flags().StringVarP(&workdir, "workdir", "w", "", "working directory")
	cmd.Flags().BoolVarP(&stdin, "interactive", "i", false, "pass stdin to the command")
	cmd.Flags().BoolVarP(&tty, "tty", "t", false, "allocate a pseudo-terminal")
	return cmd
}

func attachCmd() *cobra.Command {
	var noStdin bool
	cmd := &cobra.Command{
		Use:   "attach SANDBOX",
		Short: "Attach to a sandbox's main process",
		Long: `Attach to a sandbox's main process. Stdin is forwarded if the sandbox was
created with --interactive. Ctrl-D closes the sandbox's stdin; Ctrl-C
detaches without stopping the sandbox (the signal is not forwarded).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := api.Sandboxes.GetSandbox(cmd.Context(), &pb.GetSandboxRequest{Sandbox: args[0]})
			if err != nil {
				return err
			}
			spec := resp.GetSandbox().GetSpec()
			return runAttached(cmd.Context(), resp.GetSandbox().GetId(), spec.GetStdin() && !noStdin, spec.GetTty(), false)
		},
	}
	cmd.Flags().BoolVar(&noStdin, "no-stdin", false, "do not forward stdin")
	return cmd
}

func logsCmd() *cobra.Command {
	var (
		follow     bool
		tail       int64
		since      time.Duration
		timestamps bool
	)
	cmd := &cobra.Command{
		Use:   "logs SANDBOX",
		Short: "Print a sandbox's recorded output",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := client.LogsOptions{Follow: follow, TailLines: tail}
			if since > 0 {
				opts.Since = time.Now().Add(-since)
			}
			atLineStart := true
			return api.Logs(cmd.Context(), args[0], opts, func(m *pb.LogsResponse) error {
				w := os.Stdout
				if m.GetStream() == pb.LogStream_LOG_STREAM_STDERR {
					w = os.Stderr
				}
				if timestamps && atLineStart {
					_, _ = fmt.Fprintf(w, "%s ", m.GetTimestamp().AsTime().Local().Format(time.RFC3339Nano))
				}
				data := m.GetData()
				atLineStart = len(data) > 0 && data[len(data)-1] == '\n'
				_, err := w.Write(data)
				return err
			})
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep streaming until the sandbox exits")
	cmd.Flags().Int64VarP(&tail, "tail", "n", 0, "only the last N lines")
	cmd.Flags().DurationVar(&since, "since", 0, "only output newer than this, e.g. 10m")
	cmd.Flags().BoolVarP(&timestamps, "timestamps", "t", false, "prefix lines with timestamps")
	return cmd
}

func statsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats SANDBOX",
		Short: "Show a sandbox's resource usage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := api.Sandboxes.GetStats(cmd.Context(), &pb.GetStatsRequest{Sandbox: args[0]})
			if err != nil {
				return err
			}
			st := resp.GetStats()
			if jsonOut {
				return printJSON(st)
			}
			fmt.Printf("cpu usage:         %s\n", time.Duration(st.GetCpuUsageNanos()))
			fmt.Printf("cpu throttled:     %s\n", time.Duration(st.GetCpuThrottledNanos()))
			fmt.Printf("memory usage:      %s\n", humanBytes(st.GetMemoryUsageBytes()))
			fmt.Printf("memory workingset: %s\n", humanBytes(st.GetMemoryWorkingSetBytes()))
			if st.GetMemoryLimitBytes() > 0 {
				fmt.Printf("memory limit:      %s\n", humanBytes(st.GetMemoryLimitBytes()))
			}
			fmt.Printf("pids:              %d\n", st.GetPids())
			return nil
		},
	}
}

func nodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "node",
		Short: "Show node capacity through the active policy",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := api.Node.GetNode(cmd.Context(), &pb.GetNodeRequest{})
			if err != nil {
				return err
			}
			n := resp.GetNode()
			if jsonOut {
				return printJSON(n)
			}
			row := func(label string, r *pb.ResourceList) string {
				return fmt.Sprintf("%s\t%s\t%s\n", label,
					resource.Millicores(r.GetCpuMillicores()), resource.Bytes(r.GetMemoryBytes()))
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintf(tw, "policy:\t%s\nruntime:\t%s\nsandboxes:\t%d\nqueued:\t%d\n",
				n.GetPolicy(), n.GetRuntime(), n.GetSandboxCount(), n.GetQueuedCount())
			_, _ = fmt.Fprintf(tw, "\tCPU\tMEMORY\n")
			_, _ = fmt.Fprint(tw, row("capacity", n.GetCapacity()), row("allocatable", n.GetAllocatable()), row("allocated", n.GetAllocated()))
			return tw.Flush()
		},
	}
}

func eventsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "events",
		Short: "Stream sandbox lifecycle events",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return api.Watch(cmd.Context(), func(ev *pb.WatchResponse) error {
				if jsonOut {
					return printJSON(ev)
				}
				sb := ev.GetSandbox()
				fmt.Printf("%s %-8s %s %s %s\n", ev.GetTimestamp().AsTime().Local().Format(time.RFC3339),
					strings.ToLower(strings.TrimPrefix(ev.GetType().String(), "EVENT_TYPE_")),
					sb.GetId()[:12], sb.GetName(), phaseString(sb))
				return nil
			})
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print client and daemon versions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Println("client:", version.Version)
			resp, err := api.Node.GetVersion(cmd.Context(), &pb.GetVersionRequest{})
			if err != nil {
				if status.Code(err) == codes.Unavailable {
					return fmt.Errorf("daemon unreachable at %s", socket)
				}
				return err
			}
			fmt.Println("daemon:", resp.GetVersion())
			return nil
		},
	}
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
