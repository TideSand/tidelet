package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	pb "github.com/TideSand/tidelet/api/tidelet/v1alpha1"
	"github.com/TideSand/tidelet/pkg/rpc/client"
	"github.com/TideSand/tidelet/pkg/rpc/server"
)

// daemonFiles are what `tidelet daemon` keeps about the tideletd serving a socket: its process
// ID and the flags it was started with next to the socket, and its log under /var/log.
type daemonFiles struct{ pid, args, log string }

func filesFor(socket string) daemonFiles {
	dir := filepath.Dir(server.DefaultSocket)
	if !strings.Contains(socket, "://") {
		dir = filepath.Dir(socket)
	} else if p, ok := strings.CutPrefix(socket, "unix://"); ok {
		dir = filepath.Dir(p)
	}
	return daemonFiles{
		pid:  filepath.Join(dir, "tideletd.pid"),
		args: filepath.Join(dir, "tideletd.args"),
		log:  filepath.Join("/var/log", filepath.Base(dir), "tideletd.log"),
	}
}

// daemonPID returns the process ID in the pid file if that process is a running tideletd.
func (f daemonFiles) daemonPID() int {
	data, err := os.ReadFile(f.pid)
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	if pid <= 0 {
		return 0
	}
	comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil || strings.TrimSpace(string(comm)) != "tideletd" {
		return 0
	}
	return pid
}

// daemonVersion asks the daemon at the socket for its version; ok is false when none answers.
func daemonVersion(ctx context.Context) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	resp, err := api.Node.GetVersion(ctx, &pb.GetVersionRequest{})
	if err != nil {
		return "", false
	}
	return resp.GetVersion(), true
}

// asRoot runs this command again under sudo unless it is root already. It returns only in
// that case, or when sudo cannot be run.
func asRoot() error {
	if os.Geteuid() == 0 {
		return nil
	}
	sudo, err := exec.LookPath("sudo")
	if err != nil {
		return errors.New("this needs root, and sudo is not installed")
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	argv := []string{"sudo", self}
	if socket != server.DefaultSocket && !hasFlag(os.Args[1:], "socket") {
		argv = append(argv, "--socket", socket) // from TIDELET_SOCKET, which sudo drops
	}
	argv = append(argv, os.Args[1:]...)
	return syscall.Exec(sudo, argv, os.Environ())
}

// hasFlag reports whether args set the flag, as -name, --name, -name=v or --name=v.
func hasFlag(args []string, name string) bool {
	_, ok := flagValue(args, name)
	return ok
}

func flagValue(args []string, name string) (string, bool) {
	for i, a := range args {
		a = strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-")
		if a == name {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return "", true
		}
		if v, ok := strings.CutPrefix(a, name+"="); ok {
			return v, true
		}
	}
	return "", false
}

// withoutFlag drops a flag that takes a value from args.
func withoutFlag(args []string, name string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		a := strings.TrimPrefix(strings.TrimPrefix(args[i], "-"), "-")
		if a == name && strings.HasPrefix(args[i], "-") {
			i++
			continue
		}
		if strings.HasPrefix(a, name+"=") && strings.HasPrefix(args[i], "-") {
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func daemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Start, stop and inspect tideletd",
		Long: `Run tideletd in the background. Its process ID and flags are kept next to its socket and
its log in /var/log/<socket directory's name>/tideletd.log. Root is needed to start and stop it:
without it the command runs itself under sudo.`,
	}
	start := &cobra.Command{
		Use:   "start [tideletd flags]",
		Short: "Start tideletd in the background and wait until it serves its API",
		Long: `Start tideletd in the background and wait until it serves its API. Every argument is
passed to tideletd (tideletd --help lists its flags). Started through sudo, the daemon lets the
caller's group use its socket (--socket-group), so later commands need no sudo.`,
		Example:            "  tidelet daemon start --container-checkpoints\n  tidelet daemon start --policy tidesand --container-checkpoints --llm-proxy-listen 0.0.0.0:18800",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if hasFlag(args, "help") || hasFlag(args, "h") {
				return cmd.Help()
			}
			return daemonStart(cmd.Context(), args)
		},
	}
	stop := &cobra.Command{
		Use:   "stop",
		Short: "Stop tideletd; its sandboxes keep running, and the next daemon picks them up",
		Args:  cobra.NoArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return daemonStop() },
	}
	restart := &cobra.Command{
		Use:                "restart [tideletd flags]",
		Short:              "Stop tideletd and start it again, with the flags it had unless others are given",
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if hasFlag(args, "help") || hasFlag(args, "h") {
				return cmd.Help()
			}
			return daemonRestart(cmd.Context(), args)
		},
	}
	st := &cobra.Command{
		Use:   "status",
		Short: "Say whether tideletd runs (exit status 1 when it does not)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			f := filesFor(socket)
			version, ok := daemonVersion(cmd.Context())
			pid := f.daemonPID()
			switch {
			case ok && pid != 0:
				fmt.Printf("running: pid %d, version %s, socket %s, log %s\n", pid, version, socket, f.log)
			case ok:
				fmt.Printf("running: version %s, socket %s (not started by tidelet daemon start)\n", version, socket)
			case pid != 0:
				fmt.Printf("pid %d runs but does not answer on %s; see %s\n", pid, socket, f.log)
				return exitError{1}
			default:
				fmt.Println("not running")
				return exitError{1}
			}
			return nil
		},
	}
	var follow bool
	var lines int
	logs := &cobra.Command{
		Use:   "logs",
		Short: "Print tideletd's log",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			argv := []string{"-n", strconv.Itoa(lines)}
			if follow {
				argv = append(argv, "-f")
			}
			tail := exec.CommandContext(cmd.Context(), "tail", append(argv, filesFor(socket).log)...)
			tail.Stdout, tail.Stderr = os.Stdout, os.Stderr
			if err := tail.Run(); err != nil && cmd.Context().Err() == nil {
				return err
			}
			return nil
		},
	}
	logs.Flags().BoolVarP(&follow, "follow", "f", false, "keep printing as the log grows")
	logs.Flags().IntVarP(&lines, "tail", "n", 200, "lines from the end to print")
	cmd.AddCommand(start, stop, restart, st, logs)
	return cmd
}

// startArgs splits what `daemon start` was given into the socket the daemon will serve and
// tideletd's own arguments. The CLI's --socket, which lands here unparsed, means --listen.
func startArgs(args []string) (string, []string) {
	listen := socket
	if v, ok := flagValue(args, "socket"); ok {
		listen = v
	}
	args = withoutFlag(args, "socket")
	if v, ok := flagValue(args, "listen"); ok {
		listen = v
	} else if listen != server.DefaultSocket {
		args = append([]string{"--listen", listen}, args...)
	}
	return listen, args
}

func daemonStart(ctx context.Context, args []string) error {
	if err := asRoot(); err != nil {
		return err
	}
	listen, args := startArgs(args)
	if gid := os.Getenv("SUDO_GID"); gid != "" && gid != "0" && !hasFlag(args, "socket-group") {
		if g, err := user.LookupGroupId(gid); err == nil {
			args = append(args, "--socket-group", g.Name) // whoever ran sudo can use the daemon without it
		}
	}
	logGroup := -1 // the socket's group reads the log too
	if name, ok := flagValue(args, "socket-group"); ok {
		if g, err := user.LookupGroup(name); err == nil {
			logGroup, _ = strconv.Atoi(g.Gid)
		} else if n, err := strconv.Atoi(name); err == nil {
			logGroup = n
		}
	}
	if listen != socket {
		socket = listen
		if api != nil {
			_ = api.Close()
		}
		var err error
		if api, err = client.New(socket); err != nil {
			return err
		}
	}
	f := filesFor(socket)
	if _, ok := daemonVersion(ctx); ok {
		return fmt.Errorf("a daemon is already serving %s", socket)
	}
	if pid := f.daemonPID(); pid != 0 {
		return fmt.Errorf("tideletd (pid %d) runs but does not answer on %s; see %s", pid, socket, f.log)
	}
	bin, err := tideletdPath()
	if err != nil {
		return err
	}
	for _, p := range []string{f.pid, f.log} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
	}
	logFile, err := os.OpenFile(f.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer logFile.Close()
	if logGroup >= 0 {
		_ = logFile.Chown(0, logGroup)
	}
	logStart, _ := logFile.Seek(0, 2)
	d := exec.Command(bin, args...)
	d.Stdout, d.Stderr = logFile, logFile
	d.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // its own session: it outlives this command and its terminal
	if err := d.Start(); err != nil {
		return fmt.Errorf("start %s: %w", bin, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- d.Wait() }()
	_ = os.WriteFile(f.pid, []byte(strconv.Itoa(d.Process.Pid)+"\n"), 0o644)
	saved, _ := json.Marshal(args)
	_ = os.WriteFile(f.args, saved, 0o600)

	deadline := time.After(60 * time.Second)
	for {
		if version, ok := daemonVersion(ctx); ok {
			fmt.Printf("tideletd started: pid %d, version %s, socket %s, log %s\n", d.Process.Pid, version, socket, f.log)
			return nil
		}
		select {
		case err := <-exited:
			_ = os.Remove(f.pid)
			return fmt.Errorf("tideletd exited (%v):\n%s", err, firstLines(f.log, logStart, 5))
		case <-deadline:
			return fmt.Errorf("tideletd (pid %d) does not answer on %s after 60 s; see %s", d.Process.Pid, socket, f.log)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// firstLines returns up to n lines of a file from an offset on.
func firstLines(path string, from int64, n int) string {
	data, err := os.ReadFile(path)
	if err != nil || from > int64(len(data)) {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data[from:]), "\n"), "\n")
	if len(lines) > n {
		lines = append(lines[:n], "... (see "+path+")")
	}
	return strings.Join(lines, "\n")
}

// tideletdPath finds the daemon's binary: next to this one, else on PATH.
func tideletdPath() (string, error) {
	if self, err := os.Executable(); err == nil {
		p := filepath.Join(filepath.Dir(self), "tideletd")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	p, err := exec.LookPath("tideletd")
	if err != nil {
		return "", errors.New("tideletd is neither next to tidelet nor on PATH")
	}
	return p, nil
}

func daemonStop() error {
	if err := asRoot(); err != nil {
		return err
	}
	stopped, err := stopDaemon(filesFor(socket))
	if err != nil {
		return err
	}
	if stopped == 0 {
		fmt.Println("not running")
	} else {
		fmt.Printf("tideletd stopped (pid %d)\n", stopped)
	}
	return nil
}

// stopDaemon ends the tideletd of the pid file and waits for it; 0 means none ran.
func stopDaemon(f daemonFiles) (int, error) {
	pid := f.daemonPID()
	if pid == 0 {
		_ = os.Remove(f.pid)
		return 0, nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return 0, fmt.Errorf("stop pid %d: %w", pid, err)
	}
	for deadline := time.Now().Add(60 * time.Second); f.daemonPID() == pid; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("tideletd (pid %d) did not stop within 60 s", pid)
		}
	}
	_ = os.Remove(f.pid)
	return pid, nil
}

func daemonRestart(ctx context.Context, args []string) error {
	if err := asRoot(); err != nil {
		return err
	}
	listen, rest := startArgs(args)
	f := filesFor(listen)
	if len(rest) == 0 || (len(rest) == 2 && rest[0] == "--listen") {
		if data, err := os.ReadFile(f.args); err == nil {
			var saved []string
			if json.Unmarshal(data, &saved) == nil && len(saved) > 0 {
				args = saved
			}
		}
	}
	if _, err := stopDaemon(f); err != nil {
		return err
	}
	return daemonStart(ctx, args)
}
