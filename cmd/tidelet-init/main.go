// Command tidelet-init is the init process of a checkpointable sandbox.
//
// CRIU checkpoints a container as one process tree rooted at its init, and a
// process started by `runc exec` is not in that tree (its parent is the shim,
// outside the container), so it would be left out of a checkpoint. A
// checkpointable sandbox therefore runs this program as its init: it starts
// the sandbox's own command as its child, and starts every exec tideletd asks
// for as its child too, so the agent and its tool calls are all descendants
// of init and a checkpoint holds the whole sandbox.
//
// tideletd talks to it through a directory bind-mounted at /.tidelet/ctl:
//
//	req/<id>.json   an exec request: {"args", "env", "cwd", "stdin", "stdout", "stderr"};
//	                stdin is a regular file (or empty for /dev/null), stdout and stderr
//	                named pipes that tideletd keeps open for reading
//	<id>.kill       a request to SIGKILL the exec's process group
//	wake            a named pipe: tideletd writes a byte after leaving a request
//	<id>.status     written by init when the exec ends: its exit code (128+signal)
//	<id>.done       a named pipe tideletd reads: init writes a byte after the status
//
// Nothing is polled: init sleeps until a child ends (SIGCHLD) or tideletd writes to the
// wake pipe, and a plain pipe is all CRIU has to restore (no inotify). The program exits
// with its own command's status when that command ends, as a container's init would.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const ctl = "/.tidelet/ctl"

type request struct {
	Args   []string `json:"args"`
	Env    []string `json:"env"`
	Cwd    string   `json:"cwd"`
	Stdin  string   `json:"stdin"`
	Stdout string   `json:"stdout"`
	Stderr string   `json:"stderr"`
}

// overcommitFile holds "2"; bound over /proc/sys/vm/overcommit_memory it makes glibc give
// heap memory back with mmap instead of MADV_DONTNEED, which a sandbox whose memory is a
// private file mapping (one created from a snapshot) needs: there MADV_DONTNEED brings
// the snapshot's bytes back, not zeros.
const overcommitFile = "/.tidelet/mem/.overcommit"

func main() {
	args := os.Args[1:]
	if len(args) == 1 && args[0] == "--bind-overcommit" {
		// Run by tideletd inside a new sandbox's mount namespace, before its command starts.
		if err := syscall.Mount(overcommitFile, "/proc/sys/vm/overcommit_memory", "", syscall.MS_BIND, ""); err != nil {
			fmt.Fprintln(os.Stderr, "tidelet-init: bind overcommit_memory:", err)
			os.Exit(1)
		}
		return
	}
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		args = []string{"sleep", "infinity"}
	}
	signal.Ignore(syscall.SIGPIPE)
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	children := make(chan os.Signal, 1)
	signal.Notify(children, syscall.SIGCHLD)
	_ = os.MkdirAll(filepath.Join(ctl, "req"), 0o755)
	wake := wakeups()

	mainPid, err := spawn(args, os.Environ(), "", os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tidelet-init: start %v: %v\n", args, err)
		os.Exit(127)
	}
	execs := map[int]string{} // pid -> exec id
	known := map[string]bool{}

	for first := true; ; first = false {
		if !first { // the first pass picks up what was asked before the pipe was open
			select {
			case s := <-sigs:
				_ = syscall.Kill(mainPid, s.(syscall.Signal))
			case <-children:
			case <-wake:
			}
		}
		// Reap every child that ended: ours, and orphans re-parented to init.
		for {
			var ws syscall.WaitStatus
			pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
			if pid <= 0 || err != nil {
				break
			}
			code := ws.ExitStatus()
			if ws.Signaled() {
				code = 128 + int(ws.Signal())
			}
			if pid == mainPid {
				os.Exit(code)
			}
			if id, ok := execs[pid]; ok {
				delete(execs, pid)
				finish(id, code)
			}
		}
		// Kill requests.
		for pid, id := range execs {
			if _, err := os.Stat(filepath.Join(ctl, id+".kill")); err == nil {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_ = os.Remove(filepath.Join(ctl, id+".kill"))
			}
		}
		// New exec requests.
		entries, err := os.ReadDir(filepath.Join(ctl, "req"))
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".json") {
				continue
			}
			id := strings.TrimSuffix(name, ".json")
			if known[id] {
				continue
			}
			known[id] = true
			path := filepath.Join(ctl, "req", name)
			pid, err := startRequest(path)
			_ = os.Remove(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "tidelet-init: exec %s: %v\n", id, err)
				finish(id, 127)
				continue
			}
			execs[pid] = id
		}
	}
}

// wakeups reports each byte tideletd writes to the wake pipe. The pipe is opened read-write
// and blocking: it never reports end-of-file, and reading it parks one thread.
func wakeups() <-chan struct{} {
	ch := make(chan struct{}, 1)
	path := filepath.Join(ctl, "wake")
	_ = syscall.Mkfifo(path, 0o666)
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tidelet-init: wake pipe:", err)
		return ch
	}
	go func() {
		buf := make([]byte, 64)
		for {
			if n, err := syscall.Read(fd, buf); n <= 0 && err != syscall.EINTR {
				return
			}
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}()
	return ch
}

// finish records an exec's exit code and tells tideletd, if it is waiting.
func finish(id string, code int) {
	writeFile(filepath.Join(ctl, id+".status"), strconv.Itoa(code))
	if fd, err := syscall.Open(filepath.Join(ctl, id+".done"), syscall.O_WRONLY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0); err == nil {
		_, _ = syscall.Write(fd, []byte{1})
		_ = syscall.Close(fd)
	}
}

func startRequest(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var r request
	if err := json.Unmarshal(data, &r); err != nil {
		return 0, err
	}
	if len(r.Args) == 0 {
		return 0, fmt.Errorf("empty command")
	}
	in, err := openOr(r.Stdin, os.O_RDONLY, "/dev/null")
	if err != nil {
		return 0, err
	}
	defer in.Close()
	// The named pipes are opened read-write: an open for writing alone would block until
	// tideletd has the reading end, and tideletd holds it already, so this is only about not
	// blocking init. The child inherits the descriptors.
	out, err := openOr(r.Stdout, os.O_RDWR, "/dev/null")
	if err != nil {
		return 0, err
	}
	defer out.Close()
	errf := out
	if r.Stderr != "" && r.Stderr != r.Stdout {
		errf, err = openOr(r.Stderr, os.O_RDWR, "/dev/null")
		if err != nil {
			return 0, err
		}
		defer errf.Close()
	}
	env := r.Env
	if len(env) == 0 {
		env = os.Environ()
	}
	return spawn(r.Args, env, r.Cwd, in, out, errf)
}

func openOr(path string, flag int, fallback string) (*os.File, error) {
	if path == "" {
		path = fallback
	}
	return os.OpenFile(path, flag, 0)
}

// spawn starts args as a child in its own process group, so a kill request
// reaches everything it started.
func spawn(args, env []string, dir string, in, out, errf *os.File) (int, error) {
	bin := args[0]
	if !strings.Contains(bin, "/") {
		if p, err := lookPath(bin, env); err == nil {
			bin = p
		}
	}
	attr := &syscall.ProcAttr{
		Dir:   dir,
		Env:   env,
		Files: []uintptr{in.Fd(), out.Fd(), errf.Fd()},
		Sys:   &syscall.SysProcAttr{Setpgid: true},
	}
	return syscall.ForkExec(bin, args, attr)
}

func lookPath(file string, env []string) (string, error) {
	path := "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			path = kv[5:]
		}
	}
	for _, dir := range filepath.SplitList(path) {
		p := filepath.Join(dir, file)
		if st, err := os.Stat(p); err == nil && !st.IsDir() && st.Mode()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s not found in PATH", file)
}

func writeFile(path, content string) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err == nil {
		_ = os.Rename(tmp, path)
	}
}
