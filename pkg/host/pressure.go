package host

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// PressureTrigger is one PSI trigger on a cgroup: the kernel wakes the
// watcher when the cgroup's tasks stalled on the resource for more than
// Threshold within any Window (cgroup v2 PSI triggers: "some <threshold
// us> <window us>" written to cpu.pressure or memory.pressure, then polled
// for POLLPRI). The window must be 500 ms to 10 s.
type PressureTrigger struct {
	Resource  string // "cpu" or "memory"
	Threshold time.Duration
	Window    time.Duration
}

// WatchPressure installs the triggers on the cgroup and calls fn with the
// resource name each time one fires, until ctx ends. The kernel re-arms a
// trigger per window, so a stall that lasts gives one event per window.
// Triggers are deleted when their files close.
func WatchPressure(ctx context.Context, cgroupPath string, triggers []PressureTrigger, fn func(resource string, at time.Time)) error {
	ep, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return fmt.Errorf("epoll: %w", err)
	}
	fds := map[int32]string{}
	for _, t := range triggers {
		fd, err := syscall.Open(filepath.Join(cgroupPath, t.Resource+".pressure"), syscall.O_RDWR|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("open %s.pressure: %w", t.Resource, err)
		}
		spec := fmt.Sprintf("some %d %d", t.Threshold.Microseconds(), t.Window.Microseconds())
		if _, err := syscall.Write(fd, []byte(spec)); err != nil {
			_ = syscall.Close(fd)
			return fmt.Errorf("trigger %q on %s.pressure: %w", spec, t.Resource, err)
		}
		if err := syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Events: syscall.EPOLLPRI, Fd: int32(fd)}); err != nil {
			_ = syscall.Close(fd)
			return fmt.Errorf("epoll add: %w", err)
		}
		fds[int32(fd)] = t.Resource
	}
	go func() {
		defer func() {
			for fd := range fds {
				_ = syscall.Close(int(fd))
			}
			_ = syscall.Close(ep)
		}()
		events := make([]syscall.EpollEvent, 4)
		for ctx.Err() == nil {
			n, err := syscall.EpollWait(ep, events, 1000)
			if err != nil {
				if err == syscall.EINTR {
					continue
				}
				return
			}
			for _, ev := range events[:n] {
				if ev.Events&syscall.EPOLLERR != 0 {
					return // the trigger is gone (the cgroup was removed)
				}
				if ev.Events&syscall.EPOLLPRI != 0 {
					fn(fds[ev.Fd], time.Now())
				}
			}
		}
	}()
	return nil
}

// CgroupCPU reads a cgroup's cpu.stat: CPU used and time throttled, in
// nanoseconds. Zero when unreadable.
func CgroupCPU(cgroupPath string) (usage, throttled uint64) {
	kv := readKV(filepath.Join(cgroupPath, "cpu.stat"))
	return kv["usage_usec"] * 1000, kv["throttled_usec"] * 1000
}

// CgroupMemory reads a cgroup's memory: in use, the working set (in use
// less inactive file cache) and in swap, in bytes.
func CgroupMemory(cgroupPath string) (current, workingSet, swap uint64) {
	current = readUint(filepath.Join(cgroupPath, "memory.current"))
	inactive := readKV(filepath.Join(cgroupPath, "memory.stat"))["inactive_file"]
	workingSet = current - min(inactive, current)
	swap = readUint(filepath.Join(cgroupPath, "memory.swap.current"))
	return current, workingSet, swap
}

// CgroupExists says whether the cgroup directory is there.
func CgroupExists(cgroupPath string) bool {
	_, err := os.Stat(cgroupPath)
	return err == nil
}
