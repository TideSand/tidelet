// Package host reads the host's physical capacity and current pressure.
package host

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/TideSand/tidelet/pkg/resource"
)

// Detect returns the host's CPU count in millicores and total memory in
// bytes.
func Detect() (resource.Set, error) {
	mi, err := readMeminfo("/proc/meminfo")
	if err != nil {
		return resource.Set{}, err
	}
	return resource.Set{
		CPU:    resource.Millicores(runtime.NumCPU()) * 1000,
		Memory: resource.Bytes(mi["MemTotal"]),
	}, nil
}

// Pressure is one line of /proc/pressure/<resource> (PSI). Values are
// percentages of wall time over the trailing window.
type Pressure struct {
	Avg10  float64
	Avg60  float64
	Avg300 float64
	// Total is cumulative stall time in microseconds.
	Total uint64
}

// PSI holds the "some" and "full" lines for one resource.
type PSI struct {
	Some Pressure
	Full Pressure
}

// Sample is a point-in-time view of host-wide resource state. Policies
// receive it alongside per-sandbox stats.
type Sample struct {
	Time time.Time
	// Memory figures from /proc/meminfo, in bytes.
	MemTotal     uint64
	MemAvailable uint64
	MemFree      uint64
	SwapTotal    uint64
	SwapFree     uint64
	// Load averages from /proc/loadavg.
	Load1, Load5, Load15 float64
	// Cumulative CPU time of all CPUs from /proc/stat, in jiffies: busy is
	// everything but idle and iowait. Two samples give the busy fraction
	// over the interval between them, see BusyFraction.
	CPUBusy, CPUTotal uint64
	// Pressure stall information. Zero when the kernel does not expose it.
	CPU    PSI
	Memory PSI
	IO     PSI
	// RunWaitNanos is the cumulative time tasks spent runnable but waiting
	// for a CPU, summed over the CPUs sampled (the run_delay field of each
	// cpuN line of /proc/schedstat), in nanoseconds. Two samples give the
	// average number of tasks waiting over the interval, see WaitingTasks;
	// unlike the busy fraction it keeps rising once every CPU is busy, so
	// it tells a full machine from an overloaded one. Zero when the kernel
	// does not expose it.
	RunWaitNanos uint64
	// Bandwidth is the memory traffic of the pool's CPUs over the interval
	// since the previous sample, in bytes per second (see Traffic); zero
	// when the daemon has no traffic meter.
	Bandwidth uint64
}

// Collect reads the host sample. Missing PSI files are not an error.
func Collect() (Sample, error) {
	s := Sample{Time: time.Now()}
	mi, err := readMeminfo("/proc/meminfo")
	if err != nil {
		return s, err
	}
	s.MemTotal = mi["MemTotal"]
	s.MemAvailable = mi["MemAvailable"]
	s.MemFree = mi["MemFree"]
	s.SwapTotal = mi["SwapTotal"]
	s.SwapFree = mi["SwapFree"]
	if data, err := os.ReadFile("/proc/loadavg"); err == nil {
		f := strings.Fields(string(data))
		if len(f) >= 3 {
			s.Load1, _ = strconv.ParseFloat(f[0], 64)
			s.Load5, _ = strconv.ParseFloat(f[1], 64)
			s.Load15, _ = strconv.ParseFloat(f[2], 64)
		}
	}
	s.CPUBusy, s.CPUTotal = readCPUStat("/proc/stat")
	s.CPU = readPSI("/proc/pressure/cpu")
	s.RunWaitNanos = readRunWait("/proc/schedstat", nil)
	s.Memory = readPSI("/proc/pressure/memory")
	s.IO = readPSI("/proc/pressure/io")
	return s, nil
}

// BusyFraction is the share of CPU time spent busy between two samples,
// 0..1, or -1 when the samples do not span any time (same or missing
// counters).
func BusyFraction(prev, cur Sample) float64 {
	if cur.CPUTotal <= prev.CPUTotal || cur.CPUBusy < prev.CPUBusy {
		return -1
	}
	return float64(cur.CPUBusy-prev.CPUBusy) / float64(cur.CPUTotal-prev.CPUTotal)
}

// WaitingTasks is the average number of runnable tasks waiting for a CPU
// between two samples, -1 when unknown.
func WaitingTasks(prev, cur Sample) float64 {
	dt := cur.Time.Sub(prev.Time)
	if dt <= 0 || cur.RunWaitNanos < prev.RunWaitNanos || prev.RunWaitNanos == 0 {
		return -1
	}
	return float64(cur.RunWaitNanos-prev.RunWaitNanos) / float64(dt.Nanoseconds())
}

// readRunWait sums the run_delay field (the 8th number after the cpu name,
// schedstat version 15 and later) of the cpuN lines of a /proc/schedstat
// file, over the CPUs in only (all when nil).
func readRunWait(path string, only map[int]bool) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var total uint64
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 9 {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(f[0], "cpu"))
		if err != nil || (only != nil && !only[n]) {
			continue
		}
		v, err := strconv.ParseUint(f[8], 10, 64)
		if err == nil {
			total += v
		}
	}
	return total
}

// MemoryUsedFraction is 1 - MemAvailable/MemTotal, 0 when unknown.
func (s Sample) MemoryUsedFraction() float64 {
	if s.MemTotal == 0 {
		return 0
	}
	return 1 - float64(s.MemAvailable)/float64(s.MemTotal)
}

// readCPUStat returns (busy, total) jiffies from the aggregate "cpu" line of
// /proc/stat; iowait counts as idle. Zeros when the file cannot be read.
func readCPUStat(path string) (busy, total uint64) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		var idle uint64
		for i, f := range fields[1:] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return 0, 0
			}
			total += v
			if i == 3 || i == 4 { // idle, iowait
				idle += v
			}
		}
		return total - idle, total
	}
	return 0, 0
}

// HostMemory is the host's memory in bytes: the total and what it can still
// give without reclaim (MemAvailable), 0 when /proc/meminfo cannot be read.
// The pool cgroup's own figures miss what runs outside it on the host (the
// shims and controllers of every sandbox).
func HostMemory() (total, available uint64) {
	mi, err := readMeminfo("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	return mi["MemTotal"], mi["MemAvailable"]
}

// ProcsRunning is the number of runnable tasks on the host at this instant
// (/proc/stat procs_running: on a CPU or waiting for one; tasks throttled by
// their cgroup's cpu.max are not counted). 0 when it cannot be read.
func ProcsRunning() uint64 {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if n, ok := strings.CutPrefix(sc.Text(), "procs_running "); ok {
			v, _ := strconv.ParseUint(strings.TrimSpace(n), 10, 64)
			return v
		}
	}
	return 0
}

// readMeminfo returns /proc/meminfo values in bytes.
func readMeminfo(path string) (map[string]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	out := map[string]uint64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) >= 3 && fields[2] == "kB" {
			v *= 1024
		}
		out[strings.TrimSuffix(fields[0], ":")] = v
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if _, ok := out["MemTotal"]; !ok {
		return nil, fmt.Errorf("MemTotal not found in %s", path)
	}
	return out, nil
}

func readPSI(path string) PSI {
	var p PSI
	data, err := os.ReadFile(path)
	if err != nil {
		return p
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		var pr Pressure
		for _, kv := range fields[1:] {
			k, v, _ := strings.Cut(kv, "=")
			switch k {
			case "avg10":
				pr.Avg10, _ = strconv.ParseFloat(v, 64)
			case "avg60":
				pr.Avg60, _ = strconv.ParseFloat(v, 64)
			case "avg300":
				pr.Avg300, _ = strconv.ParseFloat(v, 64)
			case "total":
				pr.Total, _ = strconv.ParseUint(v, 10, 64)
			}
		}
		switch fields[0] {
		case "some":
			p.Some = pr
		case "full":
			p.Full = pr
		}
	}
	return p
}

// RunnableOn is the number of tasks running or waiting to run on the given
// CPUs at this instant: the sum of the run-queue lengths the scheduler's
// debug view reports for them (/sys/kernel/debug/sched/debug, root and a
// mounted debugfs). `cpus` is a cpuset list ("0-55", "0,2,4-7"); empty means
// every CPU. Unlike ProcsRunning it leaves out what runs elsewhere on the
// host (a workload driver pinned to other cores), so it is the demand on a
// pool confined to a cpuset. ok is false when the view cannot be read.
func RunnableOn(cpus string) (n uint64, ok bool) {
	f, err := os.Open("/sys/kernel/debug/sched/debug")
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()
	var want map[int]bool
	if strings.TrimSpace(cpus) != "" {
		want = parseCPUList(cpus)
	}
	return runnableIn(f, want)
}

// runnableIn sums the ".nr_running" of each "cpu#N" block in a sched debug
// view, for the CPUs in `want` (every CPU when want is nil).
func runnableIn(r io.Reader, want map[int]bool) (uint64, bool) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var total uint64
	cpu, counted, seen := -1, false, false
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, "cpu#"); ok {
			id := rest
			if i := strings.IndexAny(rest, ",: "); i >= 0 {
				id = rest[:i]
			}
			cpu, _ = strconv.Atoi(id)
			counted = false
			seen = true
			continue
		}
		if counted || cpu < 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == ".nr_running" {
			counted = true
			if want == nil || want[cpu] {
				v, _ := strconv.ParseUint(fields[2], 10, 64)
				total += v
			}
		}
	}
	return total, seen && sc.Err() == nil
}

// RunnableByCgroup is the runnable tasks of each child cgroup of `cgroupPath`
// (a cgroup v2 path under /sys/fs/cgroup), on the CPUs of `cpus` (every CPU
// when empty), keyed by the child's name: the sum over those CPUs of the
// child's ".h_nr_runnable" in the scheduler's debug view (".h_nr_running" on
// older kernels). The total of the pool's CPUs' run queues comes with it, as
// RunnableOn reads it. ok is false when the view cannot be read.
func RunnableByCgroup(cgroupPath, cpus string) (byChild map[string]uint64, total uint64, ok bool) {
	f, err := os.Open("/sys/kernel/debug/sched/debug")
	if err != nil {
		return nil, 0, false
	}
	defer func() { _ = f.Close() }()
	var want map[int]bool
	if strings.TrimSpace(cpus) != "" {
		want = parseCPUList(cpus)
	}
	rel := strings.TrimPrefix(cgroupPath, "/sys/fs/cgroup")
	if rel == "" {
		rel = "/"
	}
	return runnableByCgroupIn(f, want, rel)
}

// runnableByCgroupIn reads the per-cgroup run queues ("cfs_rq[N]:<path>"
// blocks) of the children of `parent` in a sched debug view, and the per-CPU
// totals ("cpu#N" blocks), for the CPUs in `want` (every CPU when want is nil).
func runnableByCgroupIn(r io.Reader, want map[int]bool, parent string) (map[string]uint64, uint64, bool) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	by := map[string]uint64{}
	var total uint64
	prefix := strings.TrimSuffix(parent, "/") + "/"
	cpu, child, counted, seen := -1, "", false, false
	for sc.Scan() {
		line := sc.Text()
		if rest, ok := strings.CutPrefix(line, "cpu#"); ok {
			id := rest
			if i := strings.IndexAny(rest, ",: "); i >= 0 {
				id = rest[:i]
			}
			cpu, _ = strconv.Atoi(id)
			child, counted, seen = "", false, true
			continue
		}
		if rest, ok := strings.CutPrefix(line, "cfs_rq["); ok {
			// cfs_rq[3]:/moby/02e2250c6df9dad0c5b7f3ad4df9936a
			child, counted = "", false
			if i := strings.Index(rest, "]:"); i >= 0 {
				if c, _ := strconv.Atoi(rest[:i]); c >= 0 {
					cpu = c
				}
				if name, ok := strings.CutPrefix(rest[i+2:], prefix); ok && name != "" {
					if j := strings.IndexByte(name, '/'); j >= 0 {
						name = name[:j]
					}
					child = name
				}
			}
			continue
		}
		if counted || cpu < 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		switch {
		case child == "" && fields[0] == ".nr_running":
			counted = true
			if want == nil || want[cpu] {
				v, _ := strconv.ParseUint(fields[2], 10, 64)
				total += v
			}
		case child != "" && (fields[0] == ".h_nr_runnable" || fields[0] == ".h_nr_running"):
			counted = true
			if want == nil || want[cpu] {
				v, _ := strconv.ParseUint(fields[2], 10, 64)
				by[child] += v
			}
		}
	}
	return by, total, seen && sc.Err() == nil
}

// CgroupCPUs is the CPUs a cgroup may run on (cpuset.cpus.effective), as a
// cpuset list; empty when the file cannot be read or names every CPU.
func CgroupCPUs(cgroupPath string) string {
	data, err := os.ReadFile(cgroupPath + "/cpuset.cpus.effective")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
