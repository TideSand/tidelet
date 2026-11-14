package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/TideSand/tidelet/pkg/resource"
)

// CgroupRoot is where cgroup v2 is mounted.
const CgroupRoot = "/sys/fs/cgroup"

// Scope is the cgroup that holds every sandbox of a daemon (containerd puts
// them under /sys/fs/cgroup/<namespace>, see the containerd backend's
// CgroupParent), used as the pool's boundary: its cpuset and memory.max
// are the capacity the policy works with, and its cpu.stat, memory.stat
// and pressure files say what the pool, not the whole host, is doing. A
// daemon run this way shares the machine with other work (a benchmark's
// driver, say) without that work showing in the policy's measurements or
// taking the pool's capacity.
type Scope struct {
	// Path is the cgroup's directory under CgroupRoot.
	Path string
	cpus int
}

// NewScope opens, creating it if needed, the cgroup at path (an absolute
// path under CgroupRoot, or a cgroup path like "/tidelet").
func NewScope(path string) (*Scope, error) {
	if !strings.HasPrefix(path, CgroupRoot+"/") {
		path = filepath.Join(CgroupRoot, path)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("cgroup %s: %w", path, err)
	}
	s := &Scope{Path: path}
	s.cpus = s.effectiveCPUs()
	return s, nil
}

// CgroupPath is the scope as an OCI cgroups path ("/tidelet" for
// /sys/fs/cgroup/tidelet), for the runtime to create sandboxes under.
func (s *Scope) CgroupPath() string {
	return "/" + strings.TrimPrefix(strings.TrimPrefix(s.Path, CgroupRoot), "/")
}

// Apply pins the scope to a cpuset ("0-7", empty leaves it) and caps its
// memory (0 leaves it). The controllers are enabled on the parent's
// subtree_control as needed.
func (s *Scope) Apply(cpuset string, memory resource.Bytes) error {
	parent := filepath.Dir(s.Path)
	var enable []string
	if cpuset != "" {
		enable = append(enable, "+cpuset")
	}
	if memory > 0 {
		enable = append(enable, "+memory")
	}
	if len(enable) > 0 {
		if err := os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte(strings.Join(enable, " ")), 0); err != nil && !errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("enable controllers under %s: %w", parent, err)
		}
	}
	if cpuset != "" {
		if err := os.WriteFile(filepath.Join(s.Path, "cpuset.cpus"), []byte(cpuset), 0); err != nil {
			return fmt.Errorf("cpuset.cpus of %s: %w", s.Path, err)
		}
	}
	if memory > 0 {
		if err := os.WriteFile(filepath.Join(s.Path, "memory.max"), []byte(strconv.FormatInt(int64(memory), 10)), 0); err != nil {
			return fmt.Errorf("memory.max of %s: %w", s.Path, err)
		}
	}
	s.cpus = s.effectiveCPUs()
	return nil
}

// Capacity is what the scope may use: the CPUs of its effective cpuset
// and its memory.max, falling back to the host's figures where the scope
// is unlimited.
func (s *Scope) Capacity() (resource.Set, error) {
	c, err := Detect()
	if err != nil {
		return resource.Set{}, err
	}
	if s.cpus > 0 {
		c.CPU = resource.Millicores(s.cpus) * 1000
	}
	if m := s.memoryMax(); m > 0 {
		c.Memory = resource.Bytes(m)
	}
	return c, nil
}

// CPUs is the number of CPUs in the scope's effective cpuset (the host's
// when the file cannot be read).
func (s *Scope) CPUs() int { return s.cpus }

func (s *Scope) effectiveCPUs() int {
	if data, err := os.ReadFile(filepath.Join(s.Path, "cpuset.cpus.effective")); err == nil {
		if n := countCPUList(strings.TrimSpace(string(data))); n > 0 {
			return n
		}
	}
	if c, err := Detect(); err == nil {
		return int(c.CPU / 1000)
	}
	return 1
}

// memoryMax is the scope's memory.max in bytes, 0 when "max" or unreadable.
func (s *Scope) memoryMax() uint64 {
	data, err := os.ReadFile(filepath.Join(s.Path, "memory.max"))
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	return n
}

// cpuSet is the scope's effective CPUs as a set, nil (every CPU) when the
// file cannot be read.
func (s *Scope) cpuSet() map[int]bool {
	data, err := os.ReadFile(filepath.Join(s.Path, "cpuset.cpus.effective"))
	if err != nil {
		return nil
	}
	set := parseCPUList(strings.TrimSpace(string(data)))
	if len(set) == 0 {
		return nil
	}
	return set
}

// parseCPUList reads a cpuset list such as "0-7,12-15" into a set.
func parseCPUList(list string) map[int]bool {
	out := map[int]bool{}
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			continue
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				continue
			}
		}
		for c := a; c <= b; c++ {
			out[c] = true
		}
	}
	return out
}

// countCPUList counts the CPUs in a cpuset list such as "0-7,12-15".
func countCPUList(list string) int {
	n := 0
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			continue
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				continue
			}
		}
		if b >= a {
			n += b - a + 1
		}
	}
	return n
}

// Collect reads the scope as a Sample with the same meaning as the host's:
// CPUBusy and CPUTotal are cumulative microseconds, the scope's cpu.stat
// usage against wall time over its CPUs, so BusyFraction works unchanged;
// MemTotal is the scope's memory.max (the host's memory when unlimited)
// and MemAvailable what is left of it after the scope's memory in use,
// which excludes its file cache (as MemAvailable does for the host);
// pressure is the scope's own. Swap and load averages are the host's.
func (s *Scope) Collect() (Sample, error) {
	sample, err := Collect()
	if err != nil {
		return sample, err
	}
	stat := readKV(filepath.Join(s.Path, "cpu.stat"))
	sample.CPUBusy = stat["usage_usec"]
	sample.CPUTotal = uint64(time.Now().UnixMicro()) * uint64(max(s.cpus, 1))
	if m := s.memoryMax(); m > 0 {
		sample.MemTotal = m
	}
	mem := readKV(filepath.Join(s.Path, "memory.stat"))
	current := readUint(filepath.Join(s.Path, "memory.current"))
	cache := uint64(0)
	if mem["file"] > mem["shmem"] {
		cache = mem["file"] - mem["shmem"]
	}
	used := uint64(0)
	if current > cache {
		used = current - cache
	}
	if used > sample.MemTotal {
		used = sample.MemTotal
	}
	sample.MemAvailable = sample.MemTotal - used
	sample.MemFree = sample.MemAvailable
	sample.CPU = readPSI(filepath.Join(s.Path, "cpu.pressure"))
	sample.RunWaitNanos = readRunWait("/proc/schedstat", s.cpuSet())
	sample.Memory = readPSI(filepath.Join(s.Path, "memory.pressure"))
	sample.IO = readPSI(filepath.Join(s.Path, "io.pressure"))
	return sample, nil
}

// readKV parses a cgroup "key value" file; unreadable gives an empty map.
func readKV(path string) map[string]uint64 {
	out := map[string]uint64{}
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			if v, err := strconv.ParseUint(f[1], 10, 64); err == nil {
				out[f[0]] = v
			}
		}
	}
	return out
}

func readUint(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	return n
}
