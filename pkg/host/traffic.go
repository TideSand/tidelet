package host

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Traffic meters memory traffic: the bytes the cores move to and from
// DRAM, which is what slows every other process on the host when it piles
// up, long before any CPU is short. There is no cgroup file for it; the
// CPU's performance counters have it. One counter per CPU counts the reads
// that miss the last-level cache (bus accesses or generic cache misses
// where the PMU has no such event) and, every Period events, records the
// cgroup of the task that was running: the counter totals give the
// traffic, the samples share it out among cgroups. Sixty CPUs cost sixty
// descriptors and a few thousand samples a second at saturation; the
// sandboxes themselves cost nothing.
//
// Calibration (Graviton3, Neoverse V1): streaming 32 processes
// saturates DRAM at ~250 GB/s of bus accesses (32 B beats), of which
// ll_cache_miss_rd x 64 B is the 63 % that is reads, the rest writes, so a
// read miss stands for 96 B of DRAM traffic. The bus counter cannot be
// used directly: it also counts the traffic the system-level cache serves,
// 4-10x the DRAM traffic for cache-friendly compute (BLAS), and it read
// 220-250 GB/s on a pool whose DRAM reads were 7 GB/s.
//
// Only the root user, or one with CAP_PERFMON, can open CPU-wide counters.
type Traffic struct {
	// Event is the counter used, BytesPerEvent what one event moves (96 for
	// a last-level cache read miss, a 64 B line plus the writes that come
	// with it; 32 for a bus access, one beat of the core's bus on Arm
	// Neoverse; 64 for a generic cache miss).
	Event         string
	BytesPerEvent uint64
	// Period is the number of events between two samples.
	Period uint64
	// Lost counts the samples the kernel dropped because a ring was full.
	Lost uint64

	cpus []trafficCPU
	last time.Time
}

type trafficCPU struct {
	cpu   int
	fd    int
	ring  []byte
	page  *unix.PerfEventMmapPage
	data  []byte
	count uint64
}

// Perf constants x/sys/unix does not define.
const (
	perfTypeHardware       = 0
	perfTypeRaw            = 4
	perfCountHWCacheMisses = 5
	perfSampleCgroup       = 1 << 21 // PERF_SAMPLE_CGROUP (Linux 5.7): the running task's cgroup id
	perfRecordLost         = 2
	perfRecordSample       = 9
	ringPages              = 32 // data pages per CPU: 128 KiB, 8192 samples between two reads
)

// NewTraffic opens the counters on the given CPUs (all when empty) and
// starts counting. Period is in events; 0 means 2^19 (16 MiB of bus
// accesses between samples).
func NewTraffic(cpus []int, period uint64) (*Traffic, error) {
	if len(cpus) == 0 {
		for i := 0; i < numCPUs(); i++ {
			cpus = append(cpus, i)
		}
	}
	if period == 0 {
		period = 1 << 19
	}
	typ, config, name, bpe, err := trafficEvent()
	if err != nil {
		return nil, err
	}
	t := &Traffic{Event: name, BytesPerEvent: bpe, Period: period}
	page := os.Getpagesize()
	for _, cpu := range cpus {
		attr := unix.PerfEventAttr{Type: typ, Config: config, Sample: period, Sample_type: perfSampleCgroup}
		attr.Size = uint32(unsafe.Sizeof(attr))
		fd, err := unix.PerfEventOpen(&attr, -1, cpu, -1, unix.PERF_FLAG_FD_CLOEXEC)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("perf_event_open %s on cpu %d: %w", name, cpu, err)
		}
		ring, err := unix.Mmap(fd, 0, (1+ringPages)*page, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			_ = unix.Close(fd)
			t.Close()
			return nil, fmt.Errorf("mmap perf ring on cpu %d: %w", cpu, err)
		}
		c := trafficCPU{cpu: cpu, fd: fd, ring: ring, page: (*unix.PerfEventMmapPage)(unsafe.Pointer(&ring[0]))}
		off, size := int(c.page.Data_offset), int(c.page.Data_size)
		if off == 0 || size == 0 {
			off, size = page, ringPages*page
		}
		c.data = ring[off : off+size]
		t.cpus = append(t.cpus, c)
	}
	t.last = time.Now()
	return t, nil
}

// Read returns the traffic since the previous Read (or since the counters
// were opened), in bytes per second: the total over all the CPUs, and the
// share of it that fell to each cgroup, by cgroup id (see CgroupID). Every
// sample stands for Period events; traffic sampled in no cgroup (idle, or
// the root) is only in the total.
func (t *Traffic) Read(now time.Time) (total uint64, byCgroup map[uint64]uint64) {
	dt := now.Sub(t.last).Seconds()
	t.last = now
	samples := map[uint64]uint64{}
	var events uint64
	var buf [8]byte
	for i := range t.cpus {
		c := &t.cpus[i]
		if n, err := unix.Read(c.fd, buf[:]); err == nil && n == 8 {
			count := binary.LittleEndian.Uint64(buf[:])
			if count >= c.count {
				events += count - c.count
			}
			c.count = count
		}
		head := atomic.LoadUint64(&c.page.Data_head)
		tail, lost := drain(c.data, atomic.LoadUint64(&c.page.Data_tail), head, samples)
		atomic.StoreUint64(&c.page.Data_tail, tail)
		t.Lost += lost
	}
	if dt <= 0 {
		return 0, nil
	}
	total = uint64(float64(events*t.BytesPerEvent) / dt)
	byCgroup = make(map[uint64]uint64, len(samples))
	for id, n := range samples {
		byCgroup[id] = uint64(float64(n*t.Period*t.BytesPerEvent) / dt)
	}
	return total, byCgroup
}

// drain consumes the records between tail and head of a ring's data area:
// samples are counted per cgroup id, lost records summed. It returns the
// new tail and the samples lost.
func drain(data []byte, tail, head uint64, samples map[uint64]uint64) (newTail, lost uint64) {
	size := uint64(len(data))
	var buf [24]byte
	at := func(off, n uint64) []byte {
		i := off % size
		if i+n <= size {
			return data[i : i+n]
		}
		k := copy(buf[:n], data[i:])
		copy(buf[k:n], data[:n-uint64(k)])
		return buf[:n]
	}
	for tail < head {
		hdr := at(tail, 8)
		typ, recSize := binary.LittleEndian.Uint32(hdr[0:4]), uint64(binary.LittleEndian.Uint16(hdr[6:8]))
		if recSize < 8 || tail+recSize > head {
			return head, lost // a torn record: skip to the head
		}
		switch typ {
		case perfRecordSample:
			if recSize >= 16 {
				samples[binary.LittleEndian.Uint64(at(tail+8, 8))]++
			}
		case perfRecordLost:
			if recSize >= 24 {
				lost += binary.LittleEndian.Uint64(at(tail+16, 8))
			}
		}
		tail += recSize
	}
	return tail, lost
}

// CPUs is the CPUs metered.
func (t *Traffic) CPUs() []int {
	out := make([]int, 0, len(t.cpus))
	for _, c := range t.cpus {
		out = append(out, c.cpu)
	}
	return out
}

// Close releases the counters.
func (t *Traffic) Close() {
	for _, c := range t.cpus {
		if c.ring != nil {
			_ = unix.Munmap(c.ring)
		}
		_ = unix.Close(c.fd)
	}
	t.cpus = nil
}

// trafficEvent picks the counter: the PMU's last-level cache read miss
// event (Arm: reads served by DRAM, whatever the system-level cache does),
// else its bus_access event, else the generic last-level cache miss count.
func trafficEvent() (typ uint32, config uint64, name string, bytesPerEvent uint64, err error) {
	for _, ev := range []struct {
		name  string
		bytes uint64
	}{{"ll_cache_miss_rd", 96}, {"bus_access", 32}} {
		matches, _ := filepath.Glob("/sys/bus/event_source/devices/*/events/" + ev.name)
		if len(matches) == 0 {
			continue
		}
		dev := filepath.Dir(filepath.Dir(matches[0]))
		pmuType, terr := readUintFile(filepath.Join(dev, "type"))
		spec, serr := os.ReadFile(matches[0])
		if terr != nil || serr != nil {
			continue
		}
		for _, kv := range strings.Split(strings.TrimSpace(string(spec)), ",") {
			if k, v, ok := strings.Cut(kv, "="); ok && k == "event" {
				if code, perr := strconv.ParseUint(strings.TrimPrefix(v, "0x"), 16, 64); perr == nil {
					return uint32(pmuType), code, ev.name, ev.bytes, nil
				}
			}
		}
	}
	return perfTypeHardware, perfCountHWCacheMisses, "cache-misses", 64, nil
}

func readUintFile(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

func numCPUs() int {
	var set unix.CPUSet
	if err := unix.SchedGetaffinity(0, &set); err == nil && set.Count() > 0 {
		n := 0
		for i := 0; i < len(set)*64; i++ {
			if set.IsSet(i) {
				n = i + 1
			}
		}
		return n
	}
	return 1
}

// CgroupID is the id the kernel stamps on samples of a cgroup's tasks: on
// cgroup v2 the inode number of the cgroup's directory.
func CgroupID(cgroupPath string) (uint64, bool) {
	st, err := os.Stat(cgroupPath)
	if err != nil {
		return 0, false
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return sys.Ino, true
}

// CPUList parses a cpuset list ("0-7,12") into CPU numbers.
func CPUList(list string) []int {
	var out []int
	for _, part := range strings.Split(strings.TrimSpace(list), ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(strings.TrimSpace(lo))
		if err != nil {
			continue
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(strings.TrimSpace(hi)); err != nil {
				continue
			}
		}
		for i := a; i <= b; i++ {
			out = append(out, i)
		}
	}
	return out
}

// PoolCPUs is the CPUs of a cgroup's effective cpuset, nil when unknown.
func PoolCPUs(cgroupPath string) []int {
	data, err := os.ReadFile(filepath.Join(cgroupPath, "cpuset.cpus.effective"))
	if err != nil {
		return nil
	}
	return CPUList(string(data))
}
