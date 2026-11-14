package host

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestCPUList(t *testing.T) {
	got := CPUList("0-3,8, 10-11\n")
	want := []int{0, 1, 2, 3, 8, 10, 11}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if CPUList("") != nil {
		t.Fatal("empty list should be nil")
	}
}

// record appends a perf record: header (type, misc, size) then the body.
func record(buf []byte, off int, typ uint32, body ...uint64) int {
	size := 8 + 8*len(body)
	rec := make([]byte, size)
	binary.LittleEndian.PutUint32(rec[0:4], typ)
	binary.LittleEndian.PutUint16(rec[6:8], uint16(size))
	for i, v := range body {
		binary.LittleEndian.PutUint64(rec[8+8*i:], v)
	}
	for i, b := range rec {
		buf[(off+i)%len(buf)] = b
	}
	return off + size
}

func TestDrainCountsSamplesPerCgroupAcrossTheWrap(t *testing.T) {
	data := make([]byte, 96)
	// Start late in the ring so the last record straddles its end.
	tail := 40
	head := record(data, tail, perfRecordSample, 7)
	head = record(data, head, perfRecordSample, 9)
	head = record(data, head, perfRecordSample, 7)
	head = record(data, head, perfRecordLost, 1, 5) // id 1, 5 lost; wraps
	samples := map[uint64]uint64{}
	newTail, lost := drain(data, uint64(tail), uint64(head), samples)
	if newTail != uint64(head) || lost != 5 || samples[7] != 2 || samples[9] != 1 || len(samples) != 2 {
		t.Fatalf("tail %d (head %d), lost %d, samples %v", newTail, head, lost, samples)
	}
	// A torn record (size past the head) is skipped to the head.
	data = make([]byte, 64)
	binary.LittleEndian.PutUint16(data[6:8], 48)
	if nt, _ := drain(data, 0, 16, map[uint64]uint64{}); nt != 16 {
		t.Fatalf("torn record: tail %d", nt)
	}
}

func TestTrafficLive(t *testing.T) {
	tr, err := NewTraffic([]int{0}, 1<<16)
	if err != nil {
		if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENODEV) {
			t.Skipf("no CPU-wide counters for this user: %v", err)
		}
		t.Fatal(err)
	}
	defer tr.Close()
	// Move some memory on CPU 0 and read.
	big := make([]byte, 64<<20)
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		for i := range big {
			big[i]++
		}
	}
	total, by := tr.Read(time.Now())
	t.Logf("event %s: %d MB/s total, %d cgroups sampled, lost %d", tr.Event, total>>20, len(by), tr.Lost)
	if os.Getuid() != 0 {
		return
	}
	if total == 0 {
		t.Fatal("no traffic counted")
	}
	// Attribution: a process started in a cgroup of its own shows up under
	// that cgroup's id. The probe reads a 64 MB file from the page cache
	// over and over (reads miss every cache; the counter is of read misses,
	// so a write-only probe such as dd from /dev/zero would not show).
	cg := "/sys/fs/cgroup/tidelet-traffic-test"
	if err := os.Mkdir(cg, 0o755); err != nil && !os.IsExist(err) {
		t.Skipf("no cgroup for the attribution check: %v", err)
	}
	defer func() { _ = os.Remove(cg) }()
	cgFD, err := os.Open(cg)
	if err != nil {
		t.Fatal(err)
	}
	defer cgFD.Close()
	file := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(file, make([]byte, 64<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("taskset", "-c", "0", "sh", "-c", "while :; do dd if="+file+" of=/dev/null bs=1M 2>/dev/null; done")
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(cgFD.Fd())}
	if err := cmd.Start(); err != nil {
		t.Skipf("no probe: %v", err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()
	tr.Read(time.Now())
	time.Sleep(time.Second)
	total, by = tr.Read(time.Now())
	id, _ := CgroupID(cg)
	t.Logf("dd in its cgroup (id %d): %d MB/s of %d MB/s total; sampled cgroups %d", id, by[id]>>20, total>>20, len(by))
	if by[id] < 100<<20 || by[id] > total {
		t.Fatalf("attribution: cgroup %d got %d B/s of %d", id, by[id], total)
	}
}
