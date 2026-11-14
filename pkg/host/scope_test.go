package host

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCountCPUList(t *testing.T) {
	for list, want := range map[string]int{"0-7": 8, "0-7,12-15": 12, "3": 1, "": 0, "1,2,5-6": 4, "x": 0} {
		if got := countCPUList(list); got != want {
			t.Errorf("countCPUList(%q) = %d, want %d", list, got, want)
		}
	}
}

func TestScopeCgroupPath(t *testing.T) {
	s := &Scope{Path: CgroupRoot + "/tidelet"}
	if got := s.CgroupPath(); got != "/tidelet" {
		t.Fatalf("CgroupPath = %q", got)
	}
}

func TestRunWaitSumsTheScopesCPUs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schedstat")
	data := "version 17\ntimestamp 1\ncpu0 0 0 0 0 0 0 100 1000 5\ncpu1 0 0 0 0 0 0 100 2000 5\ncpu2 0 0 0 0 0 0 100 4000 5\ndomain0 x\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readRunWait(path, nil); got != 7000 {
		t.Fatalf("all CPUs: %d, want 7000", got)
	}
	if got := readRunWait(path, parseCPUList("0,2")); got != 5000 {
		t.Fatalf("cpus 0,2: %d, want 5000", got)
	}
	prev := Sample{Time: time.Unix(0, 0), RunWaitNanos: 1_000_000_000}
	cur := Sample{Time: time.Unix(1, 0), RunWaitNanos: 3_500_000_000}
	if w := WaitingTasks(prev, cur); w != 2.5 {
		t.Fatalf("waiting tasks %v, want 2.5", w)
	}
}

func TestRunnableInSumsThePoolCPUsOnly(t *testing.T) {
	view := `Sched Debug Version: v0.11, 6.17.0 #1
cpu#0, 2500.000 MHz
  .nr_running                    : 3
  .rt_nr_running                 : 0
cpu#1, 2500.000 MHz
  .nr_running                    : 2
cpu#56, 2500.000 MHz
  .nr_running                    : 7
`
	n, ok := runnableIn(strings.NewReader(view), parseCPUList("0-55"))
	if !ok || n != 5 {
		t.Fatalf("runnableIn(0-55) = %d, %v; want 5 (cpu 56 left out)", n, ok)
	}
	n, ok = runnableIn(strings.NewReader(view), nil)
	if !ok || n != 12 {
		t.Fatalf("runnableIn(all) = %d, %v; want 12", n, ok)
	}
	if _, ok := runnableIn(strings.NewReader("no cpus here\n"), nil); ok {
		t.Fatal("a view without cpu blocks is not ok")
	}
}

func TestRunnableByCgroupSumsEachChildOnThePoolCPUs(t *testing.T) {
	view := `Sched Debug Version: v0.11, 6.17.0 #1
cpu#0, 2500.000 MHz
  .nr_running                    : 9
  .rt_nr_running                 : 0

cfs_rq[0]:/moby/aaaa
  .nr_queued                     : 8
  .h_nr_runnable                 : 8
  .h_nr_queued                   : 8
cfs_rq[0]:/moby/bbbb/sub
  .h_nr_runnable                 : 1
cfs_rq[0]:/system.slice/ssh.service
  .h_nr_runnable                 : 1
cpu#1, 2500.000 MHz
  .nr_running                    : 2
cfs_rq[1]:/moby/aaaa
  .h_nr_runnable                 : 2
cpu#56, 2500.000 MHz
  .nr_running                    : 7
cfs_rq[56]:/moby/aaaa
  .h_nr_runnable                 : 7
`
	by, total, ok := runnableByCgroupIn(strings.NewReader(view), parseCPUList("0-55"), "/moby")
	if !ok || total != 11 || by["aaaa"] != 10 || by["bbbb"] != 1 || len(by) != 2 {
		t.Fatalf("runnableByCgroupIn = %v, %d, %v; want aaaa 10, bbbb 1, total 11 (cpu 56 left out)", by, total, ok)
	}
	by, total, ok = runnableByCgroupIn(strings.NewReader(view), nil, "/moby/")
	if !ok || total != 18 || by["aaaa"] != 17 {
		t.Fatalf("runnableByCgroupIn(all) = %v, %d, %v; want aaaa 17, total 18", by, total, ok)
	}
}
