package swapin

import (
	"os"
	"testing"
)

func TestProcessOnSelf(t *testing.T) {
	// Nothing of the test process is likely in swap; the walk itself must
	// succeed and the syscall must be accepted for an empty or small set.
	n, err := Process(os.Getpid())
	if err != nil {
		t.Fatalf("Process(self) = %v", err)
	}
	if n < 0 {
		t.Fatalf("negative bytes %d", n)
	}
	if _, err := Process(1 << 30); err != nil {
		t.Fatalf("missing pid should not be an error: %v", err)
	}
}

func TestCgroupOf(t *testing.T) {
	p, err := CgroupOf(os.Getpid())
	if err != nil {
		t.Skip("no cgroup v2:", err)
	}
	if p == "" || p[0] != '/' {
		t.Fatalf("CgroupOf = %q", p)
	}
}
