package policy

import (
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
)

func TestBusyWindow(t *testing.T) {
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	n := func(at time.Duration, busy, total uint64) host.Sample {
		return host.Sample{Time: t0.Add(at), CPUBusy: busy, CPUTotal: total}
	}
	var w BusyWindow
	// Nothing seen: unknown. One sample: still unknown.
	if b := w.Busy(host.Sample{}); b != -1 {
		t.Fatalf("empty window = %v", b)
	}
	w.Observe(n(0, 100, 1000))
	if b := w.Busy(n(0, 100, 1000)); b != -1 {
		t.Fatalf("one sample = %v", b)
	}
	// Two samples, 20 % busy between them: that is the figure, and the window starts here.
	w.Observe(n(5*time.Second, 300, 2000))
	if b := w.Busy(n(5*time.Second, 300, 2000)); b < 0.19 || b > 0.21 {
		t.Fatalf("two samples = %v", b)
	}
	// A later decision measures from the previous one: 50 % over the 15 s since.
	w.Observe(n(10*time.Second, 800, 3000))
	w.Observe(n(20*time.Second, 1300, 4000))
	if b := w.Busy(n(20*time.Second, 1300, 4000)); b < 0.49 || b > 0.51 {
		t.Fatalf("window since the previous decision = %v", b)
	}
	// A decision before a new sample: the last two samples decide (50 % between 10 s and 20 s too).
	if b := w.Busy(n(20*time.Second, 1300, 4000)); b < 0.49 || b > 0.51 {
		t.Fatalf("no new sample = %v", b)
	}
	// A decision long after the previous one: the window is stale, the last two samples decide (90 %).
	w.Observe(n(100*time.Second, 1400, 12000))
	w.Observe(n(101*time.Second, 2300, 13000))
	if b := w.Busy(n(101*time.Second, 2300, 13000)); b < 0.89 || b > 0.91 {
		t.Fatalf("stale window = %v", b)
	}
}
