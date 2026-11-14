package policy

import (
	"time"

	"github.com/TideSand/tidelet/pkg/host"
)

// BusyWindow measures the host's CPU busy share for an admission decision
// made as a sandbox arrives. The policies that admit by host load used to
// check their queue every interval and measure the busy time over it; with
// arrivals paced by the workload (one request per interval, a rejected one
// repeated at the next), the window is the time since the previous
// decision, when a sample has arrived since and that decision is less than
// MaxAge old, and otherwise the last two samples the policy saw. Feed it
// every sample with Observe and ask Busy at each decision.
type BusyWindow struct {
	// MaxAge is the oldest a previous decision may be for the window to
	// start there (default a minute); past that the last two samples decide.
	MaxAge time.Duration

	last     [2]host.Sample
	samples  int
	prev     host.Sample // the node sample at the previous decision
	havePrev bool
}

// Observe folds in a usage sample.
func (w *BusyWindow) Observe(node host.Sample) {
	if node.Time.IsZero() {
		return
	}
	w.last[0], w.last[1] = w.last[1], node
	w.samples++
}

// Busy is the CPU busy share, 0..1, for a decision made now on `node`, the
// latest host sample, or -1 when there is nothing to measure yet. The next
// window starts at this sample.
func (w *BusyWindow) Busy(node host.Sample) float64 {
	maxAge := w.MaxAge
	if maxAge <= 0 {
		maxAge = time.Minute
	}
	busy := -1.0
	if w.havePrev && node.Time.After(w.prev.Time) && node.Time.Sub(w.prev.Time) <= maxAge {
		busy = host.BusyFraction(w.prev, node)
	}
	if busy < 0 && w.samples >= 2 {
		busy = host.BusyFraction(w.last[0], w.last[1])
	}
	if !node.Time.IsZero() {
		w.prev, w.havePrev = node, true
	}
	return busy
}
