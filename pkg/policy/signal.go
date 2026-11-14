package policy

import "time"

// SignalKind says what woke the policy.
type SignalKind int

// Signal kinds.
const (
	// SignalTimer: a Wake the policy asked for came due; Key names it.
	SignalTimer SignalKind = iota
	// SignalCPUPressure: the pool cgroup's CPU pressure trigger fired:
	// tasks in the pool waited for a CPU for longer than the trigger's
	// threshold within its window.
	SignalCPUPressure
	// SignalMemoryPressure: the pool's memory pressure trigger fired: tasks
	// stalled on memory (reclaim, swap-in) for longer than the threshold.
	SignalMemoryPressure
	// SignalBandwidthPressure: the pool's memory traffic, as the traffic
	// meter measured it over the last sample, is at or above the level the
	// policy asked to be told about (BandwidthTriggered). Sent once per
	// sample while it stays there.
	SignalBandwidthPressure
)

// String implements fmt.Stringer.
func (k SignalKind) String() string {
	switch k {
	case SignalTimer:
		return "timer"
	case SignalCPUPressure:
		return "cpu-pressure"
	case SignalMemoryPressure:
		return "memory-pressure"
	case SignalBandwidthPressure:
		return "bandwidth-pressure"
	}
	return "unknown"
}

// Signal is an event the policy asked to be woken by: a kernel pressure
// trigger or one of its own timers.
type Signal struct {
	Kind SignalKind
	Time time.Time
	// Key is the Wake's key (timer signals).
	Key string
}

// Signaled is implemented by policies that run on signals rather than on
// the usage sampler: the manager installs the pool's pressure triggers for
// them and delivers their timers. OnSignal is serialised with every other
// policy call.
type Signaled interface {
	OnSignal(sig Signal, view View) []Action
}

// BandwidthTriggered is implemented by policies that budget memory traffic:
// the manager meters the pool's traffic with the CPU's counters
// (host.Traffic), reports it in every sample (host.Sample.Bandwidth and,
// per sandbox, sandbox.Stats.BandwidthBytesPerSec) and sends a
// SignalBandwidthPressure, once per sample, while the pool's traffic is at
// or above BandwidthTrigger bytes per second (0: never).
type BandwidthTriggered interface {
	BandwidthTrigger() uint64
}

// Wake asks the manager for a timer: After the duration, the policy's
// OnSignal is called with a SignalTimer carrying Key. A Wake with a key
// already pending replaces the earlier timer; After 0 cancels it.
type Wake struct {
	Key   string
	After time.Duration
}

// Target implements Action: a timer has no sandbox.
func (a Wake) Target() string { return "" }
