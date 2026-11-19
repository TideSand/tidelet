// Package tidesand implements the "tidesand" policy, TideSand: a ceiling on
// the pool's measured resources (agent-aware scheduling), with two
// contingencies, speculative swapping and selective rollback, kept rare.
//
// Agent-aware scheduling (the rule). The pool's CPU has a
// ceiling of target x cores on what is measured in use plus what the
// measurement does not show yet: for `sustain` seconds after its release a
// heavy call (a model response carrying tool calls that run outside the
// agent) counts at its reservation less what its sandbox is measured to use,
// and for `settle` seconds after its start a sandbox counts at `boot` less
// what it is measured to use. A call's reservation is the demand its
// sandbox's last calls sustained (the peak of their use averaged over
// `sustain` seconds), the whole limit when that is half the limit or more,
// at most its limit. The pool's memory traffic has the same ceiling,
// `bwTarget` x `bandwidth` bytes/s, on the same terms (a call's traffic
// reservation is what its sandbox's last calls sustained; the meter is the
// manager's bus-access meter, smoothed over `smooth`). A response is released
// when its reservations fit both; otherwise it is held, oldest first, until
// something ends, and a younger response that fits passes an older one that
// does not. An arrival is admitted while no held response has waited
// `maxPass`, its boot fits the CPU ceiling, and its memory fits the memory
// target with room left for the parked residents whose responses wait.
// Idle residents burning a background load are throttled to `idleQuota`
// until their next response.
//
// Speculative swapping, rarely. Memory is residency: a sandbox waiting on
// the model holds its working set and uses no CPU. Only when the pool's
// memory passes `park` of its capacity (0.9: above the admission target,
// so the room is being eaten by the residents' growth, not by arrivals,
// and the swap is a safety valve, not a way to admit more: on a volume with
// an IOPS cap, parking at the admission target churned 13 GB through the
// swap and the pool ran at a fifth of its cores) are the idle residents
// holding most (waiting at least `parkIdle`) reclaimed to swap,
// `parkFraction` of their memory, at most two per sample and `parkRate`
// bytes per second. A parked sandbox's next response, heavy or light, has
// the memory prefetched back at once (process_madvise, outside the agent)
// and is held for at most `prefetchWait` until it is back; a heavy one then
// waits for its CPU like any other: the swap-in is held time, not
// execution, and the call starts on a warm working set.
//
// Selective rollback, rarely. A call is insured (its sandbox checkpointed at
// the model request, overlapping the model's latency, the whole container so
// that it can be stopped and resumed) only when it is big and bursty: it
// reserves at least half its limit and `minInsured` cores, the pool is
// loaded enough for a spike to matter (`insure` of the CPU budget) but not
// already in one (`busy`), the sandbox's typical dump is cheap next to its
// typical call (`cost`), fewer than `slots` dumps are in flight, and no call
// was insured in the last `insureEvery`. When the measured CPU or traffic
// has been at `pressure` of the machine's capacity for `spike`, the insured
// calls furthest above their reservations are stopped (whole-sandbox
// rollback), at most two per round and one round per `cooldown`, until the
// measured demand is back under pressure; each is restored, first in line,
// when the pool has room, and the agent re-sends its request and runs the
// call again. A call that was not insured is never rolled back.
package tidesand

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/policy"
	"github.com/TideSand/tidelet/pkg/policy/meter"
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Name is the policy's registered name.
const Name = "tidesand"

const (
	remembered    = 3    // calls back a sandbox's peaks look
	priorSamples  = 64   // pool-wide peaks, working sets, durations and dump costs the priors remember
	maxPerRound   = 2    // calls stopped per rollback round
	minShareStop  = 0.05 // of a resource: a call using less is not worth stopping
	parkPerSample = 2    // reclaims asked for per sample
	gib           = float64(1 << 30)
)

// Policy is the tidesand policy. The exported fields are its options.
type Policy struct {
	// Target is the share of the pool's cores the measured use (plus what
	// the measurement does not show yet) may reach; Floor the least a heavy
	// call reserves, in cores; MemTarget the share of the pool's memory the
	// residents' working sets may sum to at admission.
	Target, Floor, MemTarget float64
	// MaxPass is how long the oldest held response waits before it goes past
	// the ceiling onto the machine's spare room (Pressure of the cores and of
	// the bandwidth; regardless when nothing runs), not counted against the
	// ceiling while it runs, and before no arrival is admitted while it waits;
	// Book how long it waits before it books its reservation (the younger
	// responses then pass it only on what the budget has left over). Book
	// is 0, never, by default: at saturation every head ages, each booking
	// drains the pool to it, and the releases convoy (minutes without a
	// release, small calls held minutes); passing alone
	// keeps the small calls on time and lets the few whole-limit heads wait.
	// Boot is
	// what a starting sandbox counts for, less its measured use, for Settle
	// after its start, in cores; Settle is also how long after its start a
	// sandbox's working set is taken as grown; Standing the time constant of
	// the idle-use average, Sustain that of a call's, and how long a released
	// call is pending.
	MaxPass, Settle, Standing, Sustain time.Duration
	Book                               time.Duration
	Boot                               float64
	// MemPrior is what a sandbox is expected to hold before any is measured.
	MemPrior float64
	// HeavyFull: a call that peaked at half its limit or more reserves the
	// whole limit (a parallel numeric job runs at strict's speed only with
	// its limit to itself).
	HeavyFull bool
	// IdleQuota is the cpu.max, in cores, of a resident idle for IdleAfter
	// whose standing use is above it (0: never throttled).
	IdleQuota float64
	IdleAfter time.Duration
	// Bandwidth is the machine's sustainable memory traffic, bytes/s (0:
	// traffic is not gated); BwTarget the share of it the measured traffic
	// plus the pending may reach; Smooth the time constant of the traffic
	// average; BwPrior what a call is expected to move before any is measured.
	Bandwidth, BwTarget, BwPrior float64
	Smooth                       time.Duration
	// Park is the share of the pool's memory above which idle residents are
	// parked (0: never), above the memory target where the admission stops:
	// the swap takes the residents' growth past the target, not arrivals.
	// ParkFraction is the share of a parked sandbox's memory reclaimed;
	// ParkRate bytes/s of reclaim asked for at most (0: no limit); ParkIdle
	// how long a sandbox must have waited on the model to be parked;
	// PrefetchWait the longest a response waits for its memory to come back;
	// SwapShare the share of the host's swap the parked sandboxes may hold.
	Park, ParkFraction, ParkRate, SwapShare float64
	ParkIdle, PrefetchWait                  time.Duration
	// Pressure is the share of a resource's capacity (the machine's cores,
	// the bandwidth) at which the pool is saturated; Spike how long that must
	// last before a rollback round; Cooldown the least time between rounds.
	// Insure and Busy bound the CPU load (as a share of the budget) in which
	// a call is insured; MinInsured the cores a call must reserve to be;
	// Cost the share of a call's typical duration its dump may take (the
	// dump expected from its memory at DumpRate bytes/s, learned from the
	// dumps seen, or its own dumps' median when that is longer); MaxDump the
	// most memory a sandbox may hold to be insured at all: the images go to
	// a RAM tmpfs and a sandbox with many forked processes dumps several
	// times its memory (a 12 GiB one wrote 78 GB and stayed paused 5.5 min);
	// Slots the dumps in flight at most; InsureEvery the least time between
	// insured calls pool-wide.
	Pressure, Insure, Busy, MinInsured, Cost float64
	MaxDump, DumpRate                        float64
	Slots                                    int
	Spike, Cooldown, InsureEvery             time.Duration
	// Mechanisms is the ablation switch: "all" (the policy), "defer" (the
	// ceiling alone: nothing parked, nothing rolled back), "speculate"
	// (speculative swapping alone: no ceiling, responses never held for CPU
	// or traffic, nothing rolled back) or "rollback" (selective rollback
	// alone: no ceiling, nothing parked).
	Mechanisms string

	mu sync.Mutex
	// Boxes is every resident the policy knows; Held the responses waiting,
	// oldest first (a restore first of all).
	Boxes map[string]*Sandbox
	Held  []Held
	// cores and memory are the pool's capacities; measured the pool's
	// measured CPU (cores), memUsed its memory (bytes, without file cache),
	// bwUsed its memory traffic (bytes/s, smoothed).
	cores, memory             float64
	measured, memUsed, bwUsed float64
	poolCPU                   uint64
	poolAt, bwAt              time.Time
	swapTotal                 float64
	parks                     []parkRecord
	cpuPressureSince          time.Time
	bwPressureSince           time.Time
	lastRound, lastInsured    time.Time
	dumps                     int
	peaks, bwPeaks            []float64 // pool-wide, newest last: the priors
	workingSets               []float64
	durations, costs, rates   []float64
	// Status counts what the policy did, for status output and tests.
	Status Status
}

// Status summarises the policy's decisions.
type Status struct {
	Released, Held, Passed, Dispatched, Queued, ForcedOut, Escaped int
	Booked                                                         int
	Throttled, Unthrottled                                         int
	HeldNow, Residents                                             int
	Measured, Traffic                                              float64
	Parked, Prefetched, PrefetchTimeouts, LightHeld, QueuedSwap    int
	ParkedBytes                                                    float64
	Insured, SkippedQuiet, SkippedBusy, SkippedLight, SkippedCost  int
	SkippedBig                                                     int
	SkippedSlots, SkippedRate, Undumpable                          int
	Rollbacks, Restores, Reruns, Rounds                            int
}

// Sandbox is what the policy remembers of one resident.
type Sandbox struct {
	Limit   float64 // CPU limit, cores
	Started time.Time
	Call    *Call // the heavy call running, nil between calls
	// Peaks and BwPeaks hold the sustained peaks of the last calls, newest
	// last; Standing the average of the measured use between calls; Use and
	// BwUse the last measured use and traffic; WorkingSet the last measured
	// working set, bytes; Durations the last calls' durations (s), Costs the
	// last dumps' times (s).
	Peaks, BwPeaks   []float64
	Standing         float64
	Use, BwUse       float64
	WorkingSet       float64
	DumpSet          float64 // the working set at the last insured dump
	Durations, Costs []float64
	lastCPU          uint64
	lastAt           time.Time
	// Active counts the responses delivered and not yet followed by a
	// request: the sandbox is idle at zero, since IdleSince. Throttled says
	// its cpu.max is the idle quota; Alloc is its allocation otherwise.
	Active    int
	IdleSince time.Time
	Throttled bool
	Alloc     policy.Allocation
	// Stopped: rolled back, waiting for its restore. Rerun: restored, its next
	// heavy response reruns the undone call. Rolling: a rollback in flight.
	Stopped, Rerun, Rolling bool
	// InsuredCall is the call whose request was checkpointed last, InsuredOK
	// whether that checkpoint succeeded; Undumpable: CRIU refused a dump of
	// it, it is not insured again.
	InsuredCall string
	InsuredOK   bool
	Undumpable  bool
	// Parked is when its memory was reclaimed to swap while it waited (zero:
	// not parked), ParkedRSS what it held then; Unparked when the prefetch
	// of that memory was asked for.
	Parked, Unparked time.Time
	ParkedRSS        float64
}

// Call is a heavy call the policy let run.
type Call struct {
	ID        string
	Since     time.Time
	Reserve   float64 // the CPU demand it was released on
	BwReserve float64 // the traffic it was released on, bytes/s
	// Sustained is the call's use averaged over Sustain (an exponential
	// average), Peak the highest that average reached; BwSustained and BwPeak
	// the same for its traffic.
	Sustained, Peak     float64
	BwSustained, BwPeak float64
	Reads               int
	Rerun               bool // a restored call: never stopped again
	Insured             bool // checkpointed at its request: it can be undone
	Escaped             bool // went past the ceiling onto the machine's spare room: not counted against the ceiling
}

// Held is a response waiting for room (or a restore waiting for it).
// Prefetch says the response waits for the sandbox's parked memory to come
// back (asked at PrefetchSince); Light that its tools run in the agent, so
// it goes as soon as its memory is back, no fit asked; Warm that the
// prefetch is done (or was waited for long enough).
type Held struct {
	Sandbox, Call      string
	Since              time.Time
	Reserve, BwReserve float64
	Restore            bool
	Prefetch           bool
	PrefetchSince      time.Time
	Light              bool
	Warm               bool
	Booked             bool
}

// parkRecord is one reclaim the policy asked for, for the swap budget.
type parkRecord struct {
	at    time.Time
	bytes float64
}

// New builds a tidesand policy. Options: target (0..2, default 0.7),
// floor (cores, default 0), memTarget (0..1, default 0.8), maxPass
// (duration, default 30s), boot (cores, default 1), settle (duration,
// default 15s), standing and sustain (durations, default 10s), memPrior
// (bytes, default 256Mi), heavyFull (bool, default true), idleQuota (cores,
// default 0.5; 0 never throttles), idleAfter (duration, default 30s),
// bandwidth (bytes/s, default 250G: the sustainable DRAM traffic of a
// 64-core Graviton3, set it to your host's; 0 leaves
// traffic ungated), bwTarget (0..1, default 0.8), smooth (duration, default
// 3s), bwPrior (bytes/s, default 2G), park (0..1, default 0.9; 0 never
// parks), parkFraction (0..1, default 0.8), parkRate (bytes/s, default 32Mi;
// 0 no limit), parkIdle (duration, default 60s), prefetchWait (duration,
// default 10s), swapShare (0..1, default 0.8), pressure (0..1, default
// 0.85), spike (duration, default 3s), cooldown (duration, default 10s),
// insure (share of the budget, default 0.85), busy (share of the budget,
// default 1.3), minInsured (cores, default 4), cost (0..1, default 0.2),
// slots (default 2), insureEvery (duration, default 10s), mechanisms (all,
// the policy; defer, speculate or rollback: the ablations keeping one).
func New(opts map[string]string) (*Policy, error) {
	p := &Policy{Target: 0.7, Floor: 0, MemTarget: 0.8, MaxPass: 30 * time.Second, Book: 0, Boot: 1, Settle: 15 * time.Second, Standing: 10 * time.Second, Sustain: 10 * time.Second,
		MemPrior: 256 << 20, HeavyFull: true, IdleQuota: 0.5, IdleAfter: 30 * time.Second,
		Bandwidth: 250e9, BwTarget: 0.8, BwPrior: 2e9, Smooth: 3 * time.Second,
		Park: 0.9, ParkFraction: 0.8, ParkRate: 32 << 20, ParkIdle: 60 * time.Second, PrefetchWait: 10 * time.Second, SwapShare: 0.8,
		Pressure: 0.85, Spike: 3 * time.Second, Cooldown: 10 * time.Second, Insure: 0.85, Busy: 1.3, MinInsured: 4, Cost: 0.2, MaxDump: 2 * gib, DumpRate: 1 * gib, Slots: 2, InsureEvery: 10 * time.Second,
		Mechanisms: "all", Boxes: map[string]*Sandbox{}}
	for k, v := range opts {
		var err error
		num := func(dst *float64, hi float64) {
			*dst, err = strconv.ParseFloat(v, 64)
			if err == nil && (*dst <= 0 || *dst > hi) {
				err = fmt.Errorf("must be in (0, %g]", hi)
			}
		}
		share := func(dst *float64) { // 0 allowed: the mechanism off
			*dst, err = strconv.ParseFloat(v, 64)
			if err == nil && (*dst < 0 || *dst > 1) {
				err = fmt.Errorf("must be in [0, 1]")
			}
		}
		bytes := func(dst *float64, zeroOK bool) {
			var q resource.Bytes
			q, err = resource.ParseMemory(v)
			if err == nil && (q < 0 || (q == 0 && !zeroOK)) {
				err = fmt.Errorf("must be positive")
			}
			*dst = float64(q)
		}
		dur := func(dst *time.Duration) {
			*dst, err = time.ParseDuration(v)
			if err == nil && *dst <= 0 {
				err = fmt.Errorf("must be positive")
			}
		}
		switch k {
		case "target":
			num(&p.Target, 2)
		case "floor":
			num(&p.Floor, 1024)
		case "memTarget":
			num(&p.MemTarget, 1)
		case "boot":
			num(&p.Boot, 1024)
		case "maxPass":
			dur(&p.MaxPass)
		case "book":
			*(&p.Book), err = time.ParseDuration(v)
			if err == nil && p.Book < 0 {
				err = fmt.Errorf("must not be negative")
			}
		case "settle":
			dur(&p.Settle)
		case "standing":
			dur(&p.Standing)
		case "sustain":
			dur(&p.Sustain)
		case "heavyFull":
			p.HeavyFull, err = strconv.ParseBool(v)
		case "idleQuota":
			p.IdleQuota, err = strconv.ParseFloat(v, 64)
			if err == nil && p.IdleQuota < 0 {
				err = fmt.Errorf("must not be negative")
			}
		case "idleAfter":
			dur(&p.IdleAfter)
		case "memPrior":
			bytes(&p.MemPrior, false)
		case "bandwidth":
			bytes(&p.Bandwidth, true)
		case "bwTarget":
			num(&p.BwTarget, 1)
		case "smooth":
			dur(&p.Smooth)
		case "bwPrior":
			bytes(&p.BwPrior, false)
		case "park":
			share(&p.Park)
		case "parkFraction":
			num(&p.ParkFraction, 1)
		case "parkRate":
			bytes(&p.ParkRate, true)
		case "parkIdle":
			dur(&p.ParkIdle)
		case "prefetchWait":
			dur(&p.PrefetchWait)
		case "swapShare":
			num(&p.SwapShare, 1)
		case "pressure":
			num(&p.Pressure, 1)
		case "spike":
			dur(&p.Spike)
		case "cooldown":
			dur(&p.Cooldown)
		case "insure":
			share(&p.Insure)
		case "busy":
			num(&p.Busy, 4)
		case "minInsured":
			num(&p.MinInsured, 1024)
		case "cost":
			num(&p.Cost, 1)
		case "maxDump":
			bytes(&p.MaxDump, false)
		case "dumpRate":
			bytes(&p.DumpRate, false)
		case "slots":
			var n int
			n, err = strconv.Atoi(v)
			if err == nil && n < 0 {
				err = fmt.Errorf("must not be negative")
			}
			p.Slots = n
		case "insureEvery":
			dur(&p.InsureEvery)
		case "mechanisms":
			switch v {
			case "all", "defer", "speculate", "rollback":
				p.Mechanisms = v
			default:
				err = fmt.Errorf("must be all, defer, speculate or rollback")
			}
		default:
			return nil, fmt.Errorf("%s: unknown option %q", Name, k)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: option %s=%q: %w", Name, k, v, err)
		}
	}
	if p.Insure > p.Busy {
		return nil, fmt.Errorf("%s: insure (%g) must not exceed busy (%g)", Name, p.Insure, p.Busy)
	}
	switch p.Mechanisms {
	case "defer":
		p.Park, p.Slots = 0, 0
	case "speculate":
		p.Target, p.Bandwidth, p.Slots = 1e6, 0, 0 // no ceiling: the memory target and the swap are all that gate
	case "rollback":
		p.Target, p.Bandwidth, p.Park = 1e6, 0, 0
		p.Insure = 0 // every heavy call is a candidate, at the insurance's rate
	}
	return p, nil
}

var (
	_ policy.Policy             = (*Policy)(nil)
	_ policy.ModelAware         = (*Policy)(nil)
	_ policy.Checkpointer       = (*Policy)(nil)
	_ policy.CheckpointObserver = (*Policy)(nil)
	_ policy.Signaled           = (*Policy)(nil)
	_ policy.BandwidthTriggered = (*Policy)(nil)
)

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Allocatable implements policy.Policy.
func (p *Policy) Allocatable(view policy.View) resource.Set { return view.Capacity.Sub(view.Reserved) }

// OnTick implements policy.Policy: nothing runs on the tick.
func (p *Policy) OnTick(time.Time, policy.View) []policy.Action { return nil }

// BandwidthTrigger implements policy.BandwidthTriggered: the manager meters
// the pool's traffic and wakes the policy at pressure.
func (p *Policy) BandwidthTrigger() uint64 {
	if p.Bandwidth <= 0 {
		return 0
	}
	return uint64(p.Pressure * p.Bandwidth)
}

func (p *Policy) capacities(view policy.View) {
	p.cores = meter.Cores(view)
	p.memory = float64(meter.MemoryLimit(view))
}

// budget is the ceiling on the measured CPU plus what is pending; bwBudget
// the same for the traffic.
func (p *Policy) budget() float64   { return p.Target * p.cores }
func (p *Policy) bwBudget() float64 { return p.BwTarget * p.Bandwidth }

// running counts the heavy calls running.
func (p *Policy) running() int {
	n := 0
	for _, b := range p.Boxes {
		if b.Call != nil && !b.Stopped {
			n++
		}
	}
	return n
}

// reserve is what a sandbox's next heavy call reserves of the CPU: the
// highest sustained demand of its last calls, else the pool's median, else
// its limit; the whole limit when that is half the limit or more
// (HeavyFull); at least the floor and its standing use, at most its limit.
func (p *Policy) reserve(b *Sandbox) float64 {
	e := b.Limit
	if len(b.Peaks) > 0 {
		e = 0
		for _, k := range b.Peaks {
			e = max(e, k)
		}
	} else if len(p.peaks) > 0 {
		e = median(p.peaks)
	}
	if p.HeavyFull && b.Limit > 0 && e >= b.Limit/2 {
		e = b.Limit
	}
	e = max(e, p.Floor, b.Standing)
	if b.Limit > 0 {
		e = min(e, b.Limit)
	}
	return e
}

// bwReserve is what a sandbox's next heavy call reserves of the traffic: the
// highest sustained traffic of its last calls, else the pool's median, else
// the prior; at most its share of the machine's traffic by its CPU limit (as
// a call reserves at most its limit of the CPU: a streaming call can move
// the whole machine's traffic, and a reservation that large would wait for
// an idle pool, holding the responses and the admission behind it).
func (p *Policy) bwReserve(b *Sandbox) float64 {
	if p.Bandwidth <= 0 {
		return 0
	}
	e := p.BwPrior
	if len(b.BwPeaks) > 0 {
		e = 0
		for _, k := range b.BwPeaks {
			e = max(e, k)
		}
	} else if len(p.bwPeaks) > 0 {
		e = median(p.bwPeaks)
	}
	share := p.bwBudget()
	if b.Limit > 0 && p.cores > 0 {
		share = min(share, p.Bandwidth*b.Limit/p.cores)
	}
	return min(e, share)
}

// pending is what the measurement does not show yet of the CPU: for the
// calls released within Sustain, the part of their reservation their
// sandboxes are not yet measured to use; for the sandboxes started within
// Settle, the part of their boot they are not yet measured to use.
func (p *Policy) pending(now time.Time) float64 { return p.pendingOf(now, false) }

// pendingOf is pending, with the escaped calls counted too when asked (the
// machine's room is physical; the ceiling's is not theirs).
func (p *Policy) pendingOf(now time.Time, escapedToo bool) float64 {
	sum := 0.0
	for _, b := range p.Boxes {
		if b.Stopped {
			continue
		}
		if c := b.Call; c != nil {
			if now.Sub(c.Since) < p.Sustain && (escapedToo || !c.Escaped) {
				sum += max(0, c.Reserve-b.Use)
			}
		} else if now.Sub(b.Started) < p.Settle {
			sum += max(0, p.Boot-b.Use)
		}
	}
	return sum
}

// bwPending is pending for the traffic.
func (p *Policy) bwPending(now time.Time) float64 {
	sum := 0.0
	for _, b := range p.Boxes {
		if c := b.Call; c != nil && !b.Stopped && !c.Escaped && now.Sub(c.Since) < p.Sustain {
			sum += max(0, c.BwReserve-b.BwUse)
		}
	}
	return sum
}

// fits says whether a CPU demand and a traffic demand fit their budgets on
// top of the measurements plus what is pending.
func (p *Policy) fits(cpu, bw float64, now time.Time) bool {
	ecpu, ebw := p.escapedUse()
	if p.cores > 0 && p.measured-ecpu+p.pending(now)+cpu > p.budget() {
		return false
	}
	if p.Bandwidth > 0 && p.bwUsed-ebw+p.bwPending(now)+bw > p.bwBudget() {
		return false
	}
	// Whatever the ceiling leaves out, nothing is released past the machine's
	// own room: with escaped calls running, the rest fills up to that, not past
	// it. The bound belongs to the ceiling: without one (the swapping and
	// rollback ablations, Target lifted) nothing is held for the CPU at all.
	if p.Target > 1 {
		return true
	}
	return p.roomOnMachine(cpu, bw, now)
}

// escapedUse is what the calls that went past the ceiling are measured to
// use (CPU and traffic): on the machine's spare room, not the ceiling's.
func (p *Policy) escapedUse() (cpu, bw float64) {
	for _, b := range p.Boxes {
		if c := b.Call; c != nil && c.Escaped && !b.Stopped {
			cpu += b.Use
			bw += b.BwUse
		}
	}
	return cpu, bw
}

// roomOnMachine says whether a CPU demand and a traffic demand fit the
// machine itself (Pressure of the cores and of the bandwidth, the line the
// rollback watches) on top of the measurements plus what is pending.
func (p *Policy) roomOnMachine(cpu, bw float64, now time.Time) bool {
	if p.cores > 0 && p.measured+p.pendingOf(now, true)+cpu > p.Pressure*p.cores {
		return false
	}
	if p.Bandwidth > 0 && p.bwUsed+p.bwPending(now)+bw > p.Pressure*p.Bandwidth {
		return false
	}
	return true
}

// memoryFits says whether a working set fits the memory target on top of
// what is in use.
func (p *Policy) memoryFits(bytes float64) bool {
	return p.memory <= 0 || p.memUsed+bytes <= p.MemTarget*p.memory
}

// expectedMemory is what a new sandbox is expected to hold: the residents'
// median working set, else the prior.
func (p *Policy) expectedMemory() float64 {
	if m := median(p.workingSets); m > 0 {
		return m
	}
	return p.MemPrior
}

// parkedTotal is the memory the parked sandboxes will take back: what the
// swap holds for the pool.
func (p *Policy) parkedTotal() float64 {
	total := 0.0
	for _, b := range p.Boxes {
		if !b.Parked.IsZero() {
			total += b.ParkedRSS
		}
	}
	return total
}

// heldParkedMemory is what the parked sandboxes whose responses are held
// need back before they can run: room an arrival must not take.
func (p *Policy) heldParkedMemory() float64 {
	total := 0.0
	for _, h := range p.Held {
		if b := p.Boxes[h.Sandbox]; b != nil && !b.Parked.IsZero() {
			total += b.ParkedRSS
		}
	}
	return total
}

// swapFits says whether one more working set in swap still fits the share of
// the swap the pool may use (unbounded without swap figures).
func (p *Policy) swapFits(bytes float64) bool {
	return p.swapTotal <= 0 || p.parkedTotal()+bytes <= p.SwapShare*p.swapTotal
}

// oldestHeld is how long the oldest held response has waited (zero: none).
func (p *Policy) oldestHeld(now time.Time) time.Duration {
	if len(p.Held) == 0 {
		return 0
	}
	return now.Sub(p.Held[0].Since)
}

// admits says whether a sandbox may start now, and why not otherwise.
func (p *Policy) admits(now time.Time, view policy.View) (bool, string) {
	if booting := meter.Booting(view); len(booting) > 0 {
		return false, fmt.Sprintf("waiting for %s to finish booting", booting[0].Sandbox.ID)
	}
	if waited := p.oldestHeld(now); waited > p.MaxPass {
		return false, fmt.Sprintf("%d responses held, the oldest for %s: the budget is spent", len(p.Held), waited.Round(time.Second))
	}
	if !p.fits(p.Boot, 0, now) {
		return false, fmt.Sprintf("cpu: measured %.1f + pending %.1f + boot %.1f of %.1f cores (traffic %.0f + %.0f of %.0f GB/s)", p.measured, p.pending(now), p.Boot, p.budget(), p.bwUsed/1e9, p.bwPending(now)/1e9, p.bwBudget()/1e9)
	}
	want := p.expectedMemory()
	if owed := p.heldParkedMemory(); !p.memoryFits(want + owed) {
		return false, fmt.Sprintf("memory: %.1f + %.1f (+%.1f owed to parked residents) of %.1f GiB", p.memUsed/gib, want/gib, owed/gib, p.MemTarget*p.memory/gib)
	}
	if !p.swapFits(want) {
		return false, fmt.Sprintf("swap: the parked sandboxes hold %.1f of the %.1f GiB the pool may use", p.parkedTotal()/gib, p.SwapShare*p.swapTotal/gib)
	}
	return true, ""
}

// admit books a sandbox that starts now.
func (p *Policy) admit(sb sandbox.Sandbox, now time.Time) policy.Allocation {
	p.Boxes[sb.ID] = p.newBox(sb, now)
	p.Status.Dispatched++
	return p.Boxes[sb.ID].Alloc
}

// newBox is what the policy remembers of a sandbox it meets.
func (p *Policy) newBox(sb sandbox.Sandbox, now time.Time) *Sandbox {
	return &Sandbox{Limit: meter.VCPUs(sb), Started: now, IdleSince: now, Alloc: meter.Shape(sb, true)}
}

// Admit implements policy.Policy: run when no held response has waited
// MaxPass and the boot, the memory and the swap fit, else queue.
func (p *Policy) Admit(sb sandbox.Sandbox, view policy.View) policy.Decision {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacities(view)
	if _, limits := meter.Resources(sb); !limits.Fits(view.Capacity.Sub(view.Reserved)) {
		return policy.Decision{Kind: policy.Invalid, Reason: fmt.Sprintf("limits %s exceed the pool's capacity", limits)}
	}
	if ok, why := p.admits(view.Now, view); !ok {
		p.Status.Queued++
		if strings.HasPrefix(why, "swap") {
			p.Status.QueuedSwap++
		}
		return policy.Decision{Kind: policy.Queue, Reason: why}
	}
	return policy.Decision{Kind: policy.Run, Allocation: p.admit(sb, view.Now)}
}

// dispatchQueued starts the oldest queued sandbox when one may start.
func (p *Policy) dispatchQueued(now time.Time, view policy.View) []policy.Action {
	queued := view.Queued()
	if len(queued) == 0 {
		return nil
	}
	if ok, _ := p.admits(now, view); !ok {
		return nil
	}
	return []policy.Action{policy.Dispatch{ID: queued[0].Sandbox.ID, Allocation: p.admit(queued[0].Sandbox, now)}}
}

// CheckpointBefore implements policy.Checkpointer: the insurance rule, decided
// at the model request so the dump overlaps the model's latency.
func (p *Policy) CheckpointBefore(ev policy.ModelEvent) bool {
	if ev.Kind != policy.ModelRequestStarted {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.Boxes[ev.Sandbox]
	if b == nil || b.Stopped || b.Rerun || b.Undumpable || p.Slots <= 0 || p.cores <= 0 {
		return false
	}
	reserve := p.reserve(b)
	if reserve < p.MinInsured || (b.Limit > 0 && reserve < b.Limit/2) {
		p.Status.SkippedLight++
		return false // not big and bursty: it cannot cause a spike on its own
	}
	load := (p.measured + p.pending(ev.Time)) / p.budget()
	switch {
	case load < p.Insure:
		p.Status.SkippedQuiet++
		return false
	case load > p.Busy:
		p.Status.SkippedBusy++
		return false
	case p.dumps >= p.Slots:
		p.Status.SkippedSlots++
		return false
	case !p.lastInsured.IsZero() && ev.Time.Sub(p.lastInsured) < p.InsureEvery:
		p.Status.SkippedRate++
		return false
	}
	if p.MaxDump > 0 && b.WorkingSet > p.MaxDump {
		p.Status.SkippedBig++
		return false // too much memory to dump: the image would take seconds and tens of GB
	}
	cost, dur := p.typicalCost(b), p.typicalDuration(b)
	if cost > 0 && dur > 0 && cost > p.Cost*dur {
		p.Status.SkippedCost++
		return false
	}
	p.dumps++
	p.Status.Insured++
	p.lastInsured = ev.Time
	b.InsuredCall, b.InsuredOK, b.DumpSet = ev.Call, false, b.WorkingSet
	slog.Debug("tidesand insure", "sandbox", ev.Sandbox, "call", ev.Call, "reserve", fmt.Sprintf("%.2f", reserve), "load", fmt.Sprintf("%.2f", load))
	return true
}

// CheckpointDone implements policy.CheckpointObserver: what the dump cost is
// what the sandbox's next one is expected to cost.
func (p *Policy) CheckpointDone(sandboxID, callID string, took time.Duration, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dumps > 0 {
		p.dumps--
	}
	b := p.Boxes[sandboxID]
	if b == nil {
		return
	}
	if b.InsuredCall == callID {
		b.InsuredOK = err == nil
	}
	if err != nil && !b.Undumpable && strings.Contains(err.Error(), "criu") {
		b.Undumpable = true
		p.Status.Undumpable++
		slog.Info("tidesand: sandbox not insured again, CRIU cannot dump it", "sandbox", sandboxID, "err", err)
	}
	if err == nil {
		b.Costs = keep(append(b.Costs, took.Seconds()), remembered)
		p.costs = keep(append(p.costs, took.Seconds()), priorSamples)
		if took > 0 && b.DumpSet > 0 {
			p.rates = keep(append(p.rates, b.DumpSet/took.Seconds()), priorSamples)
		}
	}
}

// typicalCost is what a checkpoint of the sandbox is expected to take (s):
// its memory at the dump rate learned from the dumps seen (DumpRate before
// any), or its own dumps' median when that is longer; typicalDuration how
// long its calls take (s), its own median else the pool's (0: unknown).
func (p *Policy) typicalCost(b *Sandbox) float64 {
	rate := p.DumpRate
	if r := median(p.rates); r > 0 {
		rate = r
	}
	cost := 0.0
	if rate > 0 {
		cost = b.WorkingSet / rate
	}
	if len(b.Costs) > 0 {
		cost = max(cost, median(b.Costs))
	}
	return cost
}

func (p *Policy) typicalDuration(b *Sandbox) float64 {
	if len(b.Durations) > 0 {
		return median(b.Durations)
	}
	return median(p.durations)
}

// OnEvent implements policy.Policy.
func (p *Policy) OnEvent(ev sandbox.Event, view policy.View) []policy.Action {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacities(view)
	id := ev.Sandbox.ID
	switch {
	case ev.Type == sandbox.EventPrefetched:
		for i := range p.Held {
			if p.Held[i].Sandbox == id && p.Held[i].Prefetch {
				p.Held[i].Prefetch, p.Held[i].Warm = false, true
			}
		}
		return p.schedule(ev.Time)
	case ev.Type == sandbox.EventStarted:
		if _, ok := p.Boxes[id]; !ok {
			p.Boxes[id] = p.newBox(ev.Sandbox, ev.Time)
		}
		return p.dispatchQueued(ev.Time, view)
	case meter.Gone(ev):
		delete(p.Boxes, id)
		var out []policy.Action
		rest := p.Held[:0]
		for _, h := range p.Held {
			if h.Sandbox == id {
				if !h.Restore {
					out = append(out, policy.Release{ID: h.Sandbox, Call: h.Call})
				}
				continue
			}
			rest = append(rest, h)
		}
		p.Held = rest
		out = append(out, p.schedule(ev.Time)...)
		return append(out, p.dispatchQueued(ev.Time, view)...)
	}
	return nil
}

// OnSample implements policy.Policy: the pool's and each sandbox's use are
// measured, peaks and standing use learned, pressure tracked, then the
// contingencies run and the queue is scheduled.
func (p *Policy) OnSample(s policy.Sample, view policy.View) []policy.Action {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacities(view)
	p.observePool(s, view)
	var sets []float64
	for id, st := range s.Sandboxes {
		b := p.Boxes[id]
		if b == nil {
			continue
		}
		if !b.lastAt.IsZero() && st.CPUUsageNanos >= b.lastCPU && s.Time.After(b.lastAt) {
			rate := float64(st.CPUUsageNanos-b.lastCPU) / 1e9 / s.Time.Sub(b.lastAt).Seconds()
			b.Use = rate
			a := min(1, s.Time.Sub(b.lastAt).Seconds()/p.Sustain.Seconds())
			if c := b.Call; c != nil && !b.Stopped {
				c.Sustained += a * (rate - c.Sustained)
				c.Peak = max(c.Peak, c.Sustained)
				c.BwSustained += a * (b.BwUse - c.BwSustained)
				c.BwPeak = max(c.BwPeak, c.BwSustained)
				c.Reads++
			} else if a := min(1, s.Time.Sub(b.lastAt).Seconds()/p.Standing.Seconds()); a > 0 && b.Call == nil {
				b.Standing += a * (rate - b.Standing)
			}
		}
		b.lastCPU, b.lastAt = st.CPUUsageNanos, s.Time
		if st.BandwidthBytesPerSec > 0 && p.Bandwidth > 0 {
			b.BwUse = min(float64(st.BandwidthBytesPerSec), p.Bandwidth)
		}
		if st.MemoryWorkingSetBytes > 0 {
			b.WorkingSet = float64(st.MemoryWorkingSetBytes)
			if s.Time.Sub(b.Started) >= p.Settle {
				sets = append(sets, b.WorkingSet)
			}
		}
		if !b.Unparked.IsZero() && s.Time.Sub(b.Unparked) > p.Settle {
			b.Unparked, b.ParkedRSS = time.Time{}, 0 // the measurement shows what it holds again
		}
	}
	if len(sets) > 0 {
		p.workingSets = keep(sets, priorSamples)
	}
	var out []policy.Action
	out = append(out, p.rollback(s.Time)...)
	out = append(out, p.park(s.Time)...)
	out = append(out, p.throttle(s.Time)...)
	out = append(out, p.schedule(s.Time)...)
	return append(out, p.dispatchQueued(s.Time, view)...)
}

// observePool measures the pool's CPU (the pool cgroup's counter over the
// wall clock between reads, else the host's busy share), memory (in use
// without file cache, as the sample scopes it to the pool) and traffic
// (the bus-access meter, smoothed over Smooth and clamped to the capacity),
// and tracks how long each has been at pressure.
func (p *Policy) observePool(s policy.Sample, view policy.View) {
	if view.PoolCgroup != "" {
		read := time.Now()
		if usage, _ := host.CgroupCPU(view.PoolCgroup); usage > 0 {
			if !p.poolAt.IsZero() && usage >= p.poolCPU && read.Sub(p.poolAt) >= 200*time.Millisecond {
				p.measured = min(p.cores, float64(usage-p.poolCPU)/1e9/read.Sub(p.poolAt).Seconds())
				p.poolCPU, p.poolAt = usage, read
			} else if p.poolAt.IsZero() {
				p.poolCPU, p.poolAt = usage, read
			}
		}
	} else {
		sum := 0.0
		for _, b := range p.Boxes {
			sum += b.Use
		}
		p.measured = sum
	}
	if s.Node.MemTotal > s.Node.MemAvailable {
		p.memUsed = float64(s.Node.MemTotal - s.Node.MemAvailable)
	}
	if s.Node.SwapTotal > 0 {
		p.swapTotal = float64(s.Node.SwapTotal)
	}
	if p.Bandwidth > 0 {
		raw := min(float64(s.Node.Bandwidth), p.Bandwidth)
		if p.Smooth > 0 && !p.bwAt.IsZero() && s.Time.After(p.bwAt) {
			a := min(1, s.Time.Sub(p.bwAt).Seconds()/p.Smooth.Seconds())
			p.bwUsed += a * (raw - p.bwUsed)
		} else {
			p.bwUsed = raw
		}
		p.bwAt = s.Time
	}
	p.cpuPressureSince = pressureRun(p.cpuPressureSince, s.Time, p.cores > 0 && p.measured >= p.Pressure*p.cores)
	p.bwPressureSince = pressureRun(p.bwPressureSince, s.Time, p.Bandwidth > 0 && p.bwUsed >= p.Pressure*p.Bandwidth)
	p.Status.Measured, p.Status.Traffic = p.measured, p.bwUsed
	p.Status.Residents = len(p.Boxes)
}

// pressureRun continues or ends a run of pressure.
func pressureRun(since, now time.Time, under bool) time.Time {
	if !under {
		return time.Time{}
	}
	if since.IsZero() {
		return now
	}
	return since
}

// OnSignal implements policy.Signaled: a pressure trigger starts or continues
// a run of pressure on its resource; the rollback waits for it to last.
func (p *Policy) OnSignal(sig policy.Signal, view policy.View) []policy.Action {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacities(view)
	switch sig.Kind {
	case policy.SignalCPUPressure:
		if p.cpuPressureSince.IsZero() {
			p.cpuPressureSince = sig.Time
		}
	case policy.SignalBandwidthPressure:
		if p.bwPressureSince.IsZero() {
			p.bwPressureSince = sig.Time
		}
	default:
		return nil
	}
	return p.rollback(sig.Time)
}

// rollback is the last resort: with the CPU or the traffic at pressure for
// Spike and no round within Cooldown, stop the insured calls furthest above
// their reservations on it until the demand is back under pressure.
func (p *Policy) rollback(now time.Time) []policy.Action {
	if now.Sub(p.lastRound) < p.Cooldown {
		return nil
	}
	var (
		resource string
		demand   float64
		line     float64
		use      func(*Sandbox) float64
		reserve  func(*Call) float64
	)
	switch {
	case p.cores > 0 && !p.cpuPressureSince.IsZero() && now.Sub(p.cpuPressureSince) >= p.Spike:
		resource, demand, line = "cpu", p.measured, p.Pressure*p.cores
		use, reserve = func(b *Sandbox) float64 { return b.Use }, func(c *Call) float64 { return c.Reserve }
	case p.Bandwidth > 0 && !p.bwPressureSince.IsZero() && now.Sub(p.bwPressureSince) >= p.Spike:
		resource, demand, line = "traffic", p.bwUsed, p.Pressure*p.Bandwidth
		use, reserve = func(b *Sandbox) float64 { return b.BwUse }, func(c *Call) float64 { return c.BwReserve }
	default:
		return nil
	}
	type cand struct {
		id     string
		b      *Sandbox
		excess float64
		since  time.Time
	}
	var cands []cand
	for id, b := range p.Boxes {
		// Any insured call may be stopped: the one furthest above its reservation
		// first (the spike), the youngest among equals (the least work lost). A
		// whole-limit call can never exceed its reservation, so the rule is an
		// order, not a bar: when the machine saturates, something insured goes.
		if c := b.Call; c != nil && !b.Stopped && c.Insured && !c.Rerun && !b.Rolling && use(b) >= minShareStop*line/p.Pressure {
			cands = append(cands, cand{id, b, use(b) - reserve(c), c.Since})
		}
	}
	var out []policy.Action
	for len(cands) > 0 && demand > line && len(out) < maxPerRound {
		best := 0
		for i, c := range cands {
			if c.excess > cands[best].excess || (c.excess == cands[best].excess && c.since.After(cands[best].since)) {
				best = i
			}
		}
		v := cands[best]
		cands = append(cands[:best], cands[best+1:]...)
		demand -= use(v.b)
		v.b.Rolling = true
		slog.Info("tidesand: rolling back", "resource", resource, "sandbox", v.id, "call", v.b.Call.ID, "using", fmt.Sprintf("%.3g", use(v.b)), "reserved", fmt.Sprintf("%.3g", reserve(v.b.Call)),
			"age", now.Sub(v.b.Call.Since).Round(time.Millisecond), "pressureFor", now.Sub(p.cpuPressureSince).Round(time.Millisecond), "measured", fmt.Sprintf("%.1f of %.1f", p.measured, p.cores))
		out = append(out, policy.Rollback{ID: v.id, Call: v.b.Call.ID})
	}
	if len(out) > 0 {
		p.Status.Rounds++
		p.lastRound = now
	}
	return out
}

// park reclaims the memory of idle residents while the pool's memory is
// above the park line, the ones holding most first, within the swap budget.
func (p *Policy) park(now time.Time) []policy.Action {
	if p.Park <= 0 || p.memory <= 0 || p.memUsed <= p.Park*p.memory {
		return nil
	}
	const window = 10 * time.Second
	recent, kept := 0.0, p.parks[:0]
	for _, k := range p.parks {
		if now.Sub(k.at) < window {
			kept = append(kept, k)
			recent += k.bytes
		}
	}
	p.parks = kept
	budget := math.Inf(1)
	if p.ParkRate > 0 {
		budget = p.ParkRate*window.Seconds() - recent
	}
	var out []policy.Action
	for _, id := range p.parkable(now) {
		if len(out) >= parkPerSample || budget <= 0 {
			break
		}
		b := p.Boxes[id]
		bytes := p.ParkFraction * b.WorkingSet
		if bytes <= 0 || !p.swapFits(bytes) {
			continue
		}
		b.Parked, b.ParkedRSS, b.Unparked = now, b.WorkingSet, time.Time{}
		p.parks = append(p.parks, parkRecord{now, bytes})
		budget -= bytes
		p.Status.Parked++
		p.Status.ParkedBytes += bytes
		slog.Debug("tidesand park", "sandbox", id, "reclaim", fmt.Sprintf("%.0fM", bytes/1e6), "held", fmt.Sprintf("%.0fM", b.WorkingSet/1e6),
			"idle", now.Sub(b.IdleSince).Round(time.Second), "memory", fmt.Sprintf("%.1f of %.1f GiB", p.memUsed/gib, p.memory/gib))
		out = append(out, policy.Reclaim{ID: id, Bytes: int64(bytes)})
	}
	return out
}

// parkable lists the sandboxes whose memory may be reclaimed now: waiting on
// the model for at least ParkIdle, not parked already, not stopped, restored
// or booting; the ones holding most first.
func (p *Policy) parkable(now time.Time) []string {
	held := map[string]bool{}
	for _, h := range p.Held {
		held[h.Sandbox] = true
	}
	var ids []string
	for id, b := range p.Boxes {
		if b.Call != nil || b.Active > 0 || b.Stopped || b.Rerun || !b.Parked.IsZero() || b.IdleSince.IsZero() || now.Sub(b.IdleSince) < p.ParkIdle || b.WorkingSet <= 0 || now.Sub(b.Started) < p.Settle {
			continue
		}
		// A response of its own is held (its memory may be on its way back), or it
		// was unparked a moment ago: not idle, whatever its last request's age says.
		if held[id] || (!b.Unparked.IsZero() && now.Sub(b.Unparked) < p.ParkIdle) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := p.Boxes[ids[i]], p.Boxes[ids[j]]
		if a.WorkingSet != b.WorkingSet {
			return a.WorkingSet > b.WorkingSet
		}
		return ids[i] < ids[j]
	})
	return ids
}

// unpark asks for a parked sandbox's memory back and marks its held response
// as waiting for it.
func (p *Policy) unpark(id string, b *Sandbox, h *Held, now time.Time) policy.Action {
	b.Parked, b.Unparked = time.Time{}, now
	h.Prefetch, h.PrefetchSince = true, now
	p.Status.Prefetched++
	slog.Debug("tidesand prefetch", "sandbox", id, "call", h.Call, "held", fmt.Sprintf("%.0fM", b.ParkedRSS/1e6), "light", h.Light)
	return policy.Prefetch{ID: id}
}

// throttle caps the CPU of the residents idle for IdleAfter whose standing
// use is above the idle quota.
func (p *Policy) throttle(now time.Time) []policy.Action {
	if p.IdleQuota <= 0 {
		return nil
	}
	var out []policy.Action
	for id, b := range p.Boxes {
		if b.Throttled || b.Active > 0 || b.Call != nil || b.Stopped || b.Rerun || b.IdleSince.IsZero() || now.Sub(b.IdleSince) < p.IdleAfter || b.Standing <= p.IdleQuota {
			continue
		}
		alloc := b.Alloc
		alloc.Cgroup.CPUPeriodUS = policy.DefaultCPUPeriodUS
		alloc.Cgroup.CPUQuotaUS = policy.MillicoresToQuota(resource.Millicores(p.IdleQuota*1000), policy.DefaultCPUPeriodUS)
		b.Throttled = true
		p.Status.Throttled++
		slog.Debug("tidesand throttle", "sandbox", id, "standing", fmt.Sprintf("%.2f", b.Standing), "quota", p.IdleQuota, "idle", now.Sub(b.IdleSince).Round(time.Second))
		out = append(out, policy.Update{ID: id, Allocation: alloc})
	}
	return out
}

// deliver books a delivery to a sandbox: it is active from here, and its
// quota is given back first when it was throttled (the actions run in
// order).
func (p *Policy) deliver(id string, b *Sandbox) []policy.Action {
	b.Active++
	if !b.Throttled {
		return nil
	}
	b.Throttled = false
	p.Status.Unthrottled++
	slog.Debug("tidesand unthrottle", "sandbox", id)
	return []policy.Action{policy.Update{ID: id, Allocation: b.Alloc}}
}

// start lets a heavy call run.
func (p *Policy) start(id string, b *Sandbox, call string, reserve, bw float64, rerun, escaped bool, now time.Time, held time.Duration) {
	b.Call = &Call{ID: call, Since: now, Reserve: reserve, BwReserve: bw, Rerun: rerun, Escaped: escaped, Insured: b.InsuredOK && b.InsuredCall == call}
	if rerun {
		p.Status.Reruns++
	} else {
		p.Status.Released++
	}
	slog.Debug("tidesand release", "sandbox", id, "call", call, "reserve", fmt.Sprintf("%.2f", reserve),
		"measured", fmt.Sprintf("%.1f+%.1f of %.1f", p.measured, p.pending(now), p.budget()), "traffic", fmt.Sprintf("%.0f+%.0f of %.0f GB/s", p.bwUsed/1e9, p.bwPending(now)/1e9, p.bwBudget()/1e9),
		"insured", b.Call.Insured, "rerun", rerun, "escaped", escaped, "held", held.Round(time.Millisecond), "queue", len(p.Held))
}

// endCall closes a sandbox's running call: its peaks become the sandbox's and
// the pool's expectation, its duration what the next dump is weighed against.
func (p *Policy) endCall(b *Sandbox, now time.Time) {
	c := b.Call
	b.Call = nil
	if c == nil {
		return
	}
	if d := now.Sub(c.Since).Seconds(); d > 0 {
		b.Durations = keep(append(b.Durations, d), remembered)
		p.durations = keep(append(p.durations, d), priorSamples)
	}
	if c.Reads == 0 {
		return
	}
	peak := min(c.Peak, p.cores)
	b.Peaks = keep(append(b.Peaks, peak), remembered)
	p.peaks = keep(append(p.peaks, peak), priorSamples)
	if p.Bandwidth > 0 {
		b.BwPeaks = keep(append(b.BwPeaks, c.BwPeak), remembered)
		p.bwPeaks = keep(append(p.bwPeaks, c.BwPeak), priorSamples)
	}
}

// schedule releases what fits, oldest first (a restore first of all), a
// younger response passing an older one that does not fit; the oldest, once
// it has waited Book, books its reservation (the younger ones pass only on
// what is left, so the pool drains down to it) and, once it has waited
// MaxPass, goes regardless when no call runs. A parked sandbox's response
// has its memory prefetched at once and goes once it is back (or after
// PrefetchWait), a heavy one when it fits.
func (p *Policy) schedule(now time.Time) []policy.Action {
	var out []policy.Action
	held := p.Held
	booked, bookedBw := 0.0, 0.0 // the aged head's reservation, kept from the younger ones
	for i := 0; i < len(held); {
		h := held[i]
		b := p.Boxes[h.Sandbox]
		if b == nil {
			held = append(held[:i], held[i+1:]...)
			continue
		}
		escaped := false
		if h.Prefetch {
			if now.Sub(h.PrefetchSince) < p.PrefetchWait {
				i++
				continue // its memory is on its way back
			}
			held[i].Prefetch, held[i].Warm, h.Prefetch, h.Warm = false, true, false, true
			p.Status.PrefetchTimeouts++
		}
		if !h.Warm {
			if !b.Parked.IsZero() {
				// Its memory is in swap: bring it back first, at once.
				out = append(out, p.unpark(h.Sandbox, b, &held[i], now))
				i++
				continue
			}
			if !h.Light && !p.fits(h.Reserve+booked, h.BwReserve+bookedBw, now) {
				// It waits, and the younger responses that fit pass it: the pool is
				// never idled for it. The oldest, once it has waited its turn out (a
				// big call never fits while the small ones keep filling the room), goes
				// past the ceiling onto the machine's spare room (its reservation within
				// Pressure of the cores and of the bandwidth on top of what is measured
				// and pending) and is not counted against the ceiling while it runs, so
				// the small calls keep flowing under it: the ceiling keeps the pool on
				// time, the machine's room keeps the big calls from starving, and a
				// spike past that is the rollback's business. When no call runs at all
				// it goes regardless: the standing use of the residents must not hold
				// it for good. (Booking, off by default, is the older answer: the aged
				// head books its reservation and the younger ones pass only on what is
				// left.)
				aged := i == 0 && now.Sub(h.Since) > p.MaxPass
				if !aged || (p.running() > 0 && !p.roomOnMachine(h.Reserve, h.BwReserve, now)) {
					if i == 0 && p.Book > 0 && now.Sub(h.Since) > p.Book {
						booked, bookedBw = h.Reserve, h.BwReserve
						if !held[i].Booked {
							held[i].Booked = true
							p.Status.Booked++
							slog.Debug("tidesand book", "sandbox", h.Sandbox, "call", h.Call, "reserve", fmt.Sprintf("%.2f", h.Reserve), "waited", now.Sub(h.Since).Round(time.Second),
								"measured", fmt.Sprintf("%.1f+%.1f of %.1f", p.measured, p.pending(now), p.budget()), "queue", len(held))
						}
					}
					i++
					continue
				}
				p.Status.Escaped++
				escaped = true
			}
		}
		held = append(held[:i], held[i+1:]...)
		// A release is never stamped earlier than the hold it ends: a sample's
		// clock can trail a hold taken meanwhile by up to a second.
		at := now
		if h.Since.After(at) {
			at = h.Since
		}
		if h.Restore {
			b.Call = &Call{ID: h.Call, Since: at, Reserve: h.Reserve, BwReserve: h.BwReserve, Rerun: true}
			slog.Info("tidesand: restoring", "sandbox", h.Sandbox, "call", h.Call, "stoppedFor", at.Sub(h.Since).Round(time.Millisecond))
			out = append(out, policy.Restore{ID: h.Sandbox, Call: h.Call})
			continue
		}
		if h.Light {
			out = append(out, p.deliver(h.Sandbox, b)...)
			out = append(out, policy.Release{ID: h.Sandbox, Call: h.Call})
			continue
		}
		p.start(h.Sandbox, b, h.Call, h.Reserve, h.BwReserve, false, escaped, at, at.Sub(h.Since))
		out = append(out, p.deliver(h.Sandbox, b)...)
		out = append(out, policy.Release{ID: h.Sandbox, Call: h.Call})
	}
	p.Held = held
	p.Status.HeldNow = len(held)
	return out
}

// hold queues a response and schedules at once: one that fits and may pass
// the head goes now.
func (p *Policy) hold(h Held, why string, now time.Time) []policy.Action {
	p.Held = append(p.Held, h)
	p.Status.Held++
	p.Status.HeldNow = len(p.Held)
	slog.Debug("tidesand hold", "sandbox", h.Sandbox, "call", h.Call, "reserve", fmt.Sprintf("%.2f", h.Reserve), "on", why,
		"measured", fmt.Sprintf("%.1f+%.1f of %.1f", p.measured, p.pending(now), p.budget()), "traffic", fmt.Sprintf("%.0f+%.0f of %.0f GB/s", p.bwUsed/1e9, p.bwPending(now)/1e9, p.bwBudget()/1e9),
		"memory", fmt.Sprintf("%.1f of %.1f GiB", p.memUsed/gib, p.MemTarget*p.memory/gib), "queue", len(p.Held))
	return append([]policy.Action{policy.Hold{ID: h.Sandbox, Call: h.Call}}, p.schedule(now)...)
}

// OnModelEvent implements policy.ModelAware.
func (p *Policy) OnModelEvent(ev policy.ModelEvent, view policy.View) []policy.Action {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capacities(view)
	b := p.Boxes[ev.Sandbox]
	if b == nil {
		sv, ok := meter.Find(view, ev.Sandbox)
		if !ok || sv.Sandbox.Status.Phase != sandbox.PhaseRunning {
			return nil
		}
		b = p.newBox(sv.Sandbox, ev.Time)
		p.Boxes[ev.Sandbox] = b
	}
	switch ev.Kind {
	case policy.ModelRequestStarted:
		if b.Stopped || b.Rerun {
			return nil // the restored agent asking again: its call is the one to rerun
		}
		if b.Active > 0 {
			b.Active--
		}
		if b.Active > 0 {
			return nil // another thread's call still runs
		}
		b.IdleSince = ev.Time
		p.endCall(b, ev.Time)
		return p.schedule(ev.Time)
	case policy.ModelResponseReady:
		if b.Stopped {
			return nil
		}
		parked := !b.Parked.IsZero()
		if ev.ToolCalls == 0 || !heavy(ev.ToolNames) {
			if ev.ToolCalls > 0 {
				p.Status.Passed++
			}
			if !parked || ev.ToolCalls == 0 {
				return p.forward(ev, b)
			}
			// Its tools run in the agent, which is about to touch its memory: held until
			// that memory fits and is back.
			p.Status.LightHeld++
			return p.hold(Held{Sandbox: ev.Sandbox, Call: ev.Call, Since: ev.Time, Light: true}, "memory (parked)", ev.Time)
		}
		reserve, bw := p.reserve(b), p.bwReserve(b)
		if b.Rerun {
			b.Rerun = false
			p.start(ev.Sandbox, b, ev.Call, reserve, bw, true, false, ev.Time, 0)
			return p.forward(ev, b)
		}
		if parked {
			return p.hold(Held{Sandbox: ev.Sandbox, Call: ev.Call, Since: ev.Time, Reserve: reserve, BwReserve: bw}, "memory (parked)", ev.Time)
		}
		if len(p.Held) == 0 && p.fits(reserve, bw, ev.Time) {
			p.start(ev.Sandbox, b, ev.Call, reserve, bw, false, false, ev.Time, 0)
			return p.forward(ev, b)
		}
		why := "cpu"
		if len(p.Held) > 0 {
			why = "queue"
		} else if p.cores > 0 && p.measured+p.pending(ev.Time)+reserve <= p.budget() {
			why = "traffic"
		}
		return p.hold(Held{Sandbox: ev.Sandbox, Call: ev.Call, Since: ev.Time, Reserve: reserve, BwReserve: bw}, why, ev.Time)
	case policy.ModelResponseDelivered:
		for i, h := range p.Held {
			if h.Call == ev.Call && !h.Restore {
				p.Held = append(p.Held[:i], p.Held[i+1:]...)
				p.Status.ForcedOut++
				slog.Warn("tidesand: response forced out by the proxy's deadline; raise --llm-max-defer", "sandbox", ev.Sandbox, "call", ev.Call, "held", ev.Held)
				if !h.Light {
					b.Call = &Call{ID: ev.Call, Since: ev.Time, Reserve: h.Reserve, BwReserve: h.BwReserve}
				}
				return p.deliver(ev.Sandbox, b)
			}
		}
	case policy.ModelRolledBack:
		b.Rolling = false
		if ev.Killed == 0 {
			return p.schedule(ev.Time)
		}
		if !ev.Whole {
			slog.Warn("tidesand: the rollback was not of the whole sandbox; run tideletd with --container-checkpoints", "sandbox", ev.Sandbox)
			return nil
		}
		p.Status.Rollbacks++
		reserve, bw := 0.0, 0.0
		if b.Call != nil {
			reserve, bw = b.Call.Reserve, b.Call.BwReserve
		}
		b.Stopped, b.Call, b.Active = true, nil, 0
		p.Held = append([]Held{{Sandbox: ev.Sandbox, Call: ev.Call, Since: ev.Time, Reserve: reserve, BwReserve: bw, Restore: true}}, p.Held...)
		slog.Info("tidesand: undone, restore queued", "sandbox", ev.Sandbox, "call", ev.Call, "killed", ev.Killed)
		return p.schedule(ev.Time)
	case policy.ModelRestored:
		if ev.Status != 0 {
			slog.Warn("tidesand: restore failed; the sandbox is given up on", "sandbox", ev.Sandbox, "call", ev.Call)
			b.Stopped, b.Rerun, b.Call = false, false, nil
			return p.schedule(ev.Time)
		}
		p.Status.Restores++
		b.Stopped, b.Rerun = false, true
	}
	return nil
}

// forward lets a response that is not held go: at once when the sandbox has
// its quota, else held just long enough for the quota to come back first.
func (p *Policy) forward(ev policy.ModelEvent, b *Sandbox) []policy.Action {
	acts := p.deliver(ev.Sandbox, b)
	if len(acts) == 0 {
		return nil
	}
	return append(append([]policy.Action{policy.Hold{ID: ev.Sandbox, Call: ev.Call}}, acts...), policy.Release{ID: ev.Sandbox, Call: ev.Call})
}

func keep(s []float64, n int) []float64 {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func median(s []float64) float64 {
	if len(s) == 0 {
		return 0
	}
	c := append([]float64(nil), s...)
	sort.Float64s(c)
	return c[len(c)/2]
}

// light is the set of tools the agent runs inside its own process; a
// response carrying only these is not an execution.
var light = map[string]bool{
	"Read": true, "Write": true, "Edit": true, "MultiEdit": true, "NotebookRead": true, "NotebookEdit": true,
	"Glob": true, "Grep": true, "LS": true, "TodoRead": true, "TodoWrite": true,
	"WebFetch": true, "WebSearch": true, "Task": true, "Agent": true, "AskUserQuestion": true,
	"EnterPlanMode": true, "ExitPlanMode": true, "BashOutput": true, "KillShell": true, "KillBash": true,
}

// heavy says whether a response's tools run outside the agent: any tool
// not in the light set (Read, Edit, Grep, ...), or no tool names at all.
func heavy(tools []string) bool {
	if len(tools) == 0 {
		return true
	}
	for _, t := range tools {
		if !light[t] {
			return true
		}
	}
	return false
}
