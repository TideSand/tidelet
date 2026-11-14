package meter

import (
	"testing"
	"time"

	"github.com/TideSand/tidelet/pkg/host"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

func TestWeightShares(t *testing.T) {
	for _, w := range []uint64{1, 2, 10, 50, 100, 150, 300, 500, 1000, 9999, 10000} {
		if got := SharesWeight(WeightShares(w)); got != w {
			t.Errorf("weight %d -> shares %d -> weight %d", w, WeightShares(w), got)
		}
	}
	if SharesWeight(0) != 0 {
		t.Error("unset shares should stay unset")
	}
}

func TestMeter(t *testing.T) {
	var m Meter
	t0 := time.Now()
	s := host.Sample{Time: t0, CPUBusy: 0, CPUTotal: 1000, MemTotal: 100, MemAvailable: 60}
	l := m.Update(s)
	if l.CPU != 0 || l.Memory != 0.4 || l.MemoryBytes != 40 {
		t.Fatalf("first load = %+v", l)
	}
	s = host.Sample{Time: t0.Add(time.Second), CPUBusy: 500, CPUTotal: 2000, MemTotal: 100, MemAvailable: 60}
	s.Memory.Full.Avg10 = 2.5
	l = m.Update(s)
	if l.CPU < 0.19 || l.CPU > 0.21 || l.Stalls != 2.5 { // 0.4 x 0.5 busy
		t.Fatalf("second load = %+v", l)
	}
}

func TestRates(t *testing.T) {
	var r Rates
	t0 := time.Now()
	if _, _, ok := r.Observe(&sandbox.Stats{Timestamp: t0, CPUUsageNanos: 1e9}); ok {
		t.Fatal("first sample gave a rate")
	}
	cores, stall, ok := r.Observe(&sandbox.Stats{Timestamp: t0.Add(2 * time.Second), CPUUsageNanos: 4e9, CPUSomeStallUSec: 500_000})
	if !ok || cores != 1.5 || stall != 0.25 {
		t.Fatalf("rates = %v %v %v", cores, stall, ok)
	}
	if _, _, ok := r.Observe(&sandbox.Stats{Timestamp: t0.Add(2 * time.Second), CPUUsageNanos: 4e9}); ok {
		t.Fatal("a repeated sample gave a rate")
	}
}
