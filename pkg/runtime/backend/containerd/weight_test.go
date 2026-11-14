package containerd

import (
	"github.com/TideSand/tidelet/pkg/policy/meter"
	"testing"
)

func TestSharesToWeightMatchesMeter(t *testing.T) {
	for _, w := range []uint64{50, 100, 150, 1000} {
		sh := meter.WeightShares(w)
		if got := sharesToWeight(sh); got != meter.SharesWeight(sh) || got < w-1 || got > w+1 {
			t.Fatalf("weight %d -> shares %d -> weight %d (meter %d)", w, sh, got, meter.SharesWeight(sh))
		}
	}
}
