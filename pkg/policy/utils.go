package policy

import (
	"github.com/TideSand/tidelet/pkg/resource"
	"github.com/TideSand/tidelet/pkg/runtime"
)

// DefaultCPUPeriodUS is the CFS period used when shaping CPU quotas. It is
// the Kubernetes and Docker default.
const DefaultCPUPeriodUS = 100_000

// MillicoresToShares converts a CPU request to cgroup shares the way the
// kubelet does: 1024 shares per core, minimum 2.
func MillicoresToShares(m resource.Millicores) uint64 {
	if m == 0 {
		return 0
	}
	return max(uint64(m)*1024/1000, 2)
}

// MillicoresToQuota converts a CPU limit to a CFS quota in microseconds
// for the given period, rounded up to the kernel's minimum of 1ms.
func MillicoresToQuota(m resource.Millicores, periodUS uint64) int64 {
	if m == 0 {
		return 0
	}
	return max(int64(m)*int64(periodUS)/1000, 1000)
}

// ShapeGuaranteed produces cgroup settings where limits are hard caps,
// requests drive weight, and swap is disabled. Policies can use it as a
// starting point.
func ShapeGuaranteed(requests, limits resource.Set) runtime.Cgroup {
	cg := runtime.Cgroup{CPUShares: MillicoresToShares(requests.CPU)}
	if limits.CPU > 0 {
		cg.CPUPeriodUS = DefaultCPUPeriodUS
		cg.CPUQuotaUS = MillicoresToQuota(limits.CPU, DefaultCPUPeriodUS)
	}
	if limits.Memory > 0 {
		cg.MemoryLimitBytes = int64(limits.Memory)
		cg.MemorySwapLimitBytes = -1
	}
	return cg
}
