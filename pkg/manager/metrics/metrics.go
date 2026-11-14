// Package metrics exports the manager's view of the node as Prometheus
// metrics: capacity and allocation, sandbox counts by phase, per-sandbox
// usage from the sampler, and host pressure.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/TideSand/tidelet/pkg/manager"
	"github.com/TideSand/tidelet/pkg/runtime/sandbox"
)

// Collector reads a fresh manager snapshot on every scrape.
type Collector struct {
	mgr *manager.Manager

	nodeCPU, nodeMem                       *prometheus.Desc
	sandboxes, queued                      *prometheus.Desc
	sbCPU, sbThrottled, sbMem, sbWS, sbLim *prometheus.Desc
	sbPids, sbReqCPU, sbReqMem, sbLimCPU   *prometheus.Desc
	sbLimMem, sbSwap                       *prometheus.Desc
	memAvail, memFree, swapFree, load, psi *prometheus.Desc
	modelCalls, modelHeld, modelForced     *prometheus.Desc
	modelOpen, modelHeldSeconds            *prometheus.Desc
}

// New builds a collector.
func New(mgr *manager.Manager) *Collector {
	sbLabels := []string{"id", "name"}
	return &Collector{
		mgr:              mgr,
		nodeCPU:          prometheus.NewDesc("tidelet_node_cpu_millicores", "Node CPU by kind: capacity, reserved, allocatable, allocated.", []string{"kind"}, nil),
		nodeMem:          prometheus.NewDesc("tidelet_node_memory_bytes", "Node memory by kind: capacity, reserved, allocatable, allocated.", []string{"kind"}, nil),
		sandboxes:        prometheus.NewDesc("tidelet_sandboxes", "Number of sandboxes by phase.", []string{"phase"}, nil),
		queued:           prometheus.NewDesc("tidelet_sandboxes_queued", "Sandboxes accepted but not yet created.", nil, nil),
		sbCPU:            prometheus.NewDesc("tidelet_sandbox_cpu_usage_seconds_total", "Cumulative CPU time consumed by the sandbox.", sbLabels, nil),
		sbThrottled:      prometheus.NewDesc("tidelet_sandbox_cpu_throttled_seconds_total", "Cumulative time the sandbox was throttled by its CPU quota.", sbLabels, nil),
		sbMem:            prometheus.NewDesc("tidelet_sandbox_memory_usage_bytes", "Current memory usage including page cache.", sbLabels, nil),
		sbWS:             prometheus.NewDesc("tidelet_sandbox_memory_working_set_bytes", "Usage minus inactive file cache.", sbLabels, nil),
		sbLim:            prometheus.NewDesc("tidelet_sandbox_memory_limit_bytes", "Enforced memory limit, 0 if unlimited.", sbLabels, nil),
		sbSwap:           prometheus.NewDesc("tidelet_sandbox_memory_swap_usage_bytes", "Swap the sandbox's cgroup holds.", sbLabels, nil),
		sbPids:           prometheus.NewDesc("tidelet_sandbox_pids", "Processes in the sandbox.", sbLabels, nil),
		sbReqCPU:         prometheus.NewDesc("tidelet_sandbox_cpu_request_millicores", "Accounted CPU request.", sbLabels, nil),
		sbReqMem:         prometheus.NewDesc("tidelet_sandbox_memory_request_bytes", "Accounted memory request.", sbLabels, nil),
		sbLimCPU:         prometheus.NewDesc("tidelet_sandbox_cpu_limit_millicores", "CPU limit.", sbLabels, nil),
		sbLimMem:         prometheus.NewDesc("tidelet_sandbox_memory_limit_request_bytes", "Memory limit from the allocation.", sbLabels, nil),
		modelCalls:       prometheus.NewDesc("tidelet_model_calls_total", "Model calls the LLM proxy saw.", nil, nil),
		modelHeld:        prometheus.NewDesc("tidelet_model_responses_held_total", "Model responses the policy held back.", nil, nil),
		modelForced:      prometheus.NewDesc("tidelet_model_responses_forced_total", "Held responses the proxy delivered at its deadline.", nil, nil),
		modelOpen:        prometheus.NewDesc("tidelet_model_responses_held", "Model responses held back right now.", nil, nil),
		modelHeldSeconds: prometheus.NewDesc("tidelet_model_held_seconds_total", "Time model responses spent held back.", nil, nil),
		memAvail:         prometheus.NewDesc("tidelet_host_memory_available_bytes", "MemAvailable from /proc/meminfo.", nil, nil),
		memFree:          prometheus.NewDesc("tidelet_host_memory_free_bytes", "MemFree from /proc/meminfo.", nil, nil),
		swapFree:         prometheus.NewDesc("tidelet_host_swap_free_bytes", "SwapFree from /proc/meminfo.", nil, nil),
		load:             prometheus.NewDesc("tidelet_host_load", "Load average.", []string{"window"}, nil),
		psi:              prometheus.NewDesc("tidelet_host_pressure_ratio", "Pressure stall share (0-1) by resource, kind and window.", []string{"resource", "kind", "window"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	v := c.mgr.Snapshot()
	n := c.mgr.Node()
	gauge := func(d *prometheus.Desc, val float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, val, labels...)
	}
	counter := func(d *prometheus.Desc, val float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, val, labels...)
	}
	ms := c.mgr.Model()
	counter(c.modelCalls, float64(ms.Calls))
	counter(c.modelHeld, float64(ms.Held))
	counter(c.modelForced, float64(ms.Forced))
	gauge(c.modelOpen, float64(ms.Open))
	counter(c.modelHeldSeconds, ms.HeldTotal.Seconds())
	gauge(c.nodeCPU, float64(n.Capacity.CPU), "capacity")
	gauge(c.nodeCPU, float64(v.Reserved.CPU), "reserved")
	gauge(c.nodeCPU, float64(n.Allocatable.CPU), "allocatable")
	gauge(c.nodeCPU, float64(n.Allocated.CPU), "allocated")
	gauge(c.nodeMem, float64(n.Capacity.Memory), "capacity")
	gauge(c.nodeMem, float64(v.Reserved.Memory), "reserved")
	gauge(c.nodeMem, float64(n.Allocatable.Memory), "allocatable")
	gauge(c.nodeMem, float64(n.Allocated.Memory), "allocated")

	phases := map[sandbox.Phase]int{
		sandbox.PhasePending: 0, sandbox.PhaseQueued: 0, sandbox.PhaseCreated: 0, sandbox.PhaseRunning: 0,
		sandbox.PhaseFrozen: 0, sandbox.PhaseExited: 0, sandbox.PhaseFailed: 0,
	}
	for _, s := range v.Sandboxes {
		phases[s.Sandbox.Status.Phase]++
		labels := []string{s.Sandbox.ID, s.Sandbox.Name}
		if s.Accounted {
			a := s.Sandbox.Status.Allocated
			gauge(c.sbReqCPU, float64(a.Requests.CPU), labels...)
			gauge(c.sbReqMem, float64(a.Requests.Memory), labels...)
			gauge(c.sbLimCPU, float64(a.Limits.CPU), labels...)
			gauge(c.sbLimMem, float64(a.Limits.Memory), labels...)
		}
		if st := s.Stats; st != nil {
			counter(c.sbCPU, float64(st.CPUUsageNanos)/1e9, labels...)
			counter(c.sbThrottled, float64(st.CPUThrottledNanos)/1e9, labels...)
			gauge(c.sbMem, float64(st.MemoryUsageBytes), labels...)
			gauge(c.sbWS, float64(st.MemoryWorkingSetBytes), labels...)
			gauge(c.sbLim, float64(st.MemoryLimitBytes), labels...)
			gauge(c.sbSwap, float64(st.MemorySwapUsageBytes), labels...)
			gauge(c.sbPids, float64(st.Pids), labels...)
		}
	}
	for phase, count := range phases {
		gauge(c.sandboxes, float64(count), string(phase))
	}
	gauge(c.queued, float64(n.Queued))

	if v.Node.MemTotal > 0 {
		gauge(c.memAvail, float64(v.Node.MemAvailable))
		gauge(c.memFree, float64(v.Node.MemFree))
		gauge(c.swapFree, float64(v.Node.SwapFree))
		gauge(c.load, v.Node.Load1, "1m")
		gauge(c.load, v.Node.Load5, "5m")
		gauge(c.load, v.Node.Load15, "15m")
		for res, p := range map[string]struct{ some, full [3]float64 }{
			"cpu":    {[3]float64{v.Node.CPU.Some.Avg10, v.Node.CPU.Some.Avg60, v.Node.CPU.Some.Avg300}, [3]float64{v.Node.CPU.Full.Avg10, v.Node.CPU.Full.Avg60, v.Node.CPU.Full.Avg300}},
			"memory": {[3]float64{v.Node.Memory.Some.Avg10, v.Node.Memory.Some.Avg60, v.Node.Memory.Some.Avg300}, [3]float64{v.Node.Memory.Full.Avg10, v.Node.Memory.Full.Avg60, v.Node.Memory.Full.Avg300}},
			"io":     {[3]float64{v.Node.IO.Some.Avg10, v.Node.IO.Some.Avg60, v.Node.IO.Some.Avg300}, [3]float64{v.Node.IO.Full.Avg10, v.Node.IO.Full.Avg60, v.Node.IO.Full.Avg300}},
		} {
			for i, w := range []string{"10s", "60s", "300s"} {
				gauge(c.psi, p.some[i]/100, res, "some", w)
				gauge(c.psi, p.full[i]/100, res, "full", w)
			}
		}
	}
}
