package main

import (
	"math"
	"sort"
	"time"
)

const maxFailureSamples = 100

var (
	defaultProvisionLatencyBounds = []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
	}
	defaultReadyLatencyBounds = []time.Duration{
		time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		20 * time.Minute,
		30 * time.Minute,
	}
)

type latencyBucket struct {
	UpperBound        string   `json:"upper_bound"`
	UpperBoundSeconds *float64 `json:"upper_bound_seconds,omitempty"`
	Count             int      `json:"count"`
}

type latencySummary struct {
	SampleCount int             `json:"sample_count"`
	Average     float64         `json:"average"`
	Median      float64         `json:"median"`
	P95         float64         `json:"p95"`
	P95Method   string          `json:"p95_method"`
	P95Reliable bool            `json:"p95_reliable"`
	Max         float64         `json:"max"`
	Buckets     []latencyBucket `json:"buckets"`
}

type latencyHistogram struct {
	bounds []time.Duration
	counts []int
	count  int
	total  time.Duration
	max    time.Duration
}

func newLatencyHistogram(bounds []time.Duration) *latencyHistogram {
	copied := append([]time.Duration(nil), bounds...)
	return &latencyHistogram{
		bounds: copied,
		counts: make([]int, len(copied)+1),
	}
}

func (h *latencyHistogram) Record(value time.Duration) {
	if value < 0 {
		value = 0
	}
	index := len(h.bounds)
	for candidate, bound := range h.bounds {
		if value <= bound {
			index = candidate
			break
		}
	}
	h.counts[index]++
	h.count++
	h.total += value
	if value > h.max {
		h.max = value
	}
}

func (h *latencyHistogram) Snapshot() latencySummary {
	summary := latencySummary{
		SampleCount: h.count,
		P95Method:   "fixed_histogram_upper_bound",
		P95Reliable: h.count >= 20,
		Max:         h.max.Seconds(),
		Buckets:     make([]latencyBucket, len(h.counts)),
	}
	for index, count := range h.counts {
		if index == len(h.bounds) {
			summary.Buckets[index] = latencyBucket{
				UpperBound: "+Inf",
				Count:      count,
			}
			continue
		}
		seconds := h.bounds[index].Seconds()
		summary.Buckets[index] = latencyBucket{
			UpperBound:        h.bounds[index].String(),
			UpperBoundSeconds: &seconds,
			Count:             count,
		}
	}
	if h.count == 0 {
		return summary
	}
	summary.Average = h.total.Seconds() / float64(h.count)
	summary.Median = h.quantile(0.50)
	summary.P95 = h.quantile(0.95)
	return summary
}

func (h *latencyHistogram) quantile(quantile float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := int(math.Ceil(quantile * float64(h.count)))
	cumulative := 0
	for index, count := range h.counts {
		cumulative += count
		if cumulative < target {
			continue
		}
		if index < len(h.bounds) {
			return h.bounds[index].Seconds()
		}
		return h.max.Seconds()
	}
	return h.max.Seconds()
}

type windowLatencySummary struct {
	SampleCount int
	Average     float64
	P50         float64
	P90         float64
	P95         float64
	P99         float64
	Max         float64
}

type progressWindowSnapshot struct {
	ElapsedSeconds   float64
	Completed        int
	Success          int
	Failed           int
	TenantsPerMinute float64
	ProvisionLatency windowLatencySummary
	ReadyLatency     windowLatencySummary
}

type progressWindow struct {
	completed          int
	success            int
	failed             int
	provisionLatencies []time.Duration
	readyLatencies     []time.Duration
}

func newProgressWindow() *progressWindow {
	return &progressWindow{}
}

func (w *progressWindow) Record(result tenantResult) {
	w.completed++
	if result.Success {
		w.success++
	} else {
		w.failed++
	}
	if result.ProvisionRequests > 0 {
		w.provisionLatencies = append(
			w.provisionLatencies,
			secondsToDuration(result.ProvisionSeconds),
		)
	}
	if result.Success && result.readyMeasured {
		w.readyLatencies = append(
			w.readyLatencies,
			secondsToDuration(result.ReadySeconds),
		)
	}
}

func (w *progressWindow) SnapshotAndReset(elapsed time.Duration) progressWindowSnapshot {
	snapshot := progressWindowSnapshot{
		ElapsedSeconds:   elapsed.Seconds(),
		Completed:        w.completed,
		Success:          w.success,
		Failed:           w.failed,
		ProvisionLatency: summarizeWindowLatency(w.provisionLatencies),
		ReadyLatency:     summarizeWindowLatency(w.readyLatencies),
	}
	if elapsed > 0 {
		snapshot.TenantsPerMinute = float64(w.success) / elapsed.Minutes()
	}
	w.completed = 0
	w.success = 0
	w.failed = 0
	w.provisionLatencies = w.provisionLatencies[:0]
	w.readyLatencies = w.readyLatencies[:0]
	return snapshot
}

func summarizeWindowLatency(samples []time.Duration) windowLatencySummary {
	summary := windowLatencySummary{SampleCount: len(samples)}
	if len(samples) == 0 {
		return summary
	}
	sort.Slice(samples, func(left, right int) bool {
		return samples[left] < samples[right]
	})
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	summary.Average = total.Seconds() / float64(len(samples))
	summary.P50 = nearestRankDuration(samples, 0.50).Seconds()
	summary.P90 = nearestRankDuration(samples, 0.90).Seconds()
	summary.P95 = nearestRankDuration(samples, 0.95).Seconds()
	summary.P99 = nearestRankDuration(samples, 0.99).Seconds()
	summary.Max = samples[len(samples)-1].Seconds()
	return summary
}

func nearestRankDuration(samples []time.Duration, quantile float64) time.Duration {
	index := int(math.Ceil(quantile*float64(len(samples)))) - 1
	index = max(0, min(index, len(samples)-1))
	return samples[index]
}

type tenantFailure struct {
	Index         int    `json:"index"`
	TenantID      string `json:"tenant_id,omitempty"`
	InitialStatus string `json:"initial_status,omitempty"`
	FinalStatus   string `json:"final_status,omitempty"`
	Error         string `json:"error"`
}

type benchmarkAccumulator struct {
	summary          benchmarkSummary
	provisionLatency *latencyHistogram
	readyLatency     *latencyHistogram
	failures         []tenantFailure
}

func newBenchmarkAccumulator(requested int) *benchmarkAccumulator {
	return &benchmarkAccumulator{
		summary: benchmarkSummary{
			Requested: requested,
		},
		provisionLatency: newLatencyHistogram(defaultProvisionLatencyBounds),
		readyLatency:     newLatencyHistogram(defaultReadyLatencyBounds),
		failures:         make([]tenantFailure, 0, min(requested, maxFailureSamples)),
	}
}

func (a *benchmarkAccumulator) Record(result tenantResult) {
	a.summary.Completed++
	a.summary.ProvisionRequests += result.ProvisionRequests
	a.summary.StatusRequests += result.StatusRequests
	if result.Success {
		a.summary.Success++
	} else {
		a.summary.Failed++
		if len(a.failures) < maxFailureSamples {
			a.failures = append(a.failures, tenantFailure{
				Index:         result.Index,
				TenantID:      result.TenantID,
				InitialStatus: result.InitialStatus,
				FinalStatus:   result.FinalStatus,
				Error:         result.Error,
			})
		}
	}
	if result.ProvisionAccepted {
		a.provisionLatency.Record(secondsToDuration(result.ProvisionSeconds))
	}
	if result.Success && result.readyMeasured {
		a.readyLatency.Record(secondsToDuration(result.ReadySeconds))
	}
}

func (a *benchmarkAccumulator) Snapshot(elapsed time.Duration) (benchmarkSummary, []tenantFailure) {
	summary := a.summary
	summary.ElapsedSeconds = elapsed.Seconds()
	if elapsed > 0 {
		summary.TenantsPerMinute = float64(summary.Success) / elapsed.Minutes()
	}
	summary.ProvisionLatency = a.provisionLatency.Snapshot()
	summary.ReadyLatency = a.readyLatency.Snapshot()
	return summary, append([]tenantFailure(nil), a.failures...)
}

func secondsToDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}
