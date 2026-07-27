package main

import (
	"fmt"
	"testing"
	"time"
)

func TestLatencyHistogramMemoryIsBounded(t *testing.T) {
	t.Parallel()

	histogram := newLatencyHistogram(defaultProvisionLatencyBounds)
	for index := 0; index < 500_000; index++ {
		histogram.Record(time.Duration(index%10_000) * time.Millisecond)
	}
	snapshot := histogram.Snapshot()
	if snapshot.SampleCount != 500_000 {
		t.Fatalf("SampleCount = %d, want 500000", snapshot.SampleCount)
	}
	if len(snapshot.Buckets) != len(defaultProvisionLatencyBounds)+1 {
		t.Fatalf("buckets = %d, want %d", len(snapshot.Buckets), len(defaultProvisionLatencyBounds)+1)
	}
	if snapshot.P95 <= 0 || snapshot.Max <= 0 {
		t.Fatalf("latency snapshot = %+v", snapshot)
	}
}

func TestBenchmarkAccumulatorBoundsFailureSamples(t *testing.T) {
	t.Parallel()

	accumulator := newBenchmarkAccumulator(1_000)
	for index := 0; index < 1_000; index++ {
		accumulator.Record(tenantResult{
			Index:             index,
			TenantID:          fmt.Sprintf("tenant-%d", index),
			ProvisionAccepted: true,
			InitialStatus:     "provisioning",
			FinalStatus:       "failed",
			Error:             "terminal status",
			ProvisionSeconds:  0.25,
			ProvisionRequests: 1,
			StatusRequests:    1,
		})
	}
	summary, failures := accumulator.Snapshot(time.Minute)
	if summary.Completed != 1_000 || summary.Failed != 1_000 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(failures) != maxFailureSamples {
		t.Fatalf("failure samples = %d, want %d", len(failures), maxFailureSamples)
	}
	if summary.ProvisionLatency.SampleCount != 1_000 {
		t.Fatalf("provision latency = %+v", summary.ProvisionLatency)
	}
}

func TestBenchmarkAccumulatorRecordsReadyLatencyOnlyWhenMeasured(t *testing.T) {
	t.Parallel()

	accumulator := newBenchmarkAccumulator(2)
	accumulator.Record(tenantResult{
		Index:             0,
		ProvisionAccepted: true,
		Success:           true,
		ProvisionSeconds:  0.5,
		ProvisionRequests: 1,
	})
	accumulator.Record(tenantResult{
		Index:             1,
		ProvisionAccepted: true,
		Success:           true,
		ProvisionSeconds:  0.75,
		ReadySeconds:      2,
		ProvisionRequests: 1,
		readyMeasured:     true,
	})

	summary, _ := accumulator.Snapshot(time.Second)
	if summary.ProvisionLatency.SampleCount != 2 {
		t.Fatalf("provision latency = %+v", summary.ProvisionLatency)
	}
	if summary.ReadyLatency.SampleCount != 1 {
		t.Fatalf("ready latency = %+v", summary.ReadyLatency)
	}
}

func TestProgressWindowReportsExactLatencyAndResets(t *testing.T) {
	t.Parallel()

	window := newProgressWindow()
	for _, result := range []tenantResult{
		{
			Success:           true,
			ProvisionRequests: 1,
			ProvisionSeconds:  1,
			ReadySeconds:      2,
			readyMeasured:     true,
		},
		{
			Success:           false,
			ProvisionRequests: 1,
			ProvisionSeconds:  4,
		},
		{
			Success:           true,
			ProvisionRequests: 1,
			ProvisionSeconds:  2,
		},
		{
			Success:           false,
			ProvisionRequests: 1,
			ProvisionSeconds:  3,
		},
	} {
		window.Record(result)
	}

	snapshot := window.SnapshotAndReset(time.Minute)
	if snapshot.Completed != 4 || snapshot.Success != 2 || snapshot.Failed != 2 {
		t.Fatalf("window counts = %+v", snapshot)
	}
	if snapshot.TenantsPerMinute != 2 {
		t.Fatalf("tenants per minute = %f, want 2", snapshot.TenantsPerMinute)
	}
	provision := snapshot.ProvisionLatency
	if provision.SampleCount != 4 ||
		provision.Average != 2.5 ||
		provision.P50 != 2 ||
		provision.P90 != 4 ||
		provision.P95 != 4 ||
		provision.P99 != 4 ||
		provision.Max != 4 {
		t.Fatalf("provision latency = %+v", provision)
	}
	ready := snapshot.ReadyLatency
	if ready.SampleCount != 1 ||
		ready.Average != 2 ||
		ready.P50 != 2 ||
		ready.P90 != 2 ||
		ready.P95 != 2 ||
		ready.P99 != 2 ||
		ready.Max != 2 {
		t.Fatalf("ready latency = %+v", ready)
	}

	empty := window.SnapshotAndReset(time.Second)
	if empty.Completed != 0 ||
		empty.ProvisionLatency.SampleCount != 0 ||
		empty.ReadyLatency.SampleCount != 0 {
		t.Fatalf("window was not reset: %+v", empty)
	}
}
