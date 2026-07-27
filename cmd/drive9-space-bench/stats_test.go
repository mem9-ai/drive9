package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestLatencyHistogramUsesBoundedBuckets(t *testing.T) {
	t.Parallel()

	histogram := newLatencyHistogram(defaultLatencyBounds)
	for index := 0; index < 100_000; index++ {
		histogram.Record(time.Duration(index%10_000) * time.Microsecond)
	}
	snapshot := histogram.Snapshot()

	if snapshot.Count != 100_000 {
		t.Fatalf("count = %d", snapshot.Count)
	}
	if len(snapshot.Buckets) != len(defaultLatencyBounds)+1 {
		t.Fatalf("bucket count = %d, want %d", len(snapshot.Buckets), len(defaultLatencyBounds)+1)
	}
	if snapshot.P50Seconds <= 0 || snapshot.P95Seconds < snapshot.P50Seconds {
		t.Fatalf("percentiles = p50:%f p95:%f", snapshot.P50Seconds, snapshot.P95Seconds)
	}
}

func TestPrintLatencyHistogram(t *testing.T) {
	t.Parallel()

	histogram := newLatencyHistogram([]time.Duration{
		time.Millisecond,
		10 * time.Millisecond,
	})
	histogram.Record(500 * time.Microsecond)
	histogram.Record(5 * time.Millisecond)
	histogram.Record(20 * time.Millisecond)

	var output bytes.Buffer
	printLatencyHistogram(&output, "Write Latency Histogram", histogram.Snapshot())
	got := output.String()
	for _, want := range []string{
		"Write Latency Histogram:",
		"1ms",
		"10ms",
		"+Inf",
		"#",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("histogram missing %q:\n%s", want, got)
		}
	}
}

func TestPrintLatencyHistogramSkipsEmptySnapshot(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	printLatencyHistogram(&output, "Empty", histogramSnapshot{})
	if output.Len() != 0 {
		t.Fatalf("unexpected output: %q", output.String())
	}
}
