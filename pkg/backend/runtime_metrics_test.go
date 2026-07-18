package backend

import (
	"strings"
	"testing"
	"time"
)

func TestBackendRuntimeMetricsAggregateAcrossBackends(t *testing.T) {
	b1 := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Extractor: &staticImageExtractor{text: "one"},
		},
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:             true,
			MaxAudioBytes:       4096,
			TaskTimeout:         3 * time.Second,
			MaxExtractTextBytes: 777,
			Extractor:           &staticAudioExtractor{text: "one"},
		},
	})
	b2 := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Extractor: &staticImageExtractor{text: "two"},
		},
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:             true,
			MaxAudioBytes:       2048,
			TaskTimeout:         2 * time.Second,
			MaxExtractTextBytes: 555,
			Extractor:           &staticAudioExtractor{text: "two"},
		},
	})

	metricsText := readBackendMetrics()
	if !strings.Contains(metricsText, "drive9_module_up{module=\"image_extract\"} 1") {
		t.Fatalf("metrics missing aggregated image_extract availability: %s", metricsText)
	}
	// Image extract delivery is now durable (semantic_tasks), so queue capacity
	// and workers are always 0 — the semantic worker handles processing.
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"image_extract\",name=\"queue_capacity\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 0") {
		t.Fatalf("metrics missing aggregated image queue capacity: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"image_extract\",name=\"workers\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 0") {
		t.Fatalf("metrics missing aggregated image workers: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"audio_extract\",name=\"max_audio_bytes\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 4096") {
		t.Fatalf("metrics missing aggregated audio max bytes: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"audio_extract\",name=\"task_timeout_seconds\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 3") {
		t.Fatalf("metrics missing aggregated audio timeout: %s", metricsText)
	}

	b1.Close()

	metricsText = readBackendMetrics()
	if !strings.Contains(metricsText, "drive9_module_up{module=\"image_extract\"} 1") {
		t.Fatalf("metrics should keep image_extract available while one backend remains: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"image_extract\",name=\"queue_capacity\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 0") {
		t.Fatalf("metrics should keep image queue capacity at 0 after one backend closes: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"image_extract\",name=\"workers\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 0") {
		t.Fatalf("metrics should keep image workers at 0 after one backend closes: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"audio_extract\",name=\"max_audio_bytes\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 2048") {
		t.Fatalf("metrics should retain remaining audio max bytes: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"audio_extract\",name=\"max_extract_text_bytes\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 555") {
		t.Fatalf("metrics should retain remaining audio text limit: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_service_gauge{component=\"audio_extract\",name=\"task_timeout_seconds\",tenant_id=\"\",tidbcloud_org_id=\"guest\"} 2") {
		t.Fatalf("metrics should retain remaining audio timeout: %s", metricsText)
	}

	b2.Close()

	metricsText = readBackendMetrics()
	if !strings.Contains(metricsText, "drive9_module_up{module=\"image_extract\"} 0") {
		t.Fatalf("metrics should mark image_extract unavailable once all backends close: %s", metricsText)
	}
	if !strings.Contains(metricsText, "drive9_module_up{module=\"audio_extract\"} 0") {
		t.Fatalf("metrics should mark audio_extract unavailable once all backends close: %s", metricsText)
	}
}
