package main

import (
	"context"
	"testing"
	"time"
)

func TestRunDemoMock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runDemo(ctx, demoConfig{mock: true})
	if err != nil {
		t.Fatalf("runDemo mock: %v", err)
	}
	if result.SmallFileThreshold != 32 {
		t.Fatalf("SmallFileThreshold = %d, want 32", result.SmallFileThreshold)
	}
	if result.StatusHits != 1 {
		t.Fatalf("StatusHits = %d, want 1", result.StatusHits)
	}
	if result.DirectUploadMode != "direct_put" {
		t.Fatalf("DirectUploadMode = %q, want direct_put", result.DirectUploadMode)
	}
	if result.LargeUploadMode != "multipart_v2" {
		t.Fatalf("LargeUploadMode = %q, want multipart_v2", result.LargeUploadMode)
	}
	if result.BatchStatCount != 3 || result.BatchReadSmallCount != 3 {
		t.Fatalf("batch counts = stat %d read %d, want 3/3", result.BatchStatCount, result.BatchReadSmallCount)
	}
	if !result.PatchChecksumVerified {
		t.Fatal("PatchChecksumVerified = false, want true")
	}
}
