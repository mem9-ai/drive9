package datastore

import "testing"

func TestExtentCompactSlicesIsJuiceFSMaxSlices(t *testing.T) {
	// JuiceFS pkg/meta/base.go maxSlices = 2500. HTTP compact below that
	// must stay off the Write/Fsync path (claim_compact loop only).
	const juiceMaxSlices = 2500
	if extentCompactSlices != juiceMaxSlices {
		t.Fatalf("extentCompactSlices=%d, want JuiceFS maxSlices %d", extentCompactSlices, juiceMaxSlices)
	}
	if jfsCompactThreshold != juiceMaxSlices {
		t.Fatalf("jfsCompactThreshold=%d, want %d", jfsCompactThreshold, juiceMaxSlices)
	}
}
