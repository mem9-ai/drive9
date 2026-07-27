package meta

import (
	"strings"
	"testing"
)

func TestManagedSharedDBPoolWaveCandidateLockOrderIsStable(t *testing.T) {
	if !strings.Contains(lockManagedSharedDBPoolWaveCandidatesSQL, "ORDER BY db_id\n\t\tFOR UPDATE") {
		t.Fatalf("candidate lock query must acquire rows in stable db_id order: %s", lockManagedSharedDBPoolWaveCandidatesSQL)
	}
	if strings.Contains(lockManagedSharedDBPoolWaveCandidatesSQL, "ORDER BY CASE status") {
		t.Fatalf("candidate lock query orders by mutable status: %s", lockManagedSharedDBPoolWaveCandidatesSQL)
	}

	candidates := []managedSharedDBPoolWaveCandidate{
		{dbID: 1, status: SharedDBStatusPending},
		{dbID: 2, status: SharedDBStatusActive},
		{dbID: 3, status: SharedDBStatusProvisioning},
	}
	sortManagedSharedDBPoolWaveCandidates(candidates)
	if candidates[0].dbID != 2 || candidates[1].dbID != 3 || candidates[2].dbID != 1 {
		t.Fatalf("candidate preference order = %v, want active, provisioning, pending", candidates)
	}
}
