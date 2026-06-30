package e2ereport

import "testing"

func TestTierFromEvent(t *testing.T) {
	cases := map[string]Tier{
		"pull_request":        TierPR,
		"pull_request_target": TierPR,
		"merge_group":         TierPR,
		"push":                TierPostMerge,
		"schedule":            TierNightly,
		"workflow_dispatch":   TierManual,
		"workflow_call":       TierManual,
		"":                    TierManual,
	}
	for in, want := range cases {
		if got := TierFromEvent(in); got != want {
			t.Errorf("TierFromEvent(%q)=%q want %q", in, got, want)
		}
	}
}
