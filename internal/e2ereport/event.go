package e2ereport

// TierFromEvent maps a GitHub Actions event name to the automation tier it
// represents, so notification policy can distinguish a PR gate from a post-merge
// or nightly run.
func TierFromEvent(event string) Tier {
	switch event {
	case "pull_request", "pull_request_target", "merge_group":
		return TierPR
	case "push":
		return TierPostMerge
	case "schedule":
		return TierNightly
	default:
		// workflow_dispatch, workflow_call, and anything else are manual.
		return TierManual
	}
}
