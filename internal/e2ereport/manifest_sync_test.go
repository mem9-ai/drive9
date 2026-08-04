package e2ereport

import (
	"os"
	"regexp"
	"testing"
)

// TestSuiteManifestCoversWorkflowOutcomes pins the sync contract between
// .github/workflows/local-e2e.yml and e2e/suite-manifest.json: every suite id
// the workflow emits into /tmp/e2e-outcomes.json must carry product metadata,
// and a failure outcome for it must synthesize a classified failure through
// the real aggregator path. Without this, a newly added step id silently
// degrades to class=unknown / promise=- / owner=- in nightly owner routing.
func TestSuiteManifestCoversWorkflowOutcomes(t *testing.T) {
	workflow, err := os.ReadFile("../../.github/workflows/local-e2e.yml")
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	manifestData, err := os.ReadFile("../../e2e/suite-manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest, err := LoadManifest(manifestData)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	idRe := regexp.MustCompile(`"([a-z0-9_-]+)": "\$\{\{ steps\.[a-z0-9_-]+\.outcome \}\}"`)
	ids := idRe.FindAllStringSubmatch(string(workflow), -1)
	if len(ids) == 0 {
		t.Fatal("no step outcome ids found in workflow — extraction pattern broken?")
	}

	for _, match := range ids {
		id := match[1]
		meta, ok := manifest.Suites[id]
		if !ok {
			t.Errorf("workflow outcome id %q missing from e2e/suite-manifest.json", id)
			continue
		}
		if meta.ProductPromise == "" || meta.OwnerHint == "" {
			t.Errorf("manifest entry %q lacks product_promise/owner_hint: %+v", id, meta)
		}

		// A failure outcome for this suite must synthesize a fully classified
		// failure through the real aggregator path.
		summaries := SynthesizeSummaries(manifest, TierNightly, map[string]string{id: "failure"}, nil)
		if len(summaries) != 1 {
			t.Fatalf("synthesize %q: got %d summaries, want 1", id, len(summaries))
		}
		s := summaries[0]
		if s.ProductPromise == "" || s.OwnerHint == "" || s.FailureClass == FailureNone || s.FailureClass == FailureUnknown {
			t.Errorf("synthesized failure for %q is not fully classified: %+v", id, s)
		}
	}
}
