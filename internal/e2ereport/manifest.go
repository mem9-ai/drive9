package e2ereport

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// SuiteMeta is the declarative product metadata for one e2e suite, keyed by the
// workflow step id. It lets the aggregator synthesize a SuiteSummary for suites
// that have not yet adopted the structured JSON contract — bridging the existing
// `steps.<id>.outcome` table to the product-quality report.
type SuiteMeta struct {
	Title          string       `json:"title"`
	ProductArea    string       `json:"product_area,omitempty"`
	ProductPromise string       `json:"product_promise,omitempty"`
	OwnerHint      string       `json:"owner_hint,omitempty"`
	Tier           Tier         `json:"tier,omitempty"`
	FailureClass   FailureClass `json:"failure_class,omitempty"`
}

// SuiteManifest is the set of known suites and their product metadata.
type SuiteManifest struct {
	Suites map[string]SuiteMeta `json:"suites"`
}

// LoadManifest parses the suite manifest JSON.
func LoadManifest(data []byte) (SuiteManifest, error) {
	var m SuiteManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return SuiteManifest{}, fmt.Errorf("parse suite manifest: %w", err)
	}
	if len(m.Suites) == 0 {
		return SuiteManifest{}, fmt.Errorf("suite manifest has no suites")
	}
	return m, nil
}

// NormalizeStatus maps a GitHub Actions step outcome (success/failure/skipped/
// cancelled, or empty) to a Status. Unknown non-empty outcomes are failures so
// they are never silently treated as passing.
func NormalizeStatus(outcome string) Status {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "success":
		return StatusSuccess
	case "", "skipped":
		return StatusSkipped
	default:
		return StatusFailure
	}
}

// SynthesizeSummaries builds the suite summaries for a run. For each suite in
// outcomes (step id -> GitHub outcome): if an adopted structured summary exists
// it is used as-is; otherwise one is synthesized from the manifest metadata plus
// the step outcome. Suites present only in adopted summaries are also included.
func SynthesizeSummaries(m SuiteManifest, defaultTier Tier, outcomes map[string]string, adopted []SuiteSummary) []SuiteSummary {
	byID := make(map[string]SuiteSummary, len(adopted))
	for _, s := range adopted {
		byID[s.Suite] = s
	}

	out := make([]SuiteSummary, 0, len(outcomes)+len(adopted))
	seen := make(map[string]bool)

	ids := make([]string, 0, len(outcomes))
	for id := range outcomes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		seen[id] = true
		meta := m.Suites[id]
		status := NormalizeStatus(outcomes[id])
		if adopted, ok := byID[id]; ok {
			// Adopted summary wins for its dynamic fields, but manifest defaults
			// fill any it left blank, and the GitHub step outcome is authoritative
			// for failure: a script that exits non-zero (or leaves a stale success
			// summary behind) must not be reported as passing.
			s := fillFromManifest(adopted, meta, defaultTier)
			if status == StatusFailure {
				s = markFailed(s, meta, "step outcome reported failure")
			}
			out = append(out, s)
			continue
		}
		out = append(out, synthesize(id, status, meta, defaultTier))
	}

	// Adopted summaries for suites not in the outcomes map (e.g. emitted by a
	// suite the workflow table does not enumerate) are still included, with
	// manifest defaults filled in.
	extra := make([]string, 0)
	for id := range byID {
		if !seen[id] {
			extra = append(extra, id)
		}
	}
	sort.Strings(extra)
	for _, id := range extra {
		out = append(out, fillFromManifest(byID[id], m.Suites[id], defaultTier))
	}
	return out
}

func synthesize(id string, status Status, meta SuiteMeta, defaultTier Tier) SuiteSummary {
	s := SuiteSummary{
		Suite:          id,
		Status:         status,
		Tier:           pickTier(meta.Tier, defaultTier),
		ProductArea:    meta.ProductArea,
		ProductPromise: meta.ProductPromise,
		OwnerHint:      meta.OwnerHint,
	}
	if status == StatusFailure {
		s = markFailed(s, meta, "")
	}
	return s
}

// fillFromManifest fills zero-valued metadata fields of an adopted summary from
// the manifest, so signatures and rendering stay complete even when a producer
// emits only dynamic fields (status/metrics) and relies on manifest defaults.
func fillFromManifest(s SuiteSummary, meta SuiteMeta, defaultTier Tier) SuiteSummary {
	if s.Tier == "" {
		s.Tier = pickTier(meta.Tier, defaultTier)
	}
	if s.ProductArea == "" {
		s.ProductArea = meta.ProductArea
	}
	if s.ProductPromise == "" {
		s.ProductPromise = meta.ProductPromise
	}
	if s.OwnerHint == "" {
		s.OwnerHint = meta.OwnerHint
	}
	if s.Status == StatusFailure && s.FailureClass == FailureNone {
		s.FailureClass = pickFailureClass(meta.FailureClass)
	}
	return s
}

// markFailed forces a suite to failed, classifying it from the manifest and
// adding a default detail when the summary did not supply one.
func markFailed(s SuiteSummary, meta SuiteMeta, reason string) SuiteSummary {
	s.Status = StatusFailure
	if s.FailureClass == FailureNone {
		s.FailureClass = pickFailureClass(meta.FailureClass)
	}
	if s.Detail == "" {
		title := meta.Title
		if title == "" {
			title = s.Suite
		}
		s.Detail = title + " did not succeed"
		if reason != "" {
			s.Detail += " (" + reason + ")"
		}
	}
	return s
}

func pickTier(specific, fallback Tier) Tier {
	if specific != "" {
		return specific
	}
	return fallback
}

func pickFailureClass(c FailureClass) FailureClass {
	if c != FailureNone {
		return c
	}
	return FailureUnknown
}
