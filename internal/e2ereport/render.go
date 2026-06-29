package e2ereport

import (
	"fmt"
	"sort"
	"strings"
)

// Markdown renders the run report for GITHUB_STEP_SUMMARY: a status table grouped
// by product promise, plus concrete failed suites and any perf regressions.
func (r RunReport) Markdown() string {
	var b strings.Builder
	overall := "✅ success"
	switch {
	case !r.OverallSuccess:
		overall = "❌ failure"
	case len(r.PerfRegressed) > 0:
		overall = "⚠️ performance regression"
	}
	fmt.Fprintf(&b, "## drive9 e2e product-quality report\n\n")
	fmt.Fprintf(&b, "- overall: **%s**\n- trigger: `%s`\n", overall, triggerName(r.Context.Trigger))
	if r.Context.RunURL != "" {
		fmt.Fprintf(&b, "- run: %s\n", r.Context.RunURL)
	}
	if sig := r.FailureSignature(); sig != "" {
		fmt.Fprintf(&b, "- failure signature: `%s`\n", sig)
	}
	b.WriteString("\n### Suites by product promise\n\n")
	b.WriteString("| Suite | Promise | Status | Class | Owner |\n|---|---|---|---|---|\n")
	for _, s := range r.byPromise() {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
			dash(s.Suite), dash(s.ProductPromise), statusIcon(s.Status), dash(string(s.FailureClass)), dash(s.OwnerHint))
	}
	if len(r.Failed) > 0 {
		b.WriteString("\n### Failed suites\n\n")
		for _, s := range r.Failed {
			fmt.Fprintf(&b, "- **%s** (%s) — %s\n", s.Suite, dash(string(s.FailureClass)), dash(s.Detail))
		}
	}
	if len(r.PerfRegressed) > 0 {
		b.WriteString("\n### Performance regressions\n\n")
		for _, s := range r.PerfRegressed {
			for _, m := range s.PerfRegressions() {
				fmt.Fprintf(&b, "- **%s** %s = %.2f%s (budget %s, baseline %s)\n",
					s.Suite, m.Name, m.Value, m.Unit, fmtPtr(m.Budget), fmtPtr(m.Baseline))
			}
		}
	}
	return b.String()
}

// IssueBody renders the structured tracking-issue body, led by the failure
// signature so recurring patterns group into one thread.
func (r RunReport) IssueBody() string {
	var b strings.Builder
	kind := "failure"
	if r.OverallSuccess && len(r.PerfRegressed) > 0 {
		kind = "performance regression"
	}
	fmt.Fprintf(&b, "### drive9 e2e %s %s\n\n", triggerName(r.Context.Trigger), kind)
	fmt.Fprintf(&b, "- signature: `%s`\n", r.FailureSignature())
	if r.Context.SHA != "" {
		fmt.Fprintf(&b, "- commit: `%s`\n", r.Context.SHA)
	}
	if r.Context.RunURL != "" {
		fmt.Fprintf(&b, "- run: %s\n", r.Context.RunURL)
	}
	if len(r.Failed) > 0 {
		b.WriteString("\n**Failed suites:**\n\n")
	}
	for _, s := range r.Failed {
		fmt.Fprintf(&b, "- `%s` — class=%s, promise=%s, owner=%s", s.Suite, dash(string(s.FailureClass)), dash(s.ProductPromise), dash(s.OwnerHint))
		if s.ArtifactURL != "" {
			fmt.Fprintf(&b, ", artifact=%s", s.ArtifactURL)
		}
		b.WriteString("\n")
		if s.Detail != "" {
			fmt.Fprintf(&b, "  - %s\n", s.Detail)
		}
	}
	for _, s := range r.PerfRegressed {
		for _, m := range s.PerfRegressions() {
			fmt.Fprintf(&b, "- ⏱ perf `%s` %s=%.2f%s budget=%s baseline=%s\n",
				s.Suite, m.Name, m.Value, m.Unit, fmtPtr(m.Budget), fmtPtr(m.Baseline))
		}
	}
	return b.String()
}

// FeishuCard builds the Feishu/Lark interactive card payload for a run report.
// Returns the object to be JSON-marshalled as the message `content` (msg_type
// "interactive"). Compact and actionable per PRD: suite/signature/area/owner,
// run URL, artifact URL, blocking level.
func (r RunReport) FeishuCard() map[string]any {
	headerColor := "red"
	title := fmt.Sprintf("drive9 e2e %s failure", triggerName(r.Context.Trigger))
	if r.OverallSuccess && len(r.PerfRegressed) > 0 {
		headerColor = "orange"
		title = fmt.Sprintf("drive9 e2e %s performance regression", triggerName(r.Context.Trigger))
	}

	var lines []string
	if sig := r.FailureSignature(); sig != "" {
		lines = append(lines, fmt.Sprintf("**signature:** `%s`", sig))
	}
	lines = append(lines, fmt.Sprintf("**trigger:** %s", triggerName(r.Context.Trigger)))
	if len(r.Failed) > 0 {
		lines = append(lines, fmt.Sprintf("**failed (%d):**", len(r.Failed)))
		for _, s := range r.Failed {
			lines = append(lines, fmt.Sprintf("• %s — %s / %s (owner: %s)",
				s.Suite, dash(string(s.FailureClass)), dash(s.ProductPromise), dash(s.OwnerHint)))
		}
	}
	for _, s := range r.PerfRegressed {
		for _, m := range s.PerfRegressions() {
			lines = append(lines, fmt.Sprintf("• ⏱ %s %s=%.2f%s (budget %s, baseline %s)",
				s.Suite, m.Name, m.Value, m.Unit, fmtPtr(m.Budget), fmtPtr(m.Baseline)))
		}
	}

	elements := []any{
		map[string]any{
			"tag":  "div",
			"text": map[string]any{"tag": "lark_md", "content": strings.Join(lines, "\n")},
		},
	}
	var actions []any
	if r.Context.RunURL != "" {
		actions = append(actions, map[string]any{
			"tag":  "button",
			"text": map[string]any{"tag": "plain_text", "content": "Run"},
			"url":  r.Context.RunURL,
			"type": "primary",
		})
	}
	if len(actions) > 0 {
		elements = append(elements, map[string]any{"tag": "action", "actions": actions})
	}

	return map[string]any{
		"config": map[string]any{"wide_screen_mode": true},
		"header": map[string]any{
			"template": headerColor,
			"title":    map[string]any{"tag": "plain_text", "content": title},
		},
		"elements": elements,
	}
}

func (r RunReport) byPromise() []SuiteSummary {
	out := append([]SuiteSummary(nil), r.Suites...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ProductPromise != out[j].ProductPromise {
			return out[i].ProductPromise < out[j].ProductPromise
		}
		return out[i].Suite < out[j].Suite
	})
	return out
}

func statusIcon(s Status) string {
	switch s {
	case StatusSuccess:
		return "✅ success"
	case StatusFailure:
		return "❌ failure"
	case StatusSkipped:
		return "⏭ skipped"
	default:
		return string(s)
	}
}

func dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func fmtPtr(p *float64) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f", *p)
}
