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

	var summary []string
	if sig := r.FailureSignature(); sig != "" {
		summary = append(summary, fmt.Sprintf("**signature:** `%s`", sig))
	}
	summary = append(summary, fmt.Sprintf("**trigger:** %s", triggerName(r.Context.Trigger)))
	elements := []any{mdDiv(strings.Join(summary, "  ·  "))}

	if len(r.Failed) > 0 {
		fl := []string{fmt.Sprintf("**failed (%d):**", len(r.Failed))}
		for _, s := range r.Failed {
			fl = append(fl, fmt.Sprintf("• **%s** — <font color='%s'>%s</font> / %s (owner: %s)",
				s.Suite, failClassColor(s.FailureClass), dash(string(s.FailureClass)), dash(s.ProductPromise), dash(s.OwnerHint)))
		}
		elements = append(elements, mdDiv(strings.Join(fl, "\n")))
	}
	if len(r.PerfRegressed) > 0 {
		elements = append(elements, mdDiv("**performance regressions:**"))
		for _, s := range r.PerfRegressed {
			for _, m := range s.PerfRegressions() {
				elements = append(elements, perfFieldsDiv(s.Suite, m))
			}
		}
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

// mdDiv is a lark_md text block element.
func mdDiv(content string) map[string]any {
	return map[string]any{"tag": "div", "text": map[string]any{"tag": "lark_md", "content": content}}
}

// shortField is a half-width lark_md field for the two-column metric layout.
func shortField(content string) map[string]any {
	return map[string]any{"is_short": true, "text": map[string]any{"tag": "lark_md", "content": content}}
}

// perfFieldsDiv lays one regressed metric out in aligned columns so the numbers
// an operator acts on stand out: a full-width suite/metric header, then the
// measured value (bold red) beside budget and baseline, each with a coloured
// arrow + percentage delta (worse=red ▲, better=green ▼).
func perfFieldsDiv(suite string, m Metric) map[string]any {
	fields := []any{
		map[string]any{"is_short": false, "text": map[string]any{"tag": "lark_md", "content": fmt.Sprintf("⏱ **%s** · %s", suite, m.Name)}},
		shortField(fmt.Sprintf("**value**\n<font color='red'>**%.2f%s**</font>", m.Value, m.Unit)),
	}
	if m.Budget != nil {
		fields = append(fields, shortField(fmt.Sprintf("**budget**\n%.2f%s %s", *m.Budget, m.Unit, arrowPct(m.Value, *m.Budget))))
	}
	if m.Baseline != nil {
		fields = append(fields, shortField(fmt.Sprintf("**baseline**\n%.2f%s %s", *m.Baseline, m.Unit, arrowPct(m.Value, *m.Baseline))))
	}
	return map[string]any{"tag": "div", "fields": fields}
}

// arrowPct renders a coloured arrow + bold percentage difference of value vs ref:
// an increase is ▲ red, a decrease is ▼ green. Empty when ref is zero.
//
// This assumes lower-is-better, which holds for the latency budgets in the suite
// manifest today (e.g. p99_write_ms). If a higher-is-better metric (e.g.
// throughput) ever gets a budget, add a HigherIsBetter flag to Metric and invert
// the colours here, or the arrow will read inverted for it.
func arrowPct(value, ref float64) string {
	if ref == 0 {
		return ""
	}
	pct := (value - ref) / ref * 100
	if pct >= 0 {
		return fmt.Sprintf("<font color='red'>▲ **+%.0f%%**</font>", pct)
	}
	return fmt.Sprintf("<font color='green'>▼ **%.0f%%**</font>", pct)
}

// failClassColor maps a failure class to a lark_md font colour for quick triage.
func failClassColor(c FailureClass) string {
	switch c {
	case FailureCorrectness:
		return "red"
	case FailureInfrastructure, FailureDependency, FailurePerformance:
		return "orange"
	case FailureFlaky:
		return "grey"
	default:
		return "grey"
	}
}
