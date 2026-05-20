package report

import (
	"fmt"
	"sort"
	"strings"
)

func RenderPerfMarkdown(r PerfReport) string {
	var b strings.Builder
	title := firstNonEmpty(r.Title, "Drive9 Performance Test Report")
	fmt.Fprintf(&b, "---\ntitle: %s\n---\n\n", title)
	fmt.Fprintf(&b, "# %s\n\n", title)
	writePerfExecutiveSummary(&b, r)
	writePerfScope(&b, r.Scope)
	writePerfEnvironment(&b, r.Environment)
	writePerfInfrastructure(&b, r.Infrastructure)
	writePerfResults(&b, r.Scenarios)
	writePerfMetricDefinitions(&b, r.MetricNotes)
	writePerfObservations(&b, r.Observations)
	writePerfArtifacts(&b, r.Artifacts)
	fmt.Fprintf(&b, "## Conclusion\n\n%s\n\n", firstNonEmpty(r.Conclusion, defaultConclusion(r.OverallStatus)))
	return b.String()
}

func writePerfExecutiveSummary(b *strings.Builder, r PerfReport) {
	fmt.Fprintf(b, "## Executive Summary\n\n")
	fmt.Fprintf(b, "1. Run ID: `%s`\n", markdownText(r.RunID))
	fmt.Fprintf(b, "2. Test window: `%s` to `%s`\n", markdownText(r.StartedAt), markdownText(r.EndedAt))
	fmt.Fprintf(b, "3. Overall status: `%s`\n", markdownText(firstNonEmpty(r.OverallStatus, "INCONCLUSIVE")))
	fmt.Fprintf(b, "4. Report generator schema: `%s`\n\n", markdownText(firstNonEmpty(r.GeneratorSchema, PerfSummarySchemaVersion)))
}

func writePerfScope(b *strings.Builder, scope []string) {
	fmt.Fprintf(b, "## Scope\n\n")
	if len(scope) == 0 {
		fmt.Fprintln(b, "1. unknown")
		fmt.Fprintln(b)
		return
	}
	for i, item := range scope {
		fmt.Fprintf(b, "%d. %s\n", i+1, markdownText(item))
	}
	fmt.Fprintln(b)
}

func writePerfEnvironment(b *strings.Builder, env PerfEnvironment) {
	fmt.Fprintf(b, "## Environment\n\n")
	rows := []struct {
		name  string
		value string
	}{
		{"Host", env.Host},
		{"OS", env.OS},
		{"Kernel", env.Kernel},
		{"Architecture", env.Architecture},
		{"Product version", env.ProductVersion},
		{"Server endpoint", env.ServerEndpoint},
	}
	writeKeyValueTable(b, rows)
}

func writePerfInfrastructure(b *strings.Builder, spec InfrastructureSpec) {
	fmt.Fprintf(b, "## Infrastructure Specification\n\n")
	rows := []struct {
		name  string
		value string
	}{
		{"Provider", spec.Provider},
		{"Instance type", spec.InstanceType},
		{"vCPU", spec.VCPU},
		{"CPU model", spec.CPUModel},
		{"Memory", spec.Memory},
		{"Storage type", spec.StorageType},
		{"Storage size", spec.StorageSize},
		{"Storage IOPS", spec.StorageIOPS},
		{"Storage throughput", spec.StorageThroughput},
		{"Storage encrypted", spec.StorageEncrypted},
	}
	writeKeyValueTable(b, rows)
}

func writePerfResults(b *strings.Builder, scenarios []PerfScenario) {
	fmt.Fprintf(b, "## Results\n\n")
	fmt.Fprintln(b, "| Case | Workload | Load | Success | QPS | Throughput | Avg | p50 | p95 | p99 | Gate |")
	fmt.Fprintln(b, "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, s := range scenarios {
		success := fmt.Sprintf("%d/%d", s.Successful, s.Attempted)
		fmt.Fprintf(
			b,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			tableText(s.CaseID),
			tableText(s.Workload),
			tableText(s.Load),
			tableText(success),
			tableText(formatScenarioNumber(s, s.QPS, "ops/s")),
			tableText(formatThroughput(s)),
			tableText(formatLatency(s, s.LatencyAvg)),
			tableText(formatLatency(s, s.LatencyP50)),
			tableText(formatLatency(s, s.LatencyP95)),
			tableText(formatLatency(s, s.LatencyP99)),
			tableText(s.GateStatus),
		)
	}
	if len(scenarios) == 0 {
		fmt.Fprintln(b, "| N/A | N/A | N/A | 0/0 | N/A | N/A | N/A | N/A | N/A | N/A | non_gating |")
	}
	fmt.Fprintln(b)
}

func writePerfMetricDefinitions(b *strings.Builder, notes []string) {
	fmt.Fprintf(b, "## Metric Definitions\n\n")
	for i, note := range defaultMetricNotes() {
		fmt.Fprintf(b, "%d. %s\n", i+1, markdownText(note))
	}
	offset := len(defaultMetricNotes())
	seen := map[string]bool{}
	for _, note := range defaultMetricNotes() {
		seen[note] = true
	}
	for _, note := range notes {
		if seen[note] {
			continue
		}
		offset++
		fmt.Fprintf(b, "%d. %s\n", offset, markdownText(note))
	}
	fmt.Fprintln(b)
}

func writePerfObservations(b *strings.Builder, observations []string) {
	fmt.Fprintf(b, "## Observations\n\n")
	if len(observations) == 0 {
		observations = []string{"No blocking performance gate failures were recorded."}
	}
	for i, item := range observations {
		fmt.Fprintf(b, "%d. %s\n", i+1, markdownText(item))
	}
	fmt.Fprintln(b)
}

func writePerfArtifacts(b *strings.Builder, artifacts map[string]string) {
	fmt.Fprintf(b, "## Artifacts\n\n")
	keys := make([]string, 0, len(artifacts))
	for key := range artifacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		fmt.Fprintln(b, "1. unknown")
		fmt.Fprintln(b)
		return
	}
	for i, key := range keys {
		fmt.Fprintf(b, "%d. `%s`: `%s`\n", i+1, markdownText(key), markdownText(artifacts[key]))
	}
	fmt.Fprintln(b)
}

func writeKeyValueTable(b *strings.Builder, rows []struct {
	name  string
	value string
}) {
	fmt.Fprintln(b, "| Field | Value |")
	fmt.Fprintln(b, "|---|---|")
	for _, row := range rows {
		fmt.Fprintf(b, "| %s | %s |\n", tableText(row.name), tableText(firstNonEmpty(row.value, unknown())))
	}
	fmt.Fprintln(b)
}

func formatScenarioNumber(s PerfScenario, value float64, unit string) string {
	if s.Successful == 0 {
		return "N/A"
	}
	if value == 0 {
		return "0 " + unit
	}
	return fmt.Sprintf("%.2f %s", value, unit)
}

func formatThroughput(s PerfScenario) string {
	if s.Successful == 0 {
		return "N/A"
	}
	value := s.Throughput
	switch {
	case value >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GiB/s", value/(1024*1024*1024))
	case value >= 1024*1024:
		return fmt.Sprintf("%.2f MiB/s", value/(1024*1024))
	case value >= 1024:
		return fmt.Sprintf("%.2f KiB/s", value/1024)
	default:
		return fmt.Sprintf("%.2f B/s", value)
	}
}

func formatLatency(s PerfScenario, value float64) string {
	if s.Successful == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.2f ms", value)
}

func markdownText(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

func tableText(s string) string {
	s = markdownText(firstNonEmpty(s, unknown()))
	s = strings.ReplaceAll(s, "|", "\\|")
	return s
}
