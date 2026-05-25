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
	writePerfProductBuild(&b, r.Environment.ProductVersion)
	writePerfInfrastructure(&b, r.Infrastructure)
	writePerfWorkloadDesign(&b, r.Scenarios)
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

func writePerfProductBuild(b *strings.Builder, productVersion string) {
	productVersion = strings.TrimSpace(productVersion)
	if productVersion == "" || productVersion == unknown() {
		return
	}
	fmt.Fprintf(b, "## Drive9 Binary Version\n\n")
	fmt.Fprintln(b, "The workload was executed with the Drive9 CLI binary under test. The raw version command output was:")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "```text")
	fmt.Fprintln(b, productVersion)
	fmt.Fprintln(b, "```")
	fmt.Fprintln(b)
	if strings.Contains(productVersion, "unknown") || strings.Contains(productVersion, "version: dev") {
		fmt.Fprintln(b, "The `unknown` fields mean the binary was built without release build metadata embedded by Go ldflags. Treat this as a development build unless `version`, `git_hash`, `git_branch`, and `build_time` are populated.")
		fmt.Fprintln(b)
	}
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

func writePerfWorkloadDesign(b *strings.Builder, scenarios []PerfScenario) {
	modes := map[string]bool{}
	for _, s := range scenarios {
		switch s.Workload {
		case "upload_warm", "download_file", "download_null", "download_fsync", "upload_cold", "upload_stdin":
			modes[s.Workload] = true
		}
	}
	if len(modes) == 0 {
		return
	}
	fmt.Fprintf(b, "## Test Case Design\n\n")
	fmt.Fprintln(b, "Each result row is one fixed scenario: workload mode, file size, and concurrency. The harness starts one `drive9 fs cp` process per transfer and records one raw result row per transfer.")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "### Workload Modes")
	fmt.Fprintln(b)
	item := 1
	writeMode := func(mode, text string) {
		if !modes[mode] {
			return
		}
		fmt.Fprintf(b, "%d. %s\n", item, text)
		item++
	}
	writeMode("upload_warm", "`upload_warm`: uploads a deterministic local file to Drive9 with `drive9 fs cp local.bin :/remote/path.bin`. The local source file is generated once and reused, so the local read path is expected to be warm in the OS page cache. This mode primarily covers client upload, network send, server ingest/write, and metadata finalization.")
	writeMode("download_file", "`download_file`: downloads a Drive9 file to a local file with `drive9 fs cp :/remote/path.bin local.bin`. This mode covers Drive9 read, network receive, client write path, local filesystem writes, page-cache writeback, and local block device or EBS pressure.")
	writeMode("download_null", "`download_null`: downloads a Drive9 file to stdout and discards it with `drive9 fs cp :/remote/path.bin - > /dev/null`. This keeps Drive9 read and network receive work, but removes local destination-file writes. It is the control case for separating server/network throughput from local output-file cost.")
	writeMode("download_fsync", "`download_fsync`: downloads a Drive9 file to a local file and then explicitly fsyncs it. This checks whether durable final flush is the added bottleneck compared with ordinary `download_file`.")
	writeMode("upload_cold", "`upload_cold`: uploads a local file after attempting to drop host caches when allowed. This is intended to expose cold local-read cost before upload.")
	writeMode("upload_stdin", "`upload_stdin`: streams local bytes through stdin into `drive9 fs cp - :/remote/path.bin`. This checks upload behavior when the CLI reads from a stream instead of opening a normal source file.")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "### Scenario Matrix")
	fmt.Fprintln(b)
	matrix := cpPerfMatrixFromScenarios(scenarios)
	if len(matrix.sizesMiB) > 0 {
		fmt.Fprintf(b, "This phase runs %s files, concurrency %s, with %s.", formatMiBList(matrix.sizesMiB), formatIntList(matrix.concurrency), formatCodeList(matrix.modes))
		if modes["download_file"] && modes["download_null"] {
			fmt.Fprint(b, " The important comparison is `download_file` versus `download_null`: if `download_null` keeps scaling while `download_file` plateaus, the first bottleneck is on the local output-file path rather than Drive9 read or network receive.")
		} else if modes["download_file"] && modes["download_fsync"] {
			fmt.Fprint(b, " It compares ordinary local-file download with local-file download plus explicit fsync. If `download_fsync` is materially worse than `download_file`, durable final flush is a likely bottleneck.")
		}
		fmt.Fprintln(b)
	} else {
		fmt.Fprintln(b, "Use this section to map each workload name in the results table to the concrete `drive9 fs cp` operation that was executed.")
	}
	fmt.Fprintln(b)
}

type cpPerfMatrixSummary struct {
	sizesMiB    []int
	concurrency []int
	modes       []string
}

func cpPerfMatrixFromScenarios(scenarios []PerfScenario) cpPerfMatrixSummary {
	sizeSet := map[int]bool{}
	concurrencySet := map[int]bool{}
	modeSet := map[string]bool{}
	for _, scenario := range scenarios {
		parts, ok := parseCPPerfScenarioID(scenario.ScenarioID)
		if !ok {
			continue
		}
		sizeSet[parts.SizeMiB] = true
		concurrencySet[parts.Concurrency] = true
		modeSet[parts.Mode] = true
	}
	out := cpPerfMatrixSummary{}
	for size := range sizeSet {
		out.sizesMiB = append(out.sizesMiB, size)
	}
	for concurrency := range concurrencySet {
		out.concurrency = append(out.concurrency, concurrency)
	}
	for mode := range modeSet {
		out.modes = append(out.modes, mode)
	}
	sort.Ints(out.sizesMiB)
	sort.Ints(out.concurrency)
	sort.Slice(out.modes, func(i, j int) bool {
		left, right := cpPerfModeSort(out.modes[i]), cpPerfModeSort(out.modes[j])
		if left != right {
			return left < right
		}
		return out.modes[i] < out.modes[j]
	})
	return out
}

func formatMiBList(values []int) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%d MiB", v))
	}
	return formatEnglishList(parts)
}

func formatIntList(values []int) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, fmt.Sprint(v))
	}
	return formatEnglishList(parts)
}

func formatCodeList(values []string) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, "`"+v+"`")
	}
	return formatEnglishList(parts)
}

func formatEnglishList(values []string) string {
	switch len(values) {
	case 0:
		return "unknown"
	case 1:
		return values[0]
	case 2:
		return values[0] + " and " + values[1]
	default:
		return strings.Join(values[:len(values)-1], ", ") + ", and " + values[len(values)-1]
	}
}

func writePerfResults(b *strings.Builder, scenarios []PerfScenario) {
	fmt.Fprintf(b, "## Results\n\n")
	fmt.Fprintln(b, "| Case | Workload | Load | Success | QPS | Throughput | Min | Avg | p50 | p95 | p99 | Max | Gate |")
	fmt.Fprintln(b, "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, s := range scenarios {
		success := fmt.Sprintf("%d/%d", s.Successful, s.Attempted)
		fmt.Fprintf(
			b,
			"| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			tableText(s.CaseID),
			tableText(s.Workload),
			tableText(s.Load),
			tableText(success),
			tableText(formatScenarioNumber(s, s.QPS, "ops/s")),
			tableText(formatThroughput(s)),
			tableText(formatLatency(s, s.LatencyMin)),
			tableText(formatLatency(s, s.LatencyAvg)),
			tableText(formatLatency(s, s.LatencyP50)),
			tableText(formatLatency(s, s.LatencyP95)),
			tableText(formatLatency(s, s.LatencyP99)),
			tableText(formatLatency(s, s.LatencyMax)),
			tableText(s.GateStatus),
		)
	}
	if len(scenarios) == 0 {
		fmt.Fprintln(b, "| N/A | N/A | N/A | 0/0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | non_gating |")
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
