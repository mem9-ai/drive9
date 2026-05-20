package report

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	PerfEnvironmentSchemaVersion = "perf-environment.v1"
	PerfResultSchemaVersion      = "perf-result.v1"
	PerfSummarySchemaVersion     = "perf-summary.v1"
)

type PerfOptions struct {
	Title  string
	Output string
}

type PerfEnvironment struct {
	SchemaVersion     string `json:"schema_version"`
	RunID             string `json:"run_id"`
	Host              string `json:"host"`
	OS                string `json:"os"`
	Kernel            string `json:"kernel"`
	Architecture      string `json:"architecture"`
	ProductVersion    string `json:"product_version"`
	ServerEndpoint    string `json:"server_endpoint"`
	CloudProvider     string `json:"cloud_provider"`
	InstanceType      string `json:"instance_type"`
	VCPU              string `json:"vcpu"`
	CPUModel          string `json:"cpu_model"`
	Memory            string `json:"memory"`
	StorageType       string `json:"storage_type"`
	StorageSize       string `json:"storage_size"`
	StorageIOPS       string `json:"storage_iops"`
	StorageThroughput string `json:"storage_throughput"`
	StorageEncrypted  string `json:"storage_encrypted"`
}

type PerfResult struct {
	SchemaVersion string   `json:"schema_version"`
	RunID         string   `json:"run_id"`
	CaseID        string   `json:"case_id"`
	ScenarioID    string   `json:"scenario_id,omitempty"`
	OperationID   string   `json:"operation_id"`
	Operation     string   `json:"operation"`
	Status        string   `json:"status"`
	StartedAt     string   `json:"started_at"`
	EndedAt       string   `json:"ended_at"`
	DurationMS    float64  `json:"duration_ms"`
	Bytes         int64    `json:"bytes"`
	RequestUnits  float64  `json:"request_units"`
	LocalPath     string   `json:"local_path,omitempty"`
	RemotePath    string   `json:"remote_path,omitempty"`
	ErrorClass    string   `json:"error_class"`
	Error         string   `json:"error,omitempty"`
	ArtifactRefs  []string `json:"artifact_refs"`
}

type PerfReport struct {
	SchemaVersion   string             `json:"schema_version"`
	RunID           string             `json:"run_id"`
	Title           string             `json:"title"`
	StartedAt       string             `json:"started_at"`
	EndedAt         string             `json:"ended_at"`
	OverallStatus   string             `json:"overall_status"`
	Scope           []string           `json:"scope"`
	Environment     PerfEnvironment    `json:"environment"`
	Infrastructure  InfrastructureSpec `json:"infrastructure"`
	Scenarios       []PerfScenario     `json:"scenarios"`
	Observations    []string           `json:"observations"`
	Artifacts       map[string]string  `json:"artifacts"`
	Conclusion      string             `json:"conclusion"`
	MetricNotes     []string           `json:"metric_notes"`
	GeneratorSchema string             `json:"generator_schema"`
}

type InfrastructureSpec struct {
	Provider          string `json:"provider"`
	InstanceType      string `json:"instance_type"`
	Architecture      string `json:"architecture"`
	VCPU              string `json:"vcpu"`
	CPUModel          string `json:"cpu_model"`
	Memory            string `json:"memory"`
	StorageType       string `json:"storage_type"`
	StorageSize       string `json:"storage_size"`
	StorageIOPS       string `json:"storage_iops"`
	StorageThroughput string `json:"storage_throughput"`
	StorageEncrypted  string `json:"storage_encrypted"`
}

type PerfScenario struct {
	CaseID       string   `json:"case_id"`
	Workload     string   `json:"workload"`
	Load         string   `json:"load"`
	Attempted    int      `json:"attempted"`
	Successful   int      `json:"successful"`
	Failed       int      `json:"failed"`
	QPS          float64  `json:"qps"`
	Throughput   float64  `json:"throughput"`
	LatencyAvg   float64  `json:"latency_avg"`
	LatencyP50   float64  `json:"latency_p50"`
	LatencyP95   float64  `json:"latency_p95"`
	LatencyP99   float64  `json:"latency_p99"`
	GateStatus   string   `json:"gate_status"`
	MetricNotes  []string `json:"metric_notes"`
	FailureClass string   `json:"failure_class,omitempty"`
	Failure      string   `json:"failure,omitempty"`
}

func DefaultPerfEnvironment(runID string, manifest Manifest) PerfEnvironment {
	env := PerfEnvironment{
		SchemaVersion:     PerfEnvironmentSchemaVersion,
		RunID:             firstNonEmpty(runID, manifest.RunID),
		Host:              firstNonEmpty(manifest.Host, unknown()),
		OS:                runtime.GOOS,
		Kernel:            unknown(),
		Architecture:      runtime.GOARCH,
		ProductVersion:    firstNonEmpty(manifest.Drive9Version, unknown()),
		ServerEndpoint:    firstNonEmpty(manifest.Server, unknown()),
		CloudProvider:     unknown(),
		InstanceType:      unknown(),
		VCPU:              unknown(),
		CPUModel:          unknown(),
		Memory:            unknown(),
		StorageType:       unknown(),
		StorageSize:       unknown(),
		StorageIOPS:       unknown(),
		StorageThroughput: unknown(),
		StorageEncrypted:  unknown(),
	}
	env.applyOptionalEnv()
	return env
}

func (r *Recorder) WritePerfEnvironment(env PerfEnvironment) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	env = normalizePerfEnvironment(env)
	path := filepath.Join(r.RunDir, "perf", "environment.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeJSON(path, env)
}

func (r *Recorder) PerfResult(result PerfResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	result.SchemaVersion = PerfResultSchemaVersion
	result.RunID = firstNonEmpty(result.RunID, r.RunID)
	result.Status = firstNonEmpty(result.Status, "ok")
	result.ErrorClass = firstNonEmpty(result.ErrorClass, "")
	if result.ArtifactRefs == nil {
		result.ArtifactRefs = []string{}
	}
	path := filepath.Join(r.RunDir, "perf", "results.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return appendJSONL(path, result)
}

func GeneratePerfReport(runDir string, opts PerfOptions) (PerfReport, error) {
	manifest := Manifest{}
	if err := readJSON(filepath.Join(runDir, "manifest.json"), &manifest); err != nil {
		return PerfReport{}, err
	}
	summary := Summary{}
	if err := readJSON(filepath.Join(runDir, "summary.json"), &summary); err != nil {
		return PerfReport{}, err
	}
	gating := Gating{}
	if err := readJSON(filepath.Join(runDir, "gating.json"), &gating); err != nil {
		return PerfReport{}, err
	}

	perfDir := filepath.Join(runDir, "perf")
	if err := os.MkdirAll(perfDir, 0o755); err != nil {
		return PerfReport{}, err
	}
	reportPath := filepath.Join(perfDir, "summary.json")
	report := PerfReport{}
	if err := readJSON(reportPath, &report); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return PerfReport{}, err
		}
		env, err := readPerfEnvironment(filepath.Join(perfDir, "environment.json"), manifest)
		if err != nil {
			return PerfReport{}, err
		}
		results, err := readPerfResults(filepath.Join(perfDir, "results.jsonl"))
		if err != nil {
			return PerfReport{}, err
		}
		failures, err := readFailures(filepath.Join(runDir, "failures.jsonl"))
		if err != nil {
			return PerfReport{}, err
		}
		report = buildPerfReport(manifest, summary, gating, env, results, failures)
	}
	report = normalizePerfReport(report, manifest, summary, gating, opts)
	if err := writeJSON(reportPath, report); err != nil {
		return PerfReport{}, err
	}
	output := opts.Output
	if output == "" {
		output = filepath.Join(perfDir, "customer-report.md")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return PerfReport{}, err
	}
	if err := os.WriteFile(output, []byte(RenderPerfMarkdown(report)), 0o644); err != nil {
		return PerfReport{}, err
	}
	return report, nil
}

func readPerfEnvironment(path string, manifest Manifest) (PerfEnvironment, error) {
	env := PerfEnvironment{}
	if err := readJSON(path, &env); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DefaultPerfEnvironment(manifest.RunID, manifest), nil
		}
		return PerfEnvironment{}, err
	}
	return normalizePerfEnvironment(env), nil
}

func readPerfResults(path string) ([]PerfResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []PerfResult
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, jsonlInitialBuffer), jsonlMaxTokenSize)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var result PerfResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			return nil, err
		}
		out = append(out, normalizePerfResult(result))
	}
	return out, scanner.Err()
}

func buildPerfReport(manifest Manifest, summary Summary, gating Gating, env PerfEnvironment, results []PerfResult, failures []Failure) PerfReport {
	scenarios := rollupPerfScenarios(results, summary, failures)
	sort.Slice(scenarios, func(i, j int) bool {
		if scenarios[i].CaseID == scenarios[j].CaseID {
			return scenarios[i].Workload < scenarios[j].Workload
		}
		return scenarios[i].CaseID < scenarios[j].CaseID
	})
	report := PerfReport{
		SchemaVersion:   PerfSummarySchemaVersion,
		RunID:           manifest.RunID,
		Title:           "Drive9 Performance Test Report",
		StartedAt:       summary.StartedAt,
		EndedAt:         summary.EndedAt,
		OverallStatus:   overallPerfStatus(summary, gating),
		Scope:           defaultScope(manifest),
		Environment:     env,
		Infrastructure:  infrastructureFromEnvironment(env),
		Scenarios:       scenarios,
		Observations:    observationsFromScenarios(scenarios),
		Artifacts:       defaultPerfArtifacts(),
		Conclusion:      defaultConclusion(overallPerfStatus(summary, gating)),
		MetricNotes:     defaultMetricNotes(),
		GeneratorSchema: PerfSummarySchemaVersion,
	}
	return report
}

func rollupPerfScenarios(results []PerfResult, summary Summary, failures []Failure) []PerfScenario {
	caseStatus := map[string]string{}
	for _, c := range summary.Cases {
		caseStatus[c.ID] = c.Status
	}
	failureByCase := map[string]Failure{}
	for _, f := range failures {
		if _, ok := failureByCase[f.CaseID]; !ok {
			failureByCase[f.CaseID] = f
		}
	}
	groups := map[string][]PerfResult{}
	for _, r := range results {
		scenarioID := firstNonEmpty(r.ScenarioID, r.CaseID)
		key := r.CaseID + "\x00" + scenarioID
		groups[key] = append(groups[key], r)
	}
	out := make([]PerfScenario, 0, len(groups))
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		first := group[0]
		var successful int
		var failed int
		var bytes int64
		var requestUnits float64
		var latencies []float64
		var startedAt, endedAt time.Time
		var firstErrClass, firstErr string
		operations := map[string]bool{}
		for _, r := range group {
			operations[r.Operation] = true
			started := parsePerfTime(r.StartedAt)
			ended := parsePerfTime(r.EndedAt)
			if !started.IsZero() && (startedAt.IsZero() || started.Before(startedAt)) {
				startedAt = started
			}
			if !ended.IsZero() && (endedAt.IsZero() || ended.After(endedAt)) {
				endedAt = ended
			}
			if r.Status == "ok" {
				successful++
				bytes += r.Bytes
				if r.RequestUnits > 0 {
					requestUnits += r.RequestUnits
				} else {
					requestUnits++
				}
				if r.DurationMS >= 0 {
					latencies = append(latencies, r.DurationMS)
				}
				continue
			}
			if r.Status != "skipped" {
				failed++
			}
			if firstErr == "" && r.Error != "" {
				firstErrClass = r.ErrorClass
				firstErr = boundedString(r.Error, 240)
			}
		}
		durationSeconds := endedAt.Sub(startedAt).Seconds()
		if durationSeconds <= 0 {
			durationSeconds = sumDurationSeconds(group)
		}
		sort.Float64s(latencies)
		scenario := PerfScenario{
			CaseID:      first.CaseID,
			Workload:    joinOperations(operations),
			Load:        fmt.Sprintf("%d operations", len(group)),
			Attempted:   len(group),
			Successful:  successful,
			Failed:      failed,
			GateStatus:  gateStatusForCase(first.CaseID, caseStatus, failureByCase),
			MetricNotes: []string{"QPS uses request_units when present; otherwise each successful operation counts as one request unit."},
		}
		if successful > 0 && durationSeconds > 0 {
			scenario.QPS = requestUnits / durationSeconds
			scenario.Throughput = float64(bytes) / durationSeconds
			scenario.LatencyAvg = average(latencies)
			scenario.LatencyP50 = percentile(latencies, 0.50)
			scenario.LatencyP95 = percentile(latencies, 0.95)
			scenario.LatencyP99 = percentile(latencies, 0.99)
		}
		if firstErr != "" {
			scenario.FailureClass = firstNonEmpty(firstErrClass, "unknown")
			scenario.Failure = firstErr
		} else if f, ok := failureByCase[first.CaseID]; ok {
			scenario.FailureClass = firstNonEmpty(f.Class, "unknown")
			scenario.Failure = boundedString(f.Message, 240)
		}
		out = append(out, scenario)
	}
	return out
}

func normalizePerfReport(r PerfReport, manifest Manifest, summary Summary, gating Gating, opts PerfOptions) PerfReport {
	if r.SchemaVersion == "" {
		r.SchemaVersion = PerfSummarySchemaVersion
	}
	r.RunID = firstNonEmpty(r.RunID, manifest.RunID)
	r.Title = firstNonEmpty(opts.Title, r.Title, "Drive9 Performance Test Report")
	r.StartedAt = firstNonEmpty(r.StartedAt, summary.StartedAt)
	r.EndedAt = firstNonEmpty(r.EndedAt, summary.EndedAt)
	r.OverallStatus = firstNonEmpty(r.OverallStatus, overallPerfStatus(summary, gating))
	r.Environment = normalizePerfEnvironment(r.Environment)
	if r.Environment.RunID == unknown() || r.Environment.RunID == "" {
		r.Environment.RunID = r.RunID
	}
	if r.Environment.ProductVersion == unknown() && manifest.Drive9Version != "" {
		r.Environment.ProductVersion = manifest.Drive9Version
	}
	if r.Environment.ServerEndpoint == unknown() && manifest.Server != "" {
		r.Environment.ServerEndpoint = manifest.Server
	}
	r.Infrastructure = normalizeInfrastructure(r.Infrastructure, r.Environment)
	if len(r.Scope) == 0 {
		r.Scope = defaultScope(manifest)
	}
	if r.Artifacts == nil {
		r.Artifacts = defaultPerfArtifacts()
	}
	if len(r.MetricNotes) == 0 {
		r.MetricNotes = defaultMetricNotes()
	}
	if r.Conclusion == "" {
		r.Conclusion = defaultConclusion(r.OverallStatus)
	}
	if r.GeneratorSchema == "" {
		r.GeneratorSchema = PerfSummarySchemaVersion
	}
	sort.Slice(r.Scenarios, func(i, j int) bool {
		if r.Scenarios[i].CaseID == r.Scenarios[j].CaseID {
			return r.Scenarios[i].Workload < r.Scenarios[j].Workload
		}
		return r.Scenarios[i].CaseID < r.Scenarios[j].CaseID
	})
	for i := range r.Scenarios {
		if r.Scenarios[i].MetricNotes == nil {
			r.Scenarios[i].MetricNotes = []string{}
		}
	}
	if len(r.Observations) == 0 {
		r.Observations = observationsFromScenarios(r.Scenarios)
	}
	return r
}

func normalizePerfEnvironment(env PerfEnvironment) PerfEnvironment {
	env.SchemaVersion = firstNonEmpty(env.SchemaVersion, PerfEnvironmentSchemaVersion)
	env.RunID = firstNonEmpty(env.RunID, unknown())
	env.Host = firstNonEmpty(env.Host, unknown())
	env.OS = firstNonEmpty(env.OS, unknown())
	env.Kernel = firstNonEmpty(env.Kernel, unknown())
	env.Architecture = firstNonEmpty(env.Architecture, unknown())
	env.ProductVersion = firstNonEmpty(env.ProductVersion, unknown())
	env.ServerEndpoint = firstNonEmpty(env.ServerEndpoint, unknown())
	env.CloudProvider = firstNonEmpty(env.CloudProvider, unknown())
	env.InstanceType = firstNonEmpty(env.InstanceType, unknown())
	env.VCPU = firstNonEmpty(env.VCPU, unknown())
	env.CPUModel = firstNonEmpty(env.CPUModel, unknown())
	env.Memory = firstNonEmpty(env.Memory, unknown())
	env.StorageType = firstNonEmpty(env.StorageType, unknown())
	env.StorageSize = firstNonEmpty(env.StorageSize, unknown())
	env.StorageIOPS = firstNonEmpty(env.StorageIOPS, unknown())
	env.StorageThroughput = firstNonEmpty(env.StorageThroughput, unknown())
	env.StorageEncrypted = firstNonEmpty(env.StorageEncrypted, unknown())
	env.applyOptionalEnv()
	return env
}

func (env *PerfEnvironment) applyOptionalEnv() {
	env.CloudProvider = envOverride("DRIVE9_PERF_CLOUD_PROVIDER", env.CloudProvider)
	env.InstanceType = envOverride("DRIVE9_PERF_INSTANCE_TYPE", env.InstanceType)
	env.VCPU = envOverride("DRIVE9_PERF_VCPU", env.VCPU)
	env.CPUModel = envOverride("DRIVE9_PERF_CPU_MODEL", env.CPUModel)
	env.Memory = envOverride("DRIVE9_PERF_MEMORY", env.Memory)
	env.StorageType = envOverride("DRIVE9_PERF_STORAGE_TYPE", env.StorageType)
	env.StorageSize = envOverride("DRIVE9_PERF_STORAGE_SIZE", env.StorageSize)
	env.StorageIOPS = envOverride("DRIVE9_PERF_STORAGE_IOPS", env.StorageIOPS)
	env.StorageThroughput = envOverride("DRIVE9_PERF_STORAGE_THROUGHPUT", env.StorageThroughput)
	env.StorageEncrypted = envOverride("DRIVE9_PERF_STORAGE_ENCRYPTED", env.StorageEncrypted)
}

func normalizePerfResult(result PerfResult) PerfResult {
	result.SchemaVersion = firstNonEmpty(result.SchemaVersion, PerfResultSchemaVersion)
	result.Status = firstNonEmpty(result.Status, "ok")
	result.ErrorClass = firstNonEmpty(result.ErrorClass, "")
	if result.ScenarioID == "" {
		result.ScenarioID = result.CaseID
	}
	if result.ArtifactRefs == nil {
		result.ArtifactRefs = []string{}
	}
	return result
}

func normalizeInfrastructure(spec InfrastructureSpec, env PerfEnvironment) InfrastructureSpec {
	if spec.Provider == "" {
		spec.Provider = env.CloudProvider
	}
	if spec.InstanceType == "" {
		spec.InstanceType = env.InstanceType
	}
	if spec.Architecture == "" {
		spec.Architecture = env.Architecture
	}
	if spec.VCPU == "" {
		spec.VCPU = env.VCPU
	}
	if spec.CPUModel == "" {
		spec.CPUModel = env.CPUModel
	}
	if spec.Memory == "" {
		spec.Memory = env.Memory
	}
	if spec.StorageType == "" {
		spec.StorageType = env.StorageType
	}
	if spec.StorageSize == "" {
		spec.StorageSize = env.StorageSize
	}
	if spec.StorageIOPS == "" {
		spec.StorageIOPS = env.StorageIOPS
	}
	if spec.StorageThroughput == "" {
		spec.StorageThroughput = env.StorageThroughput
	}
	if spec.StorageEncrypted == "" {
		spec.StorageEncrypted = env.StorageEncrypted
	}
	return spec
}

func infrastructureFromEnvironment(env PerfEnvironment) InfrastructureSpec {
	return normalizeInfrastructure(InfrastructureSpec{}, env)
}

func overallPerfStatus(summary Summary, gating Gating) string {
	if summary.Status == "failed" || gating.GateStatus == "fail" || gating.GateStatus == "harness_failed" {
		return "FAIL"
	}
	if summary.Status == "inconclusive" || strings.Contains(gating.GateStatus, "non_gating") {
		return "INCONCLUSIVE"
	}
	return "PASS"
}

func gateStatusForCase(caseID string, caseStatus map[string]string, failureByCase map[string]Failure) string {
	if f, ok := failureByCase[caseID]; ok && f.Class == "harness" {
		return "harness_failed"
	}
	switch caseStatus[caseID] {
	case "passed":
		return "pass"
	case "known_bug_reproduced", "known_bug_fixed_candidate", "inconclusive":
		return "non_gating"
	case "failed":
		return "fail"
	default:
		if _, ok := failureByCase[caseID]; ok {
			return "fail"
		}
		return "non_gating"
	}
}

func defaultScope(manifest Manifest) []string {
	var scope []string
	if len(manifest.Suites) > 0 {
		scope = append(scope, "Suites: "+strings.Join(manifest.Suites, ", "))
	}
	if len(manifest.SelectedCases) > 0 {
		scope = append(scope, "Selected cases: "+strings.Join(manifest.SelectedCases, ", "))
	}
	if manifest.Server != "" {
		scope = append(scope, "Server endpoint: "+manifest.Server)
	}
	if len(scope) == 0 {
		scope = append(scope, "unknown")
	}
	return scope
}

func defaultPerfArtifacts() map[string]string {
	return map[string]string{
		"manifest":         "manifest.json",
		"events":           "events.jsonl",
		"failures":         "failures.jsonl",
		"metrics":          "metrics.jsonl",
		"harness_summary":  "summary.json",
		"gating":           "gating.json",
		"perf_environment": "perf/environment.json",
		"perf_results":     "perf/results.jsonl",
		"perf_summary":     "perf/summary.json",
		"customer_report":  "perf/customer-report.md",
	}
}

func defaultMetricNotes() []string {
	return []string{
		"QPS is successful request units divided by measured wall time.",
		"Throughput is successful bytes divided by measured wall time.",
		"Avg, p50, p95, and p99 are computed from successful operation latencies.",
		"Failed or skipped operations affect Success and Gate, but do not contribute to latency percentiles.",
	}
}

func defaultConclusion(status string) string {
	switch status {
	case "PASS":
		return "The measured workload met the defined performance gates."
	case "FAIL":
		return "One or more measured scenarios did not meet the defined performance gates."
	default:
		return "The measured workload produced an inconclusive or non-gating result."
	}
}

func observationsFromScenarios(scenarios []PerfScenario) []string {
	var out []string
	for _, s := range scenarios {
		if s.GateStatus == "pass" {
			continue
		}
		msg := fmt.Sprintf("%s gate=%s", s.CaseID, s.GateStatus)
		if s.FailureClass != "" {
			msg += " failure_class=" + s.FailureClass
		}
		if s.Failure != "" {
			msg += ": " + boundedString(s.Failure, 160)
		}
		out = append(out, msg)
	}
	if len(out) == 0 {
		out = append(out, "No blocking performance gate failures were recorded.")
	}
	return out
}

func parsePerfTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t
		}
	}
	return time.Time{}
}

func sumDurationSeconds(results []PerfResult) float64 {
	var total float64
	for _, r := range results {
		if r.Status == "ok" && r.DurationMS > 0 {
			total += r.DurationMS / 1000
		}
	}
	return total
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func joinOperations(ops map[string]bool) string {
	if len(ops) == 0 {
		return "unknown"
	}
	out := make([]string, 0, len(ops))
	for op := range ops {
		if op == "" {
			continue
		}
		out = append(out, op)
	}
	if len(out) == 0 {
		return "unknown"
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

func boundedString(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func envOverride(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func unknown() string {
	return "unknown"
}
