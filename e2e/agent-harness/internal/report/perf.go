package report

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PerfEnvironmentSchemaVersion = "perf-environment.v1"
	PerfResultSchemaVersion      = "perf-result.v1"
	PerfSummarySchemaVersion     = "perf-summary.v1"
	DefaultPerfReportTitle       = "Drive9 Performance Test Report"
)

var awsCommandOutput = func(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "aws", args...).Output()
}

type PerfOptions struct {
	Title  string
	Output string
}

func PerfMarkdownOutputPath(runDir, title, output string) string {
	if output != "" {
		return output
	}
	return filepath.Join(runDir, "perf", PerfMarkdownFilename(title))
}

func PerfMarkdownFilename(title string) string {
	slug := slugifyPerfReportTitle(firstNonEmpty(title, DefaultPerfReportTitle))
	if slug == "" {
		slug = "drive9-performance-test-report"
	}
	return slug + ".md"
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
	ScenarioID   string   `json:"scenario_id,omitempty"`
	Workload     string   `json:"workload"`
	Load         string   `json:"load"`
	Attempted    int      `json:"attempted"`
	Successful   int      `json:"successful"`
	Failed       int      `json:"failed"`
	QPS          float64  `json:"qps"`
	Throughput   float64  `json:"throughput"`
	LatencyMin   float64  `json:"latency_min"`
	LatencyAvg   float64  `json:"latency_avg"`
	LatencyP50   float64  `json:"latency_p50"`
	LatencyP95   float64  `json:"latency_p95"`
	LatencyP99   float64  `json:"latency_p99"`
	LatencyMax   float64  `json:"latency_max"`
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
	env.applyHostMetadata()
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
		output = PerfMarkdownOutputPath(runDir, report.Title, "")
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
		return perfScenarioLess(scenarios[i], scenarios[j])
	})
	observations := observationsFromScenarios(scenarios)
	if cpObservations := cpPerfBottleneckObservations(scenarios); len(cpObservations) > 0 {
		if len(observations) == 1 && observations[0] == "No blocking performance gate failures were recorded." {
			observations = cpObservations
		} else {
			observations = append(observations, cpObservations...)
		}
	}
	report := PerfReport{
		SchemaVersion:   PerfSummarySchemaVersion,
		RunID:           manifest.RunID,
		Title:           DefaultPerfReportTitle,
		StartedAt:       summary.StartedAt,
		EndedAt:         summary.EndedAt,
		OverallStatus:   overallPerfStatus(summary, gating),
		Scope:           defaultScope(manifest),
		Environment:     env,
		Infrastructure:  infrastructureFromEnvironment(env),
		Scenarios:       scenarios,
		Observations:    observations,
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
		scenarioID := firstNonEmpty(first.ScenarioID, first.CaseID)
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
		workload := joinOperations(operations)
		load := fmt.Sprintf("%d operations", len(group))
		if cp, ok := parseCPPerfScenarioID(scenarioID); ok {
			workload = cp.Mode
			load = fmt.Sprintf("%d MiB, concurrency %d, %d transfers", cp.SizeMiB, cp.Concurrency, len(group))
		}
		scenario := PerfScenario{
			CaseID:      first.CaseID,
			ScenarioID:  scenarioID,
			Workload:    workload,
			Load:        load,
			Attempted:   len(group),
			Successful:  successful,
			Failed:      failed,
			GateStatus:  gateStatusForCase(first.CaseID, caseStatus, failureByCase),
			MetricNotes: []string{"QPS uses request_units when present; otherwise each successful operation counts as one request unit."},
		}
		if successful > 0 && durationSeconds > 0 {
			scenario.QPS = requestUnits / durationSeconds
			scenario.Throughput = float64(bytes) / durationSeconds
			scenario.LatencyMin = latencies[0]
			scenario.LatencyAvg = average(latencies)
			scenario.LatencyP50 = percentile(latencies, 0.50)
			scenario.LatencyP95 = percentile(latencies, 0.95)
			scenario.LatencyP99 = percentile(latencies, 0.99)
			scenario.LatencyMax = latencies[len(latencies)-1]
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
	r.Title = firstNonEmpty(opts.Title, r.Title, DefaultPerfReportTitle)
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
		r.Artifacts = defaultPerfArtifacts(r.Title)
	}
	delete(r.Artifacts, "customer_report")
	r.Artifacts["performance_report"] = filepath.ToSlash(filepath.Join("perf", PerfMarkdownFilename(r.Title)))
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
		return perfScenarioLess(r.Scenarios[i], r.Scenarios[j])
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
	env.applyHostMetadata()
	env.applyOptionalEnv()
	return env
}

func (env *PerfEnvironment) applyHostMetadata() {
	ec2Host := looksLikeEC2Host()
	if env.OS == unknown() || env.OS == runtime.GOOS {
		env.OS = firstNonEmpty(osPrettyNameFromRelease(), env.OS)
	}
	if env.Kernel == unknown() {
		env.Kernel = firstNonEmpty(kernelNameFromProc(), env.Kernel)
	} else if strings.HasPrefix(strings.ToLower(env.OS), "ubuntu") && !strings.HasPrefix(strings.ToLower(env.Kernel), "linux ") {
		env.Kernel = "Linux " + env.Kernel
	}
	if env.VCPU == unknown() {
		env.VCPU = fmt.Sprint(runtime.NumCPU())
	}
	if env.CPUModel == unknown() {
		env.CPUModel = firstNonEmpty(cpuModelFromProc(), env.CPUModel)
	}
	if env.Memory == unknown() {
		env.Memory = firstNonEmpty(memoryFromProc(), env.Memory)
	}
	if env.CloudProvider == unknown() && ec2Host {
		env.CloudProvider = "aws"
	}
	if ec2Host {
		env.applyEC2Metadata()
	}
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

func defaultPerfArtifacts(title string) map[string]string {
	return map[string]string{
		"manifest":           "manifest.json",
		"events":             "events.jsonl",
		"failures":           "failures.jsonl",
		"metrics":            "metrics.jsonl",
		"harness_summary":    "summary.json",
		"gating":             "gating.json",
		"perf_environment":   "perf/environment.json",
		"perf_results":       "perf/results.jsonl",
		"perf_summary":       "perf/summary.json",
		"perf_evidence":      "perf/evidence/",
		"performance_report": filepath.ToSlash(filepath.Join("perf", PerfMarkdownFilename(title))),
	}
}

func defaultMetricNotes() []string {
	return []string{
		"QPS is successful request units divided by measured wall time.",
		"Throughput is successful bytes divided by measured wall time.",
		"Min, avg, p50, p95, p99, and max are computed from successful operation latencies.",
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

type cpPerfScenarioParts struct {
	Mode        string
	SizeMiB     int
	Concurrency int
}

func parseCPPerfScenarioID(s string) (cpPerfScenarioParts, bool) {
	parts := strings.Split(s, "-")
	if len(parts) != 3 || !strings.HasSuffix(parts[1], "mib") || !strings.HasPrefix(parts[2], "c") {
		return cpPerfScenarioParts{}, false
	}
	size, err := strconv.Atoi(strings.TrimSuffix(parts[1], "mib"))
	if err != nil || size <= 0 {
		return cpPerfScenarioParts{}, false
	}
	concurrency, err := strconv.Atoi(strings.TrimPrefix(parts[2], "c"))
	if err != nil || concurrency <= 0 {
		return cpPerfScenarioParts{}, false
	}
	switch parts[0] {
	case "upload_warm", "download_file", "download_null", "upload_cold", "upload_stdin", "download_fsync":
		return cpPerfScenarioParts{Mode: parts[0], SizeMiB: size, Concurrency: concurrency}, true
	default:
		return cpPerfScenarioParts{}, false
	}
}

func perfScenarioLess(a, b PerfScenario) bool {
	if a.CaseID != b.CaseID {
		return a.CaseID < b.CaseID
	}
	aCP, aOK := parseCPPerfScenarioID(a.ScenarioID)
	bCP, bOK := parseCPPerfScenarioID(b.ScenarioID)
	if aOK && bOK {
		if cpPerfModeSort(aCP.Mode) != cpPerfModeSort(bCP.Mode) {
			return cpPerfModeSort(aCP.Mode) < cpPerfModeSort(bCP.Mode)
		}
		if aCP.SizeMiB != bCP.SizeMiB {
			return aCP.SizeMiB < bCP.SizeMiB
		}
		if aCP.Concurrency != bCP.Concurrency {
			return aCP.Concurrency < bCP.Concurrency
		}
		return a.ScenarioID < b.ScenarioID
	}
	if a.Workload != b.Workload {
		return a.Workload < b.Workload
	}
	return a.ScenarioID < b.ScenarioID
}

func cpPerfModeSort(mode string) int {
	switch mode {
	case "upload_warm":
		return 0
	case "download_file":
		return 1
	case "download_null":
		return 2
	case "upload_cold":
		return 3
	case "upload_stdin":
		return 4
	case "download_fsync":
		return 5
	default:
		return 99
	}
}

func cpPerfBottleneckObservations(scenarios []PerfScenario) []string {
	type keyedScenario struct {
		parts    cpPerfScenarioParts
		scenario PerfScenario
	}
	groups := map[string][]keyedScenario{}
	for _, scenario := range scenarios {
		parts, ok := parseCPPerfScenarioID(scenario.ScenarioID)
		if !ok {
			continue
		}
		key := parts.Mode + "\x00" + strconv.Itoa(parts.SizeMiB)
		groups[key] = append(groups[key], keyedScenario{parts: parts, scenario: scenario})
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []string
	for _, key := range keys {
		group := groups[key]
		sort.Slice(group, func(i, j int) bool {
			return group[i].parts.Concurrency < group[j].parts.Concurrency
		})
		if len(group) == 0 {
			continue
		}
		mode := group[0].parts.Mode
		sizeMiB := group[0].parts.SizeMiB
		peak := 0.0
		peakConcurrency := 0
		plateauAt := 0
		prevThroughput := 0.0
		for _, item := range group {
			throughput := item.scenario.Throughput
			if item.scenario.Successful == 0 || throughput <= 0 {
				continue
			}
			if throughput > peak {
				peak = throughput
				peakConcurrency = item.parts.Concurrency
			}
			if prevThroughput > 0 && plateauAt == 0 && throughput <= prevThroughput*1.10 {
				plateauAt = item.parts.Concurrency
			}
			prevThroughput = throughput
		}
		if peak == 0 {
			out = append(out, fmt.Sprintf("cp_perf mode=%s size=%dMiB classification=inconclusive: no successful transfers were recorded.", mode, sizeMiB))
			continue
		}
		if plateauAt == 0 {
			out = append(out, fmt.Sprintf("cp_perf mode=%s size=%dMiB classification=not_observed: throughput kept scaling through concurrency %d; peak %.2f MiB/s.", mode, sizeMiB, peakConcurrency, peak/(1024*1024)))
			continue
		}
		out = append(out, fmt.Sprintf("cp_perf mode=%s size=%dMiB classification=inconclusive: throughput plateaued around concurrency %d; peak %.2f MiB/s. Use perf/evidence for CPU, network, and disk proof.", mode, sizeMiB, plateauAt, peak/(1024*1024)))
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

func slugifyPerfReportTitle(title string) string {
	title = strings.ToLower(strings.TrimSpace(title))
	var b strings.Builder
	lastDash := false
	for _, r := range title {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if b.Len() > 0 && !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
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

func readTrimmedFile(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func cpuModelFromProc() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(strings.ToLower(key))
		if key == "model name" || key == "hardware" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func memoryFromProc() string {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kib, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return ""
			}
			gib := float64(kib) / 1024 / 1024
			return fmt.Sprintf("%.1f GiB", gib)
		}
	}
	return ""
}

func osPrettyNameFromRelease() string {
	b, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || key != "PRETTY_NAME" {
			continue
		}
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"`)
		return value
	}
	return ""
}

func kernelNameFromProc() string {
	release := readTrimmedFile("/proc/sys/kernel/osrelease")
	if release == "" {
		return ""
	}
	return "Linux " + release
}

func looksLikeEC2Host() bool {
	for _, p := range []string{"/sys/hypervisor/uuid", "/sys/devices/virtual/dmi/id/product_uuid"} {
		if strings.HasPrefix(strings.ToLower(readTrimmedFile(p)), "ec2") {
			return true
		}
	}
	vendor := strings.ToLower(readTrimmedFile("/sys/devices/virtual/dmi/id/sys_vendor"))
	return strings.Contains(vendor, "amazon")
}

type ec2IdentityDocument struct {
	InstanceID   string `json:"instanceId"`
	InstanceType string `json:"instanceType"`
	Region       string `json:"region"`
}

type awsDescribeInstancesResponse struct {
	Reservations []struct {
		Instances []struct {
			RootDeviceName      string `json:"RootDeviceName"`
			BlockDeviceMappings []struct {
				DeviceName string `json:"DeviceName"`
				EBS        struct {
					VolumeID string `json:"VolumeId"`
				} `json:"Ebs"`
			} `json:"BlockDeviceMappings"`
		} `json:"Instances"`
	} `json:"Reservations"`
}

type awsDescribeVolumesResponse struct {
	Volumes []awsEBSVolume `json:"Volumes"`
}

type awsEBSVolume struct {
	VolumeType string `json:"VolumeType"`
	Size       int    `json:"Size"`
	IOPS       int    `json:"Iops"`
	Throughput int    `json:"Throughput"`
	Encrypted  bool   `json:"Encrypted"`
}

func (env *PerfEnvironment) applyEC2Metadata() {
	doc, ok := ec2IdentityFromIMDS()
	if !ok {
		return
	}
	if env.InstanceType == unknown() && doc.InstanceType != "" {
		env.InstanceType = doc.InstanceType
	}
	if !env.needsStorageMetadata() {
		return
	}
	region := firstNonEmpty(envOverride("DRIVE9_PERF_AWS_REGION", ""), os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"), doc.Region)
	if region == "" || doc.InstanceID == "" {
		return
	}
	volumeID := rootVolumeIDFromAWS(region, doc.InstanceID)
	if volumeID == "" {
		return
	}
	if volume, ok := ebsVolumeFromAWS(region, volumeID); ok {
		env.applyEBSVolume(volume)
	}
}

func (env PerfEnvironment) needsStorageMetadata() bool {
	return env.StorageType == unknown() ||
		env.StorageSize == unknown() ||
		env.StorageIOPS == unknown() ||
		env.StorageThroughput == unknown() ||
		env.StorageEncrypted == unknown()
}

func (env *PerfEnvironment) applyEBSVolume(volume awsEBSVolume) {
	if env.StorageType == unknown() && volume.VolumeType != "" {
		env.StorageType = volume.VolumeType
	}
	if env.StorageSize == unknown() && volume.Size > 0 {
		env.StorageSize = fmt.Sprintf("%d GiB", volume.Size)
	}
	if env.StorageIOPS == unknown() && volume.IOPS > 0 {
		env.StorageIOPS = fmt.Sprint(volume.IOPS)
	}
	if env.StorageThroughput == unknown() && volume.Throughput > 0 {
		env.StorageThroughput = fmt.Sprintf("%d MiB/s", volume.Throughput)
	}
	if env.StorageEncrypted == unknown() {
		env.StorageEncrypted = strconv.FormatBool(volume.Encrypted)
	}
}

func ec2IdentityFromIMDS() (ec2IdentityDocument, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	token, _ := imdsToken(ctx)
	body, err := imdsGet(ctx, "http://169.254.169.254/latest/dynamic/instance-identity/document", token)
	if err != nil {
		return ec2IdentityDocument{}, false
	}
	var doc ec2IdentityDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return ec2IdentityDocument{}, false
	}
	return doc, doc.InstanceID != "" || doc.InstanceType != "" || doc.Region != ""
}

func imdsToken(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("imds token status %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func imdsGet(ctx context.Context, url, token string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("X-aws-ec2-metadata-token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("imds get status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<10))
}

func rootVolumeIDFromAWS(region, instanceID string) string {
	args := awsBaseArgs(region)
	args = append(args, "ec2", "describe-instances", "--instance-ids", instanceID, "--output", "json")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := awsCommandOutput(ctx, args...)
	if err != nil {
		return ""
	}
	var resp awsDescribeInstancesResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		return ""
	}
	return rootVolumeIDFromDescribeInstances(resp)
}

func rootVolumeIDFromDescribeInstances(resp awsDescribeInstancesResponse) string {
	for _, reservation := range resp.Reservations {
		for _, instance := range reservation.Instances {
			for _, mapping := range instance.BlockDeviceMappings {
				if mapping.EBS.VolumeID != "" && mapping.DeviceName == instance.RootDeviceName {
					return mapping.EBS.VolumeID
				}
			}
			for _, mapping := range instance.BlockDeviceMappings {
				if mapping.EBS.VolumeID != "" {
					return mapping.EBS.VolumeID
				}
			}
		}
	}
	return ""
}

func ebsVolumeFromAWS(region, volumeID string) (awsEBSVolume, bool) {
	args := awsBaseArgs(region)
	args = append(args, "ec2", "describe-volumes", "--volume-ids", volumeID, "--output", "json")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := awsCommandOutput(ctx, args...)
	if err != nil {
		return awsEBSVolume{}, false
	}
	var resp awsDescribeVolumesResponse
	if err := json.Unmarshal(out, &resp); err != nil || len(resp.Volumes) == 0 {
		return awsEBSVolume{}, false
	}
	return resp.Volumes[0], true
}

func awsBaseArgs(region string) []string {
	var args []string
	if profile := strings.TrimSpace(os.Getenv("DRIVE9_PERF_AWS_PROFILE")); profile != "" {
		args = append(args, "--profile", profile)
	}
	if region != "" {
		args = append(args, "--region", region)
	}
	return args
}
