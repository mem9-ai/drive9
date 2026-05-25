package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/casefile"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/mountproc"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/report"
)

const cpPerfRequestUnitBytes int64 = 8 << 20

type cpPerfSpec struct {
	fileSizesMiB         []int
	concurrencyLevels    []int
	modes                []string
	transfersPerScenario int
	remoteRoot           string
	artifactRoot         string
	durableFlush         bool
	collectTelemetry     bool
}

type cpPerfTransferResult struct {
	operationID  string
	operation    string
	status       string
	started      time.Time
	duration     time.Duration
	bytes        int64
	requestUnits float64
	localPath    string
	remotePath   string
	stdout       string
	stderr       string
	exitCode     int
	errText      string
}

type cpPerfScenarioStats struct {
	scenarioID      string
	successfulBytes int64
	failed          int
	duration        time.Duration
}

func runCPPerf(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, caseRemote string, c casefile.Case) error {
	spec, err := cpPerfSpecFromWorkload(rec.RunDir, caseRemote, c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(spec.artifactRoot, 0o755); err != nil {
		return err
	}
	if spec.remoteRoot != caseRemote {
		if result := runCmd(ctx, rec, c.ID, "mkdir-cp-perf-root", env, "", drive9Bin, "fs", "mkdir", ":"+spec.remoteRoot); result.ExitCode != 0 {
			return fmt.Errorf("create cp_perf remote root: %s", result.Stderr)
		}
	}

	sourceFiles := map[int]string{}
	for _, sizeMiB := range spec.fileSizesMiB {
		source := filepath.Join(spec.artifactRoot, "sources", fmt.Sprintf("source-%dmib.bin", sizeMiB))
		if err := writeCPPerfSourceFile(ctx, source, int64(sizeMiB)<<20); err != nil {
			return err
		}
		sourceFiles[sizeMiB] = source
	}

	fixtureRemotes := map[int]string{}
	if cpPerfNeedsDownloadFixture(spec.modes) {
		fixtureDir := path.Join(spec.remoteRoot, "fixtures")
		if result := runCmd(ctx, rec, c.ID, "mkdir-cp-perf-fixtures", env, "", drive9Bin, "fs", "mkdir", ":"+fixtureDir); result.ExitCode != 0 {
			return fmt.Errorf("create cp_perf fixture dir: %s", result.Stderr)
		}
		for _, sizeMiB := range spec.fileSizesMiB {
			remote := path.Join(fixtureDir, fmt.Sprintf("source-%dmib.bin", sizeMiB))
			result := runCPPerfCommand(ctx, env, drive9Bin, "", cpPerfCommandOptions{}, "fs", "cp", sourceFiles[sizeMiB], ":"+remote)
			if result.ExitCode != 0 {
				return fmt.Errorf("upload cp_perf fixture %s: %s", remote, result.Stderr)
			}
			fixtureRemotes[sizeMiB] = remote
		}
	}

	var failedScenarios []string
	for _, mode := range spec.modes {
		for _, sizeMiB := range spec.fileSizesMiB {
			for _, concurrency := range spec.concurrencyLevels {
				stats, err := runCPPerfScenario(ctx, rec, env, drive9Bin, c, spec, sourceFiles, fixtureRemotes, mode, sizeMiB, concurrency)
				if err != nil {
					return err
				}
				if stats.failed > 0 {
					failedScenarios = append(failedScenarios, fmt.Sprintf("%s failed_transfers=%d", stats.scenarioID, stats.failed))
				}
				if c.Workload.MinBytesPerSecond > 0 && stats.successfulBytes > 0 && stats.duration > 0 {
					bps := float64(stats.successfulBytes) / stats.duration.Seconds()
					if int64(bps) < c.Workload.MinBytesPerSecond {
						recordOracleFailure(rec, c, "throughput_min", int64(bps), c.Workload.MinBytesPerSecond)
					}
				}
			}
		}
	}
	if len(failedScenarios) > 0 {
		recordOracleFailure(rec, c, "cp_perf_transfer", strings.Join(failedScenarios, "; "), "all transfers exit 0")
	}
	return nil
}

func cpPerfSpecFromWorkload(runDir, caseRemote string, c casefile.Case) (cpPerfSpec, error) {
	w := c.Workload
	spec := cpPerfSpec{
		fileSizesMiB:         append([]int{}, w.FileSizesMiB...),
		concurrencyLevels:    append([]int{}, w.ConcurrencyLevels...),
		modes:                append([]string{}, w.Modes...),
		transfersPerScenario: w.TransfersPerScenario,
		remoteRoot:           caseRemote,
		artifactRoot:         filepath.Join(runDir, "perf", "cp_perf", c.ID),
		durableFlush:         w.DurableFlush,
		collectTelemetry:     true,
	}
	if len(spec.fileSizesMiB) == 0 {
		spec.fileSizesMiB = []int{50, 100, 1024}
	}
	if len(spec.concurrencyLevels) == 0 {
		spec.concurrencyLevels = []int{1, 2, 4, 8, 16, 32}
	}
	if len(spec.modes) == 0 {
		spec.modes = []string{"upload_warm", "download_file", "download_null"}
	}
	if w.RemoteRoot != "" {
		spec.remoteRoot = path.Join(caseRemote, w.RemoteRoot)
	}
	if w.ArtifactRoot != "" {
		artifactRoot, err := safeJoinLocal(runDir, w.ArtifactRoot)
		if err != nil {
			return cpPerfSpec{}, err
		}
		spec.artifactRoot = artifactRoot
	}
	if w.CollectHostTelemetry != nil {
		spec.collectTelemetry = *w.CollectHostTelemetry
	}
	return spec, nil
}

func cpPerfNeedsDownloadFixture(modes []string) bool {
	for _, mode := range modes {
		switch mode {
		case "download_file", "download_null", "download_fsync":
			return true
		}
	}
	return false
}

func runCPPerfScenario(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin string, c casefile.Case, spec cpPerfSpec, sourceFiles map[int]string, fixtureRemotes map[int]string, mode string, sizeMiB, concurrency int) (cpPerfScenarioStats, error) {
	scenarioID := cpPerfScenarioID(mode, sizeMiB, concurrency)
	evidenceDir := filepath.Join(rec.RunDir, "perf", "evidence", scenarioID)
	evidence, err := newCPPerfEvidence(rec.RunDir, evidenceDir, spec.collectTelemetry)
	if err != nil {
		return cpPerfScenarioStats{}, err
	}
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "cp_perf_scenario_start", CommandID: scenarioID, ArtifactRefs: []string{filepath.ToSlash(evidence.relDir)}})

	telemetry, err := evidence.start(ctx)
	if err != nil {
		return cpPerfScenarioStats{}, err
	}
	if mode == "upload_cold" {
		dropCPPerfCaches(ctx, evidence)
	}
	if cpPerfModeUploads(mode) {
		remoteDir := path.Join(spec.remoteRoot, "uploads", scenarioID)
		if result := runCmd(ctx, rec, c.ID, "mkdir-"+scenarioID, env, "", drive9Bin, "fs", "mkdir", ":"+remoteDir); result.ExitCode != 0 {
			telemetry.stop(context.Background())
			return cpPerfScenarioStats{}, fmt.Errorf("create cp_perf scenario dir %s: %s", remoteDir, result.Stderr)
		}
	}

	started := time.Now()
	results := runCPPerfTransfers(ctx, env, drive9Bin, evidenceDir, spec, sourceFiles, fixtureRemotes, mode, sizeMiB, concurrency)
	duration := time.Since(started)
	telemetry.stop(context.Background())
	if err := evidence.writeCommandStderr(results); err != nil {
		return cpPerfScenarioStats{}, err
	}
	if err := cleanupCPPerfScenarioDownloads(spec, mode, scenarioID, evidence); err != nil {
		return cpPerfScenarioStats{}, err
	}
	if err := cleanupCPPerfScenarioRemoteUploads(ctx, env, drive9Bin, spec, mode, scenarioID, evidence); err != nil {
		return cpPerfScenarioStats{}, err
	}
	if err := evidence.writeNotes(); err != nil {
		return cpPerfScenarioStats{}, err
	}
	artifactRefs, err := evidence.artifactRefs()
	if err != nil {
		return cpPerfScenarioStats{}, err
	}

	stats := cpPerfScenarioStats{scenarioID: scenarioID, duration: duration}
	for _, result := range results {
		if result.status == "ok" {
			stats.successfulBytes += result.bytes
		} else {
			stats.failed++
		}
		if err := rec.PerfResult(report.PerfResult{
			CaseID:       c.ID,
			ScenarioID:   scenarioID,
			OperationID:  result.operationID,
			Operation:    result.operation,
			Status:       result.status,
			StartedAt:    result.started.UTC().Format(time.RFC3339Nano),
			EndedAt:      result.started.Add(result.duration).UTC().Format(time.RFC3339Nano),
			DurationMS:   float64(result.duration.Microseconds()) / 1000,
			Bytes:        result.bytes,
			RequestUnits: result.requestUnits,
			LocalPath:    filepath.ToSlash(result.localPath),
			RemotePath:   result.remotePath,
			ErrorClass:   cpPerfErrorClass(result.status),
			Error:        boundedForPerf(result.errText),
			ArtifactRefs: artifactRefs,
		}); err != nil {
			return cpPerfScenarioStats{}, err
		}
	}
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "cp_perf_scenario_end", CommandID: scenarioID, DurationMS: duration.Milliseconds(), ArtifactRefs: artifactRefs})
	return stats, nil
}

func cleanupCPPerfScenarioDownloads(spec cpPerfSpec, mode, scenarioID string, evidence *cpPerfEvidence) error {
	switch mode {
	case "download_file", "download_fsync":
	default:
		return nil
	}
	dir := filepath.Join(spec.artifactRoot, "downloads", scenarioID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("cleanup cp_perf downloads %s: %w", dir, err)
	}
	evidence.notef("removed local download outputs after scenario: %s", filepath.ToSlash(dir))
	return nil
}

func cleanupCPPerfScenarioRemoteUploads(ctx context.Context, env mountproc.Env, drive9Bin string, spec cpPerfSpec, mode, scenarioID string, evidence *cpPerfEvidence) error {
	if !cpPerfModeUploads(mode) {
		return nil
	}
	remoteDir := path.Join(spec.remoteRoot, "uploads", scenarioID)
	result := runCPPerfCommand(ctx, env, drive9Bin, "cleanup-"+scenarioID, cpPerfCommandOptions{cliHomeRoot: evidence.dir}, "fs", "rm", "-r", ":"+remoteDir)
	if result.ExitCode != 0 {
		evidence.notef("failed to remove remote upload outputs after scenario: %s exit=%d stderr=%s", remoteDir, result.ExitCode, boundedForPerf(result.Stderr))
		return fmt.Errorf("cleanup cp_perf remote uploads %s: %s", remoteDir, boundedForPerf(result.Stderr))
	}
	evidence.notef("removed remote upload outputs after scenario: %s", remoteDir)
	return nil
}

func runCPPerfTransfers(ctx context.Context, env mountproc.Env, drive9Bin, evidenceDir string, spec cpPerfSpec, sourceFiles map[int]string, fixtureRemotes map[int]string, mode string, sizeMiB, concurrency int) []cpPerfTransferResult {
	total := spec.transfersPerScenario
	if total <= 0 {
		total = concurrency
	}
	results := make([]cpPerfTransferResult, total)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				results[idx] = runCPPerfTransfer(ctx, env, drive9Bin, evidenceDir, spec, sourceFiles[sizeMiB], fixtureRemotes[sizeMiB], mode, sizeMiB, concurrency, idx)
			}
		}()
	}
	for idx := 0; idx < total; idx++ {
		select {
		case <-ctx.Done():
			results[idx] = cpPerfCanceledTransfer(mode, sizeMiB, concurrency, idx, ctx.Err())
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()
	return results
}

func runCPPerfTransfer(ctx context.Context, env mountproc.Env, drive9Bin, evidenceDir string, spec cpPerfSpec, sourceFile, fixtureRemote, mode string, sizeMiB, concurrency, idx int) cpPerfTransferResult {
	scenarioID := cpPerfScenarioID(mode, sizeMiB, concurrency)
	operationID := fmt.Sprintf("%s-transfer-%04d", scenarioID, idx+1)
	sizeBytes := int64(sizeMiB) << 20
	result := cpPerfTransferResult{
		operationID:  operationID,
		operation:    "cp." + mode,
		started:      time.Now(),
		requestUnits: cpPerfRequestUnits(sizeBytes),
	}
	opts := cpPerfCommandOptions{cliHomeRoot: evidenceDir}
	var args []string
	switch mode {
	case "upload_warm", "upload_cold":
		remote := path.Join(spec.remoteRoot, "uploads", scenarioID, fmt.Sprintf("transfer-%04d.bin", idx+1))
		result.localPath = sourceFile
		result.remotePath = remote
		args = []string{"fs", "cp", sourceFile, ":" + remote}
	case "upload_stdin":
		remote := path.Join(spec.remoteRoot, "uploads", scenarioID, fmt.Sprintf("transfer-%04d.bin", idx+1))
		result.localPath = sourceFile
		result.remotePath = remote
		opts.stdinPath = sourceFile
		args = []string{"fs", "cp", "-", ":" + remote}
	case "download_file", "download_fsync":
		localPath := filepath.Join(spec.artifactRoot, "downloads", scenarioID, fmt.Sprintf("transfer-%04d.bin", idx+1))
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			result.status = "failed"
			result.duration = time.Since(result.started)
			result.errText = err.Error()
			return result
		}
		result.localPath = localPath
		result.remotePath = fixtureRemote
		args = []string{"fs", "cp", ":" + fixtureRemote, localPath}
	case "download_null":
		result.localPath = os.DevNull
		result.remotePath = fixtureRemote
		opts.stdoutPath = os.DevNull
		args = []string{"fs", "cp", ":" + fixtureRemote, "-"}
	default:
		result.status = "failed"
		result.duration = time.Since(result.started)
		result.errText = "unsupported cp_perf mode " + mode
		return result
	}

	cmdResult := runCPPerfCommand(ctx, env, drive9Bin, operationID, opts, args...)
	result.stdout = cmdResult.Stdout
	result.stderr = cmdResult.Stderr
	result.exitCode = cmdResult.ExitCode
	if cmdResult.ExitCode == 0 && (mode == "download_fsync" || spec.durableFlush && cpPerfModeDownloads(mode)) && result.localPath != "" && result.localPath != os.DevNull {
		if err := fsyncPath(result.localPath); err != nil {
			cmdResult.ExitCode = 1
			result.exitCode = 1
			result.stderr = strings.TrimSpace(result.stderr + "\nfsync: " + err.Error())
		}
	}
	result.duration = time.Since(result.started)
	result.status = resultStatus(cmdResult.ExitCode)
	if result.status == "ok" {
		result.bytes = sizeBytes
	} else {
		result.errText = firstNonEmptyString(result.stderr, resultErrString(cmdResult))
	}
	return result
}

func cpPerfCanceledTransfer(mode string, sizeMiB, concurrency, idx int, err error) cpPerfTransferResult {
	scenarioID := cpPerfScenarioID(mode, sizeMiB, concurrency)
	now := time.Now()
	return cpPerfTransferResult{
		operationID:  fmt.Sprintf("%s-transfer-%04d", scenarioID, idx+1),
		operation:    "cp." + mode,
		status:       "failed",
		started:      now,
		duration:     0,
		requestUnits: cpPerfRequestUnits(int64(sizeMiB) << 20),
		errText:      err.Error(),
	}
}

func cpPerfScenarioID(mode string, sizeMiB, concurrency int) string {
	return fmt.Sprintf("%s-%dmib-c%03d", mode, sizeMiB, concurrency)
}

func cpPerfRequestUnits(bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}
	return float64((bytes + cpPerfRequestUnitBytes - 1) / cpPerfRequestUnitBytes)
}

func cpPerfModeUploads(mode string) bool {
	return mode == "upload_warm" || mode == "upload_cold" || mode == "upload_stdin"
}

func cpPerfModeDownloads(mode string) bool {
	return mode == "download_file" || mode == "download_null" || mode == "download_fsync"
}

func cpPerfErrorClass(status string) string {
	if status == "ok" || status == "skipped" {
		return ""
	}
	return "product"
}

func writeCPPerfSourceFile(ctx context.Context, file string, size int64) error {
	if info, err := os.Stat(file); err == nil && info.Size() == size {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	block := make([]byte, 1<<20)
	var written int64
	for written < size {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for i := range block {
			block[i] = byte((int64(i) + written + size) % 251)
		}
		n := int64(len(block))
		if remaining := size - written; remaining < n {
			n = remaining
		}
		if _, err := f.Write(block[:n]); err != nil {
			return err
		}
		written += n
	}
	return f.Sync()
}

func fsyncPath(file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

type cpPerfCommandOptions struct {
	stdinPath   string
	stdoutPath  string
	cliHomeRoot string
}

func runCPPerfCommand(ctx context.Context, env mountproc.Env, drive9Bin, id string, opts cpPerfCommandOptions, args ...string) mountproc.Result {
	start := time.Now()
	cmd := exec.CommandContext(ctx, drive9Bin, args...)
	cmd.Env = cpPerfCommandEnv(os.Environ(), env, opts.cliHomeRoot)
	var stdinFile *os.File
	if opts.stdinPath != "" {
		f, err := os.Open(opts.stdinPath)
		if err != nil {
			return mountproc.Result{ID: id, Args: append([]string{drive9Bin}, args...), ExitCode: 1, Duration: time.Since(start), Err: err, Stderr: err.Error()}
		}
		stdinFile = f
		cmd.Stdin = f
	}
	var stdout bytes.Buffer
	var stdoutFile *os.File
	if opts.stdoutPath != "" {
		f, err := openCommandOutput(opts.stdoutPath)
		if err != nil {
			if stdinFile != nil {
				_ = stdinFile.Close()
			}
			return mountproc.Result{ID: id, Args: append([]string{drive9Bin}, args...), ExitCode: 1, Duration: time.Since(start), Err: err, Stderr: err.Error()}
		}
		stdoutFile = f
		cmd.Stdout = f
	} else {
		cmd.Stdout = &stdout
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if stdinFile != nil {
		_ = stdinFile.Close()
	}
	if stdoutFile != nil {
		_ = stdoutFile.Close()
	}
	exit := 0
	if err != nil {
		exit = 1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		}
	}
	return mountproc.Result{ID: id, Args: append([]string{drive9Bin}, args...), Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exit, Duration: time.Since(start), Err: err}
}

func openCommandOutput(path string) (*os.File, error) {
	if path == os.DevNull {
		return os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func cpPerfCommandEnv(base []string, env mountproc.Env, cliHomeRoot string) []string {
	out := append([]string{}, base...)
	if env.Server != "" {
		out = setEnvValue(out, "DRIVE9_SERVER", env.Server)
	}
	if env.APIKey != "" {
		out = setEnvValue(out, "DRIVE9_API_KEY", env.APIKey)
	}
	out = setEnvValue(out, "DRIVE9_CLI_LOG_ENABLED", "true")
	out = setEnvValue(out, "DRIVE9_CLI_LOG_LEVEL", "debug")
	out = setEnvValue(out, "DRIVE9_CLI_LOG_MAX_SIZE_MB", "100")
	if cliHomeRoot != "" {
		home := filepath.Join(cliHomeRoot, "cli-home")
		_ = os.MkdirAll(home, 0o755)
		out = setEnvValue(out, "HOME", home)
	}
	return out
}

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	for i, item := range env {
		if strings.HasPrefix(item, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

type cpPerfEvidence struct {
	runDir   string
	dir      string
	relDir   string
	enabled  bool
	notes    []string
	noteLock sync.Mutex
}

type cpPerfTelemetry struct {
	evidence *cpPerfEvidence
	cancel   context.CancelFunc
	procs    []*cpPerfTelemetryProc
}

type cpPerfTelemetryProc struct {
	name   string
	cmd    *exec.Cmd
	stdout *os.File
	stderr *os.File
}

func newCPPerfEvidence(runDir, dir string, enabled bool) (*cpPerfEvidence, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(runDir, dir)
	if err != nil {
		return nil, err
	}
	return &cpPerfEvidence{runDir: runDir, dir: dir, relDir: filepath.ToSlash(rel), enabled: enabled}, nil
}

func (e *cpPerfEvidence) start(ctx context.Context) (*cpPerfTelemetry, error) {
	t := &cpPerfTelemetry{evidence: e}
	if !e.enabled {
		e.note("host telemetry disabled by workload config")
		return t, nil
	}
	if err := e.copyMeminfo("before"); err != nil {
		e.notef("meminfo before unavailable: %v", err)
	}
	e.captureEthtool(ctx, "before")
	telemetryCtx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	for _, tool := range []struct {
		file string
		name string
		args []string
	}{
		{file: "pidstat.txt", name: "pidstat", args: []string{"-durh", "1"}},
		{file: "mpstat.txt", name: "mpstat", args: []string{"1"}},
		{file: "iostat.txt", name: "iostat", args: []string{"-xz", "1"}},
		{file: "sar-net-dev.txt", name: "sar", args: []string{"-n", "DEV", "1"}},
	} {
		proc, err := e.startTelemetryTool(telemetryCtx, tool.file, tool.name, tool.args...)
		if err != nil {
			e.note(err.Error())
			continue
		}
		t.procs = append(t.procs, proc)
	}
	return t, nil
}

func (t *cpPerfTelemetry) stop(ctx context.Context) {
	if t == nil || t.evidence == nil {
		return
	}
	if t.cancel != nil {
		t.cancel()
	}
	done := make(chan struct{})
	go func() {
		for _, proc := range t.procs {
			err := proc.cmd.Wait()
			_ = proc.stdout.Close()
			_ = proc.stderr.Close()
			if err != nil && ctx.Err() != nil {
				t.evidence.notef("%s telemetry stopped: %v", proc.name, err)
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		for _, proc := range t.procs {
			if proc.cmd.Process != nil {
				_ = proc.cmd.Process.Kill()
			}
		}
		<-done
	}
	if !t.evidence.enabled {
		return
	}
	if err := t.evidence.copyMeminfo("after"); err != nil {
		t.evidence.notef("meminfo after unavailable: %v", err)
	}
	t.evidence.captureEthtool(context.Background(), "after")
}

func (e *cpPerfEvidence) startTelemetryTool(ctx context.Context, file, name string, args ...string) (*cpPerfTelemetryProc, error) {
	if _, err := exec.LookPath(name); err != nil {
		return nil, fmt.Errorf("%s unavailable: %v", name, err)
	}
	stdout, err := os.Create(filepath.Join(e.dir, file))
	if err != nil {
		return nil, fmt.Errorf("create %s telemetry stdout: %v", name, err)
	}
	stderr, err := os.Create(filepath.Join(e.dir, file+".stderr"))
	if err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("create %s telemetry stderr: %v", name, err)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, fmt.Errorf("start %s telemetry: %v", name, err)
	}
	return &cpPerfTelemetryProc{name: name, cmd: cmd, stdout: stdout, stderr: stderr}, nil
}

func (e *cpPerfEvidence) copyMeminfo(stage string) error {
	src, err := os.Open("/proc/meminfo")
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.Create(filepath.Join(e.dir, "meminfo-"+stage+".txt"))
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()
	_, err = io.Copy(dst, src)
	return err
}

func (e *cpPerfEvidence) captureEthtool(ctx context.Context, stage string) {
	iface := getenv("DRIVE9_PERF_NETDEV", "ens5")
	e.captureFiniteCommand(ctx, "ethtool-"+stage+".txt", "ethtool", "-S", iface)
}

func (e *cpPerfEvidence) captureFiniteCommand(ctx context.Context, file, name string, args ...string) {
	if _, err := exec.LookPath(name); err != nil {
		e.notef("%s unavailable: %v", name, err)
		return
	}
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	body := stdout.String()
	if stderr.Len() > 0 {
		body += "\nSTDERR:\n" + stderr.String()
	}
	if writeErr := os.WriteFile(filepath.Join(e.dir, file), []byte(body), 0o644); writeErr != nil {
		e.notef("write %s evidence: %v", name, writeErr)
	}
	if err != nil {
		e.notef("%s %s failed: %v", name, strings.Join(args, " "), err)
	}
}

func dropCPPerfCaches(ctx context.Context, e *cpPerfEvidence) {
	if os.Getenv("DRIVE9_PERF_ALLOW_DROP_CACHES") != "1" {
		e.note("upload_cold requested but DRIVE9_PERF_ALLOW_DROP_CACHES=1 is not set; cache drop skipped")
		return
	}
	e.captureFiniteCommand(ctx, "drop-caches-sync.txt", "sync")
	if _, err := exec.LookPath("sudo"); err != nil && os.Geteuid() != 0 {
		e.notef("drop caches skipped: sudo unavailable: %v", err)
		return
	}
	cmdCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	if os.Geteuid() == 0 {
		cmd = exec.CommandContext(cmdCtx, "tee", "/proc/sys/vm/drop_caches")
	} else {
		cmd = exec.CommandContext(cmdCtx, "sudo", "-n", "tee", "/proc/sys/vm/drop_caches")
	}
	cmd.Stdin = strings.NewReader("3\n")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	body := stdout.String()
	if stderr.Len() > 0 {
		body += "\nSTDERR:\n" + stderr.String()
	}
	_ = os.WriteFile(filepath.Join(e.dir, "drop-caches.txt"), []byte(body), 0o644)
	if err != nil {
		e.notef("drop caches failed: %v", err)
	}
}

func (e *cpPerfEvidence) writeCommandStderr(results []cpPerfTransferResult) error {
	path := filepath.Join(e.dir, "command-stderr.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for _, result := range results {
		row := map[string]any{
			"operation_id": result.operationID,
			"exit_code":    result.exitCode,
			"stderr":       boundedForPerf(result.stderr),
		}
		if strings.TrimSpace(result.stdout) != "" {
			row["stdout"] = boundedForPerf(result.stdout)
		}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}

func (e *cpPerfEvidence) note(msg string) {
	e.noteLock.Lock()
	defer e.noteLock.Unlock()
	e.notes = append(e.notes, msg)
}

func (e *cpPerfEvidence) notef(format string, args ...any) {
	e.note(fmt.Sprintf(format, args...))
}

func (e *cpPerfEvidence) writeNotes() error {
	e.noteLock.Lock()
	notes := append([]string{}, e.notes...)
	e.noteLock.Unlock()
	if len(notes) == 0 {
		notes = append(notes, "all requested host telemetry collectors started")
	}
	sort.Strings(notes)
	var b strings.Builder
	for i, note := range notes {
		fmt.Fprintf(&b, "%d. %s\n", i+1, note)
	}
	return os.WriteFile(filepath.Join(e.dir, "notes.txt"), []byte(b.String()), 0o644)
}

func (e *cpPerfEvidence) artifactRefs() ([]string, error) {
	var refs []string
	if err := filepath.WalkDir(e.dir, func(local string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(e.runDir, local)
		if err != nil {
			return err
		}
		refs = append(refs, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(refs)
	return refs, nil
}

func resultErrString(r mountproc.Result) string {
	if r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
