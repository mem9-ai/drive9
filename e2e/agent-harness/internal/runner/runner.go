// Package runner executes drive9 agent harness cases.
package runner

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/casefile"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/mountproc"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/report"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/safety"
)

type Config struct {
	ArtifactRoot     string
	MountRoot        string
	RemoteRootBase   string
	Drive9Bin        string
	Server           string
	APIKey           string
	Provision        bool
	ProvisionTimeout time.Duration
	SuiteDir         string
	Suites           []string
	CaseFilter       string
	AllowFault       bool
}

type GCConfig struct {
	RunDir         string
	Drive9Bin      string
	Server         string
	APIKey         string
	SuccessfulOnly bool
	ConfirmDelete  bool
}

type ReportConfig struct {
	RunDir string
	Format string
	Title  string
	Output string
}

type EvidenceConfig struct {
	RunDir          string
	KubeContext     string
	Namespace       string
	Selector        string
	Since           string
	Tail            int
	MetricsRawPath  string
	ApproveExternal bool
}

var (
	ErrConfirmDeleteRequired = errors.New("gc requires --confirm-delete")
	ErrGateFailed            = errors.New("harness gate failed")
	ErrUnsafeMountpoint      = errors.New("unsafe mountpoint")

	harnessHTTPClient = &http.Client{Timeout: 30 * time.Second}
)

func DefaultConfig() Config {
	return Config{
		ArtifactRoot:     "/tmp",
		MountRoot:        "/tmp",
		RemoteRootBase:   "/agent-adversarial-$RUN_ID",
		Drive9Bin:        "drive9",
		Server:           getenv("DRIVE9_BASE", "http://127.0.0.1:9009"),
		APIKey:           os.Getenv("DRIVE9_API_KEY"),
		ProvisionTimeout: 120 * time.Second,
		SuiteDir:         filepath.Join("e2e", "agent-harness", "cases"),
		Suites:           []string{"smoke"},
	}
}

func Preflight(ctx context.Context, cfg Config) error {
	if _, err := resolveBin(cfg.Drive9Bin); err != nil {
		return err
	}
	if _, err := safety.ValidateRoot("artifact-root", cfg.ArtifactRoot); err != nil {
		return err
	}
	if _, err := safety.ValidateRoot("mount-root", cfg.MountRoot); err != nil {
		return err
	}
	if _, err := safety.ValidateRoot("remote-root-base", cfg.RemoteRootBase); err != nil {
		return err
	}
	if cfg.APIKey == "" && !cfg.Provision {
		return errors.New("preflight: --api-key or --provision is required")
	}
	if cfg.APIKey != "" && !cfg.Provision {
		if err := checkStatus(ctx, cfg.Server, cfg.APIKey); err != nil {
			return err
		}
	}
	return nil
}

func Run(ctx context.Context, cfg Config) (string, error) {
	id := runID()
	if cfg.RemoteRootBase == "" {
		cfg.RemoteRootBase = "/agent-adversarial-$RUN_ID"
	}
	if strings.Contains(cfg.RemoteRootBase, "$RUN_ID") {
		cfg.RemoteRootBase = strings.ReplaceAll(cfg.RemoteRootBase, "$RUN_ID", id)
	}
	artifactRoot, err := safety.ValidateRoot("artifact-root", cfg.ArtifactRoot)
	if err != nil {
		return "", err
	}
	mountRoot, err := safety.ValidateRoot("mount-root", cfg.MountRoot)
	if err != nil {
		return "", err
	}
	remoteRootBase, err := safety.ValidateRoot("remote-root-base", cfg.RemoteRootBase)
	if err != nil {
		return "", err
	}
	drive9Bin, err := resolveBin(cfg.Drive9Bin)
	if err != nil {
		return "", err
	}
	cases, err := casefile.LoadSuites(cfg.SuiteDir, cfg.Suites)
	if err != nil {
		return "", err
	}
	cases = casefile.FilterCases(cases, cfg.CaseFilter)
	if len(cases) == 0 {
		return "", errors.New("no selected cases")
	}
	if containsSuite(cases, "fault") && !cfg.AllowFault {
		return "", errors.New("fault suite requires --allow-fault")
	}
	if requiresFreshTenant(cases) && cfg.APIKey == "" && !cfg.Provision {
		return "", errors.New("selected cases require a fresh tenant; pass --provision or provide --api-key")
	}
	apiKey := cfg.APIKey
	if apiKey == "" || cfg.Provision {
		key, err := provision(ctx, cfg.Server, cfg.ProvisionTimeout)
		if err != nil {
			return "", err
		}
		apiKey = key
	}
	runDir := filepath.Join(artifactRoot, "drive9-agent-test-"+id)
	rec, err := report.NewRecorder(runDir, id)
	if err != nil {
		return "", err
	}
	_ = captureDebug(ctx, rec, "run-start")
	manifest := report.Manifest{
		RunID:                id,
		StartedAt:            now(),
		Host:                 host(),
		HarnessGitSHA:        gitSHA(),
		Drive9Version:        drive9Version(ctx, drive9Bin),
		Server:               cfg.Server,
		Suites:               cfg.Suites,
		SelectedCases:        caseIDs(cases),
		Cases:                caseSummaries(cases),
		ArtifactRoot:         artifactRoot,
		MountRoot:            mountRoot,
		RemoteRootBase:       remoteRootBase,
		GeneratedMountpoints: map[string]string{},
		GeneratedRemoteRoots: map[string]string{},
		ProcessGroups:        map[string]int{},
		APIKeyRedacted:       redact(apiKey),
		ApprovalMode:         "phase1-local",
	}
	if err := rec.WriteManifest(manifest); err != nil {
		return runDir, err
	}
	if err := rec.WritePerfEnvironment(report.DefaultPerfEnvironment(id, manifest)); err != nil {
		return runDir, err
	}
	_ = rec.Event(report.Event{Type: "run_start"})
	env := mountproc.Env{Server: cfg.Server, APIKey: apiKey}
	for _, c := range cases {
		if err := runCase(ctx, rec, &manifest, env, drive9Bin, remoteRootBase, mountRoot, id, c); err != nil {
			_ = rec.Failure(report.Failure{CaseID: c.ID, Severity: c.Severity.Failure, Class: "harness", Oracle: "runner", ExpectedOutcome: c.ExpectedOutcome, Message: err.Error()})
		}
		if err := rec.WriteManifest(manifest); err != nil {
			return runDir, err
		}
	}
	_ = captureDebug(ctx, rec, "run-end")
	_ = rec.Event(report.Event{Type: "run_end"})
	if err := rec.WriteManifest(manifest); err != nil {
		return runDir, err
	}
	_, gating, err := report.Generate(runDir)
	if err != nil {
		return runDir, err
	}
	return runDir, gateError(runDir, gating)
}

func Regenerate(runDir string) error {
	return Report(ReportConfig{RunDir: runDir, Format: "summary"})
}

func Report(cfg ReportConfig) error {
	if cfg.RunDir == "" {
		return errors.New("report requires run dir")
	}
	format := cfg.Format
	if format == "" {
		format = "summary"
	}
	if _, _, err := report.Generate(cfg.RunDir); err != nil {
		return err
	}
	switch format {
	case "summary":
		return nil
	case "customer-perf":
		_, err := report.GeneratePerfReport(cfg.RunDir, report.PerfOptions{Title: cfg.Title, Output: cfg.Output})
		return err
	default:
		return fmt.Errorf("unsupported report format %q", cfg.Format)
	}
}

func gateError(runDir string, gating report.Gating) error {
	if gating.GateStatus != "fail" && gating.GateStatus != "harness_failed" {
		return nil
	}
	return fmt.Errorf("%w: status=%s fail=%d run_dir=%s", ErrGateFailed, gating.GateStatus, gating.Fail, runDir)
}

func runCase(ctx context.Context, rec *report.Recorder, manifest *report.Manifest, env mountproc.Env, drive9Bin, remoteRootBase, mountRoot, runID string, c casefile.Case) (retErr error) {
	caseCtx, cancelCase := context.WithTimeout(ctx, c.Timeout.Duration)
	defer cancelCase()
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "case_start"})
	caseRemote, err := safety.CaseRemoteRoot(remoteRootBase, c.RemoteRootSuffix)
	if err != nil {
		return err
	}
	mountpoint, err := safety.Mountpoint(mountRoot, runID, c.MountpointSuffix)
	if err != nil {
		return err
	}
	if err := safety.ValidateMountpointAvailable(mountpoint); err != nil {
		return err
	}
	manifest.GeneratedRemoteRoots[c.ID] = caseRemote
	manifest.GeneratedMountpoints[c.ID] = mountpoint

	if c.Workload.Type == "doctor_fuse" {
		if err := os.MkdirAll(mountpoint, 0o755); err != nil {
			return err
		}
		if err := runDoctor(caseCtx, rec, env, drive9Bin, mountpoint, c); err != nil {
			return err
		}
		_ = rec.Event(report.Event{CaseID: c.ID, Type: "case_end"})
		if shouldRemoveMountpoint(c, rec, retErr) {
			_ = os.Remove(mountpoint)
		}
		return nil
	}

	if result := runCmd(caseCtx, rec, c.ID, "mkdir-remote", env, "", drive9Bin, "fs", "mkdir", ":"+caseRemote); result.ExitCode != 0 {
		return fmt.Errorf("create remote root: %s", result.Stderr)
	}
	mount, err := startCaseMount(caseCtx, rec, manifest, env, drive9Bin, caseRemote, mountpoint, c)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer stopCancel()
		if err := stopCaseMount(stopCtx, rec, env, drive9Bin, c.ID, &mount); err != nil && mount != nil {
			_ = mountproc.KillMount(mount)
		}
		if shouldRemoveMountpoint(c, rec, retErr) {
			_ = os.Remove(mountpoint)
		}
	}()
	switch c.Workload.Type {
	case "mount_smoke":
		err = runMountSmoke(caseCtx, rec, env, drive9Bin, caseRemote, mountpoint, c)
	case "path_matrix":
		err = runPathMatrix(caseCtx, rec, env, drive9Bin, caseRemote, mountpoint, c)
	case "dual_mount_visibility":
		err = runDualMountVisibility(caseCtx, rec, manifest, env, drive9Bin, caseRemote, mountRoot, runID, mountpoint, c)
	case "fio":
		err = runFIO(caseCtx, rec, mountpoint, c)
	case "small_file_storm":
		err = runSmallFileStorm(caseCtx, rec, mountpoint, c)
	case "parallel_writes":
		err = runParallelWrites(caseCtx, rec, mountpoint, c)
	case "git_workflow":
		err = runGitWorkflow(caseCtx, rec, mountpoint, c)
	case "open_fd_unmount":
		err = runOpenFDUnmount(caseCtx, rec, env, drive9Bin, mountpoint, c, &mount)
	case "kill_during_write":
		err = runKillDuringWrite(caseCtx, rec, manifest, env, drive9Bin, caseRemote, mountpoint, c, &mount)
	default:
		err = fmt.Errorf("unsupported workload %q", c.Workload.Type)
	}
	if err != nil {
		return err
	}
	if err := verifyRemount(caseCtx, rec, manifest, env, drive9Bin, caseRemote, mountpoint, c, &mount); err != nil {
		return err
	}
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "case_end"})
	return nil
}

func startCaseMount(ctx context.Context, rec *report.Recorder, manifest *report.Manifest, env mountproc.Env, drive9Bin, caseRemote, mountpoint string, c casefile.Case) (*mountproc.Mount, error) {
	mountCtx, cancel := context.WithTimeout(ctx, mountReadyTimeout(c))
	defer cancel()
	logPath := filepath.Join(rec.RunDir, "mount", c.ID+".log")
	started := time.Now()
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "mount_start", ArtifactRefs: []string{filepath.ToSlash(logPath)}})
	mount, err := mountproc.StartMount(context.Background(), c.ID, env, drive9Bin, caseRemote, mountpoint, c.SyncMode, logPath)
	if err != nil {
		return nil, err
	}
	manifest.ProcessGroups[c.ID] = mount.ProcessGroup
	if err := mountproc.WaitMounted(mountCtx, mountpoint, safety.IsMounted); err != nil {
		_ = mountproc.KillMount(mount)
		return nil, err
	}
	elapsed := time.Since(started)
	_ = rec.Event(report.Event{CaseID: c.ID, Type: "mount_ready", DurationMS: elapsed.Milliseconds(), ArtifactRefs: []string{filepath.ToSlash(logPath)}})
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "mount_startup_ms", Value: float64(elapsed.Milliseconds()), Unit: "ms", Source: "drive9 mount"})
	return mount, nil
}

func stopCaseMount(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, caseID string, mount **mountproc.Mount) error {
	if mount == nil || *mount == nil {
		return nil
	}
	mountpoint := (*mount).Mountpoint
	_ = rec.Event(report.Event{CaseID: caseID, Type: "unmount_start"})
	result := mountproc.Stop(ctx, env, drive9Bin, mountpoint)
	recordResult(rec, caseID, result)
	if result.ExitCode != 0 {
		return fmt.Errorf("unmount %s: %s", mountpoint, result.Stderr)
	}
	if err := waitUnmounted(ctx, mountpoint); err != nil {
		return err
	}
	_ = rec.Event(report.Event{CaseID: caseID, Type: "unmount_end"})
	*mount = nil
	return nil
}

func waitUnmounted(ctx context.Context, mountpoint string) error {
	return poll(ctx, func() error {
		mounted, err := safety.IsMounted(mountpoint)
		if err != nil {
			return err
		}
		if mounted {
			return fmt.Errorf("still mounted: %s", mountpoint)
		}
		return nil
	})
}

func verifyRemount(ctx context.Context, rec *report.Recorder, manifest *report.Manifest, env mountproc.Env, drive9Bin, caseRemote, mountpoint string, c casefile.Case, mount **mountproc.Mount) error {
	paths := remountPaths(c)
	if len(paths) == 0 {
		return nil
	}
	before := map[string]string{}
	for _, rel := range paths {
		local, err := safeJoinLocal(mountpoint, rel)
		if err != nil {
			return err
		}
		sum, err := hashFile(local)
		if err != nil {
			recordOracleFailure(rec, c, "remount_hash_equal", err.Error(), "pre-remount hash")
			continue
		}
		before[rel] = sum
	}
	if len(before) == 0 {
		return nil
	}
	if err := stopCaseMount(ctx, rec, env, drive9Bin, c.ID, mount); err != nil {
		return err
	}
	nextMount, err := startCaseMount(ctx, rec, manifest, env, drive9Bin, caseRemote, mountpoint, c)
	if err != nil {
		return err
	}
	*mount = nextMount
	expected := expectedRemountHashes(c)
	for rel, want := range before {
		local, err := safeJoinLocal(mountpoint, rel)
		if err != nil {
			return err
		}
		got, err := hashFile(local)
		if err != nil {
			recordOracleFailure(rec, c, "remount_hash_equal", err.Error(), want)
			continue
		}
		if got != want {
			recordOracleFailure(rec, c, "remount_hash_equal", got, want)
		}
		if expectedHash := expected[rel]; expectedHash != "" && got != expectedHash {
			recordOracleFailure(rec, c, "remount_hash_equal", got, expectedHash)
		}
	}
	return nil
}

func runMountSmoke(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, remoteRoot, mountpoint string, c casefile.Case) error {
	totalBytes := 0
	for i, f := range c.Workload.Files {
		totalBytes += len(f.Content)
		if i%2 == 0 {
			tmp := filepath.Join(rec.RunDir, "debug", c.ID+"-"+safeName(f.RelativePath))
			if err := os.MkdirAll(filepath.Dir(tmp), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(tmp, []byte(f.Content), 0o644); err != nil {
				return err
			}
			remote := path.Join(remoteRoot, f.RelativePath)
			result := runCmd(ctx, rec, c.ID, "cli-write-"+f.RelativePath, env, "", drive9Bin, "fs", "cp", tmp, ":"+remote)
			if result.ExitCode != 0 {
				recordOracleFailure(rec, c, "fuse_write_success", result.Stderr, "exit 0")
				continue
			}
			local, err := safeJoinLocal(mountpoint, f.RelativePath)
			if err != nil {
				return err
			}
			if err := waitLocalContent(ctx, c, local, f.Content); err != nil {
				recordOracleFailure(rec, c, "cli_read_equals", err.Error(), f.Content)
			}
			continue
		}
		local, err := safeJoinLocal(mountpoint, f.RelativePath)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(local, []byte(f.Content), 0o644); err != nil {
			recordOracleFailure(rec, c, "fuse_write_success", err.Error(), "write success")
			continue
		}
		remote := path.Join(remoteRoot, f.RelativePath)
		if err := waitRemoteContent(ctx, rec, env, drive9Bin, c, remote, f.Content); err != nil {
			recordOracleFailure(rec, c, "cli_read_equals", err.Error(), f.Content)
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "bytes_written", Value: float64(totalBytes), Unit: "bytes", Source: c.Workload.Type})
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "file_count", Value: float64(len(c.Workload.Files)), Unit: "files", Source: c.Workload.Type})
	return nil
}

func runPathMatrix(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, remoteRoot, mountpoint string, c casefile.Case) error {
	totalBytes := 0
	for _, p := range c.Workload.Paths {
		content := c.Workload.ContentTemplate
		if content == "" {
			content = c.ID + ":" + p
		}
		totalBytes += len(content)
		local, err := safeJoinLocal(mountpoint, p)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
			recordOracleFailure(rec, c, "fuse_write_success", err.Error(), "write success")
			continue
		}
		if err := waitRemoteContent(ctx, rec, env, drive9Bin, c, path.Join(remoteRoot, p), content); err != nil {
			recordOracleFailure(rec, c, "cli_read_equals", err.Error(), content)
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "bytes_written", Value: float64(totalBytes), Unit: "bytes", Source: c.Workload.Type})
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "file_count", Value: float64(len(c.Workload.Paths)), Unit: "files", Source: c.Workload.Type})
	return nil
}

func runDualMountVisibility(ctx context.Context, rec *report.Recorder, manifest *report.Manifest, env mountproc.Env, drive9Bin, caseRemote, mountRoot, runID, primaryMountpoint string, c casefile.Case) error {
	secondMountpoint, err := safety.Mountpoint(mountRoot, runID, c.MountpointSuffix+"-b")
	if err != nil {
		return err
	}
	if err := safety.ValidateMountpointAvailable(secondMountpoint); err != nil {
		return err
	}
	manifest.GeneratedMountpoints[c.ID+"-b"] = secondMountpoint
	secondMount, err := startCaseMount(ctx, rec, manifest, env, drive9Bin, caseRemote, secondMountpoint, c)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := stopCaseMount(stopCtx, rec, env, drive9Bin, c.ID, &secondMount); err != nil && secondMount != nil {
			_ = mountproc.KillMount(secondMount)
		}
		if shouldRemoveMountpoint(c, rec, nil) {
			_ = os.Remove(secondMountpoint)
		}
	}()
	for i, f := range c.Workload.Files {
		fromA := i%2 == 0
		src := primaryMountpoint
		dst := secondMountpoint
		if !fromA {
			src, dst = secondMountpoint, primaryMountpoint
		}
		rel := f.RelativePath
		if rel == "" {
			rel = fmt.Sprintf("dual-%d.txt", i)
		}
		content := f.Content
		if content == "" {
			content = fmt.Sprintf("%s:%d", c.ID, i)
		}
		local, err := safeJoinLocal(src, rel)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
			recordOracleFailure(rec, c, "dual_mount_visibility", err.Error(), "write success")
			continue
		}
		dstLocal, err := safeJoinLocal(dst, rel)
		if err != nil {
			return err
		}
		if err := waitLocalContent(ctx, c, dstLocal, content); err != nil {
			recordOracleFailure(rec, c, "dual_mount_visibility", err.Error(), content)
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "file_count", Value: float64(len(c.Workload.Files)), Unit: "files", Source: c.Workload.Type})
	return nil
}

func runFIO(ctx context.Context, rec *report.Recorder, mountpoint string, c casefile.Case) error {
	fio := c.Workload.FioBinary
	if fio == "" {
		fio = "fio"
	}
	if _, err := resolveBin(fio); err != nil {
		return fmt.Errorf("fio workload requires %q in PATH or workload.fio_binary: %w", fio, err)
	}
	blockBytes := c.Workload.BlockBytes
	if blockBytes <= 0 {
		blockBytes = 1 << 20
	}
	output := filepath.Join(rec.RunDir, "metrics", c.ID+"-fio.json")
	target := filepath.Join(mountpoint, "fio-seq.dat")
	args := []string{
		"--name=" + c.ID,
		"--filename=" + target,
		"--size=" + fmt.Sprint(c.Workload.SizeBytes),
		"--bs=" + fmt.Sprint(blockBytes),
		"--rw=write",
		"--ioengine=sync",
		"--fsync=1",
		"--output-format=json",
	}
	writeStarted := time.Now()
	result := mountproc.Run(ctx, "fio-write", mountproc.Env{}, "", fio, args...)
	recordResult(rec, c.ID, result)
	_ = os.WriteFile(output, []byte(result.Stdout), 0o644)
	recordPerfResult(rec, c.ID, "fio-write", "fuse_write", resultStatus(result.ExitCode), writeStarted, result.Duration, perfBytes(result.ExitCode, c.Workload.SizeBytes), 1, result.Stderr, []string{filepath.ToSlash(output)})
	if result.ExitCode != 0 {
		recordOracleFailure(rec, c, "throughput_min", result.Stderr, "fio exit 0")
		return nil
	}
	throughput := fioWriteBytesPerSecond(result.Stdout)
	if throughput > 0 {
		_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "mount_perf_counter", Value: throughput, Unit: "bytes", Source: "fio.write_bw_bytes_per_second", ArtifactRefs: []string{filepath.ToSlash(output)}})
	}
	if c.Workload.MinBytesPerSecond > 0 && int64(throughput) < c.Workload.MinBytesPerSecond {
		recordOracleFailure(rec, c, "throughput_min", int64(throughput), c.Workload.MinBytesPerSecond)
	}
	start := time.Now()
	if _, err := hashFile(target); err != nil {
		recordPerfResult(rec, c.ID, "fio-readback", "fuse_read", "failed", start, time.Since(start), 0, 1, err.Error(), nil)
		recordOracleFailure(rec, c, "cli_read_equals", err.Error(), "readable fio output")
	} else {
		elapsed := time.Since(start)
		recordPerfResult(rec, c.ID, "fio-readback", "fuse_read", "ok", start, elapsed, c.Workload.SizeBytes, 1, "", nil)
		if elapsed > 0 {
			_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "bytes_read", Value: float64(c.Workload.SizeBytes), Unit: "bytes", Source: "fio-readback"})
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "bytes_written", Value: float64(c.Workload.SizeBytes), Unit: "bytes", Source: c.Workload.Type})
	return nil
}

func runSmallFileStorm(ctx context.Context, rec *report.Recorder, mountpoint string, c casefile.Case) error {
	count := c.Workload.FileCount
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rel := filepath.Join("small-files", fmt.Sprintf("%05d.txt", i))
		content := fmt.Sprintf("%s:%05d\n", c.ID, i)
		local, err := safeJoinLocal(mountpoint, filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
			recordOracleFailure(rec, c, "fuse_write_success", err.Error(), "write success")
			continue
		}
	}
	entries, err := os.ReadDir(filepath.Join(mountpoint, "small-files"))
	if err != nil {
		recordOracleFailure(rec, c, "file_count_equals", err.Error(), count)
		return nil
	}
	if len(entries) != count {
		recordOracleFailure(rec, c, "file_count_equals", len(entries), count)
	}
	samples := c.Workload.ReadSampleCount
	if samples <= 0 || samples > count {
		samples = min(count, 10)
	}
	for i := 0; i < samples; i++ {
		idx := 0
		if samples > 1 {
			idx = i * (count - 1) / (samples - 1)
		}
		want := fmt.Sprintf("%s:%05d\n", c.ID, idx)
		local, err := safeJoinLocal(mountpoint, path.Join("small-files", fmt.Sprintf("%05d.txt", idx)))
		if err != nil {
			return err
		}
		if err := waitLocalContent(ctx, c, local, want); err != nil {
			recordOracleFailure(rec, c, "cli_read_equals", err.Error(), want)
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "file_count", Value: float64(count), Unit: "files", Source: c.Workload.Type})
	return nil
}

func runParallelWrites(ctx context.Context, rec *report.Recorder, mountpoint string, c casefile.Case) error {
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	for i := 0; i < c.Workload.ParallelWriters; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			file := filepath.Join(mountpoint, "parallel", fmt.Sprintf("writer-%02d.bin", i))
			if err := writeSizedFile(ctx, file, c.Workload.WriterBytes, byte('A'+i%26)); err != nil {
				mu.Lock()
				errs = append(errs, err.Error())
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		recordPerfResult(rec, c.ID, "parallel-writes", "fuse_write", "failed", start, time.Since(start), 0, float64(c.Workload.ParallelWriters), strings.Join(errs, "; "), nil)
		recordOracleFailure(rec, c, "fuse_write_success", errs, "all writers succeed")
		return nil
	}
	total := c.Workload.WriterBytes * int64(c.Workload.ParallelWriters)
	elapsed := time.Since(start)
	recordPerfResult(rec, c.ID, "parallel-writes", "fuse_write", "ok", start, elapsed, total, float64(c.Workload.ParallelWriters), "", nil)
	if elapsed > 0 {
		bps := float64(total) / elapsed.Seconds()
		_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "mount_perf_counter", Value: bps, Unit: "bytes", Source: "parallel_write_bytes_per_second"})
		if c.Workload.MinBytesPerSecond > 0 && int64(bps) < c.Workload.MinBytesPerSecond {
			recordOracleFailure(rec, c, "throughput_min", int64(bps), c.Workload.MinBytesPerSecond)
		}
	}
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "bytes_written", Value: float64(total), Unit: "bytes", Source: c.Workload.Type})
	return nil
}

func runGitWorkflow(ctx context.Context, rec *report.Recorder, mountpoint string, c casefile.Case) error {
	git := c.Workload.GitBinary
	if git == "" {
		git = "git"
	}
	repoPath, err := safeJoinLocal(mountpoint, c.Workload.CloneDir)
	if err != nil {
		return err
	}
	timeout := c.Workload.GitTimeout.Duration
	if timeout == 0 {
		timeout = c.Timeout.Duration
	}
	gitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cloneURL, err := resolveCloneURL(gitCtx, rec, c, git)
	if err != nil {
		return err
	}
	result := mountproc.Run(gitCtx, "git-clone", mountproc.Env{}, mountpoint, git, "clone", "--depth", "1", cloneURL, c.Workload.CloneDir)
	recordResult(rec, c.ID, result)
	_ = rec.Metric(report.Metric{CaseID: c.ID, Name: "git_clone_duration_ms", Value: float64(result.Duration.Milliseconds()), Unit: "ms", Source: "git clone"})
	if result.ExitCode != 0 {
		recordOracleFailure(rec, c, "git_clone", result.Stderr, "exit 0")
		return nil
	}
	checkNoGitLocks(rec, c, repoPath, "after clone")
	before := gitCommitCount(gitCtx, git, repoPath)
	userName := c.Workload.GitUserName
	if userName == "" {
		userName = "Drive9 Harness"
	}
	userEmail := c.Workload.GitUserEmail
	if userEmail == "" {
		userEmail = "drive9-harness@example.invalid"
	}
	if result := mountproc.Run(gitCtx, "git-config-name", mountproc.Env{}, repoPath, git, "config", "user.name", userName); result.ExitCode != 0 {
		recordResult(rec, c.ID, result)
		recordOracleFailure(rec, c, "git_config", result.Stderr, "exit 0")
		return nil
	} else {
		recordResult(rec, c.ID, result)
	}
	if result := mountproc.Run(gitCtx, "git-config-email", mountproc.Env{}, repoPath, git, "config", "user.email", userEmail); result.ExitCode != 0 {
		recordResult(rec, c.ID, result)
		recordOracleFailure(rec, c, "git_config", result.Stderr, "exit 0")
		return nil
	} else {
		recordResult(rec, c.ID, result)
	}
	mutationPath, err := safeJoinLocal(repoPath, c.Workload.Mutation.Path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(mutationPath), 0o755); err != nil {
		return err
	}
	flag := os.O_CREATE | os.O_WRONLY
	if c.Workload.Mutation.Mode == "append" {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(mutationPath, flag, 0o644)
	if err != nil {
		recordOracleFailure(rec, c, "git_mutation", err.Error(), "write success")
		return nil
	}
	if _, err := f.WriteString(c.Workload.Mutation.Content); err != nil {
		_ = f.Close()
		recordOracleFailure(rec, c, "git_mutation", err.Error(), "write success")
		return nil
	}
	if err := f.Close(); err != nil {
		recordOracleFailure(rec, c, "git_mutation", err.Error(), "close success")
		return nil
	}
	if result := mountproc.Run(gitCtx, "git-add", mountproc.Env{}, repoPath, git, "add", c.Workload.Mutation.Path); result.ExitCode != 0 {
		recordResult(rec, c.ID, result)
		recordOracleFailure(rec, c, "git_add", result.Stderr, "exit 0")
		return nil
	} else {
		recordResult(rec, c.ID, result)
	}
	if result := mountproc.Run(gitCtx, "git-commit", mountproc.Env{}, repoPath, git, "commit", "-m", c.Workload.CommitMessage); result.ExitCode != 0 {
		recordResult(rec, c.ID, result)
		recordOracleFailure(rec, c, "git_commit", result.Stderr, "exit 0")
		return nil
	} else {
		recordResult(rec, c.ID, result)
	}
	after := gitCommitCount(gitCtx, git, repoPath)
	if after-before != c.Workload.ExpectedCommitDelta {
		recordOracleFailure(rec, c, "git_commit_count", after-before, c.Workload.ExpectedCommitDelta)
	}
	status := mountproc.Run(gitCtx, "git-status", mountproc.Env{}, repoPath, git, "status", "--porcelain")
	recordResult(rec, c.ID, status)
	if strings.TrimSpace(status.Stdout) != strings.TrimSpace(c.Workload.ExpectedStatus) {
		recordOracleFailure(rec, c, "git_status_equals", status.Stdout, c.Workload.ExpectedStatus)
	}
	checkNoGitLocks(rec, c, repoPath, "after workflow")
	return nil
}

func runDoctor(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, mountpoint string, c casefile.Case) error {
	result := runCmd(ctx, rec, c.ID, "doctor-fuse", env, "", drive9Bin, "doctor", "fuse", "--mountpoint", mountpoint, "--server", env.Server)
	want := 0
	if c.Workload.ExpectExit != nil {
		want = *c.Workload.ExpectExit
	}
	if result.ExitCode != want {
		if c.Workload.AllowNonzeroWhenNoAllowOther && doctorOnlyNoAllowOtherFailed(result.Stdout+result.Stderr) {
			return nil
		}
		recordOracleFailure(rec, c, "command_exit", result.ExitCode, want)
	}
	return nil
}

func doctorOnlyNoAllowOtherFailed(output string) bool {
	failures := 0
	noAllowOther := false
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "FAIL ") {
			continue
		}
		failures++
		if strings.Contains(line, "/etc/fuse.conf user_allow_other:") {
			noAllowOther = true
		}
	}
	return failures == 1 && noAllowOther
}

func runOpenFDUnmount(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin, mountpoint string, c casefile.Case, mount **mountproc.Mount) error {
	local := filepath.Join(mountpoint, "held-open.txt")
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(local, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		recordOracleFailure(rec, c, "unmount_busy_then_clean", err.Error(), "open file")
		return nil
	}
	if _, err := f.WriteString("held open by harness\n"); err != nil {
		_ = f.Close()
		recordOracleFailure(rec, c, "unmount_busy_then_clean", err.Error(), "write held-open file")
		return nil
	}
	if c.Workload.HoldOpenDuration.Duration > 0 {
		timer := time.NewTimer(c.Workload.HoldOpenDuration.Duration)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = f.Close()
			return ctx.Err()
		case <-timer.C:
		}
	}
	firstCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	first := mountproc.Stop(firstCtx, env, drive9Bin, mountpoint)
	cancel()
	recordResult(rec, c.ID, first)
	_ = f.Close()
	if first.ExitCode == 0 {
		if err := waitUnmounted(ctx, mountpoint); err != nil {
			recordOracleFailure(rec, c, "unmount_busy_then_clean", err.Error(), "unmounted after first umount")
			return nil
		}
		*mount = nil
		return nil
	}
	if err := stopCaseMount(ctx, rec, env, drive9Bin, c.ID, mount); err != nil {
		recordOracleFailure(rec, c, "unmount_busy_then_clean", err.Error(), "clean unmount after fd close")
	}
	return nil
}

func runKillDuringWrite(ctx context.Context, rec *report.Recorder, manifest *report.Manifest, env mountproc.Env, drive9Bin, caseRemote, mountpoint string, c casefile.Case, mount **mountproc.Mount) error {
	target := filepath.Join(mountpoint, "kill-during-write.bin")
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeSizedFile(ctx, target, c.Workload.WriterBytes, 'K')
	}()
	timer := time.NewTimer(c.Workload.KillAfter.Duration)
	var writerErr error
	writerFinished := false
	killed := false
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case writerErr = <-writerDone:
		writerFinished = true
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case <-timer.C:
		killed = true
		if mount != nil && *mount != nil {
			_ = mountproc.KillMount(*mount)
			*mount = nil
			_ = rec.Event(report.Event{CaseID: c.ID, Type: "mount_end", Message: "mount process group killed during active write"})
		}
	}
	if killed && !writerFinished {
		drainTimer := time.NewTimer(2 * time.Second)
		select {
		case writerErr = <-writerDone:
			drainTimer.Stop()
		case <-ctx.Done():
			drainTimer.Stop()
			return ctx.Err()
		case <-drainTimer.C:
			writerErr = errors.New("writer interrupted before acknowledged completion")
		}
	}
	if killed {
		nextMount, err := startCaseMount(ctx, rec, manifest, env, drive9Bin, caseRemote, mountpoint, c)
		if err != nil {
			return err
		}
		*mount = nextMount
	}
	info, statErr := os.Stat(target)
	if writerErr != nil {
		observed := map[string]any{"writer_error": writerErr.Error()}
		if statErr == nil {
			observed["recovered_bytes"] = info.Size()
		} else {
			observed["stat_error"] = statErr.Error()
		}
		recordFailure(rec, c, "recovery_classified", "inconclusive", observed, "partial/missing data classified")
		return nil
	}
	if statErr != nil {
		recordOracleFailure(rec, c, "recovery_classified", statErr.Error(), "completed writer output exists")
		return nil
	}
	if info.Size() != c.Workload.WriterBytes {
		recordOracleFailure(rec, c, "recovery_classified", info.Size(), c.Workload.WriterBytes)
	}
	return nil
}

func runCmd(ctx context.Context, rec *report.Recorder, caseID, id string, env mountproc.Env, dir, name string, args ...string) mountproc.Result {
	_ = rec.Event(report.Event{CaseID: caseID, Type: "command_start", CommandID: id})
	result := mountproc.Run(ctx, id, env, dir, name, args...)
	recordResult(rec, caseID, result)
	return result
}

func recordResult(rec *report.Recorder, caseID string, result mountproc.Result) {
	exit := result.ExitCode
	_ = rec.Event(report.Event{CaseID: caseID, Type: "command_end", CommandID: result.ID, ExitCode: &exit, DurationMS: result.Duration.Milliseconds()})
	_ = rec.Metric(report.Metric{CaseID: caseID, Name: "command_duration_ms", Value: float64(result.Duration.Milliseconds()), Unit: "ms", Source: result.ID})
}

func recordPerfResult(rec *report.Recorder, caseID, operationID, operation, status string, started time.Time, duration time.Duration, bytes int64, requestUnits float64, errText string, artifactRefs []string) {
	if duration < 0 {
		duration = 0
	}
	if started.IsZero() {
		started = time.Now()
	}
	errorClass := ""
	if status != "ok" && status != "skipped" {
		errorClass = "product"
	}
	_ = rec.PerfResult(report.PerfResult{
		CaseID:       caseID,
		ScenarioID:   caseID,
		OperationID:  operationID,
		Operation:    operation,
		Status:       status,
		StartedAt:    started.UTC().Format(time.RFC3339Nano),
		EndedAt:      started.Add(duration).UTC().Format(time.RFC3339Nano),
		DurationMS:   float64(duration.Microseconds()) / 1000,
		Bytes:        bytes,
		RequestUnits: requestUnits,
		ErrorClass:   errorClass,
		Error:        boundedForPerf(errText),
		ArtifactRefs: artifactRefs,
	})
}

func resultStatus(exitCode int) string {
	if exitCode == 0 {
		return "ok"
	}
	return "failed"
}

func perfBytes(exitCode int, bytes int64) int64 {
	if exitCode == 0 {
		return bytes
	}
	return 0
}

func boundedForPerf(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 240 {
		return s
	}
	return s[:237] + "..."
}

func recordOracleFailure(rec *report.Recorder, c casefile.Case, oracle string, observed, expected any) {
	recordFailure(rec, c, oracle, "product", observed, expected)
}

func recordFailure(rec *report.Recorder, c casefile.Case, oracle, class string, observed, expected any) {
	reproPath := filepath.ToSlash(filepath.Join("repro", c.ID+".sh"))
	_ = writeRepro(filepath.Join(rec.RunDir, reproPath), c)
	_ = rec.Failure(report.Failure{CaseID: c.ID, Severity: c.Severity.Failure, Class: class, Oracle: oracle, ExpectedOutcome: c.ExpectedOutcome, Message: "oracle failed", Observed: observed, Expected: expected, ReproPath: reproPath})
}

func waitLocalContent(ctx context.Context, c casefile.Case, local, want string) error {
	waitCtx, cancel := context.WithTimeout(ctx, c.RemoteVisibilityTimeout())
	defer cancel()
	return pollWith(waitCtx, c.CommandRetryCount(), c.CommandRetrySleep(), func() error {
		b, err := os.ReadFile(local)
		if err != nil {
			return err
		}
		if string(b) != want {
			return fmt.Errorf("content %q", string(b))
		}
		return nil
	})
}

func waitRemoteContent(ctx context.Context, rec *report.Recorder, env mountproc.Env, drive9Bin string, c casefile.Case, remote, want string) error {
	waitCtx, cancel := context.WithTimeout(ctx, c.RemoteVisibilityTimeout())
	defer cancel()
	return pollWith(waitCtx, c.CommandRetryCount(), c.CommandRetrySleep(), func() error {
		result := runCmd(waitCtx, rec, c.ID, "cli-cat-"+safeName(remote), env, "", drive9Bin, "fs", "cat", ":"+remote)
		if result.ExitCode != 0 {
			return fmt.Errorf("%s", result.Stderr)
		}
		if result.Stdout != want {
			return fmt.Errorf("content %q", result.Stdout)
		}
		return nil
	})
}

func pollWith(ctx context.Context, attempts int, sleep time.Duration, fn func() error) error {
	if attempts <= 0 {
		attempts = 1
	}
	if sleep <= 0 {
		sleep = 250 * time.Millisecond
	}
	var last error
	for i := 0; i < attempts; i++ {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		if i == attempts-1 {
			break
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			if last != nil {
				return last
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func poll(ctx context.Context, fn func() error) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return last
		case <-ticker.C:
		}
	}
}

func safeJoinLocal(root, rel string) (string, error) {
	if err := casefile.ValidateRelativeSlashPath("local relative path", rel); err != nil {
		return "", err
	}
	cleanRoot := filepath.Clean(root)
	joined := filepath.Clean(filepath.Join(cleanRoot, filepath.FromSlash(rel)))
	if joined == cleanRoot || !strings.HasPrefix(joined, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes root %q", rel, cleanRoot)
	}
	return joined, nil
}

func gitCommitCount(ctx context.Context, git, repo string) int {
	result := mountproc.Run(ctx, "git-count", mountproc.Env{}, repo, git, "rev-list", "--count", "HEAD")
	if result.ExitCode != 0 {
		return 0
	}
	var n int
	_, _ = fmt.Sscanf(strings.TrimSpace(result.Stdout), "%d", &n)
	return n
}

func resolveCloneURL(ctx context.Context, rec *report.Recorder, c casefile.Case, git string) (string, error) {
	if !strings.HasPrefix(c.Workload.CloneURL, "fixture://") {
		return c.Workload.CloneURL, nil
	}
	name := strings.TrimPrefix(c.Workload.CloneURL, "fixture://")
	switch name {
	case "basic":
		repoPath := filepath.Join(rec.RunDir, "debug", "git-fixtures", c.ID, "basic")
		if err := os.MkdirAll(repoPath, 0o755); err != nil {
			return "", err
		}
		if _, err := os.Stat(filepath.Join(repoPath, ".git")); os.IsNotExist(err) {
			if result := mountproc.Run(ctx, "fixture-git-init", mountproc.Env{}, repoPath, git, "init", "."); result.ExitCode != 0 {
				recordResult(rec, c.ID, result)
				return "", fmt.Errorf("create git fixture: %s", result.Stderr)
			} else {
				recordResult(rec, c.ID, result)
			}
			if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("drive9 harness fixture\n"), 0o644); err != nil {
				return "", err
			}
			recordResult(rec, c.ID, mountproc.Run(ctx, "fixture-git-config-name", mountproc.Env{}, repoPath, git, "config", "user.name", "Drive9 Harness"))
			recordResult(rec, c.ID, mountproc.Run(ctx, "fixture-git-config-email", mountproc.Env{}, repoPath, git, "config", "user.email", "drive9-harness@example.invalid"))
			if result := mountproc.Run(ctx, "fixture-git-add", mountproc.Env{}, repoPath, git, "add", "README.md"); result.ExitCode != 0 {
				recordResult(rec, c.ID, result)
				return "", fmt.Errorf("stage git fixture: %s", result.Stderr)
			} else {
				recordResult(rec, c.ID, result)
			}
			if result := mountproc.Run(ctx, "fixture-git-commit", mountproc.Env{}, repoPath, git, "commit", "-m", "initial fixture"); result.ExitCode != 0 {
				recordResult(rec, c.ID, result)
				return "", fmt.Errorf("commit git fixture: %s", result.Stderr)
			} else {
				recordResult(rec, c.ID, result)
			}
		}
		return "file://" + filepath.ToSlash(repoPath), nil
	default:
		return "", fmt.Errorf("unknown git fixture %q", name)
	}
}

func checkNoGitLocks(rec *report.Recorder, c casefile.Case, repoPath, stage string) {
	globs := []string{".git/*.lock"}
	for _, o := range c.Oracles {
		if o.Type == "no_git_locks" && len(o.Globs) > 0 {
			globs = o.Globs
			break
		}
	}
	var matches []string
	for _, glob := range globs {
		pattern, err := safeJoinLocal(repoPath, glob)
		if err != nil {
			recordOracleFailure(rec, c, "no_git_locks", err.Error(), "safe lock glob")
			continue
		}
		found, _ := filepath.Glob(pattern)
		matches = append(matches, found...)
	}
	if len(matches) > 0 {
		recordOracleFailure(rec, c, "no_git_locks", map[string]any{"stage": stage, "matches": matches}, "no lock files")
	}
}

func mountReadyTimeout(c casefile.Case) time.Duration {
	timeout := c.MountReadyTimeout()
	if c.Timeout.Duration > 0 && c.Timeout.Duration < timeout {
		return c.Timeout.Duration
	}
	return timeout
}

func remountPaths(c casefile.Case) []string {
	if c.Workload.Remount != nil && !*c.Workload.Remount {
		return nil
	}
	for _, o := range c.Oracles {
		if o.Type == "remount_hash_equal" && len(o.Paths) > 0 {
			return cleanRelativePaths(o.Paths)
		}
	}
	switch c.Workload.Type {
	case "mount_smoke":
		out := make([]string, 0, len(c.Workload.Files))
		for _, f := range c.Workload.Files {
			out = append(out, f.RelativePath)
		}
		return cleanRelativePaths(out)
	case "path_matrix":
		return cleanRelativePaths(c.Workload.Paths)
	case "dual_mount_visibility":
		out := make([]string, 0, len(c.Workload.Files))
		for _, f := range c.Workload.Files {
			out = append(out, f.RelativePath)
		}
		return cleanRelativePaths(out)
	case "small_file_storm":
		out := make([]string, 0, c.Workload.FileCount)
		for i := 0; i < c.Workload.FileCount; i++ {
			out = append(out, path.Join("small-files", fmt.Sprintf("%05d.txt", i)))
		}
		return cleanRelativePaths(out)
	case "parallel_writes":
		out := make([]string, 0, c.Workload.ParallelWriters)
		for i := 0; i < c.Workload.ParallelWriters; i++ {
			out = append(out, path.Join("parallel", fmt.Sprintf("writer-%02d.bin", i)))
		}
		return cleanRelativePaths(out)
	case "git_workflow":
		out := make([]string, 0, len(c.Workload.RemountVerifyPaths))
		for _, v := range c.Workload.RemountVerifyPaths {
			out = append(out, path.Join(c.Workload.CloneDir, v.Path))
		}
		return cleanRelativePaths(out)
	default:
		return nil
	}
}

func expectedRemountHashes(c casefile.Case) map[string]string {
	out := map[string]string{}
	for _, o := range c.Oracles {
		if o.Type == "remount_hash_equal" {
			for p, h := range o.ExpectedHashes {
				out[path.Clean(p)] = h
			}
		}
	}
	if c.Workload.Type == "git_workflow" {
		for _, v := range c.Workload.RemountVerifyPaths {
			if v.SHA256 != "" {
				out[path.Join(c.Workload.CloneDir, v.Path)] = v.SHA256
			}
		}
	}
	return out
}

func cleanRelativePaths(paths []string) []string {
	out := make([]string, 0, len(paths))
	seen := map[string]bool{}
	for _, p := range paths {
		clean := path.Clean(strings.TrimPrefix(p, "/"))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || seen[clean] {
			continue
		}
		seen[clean] = true
		out = append(out, clean)
	}
	return out
}

func hashFile(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeSizedFile(ctx context.Context, file string, size int64, fill byte) error {
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	block := make([]byte, 1<<20)
	for i := range block {
		block[i] = fill
	}
	var written int64
	for written < size {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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

func fioWriteBytesPerSecond(stdout string) float64 {
	var body struct {
		Jobs []struct {
			Write struct {
				BWBytes float64 `json:"bw_bytes"`
			} `json:"write"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(stdout), &body); err != nil {
		return 0
	}
	var total float64
	for _, job := range body.Jobs {
		total += job.Write.BWBytes
	}
	return total
}

func writeRepro(path string, c casefile.Case) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\n\ndrive9-agent-harness run --suite %q --case %q \"$@\"\n", c.Suite, c.ID)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		return err
	}
	return nil
}

func shouldRemoveMountpoint(c casefile.Case, rec *report.Recorder, caseErr error) bool {
	switch c.Cleanup {
	case "always":
		return true
	case "retain_on_failure":
		return caseErr == nil && !caseHadFailures(rec, c.ID)
	default:
		return false
	}
}

func caseHadFailures(rec *report.Recorder, caseID string) bool {
	f, err := os.Open(filepath.Join(rec.RunDir, "failures.jsonl"))
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return true
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var failure report.Failure
		if err := json.Unmarshal(scanner.Bytes(), &failure); err != nil {
			return true
		}
		if failure.CaseID == caseID {
			return true
		}
	}
	return scanner.Err() != nil
}

func requiresFreshTenant(cases []casefile.Case) bool {
	for _, c := range cases {
		if c.RequiresFreshTenant() {
			return true
		}
	}
	return false
}

func containsSuite(cases []casefile.Case, suite string) bool {
	for _, c := range cases {
		if c.Suite == suite {
			return true
		}
	}
	return false
}

func captureDebug(ctx context.Context, rec *report.Recorder, label string) error {
	debugDir := filepath.Join(rec.RunDir, "debug")
	if err := os.MkdirAll(debugDir, 0o755); err != nil {
		return err
	}
	for _, cmd := range []struct {
		id   string
		name string
		args []string
	}{
		{id: "mount-table-" + label, name: "mount"},
		{id: "ps-" + label, name: "ps", args: []string{"-ax"}},
	} {
		result := mountproc.Run(ctx, cmd.id, mountproc.Env{}, "", cmd.name, cmd.args...)
		path := filepath.Join(debugDir, cmd.id+".txt")
		body := result.Stdout
		if result.Stderr != "" {
			body += "\nSTDERR:\n" + result.Stderr
		}
		_ = os.WriteFile(path, []byte(body), 0o644)
	}
	return nil
}

func GC(ctx context.Context, cfg GCConfig) error {
	if !cfg.ConfirmDelete {
		return ErrConfirmDeleteRequired
	}
	manifest := report.Manifest{}
	if err := readJSON(filepath.Join(cfg.RunDir, "manifest.json"), &manifest); err != nil {
		return err
	}
	if cfg.SuccessfulOnly {
		gating := report.Gating{}
		if err := readJSON(filepath.Join(cfg.RunDir, "gating.json"), &gating); err != nil {
			return err
		}
		if gating.GateStatus == "fail" || gating.GateStatus == "harness_failed" {
			return fmt.Errorf("refusing successful-only GC for gate status %q", gating.GateStatus)
		}
	}
	drive9Bin := cfg.Drive9Bin
	if drive9Bin == "" {
		drive9Bin = "drive9"
	}
	env := mountproc.Env{Server: cfg.Server, APIKey: cfg.APIKey}
	if cfg.Server == "" {
		env.Server = manifest.Server
	}
	for id, mp := range manifest.GeneratedMountpoints {
		cleanMP := filepath.Clean(mp)
		cleanRoot := filepath.Clean(manifest.MountRoot)
		if cleanMP == cleanRoot || !strings.HasPrefix(cleanMP, cleanRoot+string(os.PathSeparator)) {
			return fmt.Errorf("%w for %s: %q", ErrUnsafeMountpoint, id, mp)
		}
		mounted, err := safety.IsMounted(mp)
		if err != nil {
			return fmt.Errorf("check mountpoint %s: %w", mp, err)
		}
		if mounted {
			result := mountproc.Stop(ctx, env, drive9Bin, mp)
			if result.ExitCode != 0 {
				return fmt.Errorf("unmount %s (%s): %s", id, mp, result.Stderr)
			}
		}
		if err := os.RemoveAll(mp); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove mountpoint %s: %w", mp, err)
		}
	}
	for id, remote := range manifest.GeneratedRemoteRoots {
		if remote == "" || remote == manifest.RemoteRootBase || !strings.HasPrefix(remote, strings.TrimRight(manifest.RemoteRootBase, "/")+"/") {
			return fmt.Errorf("refusing to delete unsafe remote root for %s: %q", id, remote)
		}
		result := mountproc.Run(ctx, "gc-remote-"+id, env, "", drive9Bin, "fs", "rm", "-r", ":"+remote)
		if result.ExitCode != 0 {
			return fmt.Errorf("delete remote root %s: %s", remote, result.Stderr)
		}
	}
	return nil
}

func CollectServerEvidence(ctx context.Context, cfg EvidenceConfig) error {
	if !cfg.ApproveExternal {
		return errors.New("collect-server-evidence requires --approve-external")
	}
	if cfg.RunDir == "" {
		return errors.New("collect-server-evidence requires run dir")
	}
	debugDir := filepath.Join(cfg.RunDir, "debug", "server")
	metricsDir := filepath.Join(cfg.RunDir, "metrics")
	if err := os.MkdirAll(debugDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(metricsDir, 0o755); err != nil {
		return err
	}
	if cfg.KubeContext != "" {
		if cfg.Namespace == "" {
			cfg.Namespace = "dat9"
		}
		if cfg.Selector == "" {
			cfg.Selector = "app=dat9-server"
		}
		if cfg.Since == "" {
			cfg.Since = "10m"
		}
		if cfg.Tail == 0 {
			cfg.Tail = 500
		}
		logs := mountproc.Run(ctx, "kubectl-logs", mountproc.Env{}, "", "kubectl", "--context", cfg.KubeContext, "-n", cfg.Namespace, "logs", "-l", cfg.Selector, "--since="+cfg.Since, fmt.Sprintf("--tail=%d", cfg.Tail))
		if err := os.WriteFile(filepath.Join(debugDir, "server-logs.jsonl"), []byte(logs.Stdout), 0o644); err != nil {
			return err
		}
		if logs.ExitCode != 0 {
			return fmt.Errorf("kubectl logs: %s", logs.Stderr)
		}
		metrics := mountproc.Run(ctx, "kubectl-metrics", mountproc.Env{}, "", "kubectl", "--context", cfg.KubeContext, "get", "--raw", "/api/v1/namespaces/"+cfg.Namespace+"/services/dat9-server:http/proxy/metrics")
		if err := os.WriteFile(filepath.Join(metricsDir, "server-after.prom"), []byte(metrics.Stdout), 0o644); err != nil {
			return err
		}
		if metrics.ExitCode != 0 {
			return fmt.Errorf("kubectl metrics: %s", metrics.Stderr)
		}
	}
	if cfg.MetricsRawPath != "" {
		b, err := os.ReadFile(cfg.MetricsRawPath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(metricsDir, "server-after.prom"), b, 0o644); err != nil {
			return err
		}
	}
	summary := map[string]any{
		"schema_version": report.SchemaVersion,
		"run_dir":        cfg.RunDir,
		"kube_context":   cfg.KubeContext,
		"namespace":      cfg.Namespace,
		"selector":       cfg.Selector,
	}
	return writeJSON(filepath.Join(debugDir, "server-evidence.json"), summary)
}

func provision(ctx context.Context, server string, timeout time.Duration) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(server, "/")+"/v1/provision", nil)
	if err != nil {
		return "", err
	}
	resp, err := harnessHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("provision returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		APIKey string `json:"api_key"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.APIKey == "" {
		return "", errors.New("provision response missing api_key")
	}
	statusCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pollStatus(statusCtx, server, body.APIKey); err != nil {
		return "", err
	}
	return body.APIKey, nil
}

func checkStatus(ctx context.Context, server, apiKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(server, "/")+"/v1/status", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := harnessHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func pollStatus(ctx context.Context, server, apiKey string) error {
	return poll(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(server, "/")+"/v1/status", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := harnessHTTPClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("status returned HTTP %d", resp.StatusCode)
		}
		var body struct {
			Status string `json:"status"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if body.Status != "active" {
			return fmt.Errorf("tenant status %q", body.Status)
		}
		return nil
	})
}

func resolveBin(bin string) (string, error) {
	if strings.Contains(bin, string(os.PathSeparator)) {
		if _, err := os.Stat(bin); err != nil {
			return "", err
		}
		return bin, nil
	}
	return exec.LookPath(bin)
}

func drive9Version(ctx context.Context, bin string) string {
	result := mountproc.Run(ctx, "drive9-version", mountproc.Env{}, "", bin, "--version")
	if result.ExitCode != 0 {
		return "unknown"
	}
	return strings.TrimSpace(result.Stdout + result.Stderr)
}

func gitSHA() string {
	result := mountproc.Run(context.Background(), "git-sha", mountproc.Env{}, "", "git", "rev-parse", "HEAD")
	if result.ExitCode != 0 {
		return "unknown"
	}
	return strings.TrimSpace(result.Stdout)
}

func host() string {
	name, err := os.Hostname()
	if err != nil {
		name = "unknown"
	}
	return name + " " + runtime.GOOS + "/" + runtime.GOARCH
}

func runID() string {
	ts := time.Now().UTC().Format("20060102T150405Z")
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000Z")
	}
	return ts + "-" + hex.EncodeToString(suffix[:])
}

func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func caseIDs(cases []casefile.Case) []string {
	out := make([]string, 0, len(cases))
	for _, c := range cases {
		out = append(out, c.ID)
	}
	return out
}

func caseSummaries(cases []casefile.Case) []report.CaseSummary {
	out := make([]report.CaseSummary, 0, len(cases))
	for _, c := range cases {
		out = append(out, report.CaseSummary{ID: c.ID, Suite: c.Suite, ExpectedOutcome: c.ExpectedOutcome, Status: "passed"})
	}
	return out
}

func redact(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 8 {
		return "redacted"
	}
	return key[:4] + "..." + key[len(key)-4:]
}

func safeName(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
