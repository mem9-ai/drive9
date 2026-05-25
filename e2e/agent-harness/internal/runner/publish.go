package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/mountproc"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/report"
)

const publishSchemaVersion = "perf-publish.v1"

type PublishPerfConfig struct {
	RunDir        string
	WorkspaceRoot string
	Drive9Bin     string
	Server        string
	APIKey        string
	Title         string
}

type PublishManifest struct {
	SchemaVersion string            `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Title         string            `json:"title"`
	UploadedAt    string            `json:"uploaded_at"`
	SourceServer  string            `json:"source_server"`
	WorkspaceRoot string            `json:"workspace_root"`
	RemoteRunPath string            `json:"remote_run_path"`
	Workload      string            `json:"workload"`
	GateStatus    string            `json:"gate_status"`
	Status        string            `json:"status"`
	ArtifactPaths map[string]string `json:"artifact_paths"`
	Errors        []string          `json:"errors,omitempty"`
}

type publishIndex struct {
	SchemaVersion string              `json:"schema_version"`
	UpdatedAt     string              `json:"updated_at"`
	Entries       []publishIndexEntry `json:"entries"`
}

type publishIndexEntry struct {
	Title           string `json:"title"`
	RunID           string `json:"run_id"`
	TestWindow      string `json:"test_window"`
	Workload        string `json:"workload"`
	ServiceEndpoint string `json:"service_endpoint"`
	GateStatus      string `json:"gate_status"`
	ReportPath      string `json:"report_path"`
	SummaryPath     string `json:"summary_path"`
	PublishedAt     string `json:"published_at"`
}

type publishFile struct {
	local string
	rel   string
}

func PublishPerf(ctx context.Context, cfg PublishPerfConfig) (PublishManifest, error) {
	if cfg.RunDir == "" {
		return PublishManifest{}, fmt.Errorf("publish-perf requires run dir")
	}
	if cfg.Drive9Bin == "" {
		cfg.Drive9Bin = "drive9"
	}
	if cfg.WorkspaceRoot == "" {
		cfg.WorkspaceRoot = ":/performance-reports"
	}
	rootPath, err := workspacePath(cfg.WorkspaceRoot)
	if err != nil {
		return PublishManifest{}, err
	}
	perfReport, err := report.GeneratePerfReport(cfg.RunDir, report.PerfOptions{Title: cfg.Title})
	if err != nil {
		return PublishManifest{}, err
	}
	manifest := report.Manifest{}
	if err := readJSON(filepath.Join(cfg.RunDir, "manifest.json"), &manifest); err != nil {
		return PublishManifest{}, err
	}
	if cfg.Server == "" {
		cfg.Server = manifest.Server
	}
	env := mountproc.Env{Server: cfg.Server, APIKey: cfg.APIKey}
	remoteRunPath := publishRunPath(rootPath, manifest, perfReport)
	reportRelPath := path.Join("perf", report.PerfMarkdownFilename(perfReport.Title))
	files, err := collectPublishFiles(cfg.RunDir, reportRelPath)
	if err != nil {
		return PublishManifest{}, err
	}
	artifactPaths := map[string]string{}
	var uploadErrors []string
	if err := drive9Mkdir(ctx, cfg.Drive9Bin, env, rootPath); err != nil {
		uploadErrors = append(uploadErrors, err.Error())
	}
	createdDirs := map[string]bool{}
	for _, f := range files {
		remote := path.Join(remoteRunPath, filepath.ToSlash(f.rel))
		dir := path.Dir(remote)
		if !createdDirs[dir] {
			if err := drive9Mkdir(ctx, cfg.Drive9Bin, env, dir); err != nil {
				uploadErrors = append(uploadErrors, err.Error())
				createdDirs[dir] = true
				continue
			}
			createdDirs[dir] = true
		}
		if err := drive9Upload(ctx, cfg.Drive9Bin, env, f.local, remote); err != nil {
			uploadErrors = append(uploadErrors, err.Error())
			continue
		}
		artifactPaths[f.rel] = ":" + remote
	}
	publishedAt := time.Now().UTC().Format(time.RFC3339Nano)
	status := "succeeded"
	if len(uploadErrors) > 0 {
		status = "partial_failed"
	}
	pubRemote := path.Join(remoteRunPath, "perf", "publish-manifest.json")
	artifactPaths["perf/publish-manifest.json"] = ":" + pubRemote
	pub := PublishManifest{
		SchemaVersion: publishSchemaVersion,
		RunID:         perfReport.RunID,
		Title:         perfReport.Title,
		UploadedAt:    publishedAt,
		SourceServer:  cfg.Server,
		WorkspaceRoot: ":" + rootPath,
		RemoteRunPath: ":" + remoteRunPath,
		Workload:      workloadName(perfReport),
		GateStatus:    perfReport.OverallStatus,
		Status:        status,
		ArtifactPaths: artifactPaths,
		Errors:        uploadErrors,
	}
	pubPath := filepath.Join(cfg.RunDir, "perf", "publish-manifest.json")
	if err := writeJSON(pubPath, pub); err != nil {
		return pub, err
	}
	if err := drive9Mkdir(ctx, cfg.Drive9Bin, env, path.Dir(pubRemote)); err != nil {
		pub.Errors = append(pub.Errors, err.Error())
		_ = writeJSON(pubPath, pub)
		return pub, err
	}
	if err := drive9Upload(ctx, cfg.Drive9Bin, env, pubPath, pubRemote); err != nil {
		pub.Errors = append(pub.Errors, err.Error())
		_ = writeJSON(pubPath, pub)
		return pub, err
	}
	if len(uploadErrors) > 0 {
		return pub, fmt.Errorf("publish-perf uploaded partial bundle: %s", strings.Join(uploadErrors, "; "))
	}
	entry := publishIndexEntry{
		Title:           perfReport.Title,
		RunID:           perfReport.RunID,
		TestWindow:      perfReport.StartedAt + " to " + perfReport.EndedAt,
		Workload:        workloadName(perfReport),
		ServiceEndpoint: perfReport.Environment.ServerEndpoint,
		GateStatus:      perfReport.OverallStatus,
		ReportPath:      ":" + path.Join(remoteRunPath, reportRelPath),
		SummaryPath:     ":" + path.Join(remoteRunPath, "perf", "summary.json"),
		PublishedAt:     publishedAt,
	}
	if err := updatePublishIndex(ctx, cfg, env, rootPath, entry); err != nil {
		return pub, fmt.Errorf("publish-perf uploaded bundle but failed to update index: %w", err)
	}
	return pub, nil
}

func collectPublishFiles(runDir, reportRelPath string) ([]publishFile, error) {
	seen := map[string]bool{}
	var files []publishFile
	add := func(rel string) error {
		rel = filepath.ToSlash(filepath.Clean(rel))
		if seen[rel] {
			return nil
		}
		local := filepath.Join(runDir, filepath.FromSlash(rel))
		info, err := os.Stat(local)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		seen[rel] = true
		files = append(files, publishFile{local: local, rel: rel})
		return nil
	}
	for _, rel := range []string{
		"manifest.json",
		"events.jsonl",
		"failures.jsonl",
		"metrics.jsonl",
		"summary.json",
		"gating.json",
		"summary.md",
		reportRelPath,
		"perf/environment.json",
		"perf/results.jsonl",
		"perf/summary.json",
	} {
		if err := add(rel); err != nil {
			return nil, err
		}
	}
	for _, dir := range []string{"debug", "metrics", "repro", filepath.Join("perf", "evidence")} {
		root := filepath.Join(runDir, dir)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		}
		if err := filepath.WalkDir(root, func(local string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(runDir, local)
			if err != nil {
				return err
			}
			return add(rel)
		}); err != nil {
			return nil, err
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, nil
}

func updatePublishIndex(ctx context.Context, cfg PublishPerfConfig, env mountproc.Env, rootPath string, entry publishIndexEntry) error {
	indexRemote := path.Join(rootPath, "index.json")
	index := publishIndex{SchemaVersion: publishSchemaVersion, Entries: nil}
	result := mountproc.Run(ctx, "publish-index-read", env, "", cfg.Drive9Bin, "fs", "cat", ":"+indexRemote)
	if result.ExitCode == 0 && strings.TrimSpace(result.Stdout) != "" {
		if err := json.Unmarshal([]byte(result.Stdout), &index); err != nil {
			return fmt.Errorf("parse existing index: %w", err)
		}
	}
	index.SchemaVersion = publishSchemaVersion
	index.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	entries := make([]publishIndexEntry, 0, len(index.Entries)+1)
	for _, existing := range index.Entries {
		if existing.RunID == entry.RunID {
			continue
		}
		entries = append(entries, existing)
	}
	entries = append(entries, entry)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].PublishedAt > entries[j].PublishedAt
	})
	index.Entries = entries
	tmp := filepath.Join(cfg.RunDir, "perf", "index.json")
	if err := writeJSON(tmp, index); err != nil {
		return err
	}
	if err := drive9Mkdir(ctx, cfg.Drive9Bin, env, rootPath); err != nil {
		return err
	}
	return drive9Upload(ctx, cfg.Drive9Bin, env, tmp, indexRemote)
}

func drive9Mkdir(ctx context.Context, drive9Bin string, env mountproc.Env, remotePath string) error {
	result := mountproc.Run(ctx, "publish-mkdir", env, "", drive9Bin, "fs", "mkdir", ":"+remotePath)
	if result.ExitCode != 0 {
		return fmt.Errorf("mkdir :%s: %s", remotePath, strings.TrimSpace(result.Stderr))
	}
	return nil
}

func drive9Upload(ctx context.Context, drive9Bin string, env mountproc.Env, localPath, remotePath string) error {
	result := mountproc.Run(ctx, "publish-upload", env, "", drive9Bin, "fs", "cp", localPath, ":"+remotePath)
	if result.ExitCode != 0 {
		return fmt.Errorf("upload %s to :%s: %s", filepath.ToSlash(localPath), remotePath, strings.TrimSpace(result.Stderr))
	}
	return nil
}

func workspacePath(root string) (string, error) {
	if root == "" {
		root = ":/performance-reports"
	}
	if strings.HasPrefix(root, ":") {
		root = strings.TrimPrefix(root, ":")
	}
	root = path.Clean(root)
	if root == "." || !strings.HasPrefix(root, "/") {
		return "", fmt.Errorf("workspace root must be a remote absolute path, got %q", root)
	}
	return root, nil
}

func publishRunPath(root string, manifest report.Manifest, perfReport report.PerfReport) string {
	date := reportDate(perfReport.StartedAt)
	suite := "unknown-suite"
	if len(manifest.Suites) > 0 {
		suite = safeRemoteSegment(strings.Join(manifest.Suites, "+"))
	}
	return path.Join(root, suite, date, perfReport.RunID)
}

func reportDate(startedAt string) string {
	if t, err := time.Parse(time.RFC3339Nano, startedAt); err == nil {
		return t.Format("2006-01-02")
	}
	if len(startedAt) >= len("2006-01-02") {
		return startedAt[:len("2006-01-02")]
	}
	return time.Now().UTC().Format("2006-01-02")
}

func workloadName(perfReport report.PerfReport) string {
	seen := map[string]bool{}
	var out []string
	for _, s := range perfReport.Scenarios {
		if s.Workload == "" || seen[s.Workload] {
			continue
		}
		seen[s.Workload] = true
		out = append(out, s.Workload)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return "unknown"
	}
	return strings.Join(out, ", ")
}

func safeRemoteSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown"
	}
	return out
}
