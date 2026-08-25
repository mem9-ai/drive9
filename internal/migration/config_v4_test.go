package migration

import (
	"path/filepath"
	"strings"
	"testing"
)

const validV4ConfigYAML = `version: v4
drive9:
  endpoint: https://drive9.example.com
job_defaults:
  sync:
    grace_period: 60s
  performance:
    max_bytes_per_second: 1024
    small_file_workers: 2
    large_file_workers: 1
spaces:
  space-a:
    credential_ref: space-a-key
  space-b:
    credential_ref: space-b-key
ebs_sources:
  - volume_id: vol-001
    node_name: node-a
    root: /ebs/001
    jobs:
      - job_id: vol-001-user-a
        subpath: /A
        target:
          space_ref: space-a
          prefix: /
      - job_id: vol-001-user-b
        subpath: /B
        target:
          space_ref: space-b
          prefix: /
`

func TestLoadConfigAcceptsV4EBSSubpathMappings(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validV4ConfigYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Version != "v4" {
		t.Fatalf("version = %q, want v4", cfg.Version)
	}
}

func TestLoadConfigRejectsV3(t *testing.T) {
	body := strings.Replace(validConfigYAML, "version: v4", "version: v3", 1)
	if _, err := LoadConfig(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), `config version must be "v4"`) {
		t.Fatalf("v3 error = %v", err)
	}
}

func TestLoadConfigRejectsOverlappingV4Subpaths(t *testing.T) {
	body := strings.Replace(validV4ConfigYAML, "        subpath: /B", "        subpath: /A/child", 1)
	if _, err := LoadConfig(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "overlapping source subpaths") {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestLoadConfigRejectsInvalidV4JobIdentityAndSubpath(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "duplicate Job ID", body: strings.Replace(validV4ConfigYAML, "job_id: vol-001-user-b", "job_id: vol-001-user-a", 1)},
		{name: "unsafe Job ID", body: strings.Replace(validV4ConfigYAML, "job_id: vol-001-user-a", "job_id: ../user-a", 1)},
		{name: "relative subpath", body: strings.Replace(validV4ConfigYAML, "subpath: /A", "subpath: A", 1)},
		{name: "traversing subpath", body: strings.Replace(validV4ConfigYAML, "subpath: /A", "subpath: /A/../B", 1)},
		{name: "backslash subpath", body: strings.Replace(validV4ConfigYAML, "subpath: /A", `subpath: /A\\B`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, tc.body)); err == nil {
				t.Fatal("invalid v4 mapping accepted")
			}
		})
	}
}

func TestLoadRuntimeStartupResolvesEverySubpathWithoutReadingCredentials(t *testing.T) {
	runtime, err := LoadRuntimeStartup(
		writeConfig(t, validV4ConfigYAML), "node-a", string(PhaseSyncing), t.TempDir(), "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.Jobs) != 2 {
		t.Fatalf("Jobs=%d", len(runtime.Jobs))
	}
	for index, expected := range []struct {
		jobID   string
		subpath string
		root    string
	}{{"vol-001-user-a", "/A", "/ebs/001/A"}, {"vol-001-user-b", "/B", "/ebs/001/B"}} {
		job := runtime.Jobs[index].Job
		if job.JobID != expected.jobID || job.Subpath != expected.subpath || job.EBSRoot != "/ebs/001" || job.Source.Root != filepath.Clean(expected.root) {
			t.Fatalf("Job %d=%+v", index, job)
		}
	}
}

func TestConfigHashCoversJobIDAndSubpath(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, validV4ConfigYAML))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	original, err := ConfigHash(cfg, job)
	if err != nil {
		t.Fatal(err)
	}
	job.JobID = "replacement-job"
	changedID, err := ConfigHash(cfg, job)
	if err != nil {
		t.Fatal(err)
	}
	job = cfg.Jobs[0]
	job.Subpath, job.Source.Root = "/replacement", "/ebs/001/replacement"
	changedSubpath, err := ConfigHash(cfg, job)
	if err != nil {
		t.Fatal(err)
	}
	if original == changedID || original == changedSubpath {
		t.Fatalf("hashes original=%s id=%s subpath=%s", original, changedID, changedSubpath)
	}
}
