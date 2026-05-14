// Package casefile loads and validates drive9 agent harness suite files.
package casefile

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	ErrParse      = errors.New("casefile parse")
	ErrValidation = errors.New("casefile validation")
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type SuiteFile struct {
	Defaults Defaults `yaml:"defaults"`
	Cases    []Case   `yaml:"cases"`
}

type Defaults struct {
	Timeout                 Duration `yaml:"timeout"`
	Cleanup                 string   `yaml:"cleanup"`
	MountReadyTimeout       Duration `yaml:"mount_ready_timeout"`
	RemoteVisibilityTimeout Duration `yaml:"remote_visibility_timeout"`
	CommandRetryCount       int      `yaml:"command_retry_count"`
	CommandRetrySleep       Duration `yaml:"command_retry_sleep"`
	RequiresFreshTenant     *bool    `yaml:"requires_fresh_tenant"`
}

type Case struct {
	ID               string       `yaml:"id" json:"id"`
	Suite            string       `yaml:"suite" json:"suite"`
	SyncMode         string       `yaml:"sync_mode" json:"sync_mode,omitempty"`
	ExpectedOutcome  string       `yaml:"expected_outcome" json:"expected_outcome"`
	RemoteRootSuffix string       `yaml:"remote_root_suffix" json:"remote_root_suffix"`
	MountpointSuffix string       `yaml:"mountpoint_suffix" json:"mountpoint_suffix"`
	Timeout          Duration     `yaml:"timeout" json:"-"`
	Cleanup          string       `yaml:"cleanup" json:"cleanup"`
	RequiresFresh    *bool        `yaml:"requires_fresh_tenant" json:"requires_fresh_tenant,omitempty"`
	Workload         Workload     `yaml:"workload" json:"workload"`
	Oracles          []OracleSpec `yaml:"oracles" json:"oracles"`
	Severity         SeveritySpec `yaml:"severity" json:"severity"`
	defaults         Defaults
	SourceFile       string `yaml:"-" json:"source_file,omitempty"`
}

type SeveritySpec struct {
	Failure string `yaml:"failure" json:"failure"`
}

type Workload struct {
	Type                         string       `yaml:"type" json:"type"`
	Files                        []FileSpec   `yaml:"files" json:"files,omitempty"`
	Paths                        []string     `yaml:"paths" json:"paths,omitempty"`
	ContentTemplate              string       `yaml:"content_template" json:"content_template,omitempty"`
	ReadAfterWriteTimeout        Duration     `yaml:"read_after_write_timeout" json:"-"`
	Remount                      *bool        `yaml:"remount" json:"remount,omitempty"`
	SizeBytes                    int64        `yaml:"size_bytes" json:"size_bytes,omitempty"`
	BlockBytes                   int64        `yaml:"block_bytes" json:"block_bytes,omitempty"`
	FileCount                    int          `yaml:"file_count" json:"file_count,omitempty"`
	ParallelWriters              int          `yaml:"parallel_writers" json:"parallel_writers,omitempty"`
	WriterBytes                  int64        `yaml:"writer_bytes" json:"writer_bytes,omitempty"`
	ReadSampleCount              int          `yaml:"read_sample_count" json:"read_sample_count,omitempty"`
	MinBytesPerSecond            int64        `yaml:"min_bytes_per_second" json:"min_bytes_per_second,omitempty"`
	FioBinary                    string       `yaml:"fio_binary" json:"fio_binary,omitempty"`
	HoldOpenDuration             Duration     `yaml:"hold_open_duration" json:"-"`
	KillAfter                    Duration     `yaml:"kill_after" json:"-"`
	CloneURL                     string       `yaml:"clone_url" json:"clone_url,omitempty"`
	GitBinary                    string       `yaml:"git_binary" json:"git_binary,omitempty"`
	GitTimeout                   Duration     `yaml:"git_timeout" json:"-"`
	GitUserName                  string       `yaml:"git_user_name" json:"git_user_name,omitempty"`
	GitUserEmail                 string       `yaml:"git_user_email" json:"git_user_email,omitempty"`
	CloneDir                     string       `yaml:"clone_dir" json:"clone_dir,omitempty"`
	Mutation                     MutationSpec `yaml:"mutation" json:"mutation,omitempty"`
	CommitMessage                string       `yaml:"commit_message" json:"commit_message,omitempty"`
	ExpectedLocks                []string     `yaml:"expected_locks" json:"expected_locks,omitempty"`
	ExpectedStatus               string       `yaml:"expected_status" json:"expected_status,omitempty"`
	ExpectedCommitDelta          int          `yaml:"expected_commit_delta" json:"expected_commit_delta,omitempty"`
	RemountVerifyPaths           []VerifySpec `yaml:"remount_verify_paths" json:"remount_verify_paths,omitempty"`
	ExpectExit                   *int         `yaml:"expect_exit" json:"expect_exit,omitempty"`
	AllowNonzeroWhenNoAllowOther bool         `yaml:"allow_nonzero_when_no_allow_other" json:"allow_nonzero_when_no_allow_other,omitempty"`
}

type FileSpec struct {
	RelativePath string `yaml:"relative_path" json:"relative_path"`
	Content      string `yaml:"content" json:"content"`
}

type MutationSpec struct {
	Path    string `yaml:"path" json:"path"`
	Mode    string `yaml:"mode" json:"mode"`
	Content string `yaml:"content" json:"content"`
}

type VerifySpec struct {
	Path   string `yaml:"path" json:"path"`
	SHA256 string `yaml:"sha256" json:"sha256"`
}

type OracleSpec struct {
	Type           string            `yaml:"type" json:"type"`
	Path           string            `yaml:"path" json:"path,omitempty"`
	ExpectedBytes  int64             `yaml:"expected_bytes" json:"expected_bytes,omitempty"`
	CommandID      string            `yaml:"command_id" json:"command_id,omitempty"`
	RemotePath     string            `yaml:"remote_path" json:"remote_path,omitempty"`
	SHA256         string            `yaml:"sha256" json:"sha256,omitempty"`
	Paths          []string          `yaml:"paths" json:"paths,omitempty"`
	ExpectedHashes map[string]string `yaml:"expected_hashes" json:"expected_hashes,omitempty"`
	Globs          []string          `yaml:"globs" json:"globs,omitempty"`
	ExpectedStatus string            `yaml:"expected_status" json:"expected_status,omitempty"`
	RepoPath       string            `yaml:"repo_path" json:"repo_path,omitempty"`
	ExpectedDelta  int               `yaml:"expected_delta" json:"expected_delta,omitempty"`
	ExpectedExit   *int              `yaml:"expected_exit" json:"expected_exit,omitempty"`
}

func LoadSuites(dir string, names []string) ([]Case, error) {
	var all []Case
	for _, name := range names {
		path := filepath.Join(dir, name+".yaml")
		cases, err := LoadFile(path)
		if err != nil {
			return nil, err
		}
		all = append(all, cases...)
	}
	return all, nil
}

func LoadFile(path string) ([]Case, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrParse, path, err)
	}
	var sf SuiteFile
	if err := yaml.Unmarshal(b, &sf); err != nil {
		return nil, fmt.Errorf("%w: unmarshal %s: %v", ErrParse, path, err)
	}
	if len(sf.Cases) == 0 {
		return nil, fmt.Errorf("%w: %s has no cases", ErrValidation, path)
	}
	for i := range sf.Cases {
		applyDefaults(&sf.Cases[i], sf.Defaults)
		sf.Cases[i].SourceFile = path
		if err := Validate(sf.Cases[i]); err != nil {
			return nil, fmt.Errorf("%w: %s case %q: %v", ErrValidation, path, sf.Cases[i].ID, err)
		}
	}
	return sf.Cases, nil
}

func applyDefaults(c *Case, d Defaults) {
	c.defaults = d
	if c.Timeout.Duration == 0 {
		c.Timeout = d.Timeout
	}
	if c.Cleanup == "" {
		c.Cleanup = d.Cleanup
	}
	if c.RequiresFresh == nil {
		c.RequiresFresh = d.RequiresFreshTenant
	}
}

func Validate(c Case) error {
	if c.ID == "" {
		return errors.New("id is required")
	}
	if !oneOf(c.Suite, "smoke", "regression", "stress", "fault") {
		return fmt.Errorf("invalid suite %q", c.Suite)
	}
	if !oneOf(c.ExpectedOutcome, "baseline_pass", "bug_reproduced", "fix_verified") {
		return fmt.Errorf("invalid expected_outcome %q", c.ExpectedOutcome)
	}
	if c.SyncMode != "" && !oneOf(c.SyncMode, "strict", "interactive", "auto") {
		return fmt.Errorf("invalid sync_mode %q", c.SyncMode)
	}
	if c.RemoteRootSuffix == "" || c.MountpointSuffix == "" {
		return errors.New("remote_root_suffix and mountpoint_suffix are required")
	}
	if c.Timeout.Duration <= 0 {
		return errors.New("timeout must be > 0")
	}
	if !oneOf(c.Cleanup, "always", "retain_on_failure", "never") {
		return fmt.Errorf("invalid cleanup %q", c.Cleanup)
	}
	if !oneOf(c.Severity.Failure, "P0", "P1", "P2", "P3") {
		return fmt.Errorf("invalid failure severity %q", c.Severity.Failure)
	}
	if err := validateWorkload(c.Workload); err != nil {
		return err
	}
	if len(c.Oracles) == 0 {
		return errors.New("at least one oracle is required")
	}
	for _, o := range c.Oracles {
		if err := validateOracle(c.Workload, o); err != nil {
			return err
		}
	}
	return nil
}

func ValidateRelativeSlashPath(field, p string) error {
	if p == "" {
		return fmt.Errorf("%s is required", field)
	}
	if strings.Contains(p, "\\") {
		return fmt.Errorf("%s %q must use slash separators", field, p)
	}
	if path.IsAbs(p) {
		return fmt.Errorf("%s %q must be relative", field, p)
	}
	parts := strings.Split(p, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("%s %q contains unsafe path segment", field, p)
		}
	}
	if clean := path.Clean(p); clean != p {
		return fmt.Errorf("%s %q is not clean", field, p)
	}
	return nil
}

func validateWorkload(w Workload) error {
	switch w.Type {
	case "mount_smoke":
		if len(w.Files) == 0 {
			return errors.New("mount_smoke requires files")
		}
		for _, f := range w.Files {
			if err := ValidateRelativeSlashPath("mount_smoke file relative_path", f.RelativePath); err != nil {
				return err
			}
		}
	case "path_matrix":
		if len(w.Paths) == 0 {
			return errors.New("path_matrix requires paths")
		}
		for _, p := range w.Paths {
			if err := ValidateRelativeSlashPath("path_matrix path", p); err != nil {
				return err
			}
		}
	case "dual_mount_visibility":
		if len(w.Files) == 0 {
			return errors.New("dual_mount_visibility requires files")
		}
		for _, f := range w.Files {
			if err := ValidateRelativeSlashPath("dual_mount_visibility file relative_path", f.RelativePath); err != nil {
				return err
			}
		}
	case "fio":
		if w.SizeBytes <= 0 {
			return errors.New("fio requires size_bytes")
		}
	case "small_file_storm":
		if w.FileCount <= 0 {
			return errors.New("small_file_storm requires file_count")
		}
	case "parallel_writes":
		if w.ParallelWriters <= 0 || w.WriterBytes <= 0 {
			return errors.New("parallel_writes requires parallel_writers and writer_bytes")
		}
	case "git_workflow":
		if w.CloneURL == "" || w.CloneDir == "" || w.Mutation.Path == "" || w.CommitMessage == "" {
			return errors.New("git_workflow requires clone_url, clone_dir, mutation.path, and commit_message")
		}
		if err := ValidateRelativeSlashPath("git_workflow clone_dir", w.CloneDir); err != nil {
			return err
		}
		if err := ValidateRelativeSlashPath("git_workflow mutation.path", w.Mutation.Path); err != nil {
			return err
		}
		if !oneOf(w.Mutation.Mode, "append", "overwrite") {
			return fmt.Errorf("invalid git mutation mode %q", w.Mutation.Mode)
		}
		if len(w.ExpectedLocks) != 0 {
			return errors.New("git_workflow expected_locks must encode correct behavior and be empty in phase 1")
		}
		for _, p := range w.RemountVerifyPaths {
			if err := ValidateRelativeSlashPath("git_workflow remount_verify_paths.path", p.Path); err != nil {
				return err
			}
		}
	case "doctor_fuse":
		if w.ExpectExit == nil {
			return errors.New("doctor_fuse requires expect_exit")
		}
	case "open_fd_unmount":
		if w.HoldOpenDuration.Duration < 0 {
			return errors.New("open_fd_unmount hold_open_duration must be >= 0")
		}
	case "kill_during_write":
		if w.WriterBytes <= 0 || w.KillAfter.Duration <= 0 {
			return errors.New("kill_during_write requires writer_bytes and kill_after")
		}
	default:
		return fmt.Errorf("invalid workload type %q", w.Type)
	}
	return nil
}

func validateOracle(w Workload, o OracleSpec) error {
	if o.Path != "" {
		if err := ValidateRelativeSlashPath("oracle path", o.Path); err != nil {
			return err
		}
	}
	if o.RemotePath != "" {
		if err := ValidateRelativeSlashPath("oracle remote_path", o.RemotePath); err != nil {
			return err
		}
	}
	for _, p := range o.Paths {
		if err := ValidateRelativeSlashPath("oracle paths", p); err != nil {
			return err
		}
	}
	for p := range o.ExpectedHashes {
		if err := ValidateRelativeSlashPath("oracle expected_hashes path", p); err != nil {
			return err
		}
	}
	for _, p := range o.Globs {
		if err := ValidateRelativeSlashPath("oracle globs", p); err != nil {
			return err
		}
	}
	if o.RepoPath != "" {
		if err := ValidateRelativeSlashPath("oracle repo_path", o.RepoPath); err != nil {
			return err
		}
	}
	switch o.Type {
	case "fuse_write_success":
		if w.Type != "mount_smoke" && w.Type != "path_matrix" && w.Type != "parallel_writes" && o.Path == "" {
			return errors.New("fuse_write_success needs path unless derived from workload")
		}
	case "cli_read_equals":
		if w.Type != "mount_smoke" && w.Type != "path_matrix" && w.Type != "small_file_storm" && o.RemotePath == "" {
			return errors.New("cli_read_equals needs remote_path unless derived from workload")
		}
	case "remount_hash_equal":
		if w.Type == "git_workflow" && len(w.RemountVerifyPaths) == 0 && len(o.Paths) == 0 {
			return errors.New("remount_hash_equal needs remount_verify_paths or explicit paths")
		}
		if w.Type != "mount_smoke" && w.Type != "path_matrix" && w.Type != "git_workflow" &&
			w.Type != "dual_mount_visibility" && w.Type != "small_file_storm" && w.Type != "parallel_writes" && len(o.Paths) == 0 {
			return errors.New("remount_hash_equal needs paths unless derived from workload")
		}
	case "no_git_locks", "git_status_equals", "git_commit_count":
		if w.Type != "git_workflow" {
			return fmt.Errorf("%s requires git_workflow", o.Type)
		}
	case "dual_mount_visibility":
		if w.Type != "dual_mount_visibility" {
			return errors.New("dual_mount_visibility oracle requires dual_mount_visibility workload")
		}
	case "throughput_min":
		if w.Type != "fio" && w.Type != "parallel_writes" {
			return errors.New("throughput_min oracle requires fio or parallel_writes workload")
		}
	case "file_count_equals":
		if w.Type != "small_file_storm" {
			return errors.New("file_count_equals oracle requires small_file_storm workload")
		}
	case "unmount_busy_then_clean":
		if w.Type != "open_fd_unmount" {
			return errors.New("unmount_busy_then_clean oracle requires open_fd_unmount workload")
		}
	case "recovery_classified":
		if w.Type != "kill_during_write" {
			return errors.New("recovery_classified oracle requires kill_during_write workload")
		}
	case "command_exit":
		if w.Type != "doctor_fuse" && o.CommandID == "" {
			return errors.New("command_exit needs command_id unless derived from workload")
		}
	default:
		return fmt.Errorf("invalid oracle type %q", o.Type)
	}
	return nil
}

func oneOf(v string, allowed ...string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}

func FilterCases(cases []Case, selected string) []Case {
	if strings.TrimSpace(selected) == "" {
		return cases
	}
	want := map[string]bool{}
	for _, s := range strings.Split(selected, ",") {
		want[strings.TrimSpace(s)] = true
	}
	var out []Case
	for _, c := range cases {
		if want[c.ID] {
			out = append(out, c)
		}
	}
	return out
}

func (c Case) RequiresFreshTenant() bool {
	return c.RequiresFresh != nil && *c.RequiresFresh
}

func (c Case) MountReadyTimeout() time.Duration {
	if c.defaults.MountReadyTimeout.Duration > 0 {
		return c.defaults.MountReadyTimeout.Duration
	}
	return 20 * time.Second
}

func (c Case) RemoteVisibilityTimeout() time.Duration {
	if c.Workload.ReadAfterWriteTimeout.Duration > 0 {
		return c.Workload.ReadAfterWriteTimeout.Duration
	}
	if c.defaults.RemoteVisibilityTimeout.Duration > 0 {
		return c.defaults.RemoteVisibilityTimeout.Duration
	}
	return 5 * time.Second
}

func (c Case) CommandRetryCount() int {
	if c.defaults.CommandRetryCount > 0 {
		return c.defaults.CommandRetryCount
	}
	return 1
}

func (c Case) CommandRetrySleep() time.Duration {
	if c.defaults.CommandRetrySleep.Duration > 0 {
		return c.defaults.CommandRetrySleep.Duration
	}
	return 250 * time.Millisecond
}
