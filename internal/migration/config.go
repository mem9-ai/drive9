// Package migration implements Drive9 Migration V1 EBS subpath workers.
package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"gopkg.in/yaml.v3"
)

const (
	ConfigVersion               = "v4"
	DefaultGracePeriod          = time.Minute
	DefaultCredentialRoot       = "/var/run/secrets/drive9-migration"
	MigrationNodeNameEnv        = "DRIVE9_MIGRATION_NODE_NAME"
	MigrationPhaseEnv           = "DRIVE9_MIGRATION_PHASE"
	minimumGracePeriod          = 30 * time.Second
	maximumGracePeriod          = 10 * time.Minute
	maximumConfigBytes    int64 = 1 << 20
)

var (
	volumeIDPattern      = regexp.MustCompile(`^vol-[0-9a-f]+$`)
	credentialRefPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	jobIDPattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	// ErrInvalidPhase classifies illegal, ambiguous, or regressive startup phases.
	ErrInvalidPhase = errors.New("invalid migration phase")
	// ErrIllegalAction classifies a phase-incompatible control operation.
	ErrIllegalAction = errors.New("illegal migration action")
	// ErrControlUnavailable classifies an unavailable local Worker socket.
	ErrControlUnavailable = errors.New("migration control unavailable")
	// ErrControlOutcomeUnknown means a mutation was accepted but its terminal response was lost.
	ErrControlOutcomeUnknown = errors.New("migration control outcome is unknown")
)

// Duration is a strict YAML duration.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(value)
	return nil
}

type Drive9Config struct {
	Endpoint string `yaml:"endpoint" json:"endpoint"`
}

type SyncDefaults struct {
	GracePeriod Duration `yaml:"grace_period,omitempty" json:"grace_period"`
}

type PerformanceDefaults struct {
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second" json:"max_bytes_per_second"`
	SmallFileWorkers  int   `yaml:"small_file_workers" json:"small_file_workers"`
	LargeFileWorkers  int   `yaml:"large_file_workers" json:"large_file_workers"`
}

type JobDefaults struct {
	Sync        SyncDefaults        `yaml:"sync" json:"sync"`
	Performance PerformanceDefaults `yaml:"performance" json:"performance"`
}

type SpaceConfig struct {
	CredentialRef string `yaml:"credential_ref" json:"credential_ref"`
}

type SourceConfig struct {
	Type string `yaml:"type" json:"type"`
	Root string `yaml:"root" json:"root"`
}

type TargetConfig struct {
	SpaceRef string `yaml:"space_ref" json:"space_ref"`
	Prefix   string `yaml:"prefix" json:"prefix"`
}

// JobConfig is one configured EBS subpath to Drive9 target mapping.
type JobConfig struct {
	JobID   string       `yaml:"job_id" json:"job_id"`
	Subpath string       `yaml:"subpath" json:"subpath"`
	Target  TargetConfig `yaml:"target" json:"target"`
}

// EBSSourceConfig groups all independent Jobs beneath one mounted EBS root.
type EBSSourceConfig struct {
	VolumeID string      `yaml:"volume_id" json:"volume_id"`
	NodeName string      `yaml:"node_name" json:"node_name"`
	Root     string      `yaml:"root" json:"root"`
	Jobs     []JobConfig `yaml:"jobs" json:"jobs"`
}

// Job is one resolved EBS subpath to Drive9 Space/Prefix mapping.
type Job struct {
	JobID    string       `json:"job_id"`
	VolumeID string       `json:"volume_id"`
	NodeName string       `json:"node_name"`
	EBSRoot  string       `json:"ebs_root"`
	Subpath  string       `json:"subpath"`
	Source   SourceConfig `json:"source"`
	Target   TargetConfig `json:"target"`
}

// Config is the strict version-v4 static startup configuration.
type Config struct {
	Version     string                 `yaml:"version" json:"version"`
	Drive9      Drive9Config           `yaml:"drive9" json:"drive9"`
	JobDefaults JobDefaults            `yaml:"job_defaults" json:"job_defaults"`
	Spaces      map[string]SpaceConfig `yaml:"spaces" json:"spaces"`
	EBSSources  []EBSSourceConfig      `yaml:"ebs_sources" json:"ebs_sources"`
	Jobs        []Job                  `yaml:"-" json:"-"`
}

// LoadConfig decodes exactly one bounded YAML document and validates V4 fields.
func LoadConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("open migration config: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat migration config: %w", err)
	}
	if info.Size() > maximumConfigBytes {
		return nil, fmt.Errorf("migration config exceeds %d bytes", maximumConfigBytes)
	}

	decoder := yaml.NewDecoder(io.LimitReader(file, maximumConfigBytes+1))
	decoder.KnownFields(true)
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode migration config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("multiple YAML documents")
		}
		return nil, fmt.Errorf("decode migration config trailing data: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if c.Version != ConfigVersion {
		return fmt.Errorf("config version must be %q", ConfigVersion)
	}
	endpoint, err := url.Parse(c.Drive9.Endpoint)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return fmt.Errorf("drive9 endpoint must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	c.Drive9.Endpoint = strings.TrimRight(c.Drive9.Endpoint, "/")
	if c.JobDefaults.Sync.GracePeriod == 0 {
		c.JobDefaults.Sync.GracePeriod = Duration(DefaultGracePeriod)
	}
	grace := time.Duration(c.JobDefaults.Sync.GracePeriod)
	if grace < minimumGracePeriod || grace > maximumGracePeriod {
		return fmt.Errorf("grace_period must be between %s and %s", minimumGracePeriod, maximumGracePeriod)
	}
	performance := c.JobDefaults.Performance
	if performance.MaxBytesPerSecond <= 0 || performance.SmallFileWorkers <= 0 || performance.LargeFileWorkers <= 0 {
		return fmt.Errorf("job performance limits and worker counts must be positive")
	}
	if len(c.Spaces) == 0 || len(c.EBSSources) == 0 {
		return fmt.Errorf("config must declare at least one Space and EBS source")
	}
	for name, space := range c.Spaces {
		if name == "" {
			return fmt.Errorf("space name must not be empty")
		}
		if err := validateCredentialRef(space.CredentialRef); err != nil {
			return fmt.Errorf("space %q: %w", name, err)
		}
	}
	seenVolumes := make(map[string]struct{}, len(c.EBSSources))
	seenNodes := make(map[string]struct{}, len(c.EBSSources))
	seenJobs := make(map[string]struct{})
	resolved := make([]Job, 0)
	for sourceIndex := range c.EBSSources {
		source := &c.EBSSources[sourceIndex]
		if !volumeIDPattern.MatchString(source.VolumeID) {
			return fmt.Errorf("EBS source %d has invalid volume_id", sourceIndex)
		}
		if _, exists := seenVolumes[source.VolumeID]; exists {
			return fmt.Errorf("duplicate volume_id %q", source.VolumeID)
		}
		seenVolumes[source.VolumeID] = struct{}{}
		if source.NodeName == "" {
			return fmt.Errorf("EBS source %q has empty node_name", source.VolumeID)
		}
		if _, exists := seenNodes[source.NodeName]; exists {
			return fmt.Errorf("duplicate node_name %q", source.NodeName)
		}
		seenNodes[source.NodeName] = struct{}{}
		if !filepath.IsAbs(source.Root) || filepath.Clean(source.Root) != source.Root {
			return fmt.Errorf("EBS source %q requires a clean absolute root", source.VolumeID)
		}
		if len(source.Jobs) == 0 {
			return fmt.Errorf("EBS source %q must declare at least one Job", source.VolumeID)
		}
		for jobIndex := range source.Jobs {
			configured := &source.Jobs[jobIndex]
			if err := validateJobID(configured.JobID); err != nil {
				return fmt.Errorf("EBS source %q Job %d: %w", source.VolumeID, jobIndex, err)
			}
			if _, exists := seenJobs[configured.JobID]; exists {
				return fmt.Errorf("duplicate job_id %q", configured.JobID)
			}
			seenJobs[configured.JobID] = struct{}{}
			if err := validateSourceSubpath(configured.Subpath); err != nil {
				return fmt.Errorf("job %q: %w", configured.JobID, err)
			}
			if _, ok := c.Spaces[configured.Target.SpaceRef]; !ok {
				return fmt.Errorf("job %q references unknown Space %q", configured.JobID, configured.Target.SpaceRef)
			}
			if !strings.HasPrefix(configured.Target.Prefix, "/") || path.Clean(configured.Target.Prefix) != configured.Target.Prefix {
				return fmt.Errorf("job %q requires a clean absolute target prefix", configured.JobID)
			}
			for prior := range jobIndex {
				if prefixesOverlap(source.Jobs[prior].Subpath, configured.Subpath) {
					return fmt.Errorf("EBS source %q has overlapping source subpaths %q and %q", source.VolumeID, source.Jobs[prior].Subpath, configured.Subpath)
				}
			}
			resolved = append(resolved, resolveJob(*source, *configured))
		}
	}
	c.Jobs = resolved
	return nil
}

func validateJobID(jobID string) error {
	if !jobIDPattern.MatchString(jobID) || jobID == "." || jobID == ".." {
		return fmt.Errorf("invalid job_id")
	}
	return nil
}

func validateSourceSubpath(subpath string) error {
	canonical, err := pathutil.Canonicalize(subpath)
	if err != nil || canonical != subpath {
		return fmt.Errorf("requires an absolute clean UTF-8 NFC subpath")
	}
	return nil
}

func effectiveSourceRoot(root, subpath string) string {
	if subpath == "/" {
		return root
	}
	return filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(subpath, "/")))
}

func resolveJob(source EBSSourceConfig, configured JobConfig) Job {
	return Job{
		JobID: configured.JobID, VolumeID: source.VolumeID, NodeName: source.NodeName,
		EBSRoot: source.Root, Subpath: configured.Subpath,
		Source: SourceConfig{Type: "ebs", Root: effectiveSourceRoot(source.Root, configured.Subpath)},
		Target: configured.Target,
	}
}

// SelectSource resolves exactly one EBS Source assigned to nodeName.
func (c *Config) SelectSource(nodeName string) (EBSSourceConfig, error) {
	if nodeName == "" {
		return EBSSourceConfig{}, fmt.Errorf("%s is required", MigrationNodeNameEnv)
	}
	var selected *EBSSourceConfig
	for i := range c.EBSSources {
		if c.EBSSources[i].NodeName != nodeName {
			continue
		}
		if selected != nil {
			return EBSSourceConfig{}, fmt.Errorf("node %q resolves more than one EBS source", nodeName)
		}
		selected = &c.EBSSources[i]
	}
	if selected == nil {
		return EBSSourceConfig{}, fmt.Errorf("node %q resolves no EBS source", nodeName)
	}
	return *selected, nil
}

// SelectJob retains the single-Job helper for Job-local tests and callers.
func (c *Config) SelectJob(nodeName string) (Job, error) {
	source, err := c.SelectSource(nodeName)
	if err != nil {
		return Job{}, err
	}
	if len(source.Jobs) != 1 {
		return Job{}, fmt.Errorf("node %q resolves %d Jobs", nodeName, len(source.Jobs))
	}
	return resolveJob(source, source.Jobs[0]), nil
}

// Phase is the non-regressible per-Job migration phase.
type Phase string

const (
	PhaseSyncing            Phase = "SYNCING"
	PhaseDualWriteRepairing Phase = "DUAL_WRITE_REPAIRING"
	PhaseCutoverReady       Phase = "CUTOVER_READY"
)

func phaseRank(phase Phase) int {
	switch phase {
	case PhaseSyncing:
		return 1
	case PhaseDualWriteRepairing:
		return 2
	case PhaseCutoverReady:
		return 3
	default:
		return 0
	}
}

// ReadStartupPhase chooses exactly one startup source and rejects rollback.
func ReadStartupPhase(configPath, environmentValue string, highestApplied Phase) (Phase, error) {
	phasePath := filepath.Join(filepath.Dir(configPath), "phase")
	_, statErr := os.Stat(phasePath)
	fileExists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return "", fmt.Errorf("%w: inspect phase file: %v", ErrInvalidPhase, statErr)
	}
	if fileExists == (environmentValue != "") {
		return "", fmt.Errorf("%w: exactly one phase file or %s is required", ErrInvalidPhase, MigrationPhaseEnv)
	}
	value := environmentValue
	if fileExists {
		body, err := os.ReadFile(phasePath)
		if err != nil {
			return "", fmt.Errorf("%w: read phase file: %v", ErrInvalidPhase, err)
		}
		value = strings.TrimSpace(string(body))
	}
	desired := Phase(value)
	if desired != PhaseSyncing && desired != PhaseDualWriteRepairing && desired != PhaseCutoverReady {
		return "", fmt.Errorf("%w: unsupported startup value %q", ErrInvalidPhase, value)
	}
	if highestApplied != "" && phaseRank(highestApplied) == 0 {
		return "", fmt.Errorf("%w: corrupt highest applied phase", ErrInvalidPhase)
	}
	if phaseRank(desired) < phaseRank(highestApplied) {
		return "", fmt.Errorf("%w: phase rollback from %s to %s", ErrInvalidPhase, highestApplied, desired)
	}
	return desired, nil
}

// CredentialSource rereads one projected Secret file on every call.
type CredentialSource struct {
	path string
}

func validateCredentialRef(ref string) error {
	if !credentialRefPattern.MatchString(ref) || ref == "." || ref == ".." {
		return fmt.Errorf("invalid credential_ref")
	}
	return nil
}

// NewCredentialSource confines credential_ref to one Secret Volume root.
func NewCredentialSource(root, ref string) (CredentialSource, error) {
	if err := validateCredentialRef(ref); err != nil {
		return CredentialSource{}, err
	}
	return CredentialSource{path: filepath.Join(filepath.Clean(root), ref)}, nil
}

// Read loads the current non-empty key without retaining it in the source.
func (s CredentialSource) Read() (string, error) {
	body, err := os.ReadFile(s.path)
	if err != nil {
		return "", fmt.Errorf("read credential %q: %w", filepath.Base(s.path), err)
	}
	key := strings.TrimSpace(string(body))
	if key == "" {
		return "", fmt.Errorf("credential %q is empty", filepath.Base(s.path))
	}
	return key, nil
}

// Startup is a secret-free immutable startup snapshot plus a reloadable source.
type Startup struct {
	Config         *Config          `json:"config"`
	Job            Job              `json:"job"`
	Space          SpaceConfig      `json:"space"`
	Phase          Phase            `json:"phase"`
	ConfigHash     string           `json:"config_hash"`
	Credential     CredentialSource `json:"-"`
	acceptedSource sourceMountIdentity
	mountProbe     sourceMountProbe
}

// RuntimeStartup is the secret-free startup snapshot for one EBS process.
type RuntimeStartup struct {
	Config *Config         `json:"config"`
	Source EBSSourceConfig `json:"source"`
	Phase  Phase           `json:"phase"`
	Jobs   []*Startup      `json:"jobs"`
}

// ConfigHash hashes only one Job's normalized immutable configuration.
func ConfigHash(cfg *Config, job Job) (string, error) {
	space, ok := cfg.Spaces[job.Target.SpaceRef]
	if !ok {
		return "", fmt.Errorf("config hash references unknown Space %q", job.Target.SpaceRef)
	}
	type stableJob struct {
		JobID    string       `json:"job_id"`
		VolumeID string       `json:"volume_id"`
		EBSRoot  string       `json:"ebs_root"`
		Subpath  string       `json:"subpath"`
		Source   SourceConfig `json:"source"`
		Target   TargetConfig `json:"target"`
	}
	body, err := json.Marshal(struct {
		Version     string       `json:"version"`
		Drive9      Drive9Config `json:"drive9"`
		JobDefaults JobDefaults  `json:"job_defaults"`
		Job         stableJob    `json:"job"`
		Space       SpaceConfig  `json:"space"`
	}{
		Version: cfg.Version, Drive9: cfg.Drive9, JobDefaults: cfg.JobDefaults,
		Job: stableJob{
			JobID: job.JobID, VolumeID: job.VolumeID, EBSRoot: job.EBSRoot,
			Subpath: job.Subpath, Source: job.Source, Target: job.Target,
		},
		Space: space,
	})
	if err != nil {
		return "", fmt.Errorf("marshal config hash input: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// LoadRuntimeStartup resolves all Jobs for one local EBS without retaining keys.
func LoadRuntimeStartup(configPath, nodeName, environmentPhase, credentialRoot string, highestApplied Phase) (*RuntimeStartup, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	source, err := cfg.SelectSource(nodeName)
	if err != nil {
		return nil, err
	}
	phase, err := ReadStartupPhase(configPath, environmentPhase, highestApplied)
	if err != nil {
		return nil, err
	}
	runtime := &RuntimeStartup{Config: cfg, Source: source, Phase: phase, Jobs: make([]*Startup, 0, len(source.Jobs))}
	for _, configured := range source.Jobs {
		job := resolveJob(source, configured)
		space := cfg.Spaces[job.Target.SpaceRef]
		credential, err := NewCredentialSource(credentialRoot, space.CredentialRef)
		if err != nil {
			return nil, fmt.Errorf("resolve credential for Job %q: %w", job.JobID, err)
		}
		hash, err := ConfigHash(cfg, job)
		if err != nil {
			return nil, err
		}
		runtime.Jobs = append(runtime.Jobs, &Startup{
			Config: cfg, Job: job, Space: space, Phase: phase,
			ConfigHash: hash, Credential: credential,
		})
	}
	return runtime, nil
}

// LoadStartup resolves one local Job for Job-local tests and callers.
func LoadStartup(configPath, nodeName, environmentPhase, credentialRoot string, highestApplied Phase) (*Startup, error) {
	runtime, err := LoadRuntimeStartup(configPath, nodeName, environmentPhase, credentialRoot, highestApplied)
	if err != nil {
		return nil, err
	}
	if len(runtime.Jobs) != 1 {
		return nil, fmt.Errorf("node %q resolves %d Jobs", nodeName, len(runtime.Jobs))
	}
	startup := runtime.Jobs[0]
	if _, err := startup.Credential.Read(); err != nil {
		return nil, err
	}
	return startup, nil
}
