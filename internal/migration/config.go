// Package migration implements the single-Job Drive9 Migration V1 worker.
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

	"gopkg.in/yaml.v3"
)

const (
	ConfigVersion               = "v3"
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
	// ErrInvalidPhase classifies illegal, ambiguous, or regressive startup phases.
	ErrInvalidPhase = errors.New("invalid migration phase")
	// ErrIllegalAction classifies a phase-incompatible control operation.
	ErrIllegalAction = errors.New("illegal migration action")
	// ErrControlUnavailable classifies an unavailable local Worker socket.
	ErrControlUnavailable = errors.New("migration control unavailable")
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

// Job is one stable EBS volume to Drive9 Space/Prefix mapping.
type Job struct {
	VolumeID string       `yaml:"volume_id" json:"volume_id"`
	NodeName string       `yaml:"node_name" json:"node_name"`
	Source   SourceConfig `yaml:"source" json:"source"`
	Target   TargetConfig `yaml:"target" json:"target"`
}

// Config is the strict version-v3 static startup configuration.
type Config struct {
	Version     string                 `yaml:"version" json:"version"`
	Drive9      Drive9Config           `yaml:"drive9" json:"drive9"`
	JobDefaults JobDefaults            `yaml:"job_defaults" json:"job_defaults"`
	Spaces      map[string]SpaceConfig `yaml:"spaces" json:"spaces"`
	Jobs        []Job                  `yaml:"jobs" json:"jobs"`
}

// LoadConfig decodes exactly one bounded YAML document and validates V3 fields.
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
	if len(c.Spaces) == 0 || len(c.Jobs) == 0 {
		return fmt.Errorf("config must declare at least one Space and Job")
	}
	for name, space := range c.Spaces {
		if name == "" {
			return fmt.Errorf("space name must not be empty")
		}
		if err := validateCredentialRef(space.CredentialRef); err != nil {
			return fmt.Errorf("space %q: %w", name, err)
		}
	}
	seenVolumes := make(map[string]struct{}, len(c.Jobs))
	for i := range c.Jobs {
		job := &c.Jobs[i]
		if !volumeIDPattern.MatchString(job.VolumeID) {
			return fmt.Errorf("job %d has invalid volume_id", i)
		}
		if _, exists := seenVolumes[job.VolumeID]; exists {
			return fmt.Errorf("duplicate volume_id %q", job.VolumeID)
		}
		seenVolumes[job.VolumeID] = struct{}{}
		if job.NodeName == "" {
			return fmt.Errorf("job %q has empty node_name", job.VolumeID)
		}
		if job.Source.Type != "ebs" || !filepath.IsAbs(job.Source.Root) || filepath.Clean(job.Source.Root) != job.Source.Root {
			return fmt.Errorf("job %q requires a clean absolute EBS source root", job.VolumeID)
		}
		if _, ok := c.Spaces[job.Target.SpaceRef]; !ok {
			return fmt.Errorf("job %q references unknown Space %q", job.VolumeID, job.Target.SpaceRef)
		}
		if !strings.HasPrefix(job.Target.Prefix, "/") || path.Clean(job.Target.Prefix) != job.Target.Prefix {
			return fmt.Errorf("job %q requires a clean absolute target prefix", job.VolumeID)
		}
	}
	return nil
}

// SelectJob resolves exactly one Job assigned to nodeName.
func (c *Config) SelectJob(nodeName string) (Job, error) {
	if nodeName == "" {
		return Job{}, fmt.Errorf("%s is required", MigrationNodeNameEnv)
	}
	var selected *Job
	for i := range c.Jobs {
		if c.Jobs[i].NodeName != nodeName {
			continue
		}
		if selected != nil {
			return Job{}, fmt.Errorf("node %q resolves more than one Job", nodeName)
		}
		selected = &c.Jobs[i]
	}
	if selected == nil {
		return Job{}, fmt.Errorf("node %q resolves no Job", nodeName)
	}
	return *selected, nil
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
	if desired != PhaseSyncing && desired != PhaseDualWriteRepairing {
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
	Config     *Config          `json:"config"`
	Job        Job              `json:"job"`
	Space      SpaceConfig      `json:"space"`
	Phase      Phase            `json:"phase"`
	ConfigHash string           `json:"config_hash"`
	Credential CredentialSource `json:"-"`
}

// ConfigHash hashes only one Job's normalized immutable configuration.
func ConfigHash(cfg *Config, job Job) (string, error) {
	space, ok := cfg.Spaces[job.Target.SpaceRef]
	if !ok {
		return "", fmt.Errorf("config hash references unknown Space %q", job.Target.SpaceRef)
	}
	body, err := json.Marshal(struct {
		Version     string       `json:"version"`
		Drive9      Drive9Config `json:"drive9"`
		JobDefaults JobDefaults  `json:"job_defaults"`
		Job         Job          `json:"job"`
		Space       SpaceConfig  `json:"space"`
	}{cfg.Version, cfg.Drive9, cfg.JobDefaults, job, space})
	if err != nil {
		return "", fmt.Errorf("marshal config hash input: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// LoadStartup resolves and validates one local Job without retaining its key.
func LoadStartup(configPath, nodeName, environmentPhase, credentialRoot string, highestApplied Phase) (*Startup, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	job, err := cfg.SelectJob(nodeName)
	if err != nil {
		return nil, err
	}
	phase, err := ReadStartupPhase(configPath, environmentPhase, highestApplied)
	if err != nil {
		return nil, err
	}
	space := cfg.Spaces[job.Target.SpaceRef]
	credential, err := NewCredentialSource(credentialRoot, space.CredentialRef)
	if err != nil {
		return nil, fmt.Errorf("resolve credential: %w", err)
	}
	if _, err := credential.Read(); err != nil {
		return nil, err
	}
	hash, err := ConfigHash(cfg, job)
	if err != nil {
		return nil, err
	}
	return &Startup{Config: cfg, Job: job, Space: space, Phase: phase, ConfigHash: hash, Credential: credential}, nil
}
