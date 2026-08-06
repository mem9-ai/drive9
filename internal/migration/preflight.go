package migration

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/mem9-ai/drive9/pkg/client"
)

const ControlPrefix = "/.drive9-migration"

var ErrPreflight = errors.New("migration preflight failed")

type PreflightResult struct {
	VolumeID                string `json:"volume_id"`
	NodeName                string `json:"node_name"`
	SourceRoot              string `json:"source_root"`
	SpaceRef                string `json:"space_ref"`
	Prefix                  string `json:"prefix"`
	CredentialRef           string `json:"credential_ref"`
	ConfigHash              string `json:"config_hash"`
	ControlPrefix           string `json:"control_prefix"`
	EntryCount              int    `json:"entry_count"`
	DirectoryCount          int    `json:"directory_count"`
	LogicalBytes            int64  `json:"logical_bytes"`
	VolumeIdentityVerified  bool   `json:"volume_identity_verified"`
	RequiredCapabilities    bool   `json:"required_capabilities"`
	EventReportingAvailable bool   `json:"event_reporting_available"`
	MaxUploadBytes          int64  `json:"max_upload_bytes"`
	InlineThreshold         int64  `json:"inline_threshold"`
	TargetEmpty             bool   `json:"target_empty"`
	RecoveryControlPresent  bool   `json:"recovery_control_present"`
}

// ValidateMappings checks the complete batch before any selected-Job probe.
func ValidateMappings(cfg *Config) error {
	if cfg == nil {
		return errors.New("missing configuration")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	bySpace := make(map[string][]string)
	nodes := make(map[string]struct{})
	roots := make(map[string]struct{})
	for _, job := range cfg.Jobs {
		if _, exists := nodes[job.NodeName]; exists {
			return fmt.Errorf("duplicate node_name %q", job.NodeName)
		}
		nodes[job.NodeName] = struct{}{}
		if _, exists := roots[job.Source.Root]; exists {
			return fmt.Errorf("duplicate Source Root %q", job.Source.Root)
		}
		roots[job.Source.Root] = struct{}{}
		prefix, err := validateTargetPrefix(job.Target.Prefix)
		if err != nil {
			return fmt.Errorf("job %q: %w", job.VolumeID, err)
		}
		bySpace[job.Target.SpaceRef] = append(bySpace[job.Target.SpaceRef], prefix)
	}
	for space, prefixes := range bySpace {
		for i, left := range prefixes {
			if left == "/" && len(prefixes) > 1 {
				return fmt.Errorf("space %q root prefix cannot be shared", space)
			}
			for _, right := range prefixes[i+1:] {
				if prefixesOverlap(left, right) {
					return fmt.Errorf("space %q has overlapping prefixes %q and %q", space, left, right)
				}
			}
		}
	}
	return nil
}

func validateTargetPrefix(prefix string) (string, error) {
	if !utf8.ValidString(prefix) || norm.NFC.String(prefix) != prefix || !strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix {
		return "", fmt.Errorf("target prefix must be absolute, clean UTF-8 NFC")
	}
	if prefix == ControlPrefix || strings.HasPrefix(prefix, ControlPrefix+"/") {
		return "", fmt.Errorf("target prefix overlaps reserved control prefix")
	}
	return prefix, nil
}

func prefixesOverlap(left, right string) bool {
	return left == right || left == "/" || right == "/" || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

// Preflight performs the shared read-only plan/run gate for one local Job.
func Preflight(ctx context.Context, startup *Startup) (PreflightResult, error) {
	return preflightWithVerifier(ctx, startup, verifyMountedVolume)
}

func preflightWithVerifier(ctx context.Context, startup *Startup, verifyVolume func(string, string) (bool, error)) (PreflightResult, error) {
	if startup == nil || startup.Config == nil {
		return PreflightResult{}, fmt.Errorf("%w: missing startup snapshot", ErrPreflight)
	}
	if err := ValidateMappings(startup.Config); err != nil {
		return PreflightResult{}, fmt.Errorf("%w: static mapping: %v", ErrPreflight, err)
	}
	scanner, err := NewScanner(startup.Job.Source.Root)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: source: %v", ErrPreflight, err)
	}
	scan, err := scanner.Scan(ctx)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: source scan: %w", ErrPreflight, err)
	}
	for _, finding := range scan.Findings {
		if finding.Severity == SeverityBlocker {
			return PreflightResult{}, fmt.Errorf("%w: source blocker %s", ErrPreflight, finding.Kind)
		}
	}
	if startup.Job.Target.Prefix == "/" {
		for sourcePath := range scan.Entries {
			if sourcePath == ControlPrefix || strings.HasPrefix(sourcePath, ControlPrefix+"/") {
				return PreflightResult{}, fmt.Errorf("%w: source collides with reserved control prefix", ErrPreflight)
			}
		}
	}
	verified, err := verifyVolume(startup.Job.Source.Root, startup.Job.VolumeID)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: volume identity: %w", ErrPreflight, err)
	}
	key, err := startup.Credential.Read()
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: credential: %w", ErrPreflight, err)
	}
	api := client.New(startup.Config.Drive9.Endpoint, key)
	caps, err := api.GetMigrationCapabilities(ctx)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: capabilities: %w", ErrPreflight, err)
	}
	missing := missingCapabilities(caps)
	if missing != "" {
		return PreflightResult{}, fmt.Errorf("%w: required capability %s is unavailable", ErrPreflight, missing)
	}
	entries, err := api.ListCtx(ctx, listPath(startup.Job.Target.Prefix))
	if err != nil && !client.IsNotFound(err) {
		return PreflightResult{}, fmt.Errorf("%w: target listing: %w", ErrPreflight, err)
	}
	targetEmpty := len(entries) == 0
	controlPresent := false
	for _, entry := range entries {
		if startup.Job.Target.Prefix == "/" && strings.TrimSuffix(entry.Name, "/") == strings.TrimPrefix(ControlPrefix, "/") && entry.IsDir {
			controlPresent = true
			continue
		}
		return PreflightResult{}, fmt.Errorf("%w: target prefix is not empty", ErrPreflight)
	}
	return PreflightResult{
		VolumeID: startup.Job.VolumeID, NodeName: startup.Job.NodeName,
		SourceRoot: startup.Job.Source.Root, SpaceRef: startup.Job.Target.SpaceRef,
		Prefix: startup.Job.Target.Prefix, CredentialRef: startup.Space.CredentialRef,
		ConfigHash: startup.ConfigHash, ControlPrefix: ControlPrefix,
		EntryCount: scan.EntryCount, DirectoryCount: scan.DirectoryCount, LogicalBytes: scan.LogicalBytes,
		VolumeIdentityVerified: verified, RequiredCapabilities: true, EventReportingAvailable: caps.EventIngest,
		MaxUploadBytes: api.MaxUploadBytes(ctx), InlineThreshold: api.CachedSmallFileThreshold(),
		TargetEmpty: targetEmpty, RecoveryControlPresent: controlPresent,
	}, nil
}

func missingCapabilities(caps client.MigrationCapabilities) string {
	switch {
	case !caps.ChecksumRead:
		return "checksum_read"
	case !caps.ChecksumComplete:
		return "checksum_complete"
	case !caps.ConditionalCreate:
		return "conditional_create"
	case !caps.ConditionalUpdate:
		return "conditional_update"
	default:
		return ""
	}
}

func listPath(prefix string) string {
	if prefix == "/" {
		return prefix
	}
	return prefix + "/"
}

func verifyMountedVolume(root, volumeID string) (bool, error) {
	serial, available, err := platformVolumeSerial(root)
	if err != nil || !available {
		return false, err
	}
	normalize := func(value string) string {
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", ""))
	}
	if !strings.Contains(normalize(serial), normalize(volumeID)) {
		return false, fmt.Errorf("mounted volume serial does not match volume_id")
	}
	return true, nil
}
