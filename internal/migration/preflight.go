package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const ControlPrefix = "/.drive9-migration"

var ErrPreflight = errors.New("migration preflight failed")

type PreflightResult struct {
	VolumeID                string  `json:"volume_id"`
	NodeName                string  `json:"node_name"`
	SourceRoot              string  `json:"source_root"`
	SpaceRef                string  `json:"space_ref"`
	Prefix                  string  `json:"prefix"`
	CredentialRef           string  `json:"credential_ref"`
	ConfigHash              string  `json:"config_hash"`
	ControlPrefix           string  `json:"control_prefix"`
	EntryCount              int     `json:"entry_count"`
	DirectoryCount          int     `json:"directory_count"`
	LogicalBytes            int64   `json:"logical_bytes"`
	RegularFileCount        int     `json:"regular_file_count"`
	LargestFileBytes        int64   `json:"largest_file_bytes"`
	InlineFileCount         int     `json:"inline_file_count"`
	InlineLogicalBytes      int64   `json:"inline_logical_bytes"`
	MultipartFileCount      int     `json:"multipart_file_count"`
	MultipartLogicalBytes   int64   `json:"multipart_logical_bytes"`
	SmallFileRatio          float64 `json:"small_file_ratio"`
	VolumeIdentityVerified  bool    `json:"volume_identity_verified"`
	RequiredCapabilities    bool    `json:"required_capabilities"`
	EventReportingAvailable bool    `json:"event_reporting_available"`
	MaxUploadBytes          int64   `json:"max_upload_bytes"`
	InlineThreshold         int64   `json:"inline_threshold"`
	TargetEmpty             bool    `json:"target_empty"`
	RecoveryControlPresent  bool    `json:"recovery_control_present"`
}

// ValidateMappings checks the complete batch before any selected-Job probe.
func ValidateMappings(cfg *Config) error {
	if cfg == nil {
		return errors.New("missing configuration")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	byCredential := make(map[string][]string)
	nodes := make(map[string]struct{})
	for _, job := range cfg.Jobs {
		if _, exists := nodes[job.NodeName]; exists {
			return fmt.Errorf("duplicate node_name %q", job.NodeName)
		}
		nodes[job.NodeName] = struct{}{}
		prefix, err := validateTargetPrefix(job.Target.Prefix)
		if err != nil {
			return fmt.Errorf("job %q: %w", job.VolumeID, err)
		}
		credentialRef := cfg.Spaces[job.Target.SpaceRef].CredentialRef
		byCredential[credentialRef] = append(byCredential[credentialRef], prefix)
	}
	for credentialRef, prefixes := range byCredential {
		for i, left := range prefixes {
			if left == "/" && len(prefixes) > 1 {
				return fmt.Errorf("credential_ref %q root prefix cannot be shared", credentialRef)
			}
			for _, right := range prefixes[i+1:] {
				if prefixesOverlap(left, right) {
					return fmt.Errorf("credential_ref %q has overlapping prefixes %q and %q", credentialRef, left, right)
				}
			}
		}
	}
	return nil
}

func validateTargetPrefix(prefix string) (string, error) {
	canonical, err := pathutil.Canonicalize(prefix)
	if err != nil || canonical != prefix {
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
	return preflightWithProbe(ctx, startup, sourceMountProbeFor(startup), nil)
}

func preflightWithVerifier(ctx context.Context, startup *Startup, verifyVolume func(string, string) (bool, error)) (PreflightResult, error) {
	return preflightWithChecks(ctx, startup, verifyVolume, nil)
}

func preflightWithChecks(ctx context.Context, startup *Startup, verifyVolume func(string, string) (bool, error), openFile func(*os.Root, string) (*os.File, error)) (PreflightResult, error) {
	probe := func(root, volumeID string) (sourceMountIdentity, error) {
		identity, err := observeSourceRoot(root)
		if err != nil {
			return sourceMountIdentity{}, err
		}
		identity.VolumeIdentityVerified, err = verifyVolume(root, volumeID)
		return identity, err
	}
	return preflightWithProbe(ctx, startup, probe, openFile)
}

func preflightWithProbe(ctx context.Context, startup *Startup, probe func(string, string) (sourceMountIdentity, error), openFile func(*os.Root, string) (*os.File, error)) (PreflightResult, error) {
	if startup == nil || startup.Config == nil {
		return PreflightResult{}, fmt.Errorf("%w: missing startup snapshot", ErrPreflight)
	}
	if err := ValidateMappings(startup.Config); err != nil {
		return PreflightResult{}, fmt.Errorf("%w: static mapping: %v", ErrPreflight, err)
	}
	initialMountIdentity, err := probe(startup.Job.Source.Root, startup.Job.VolumeID)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: initial volume identity: %w", ErrPreflight, err)
	}
	scanner, err := NewScanner(startup.Job.Source.Root)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: source: %v", ErrPreflight, err)
	}
	if openFile != nil {
		scanner.openFile = openFile
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
	if err := verifySourceReadAccess(ctx, scanner, scan); err != nil {
		return PreflightResult{}, fmt.Errorf("%w: source read access: %w", ErrPreflight, err)
	}
	if startup.Job.Target.Prefix == "/" {
		for sourcePath := range scan.Entries {
			if sourcePath == ControlPrefix || strings.HasPrefix(sourcePath, ControlPrefix+"/") {
				return PreflightResult{}, fmt.Errorf("%w: source collides with reserved control prefix", ErrPreflight)
			}
		}
	}
	mountIdentity, err := probe(startup.Job.Source.Root, startup.Job.VolumeID)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: final volume identity: %w", ErrPreflight, err)
	}
	if mountIdentity != initialMountIdentity {
		return PreflightResult{}, fmt.Errorf("%w: volume identity changed during source validation: %w", ErrPreflight, ErrSourceMountChanged)
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
	maxUploadBytes, inlineThreshold := api.MaxUploadBytes(ctx), api.CachedSmallFileThreshold()
	distribution, err := fileDistribution(scan, maxUploadBytes, inlineThreshold)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: %w", ErrPreflight, err)
	}
	checkpoint, err := NewCheckpointStore(api).Load(ctx, startup.Job.VolumeID)
	if err != nil {
		return PreflightResult{}, fmt.Errorf("%w: checkpoint: %w", ErrPreflight, err)
	}
	recoveryControlPresent := checkpoint.Revision > 0
	if recoveryControlPresent {
		desired := checkpointFromStartup(startup)
		if !sameCheckpointIdentity(checkpoint.Checkpoint, desired) {
			return PreflightResult{}, fmt.Errorf("%w: checkpoint: %w: immutable job identity", ErrPreflight, ErrCheckpointMismatch)
		}
		if !checkpoint.Checkpoint.FenceIntent && phaseRank(startup.Phase) < phaseRank(checkpoint.Checkpoint.HighestPhase) {
			return PreflightResult{}, fmt.Errorf("%w: checkpoint: %w: requested phase rollback", ErrPreflight, ErrCheckpointMismatch)
		}
	}
	entries, err := api.ListCtx(ctx, listPath(startup.Job.Target.Prefix))
	if err != nil && !client.IsNotFound(err) {
		return PreflightResult{}, fmt.Errorf("%w: target listing: %w", ErrPreflight, err)
	}
	targetEmpty := true
	for _, entry := range entries {
		if startup.Job.Target.Prefix == "/" && strings.TrimSuffix(entry.Name, "/") == strings.TrimPrefix(ControlPrefix, "/") && entry.IsDir {
			continue
		}
		targetEmpty = false
	}
	if !targetEmpty && !recoveryControlPresent {
		return PreflightResult{}, fmt.Errorf("%w: target prefix is not empty and no matching checkpoint exists", ErrPreflight)
	}
	startup.acceptedSource = initialMountIdentity
	return PreflightResult{
		VolumeID: startup.Job.VolumeID, NodeName: startup.Job.NodeName,
		SourceRoot: startup.Job.Source.Root, SpaceRef: startup.Job.Target.SpaceRef,
		Prefix: startup.Job.Target.Prefix, CredentialRef: startup.Space.CredentialRef,
		ConfigHash: startup.ConfigHash, ControlPrefix: ControlPrefix,
		EntryCount: scan.EntryCount, DirectoryCount: scan.DirectoryCount, LogicalBytes: scan.LogicalBytes,
		RegularFileCount: distribution.regularFiles, LargestFileBytes: distribution.largestFileBytes,
		InlineFileCount: distribution.inlineFiles, InlineLogicalBytes: distribution.inlineBytes,
		MultipartFileCount: distribution.multipartFiles, MultipartLogicalBytes: distribution.multipartBytes,
		SmallFileRatio:         distribution.smallFileRatio,
		VolumeIdentityVerified: mountIdentity.VolumeIdentityVerified, RequiredCapabilities: true, EventReportingAvailable: caps.EventIngest,
		MaxUploadBytes: maxUploadBytes, InlineThreshold: inlineThreshold,
		TargetEmpty: targetEmpty, RecoveryControlPresent: recoveryControlPresent,
	}, nil
}

func verifySourceReadAccess(ctx context.Context, scanner *Scanner, scan ScanResult) error {
	for _, sourcePath := range sortedSourcePaths(scan.Entries) {
		entry := scan.Entries[sourcePath]
		if entry.Kind != EntryRegular {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		file, err := scanner.openStableSource(sourceLocalPath(entry), entry.Version)
		if err != nil {
			return err
		}
		if err := file.close(); err != nil {
			return err
		}
	}
	return nil
}

type observedFileDistribution struct {
	regularFiles, inlineFiles, multipartFiles     int
	largestFileBytes, inlineBytes, multipartBytes int64
	smallFileRatio                                float64
}

func fileDistribution(scan ScanResult, maxUploadBytes, inlineThreshold int64) (observedFileDistribution, error) {
	var result observedFileDistribution
	for _, entry := range scan.Entries {
		if entry.Kind != EntryRegular {
			continue
		}
		result.regularFiles++
		result.largestFileBytes = max(result.largestFileBytes, entry.Version.Size)
		if entry.Version.Size > maxUploadBytes {
			return observedFileDistribution{}, fmt.Errorf("regular file exceeds max_upload_bytes")
		}
		if entry.Version.Size == 0 || entry.Version.Size < inlineThreshold {
			result.inlineFiles++
			result.inlineBytes += entry.Version.Size
		} else {
			result.multipartFiles++
			result.multipartBytes += entry.Version.Size
		}
	}
	if result.regularFiles > 0 {
		result.smallFileRatio = float64(result.inlineFiles) / float64(result.regularFiles)
	}
	return result, nil
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
