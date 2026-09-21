// Package promotion defines the immutable contracts used by local-to-remote
// persistence promotion.
package promotion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

var (
	ErrInvalidManifest           = errors.New("invalid promotion manifest")
	ErrManifestLimitExceeded     = errors.New("promotion manifest limit exceeded")
	ErrStorageBackendUnsupported = errors.New("promotion storage backend unsupported")
	ErrInvalidPlan               = errors.New("invalid promotion plan")
	ErrPlanExpired               = errors.New("promotion plan expired")
	ErrPlanStale                 = errors.New("promotion plan capability is stale")
	ErrInvalidMigrationID        = errors.New("invalid promotion migration id")
	ErrInvalidAllocationProof    = errors.New("invalid promotion allocation proof")
)

// EntryType is the kind of one immutable manifest entry.
type EntryType string

const (
	EntryTypeFile      EntryType = "file"
	EntryTypeDirectory EntryType = "directory"
	EntryTypeSymlink   EntryType = "symlink"
)

// ManifestLimits are the transaction and namespace limits captured in a plan.
// Every value must be positive. InlineThreshold is an exclusive file-size
// bound, matching Dat9Backend.shouldStoreInDB.
type ManifestLimits struct {
	MaxEntries          uint64 `json:"max_entries"`
	MaxMetadataBytes    uint64 `json:"max_metadata_bytes"`
	MaxTotalInlineBytes uint64 `json:"max_total_inline_bytes"`
	MaxPathBytes        uint64 `json:"max_path_bytes"`
	MaxSymlinkBytes     uint64 `json:"max_symlink_bytes"`
	MaxDepth            uint64 `json:"max_depth"`
	InlineThreshold     uint64 `json:"inline_threshold"`
}

// ManifestEntry is one canonical path tuple. RelativePath never begins or
// ends with '/'. EntryHash authenticates every field other than itself.
type ManifestEntry struct {
	RelativePath           string    `json:"relative_path"`
	Type                   EntryType `json:"type"`
	Mode                   uint32    `json:"mode"`
	MtimeNS                int64     `json:"mtime_ns"`
	SymlinkTarget          string    `json:"symlink_target"`
	ExpectedSizeBytes      uint64    `json:"expected_size_bytes"`
	ExpectedChecksumSHA256 string    `json:"expected_checksum_sha256"`
	EntryHash              string    `json:"entry_hash"`
}

// CanonicalManifest is a validated, strictly path-ordered manifest plus the
// summary bound into PlanImport and CreateImport.
type CanonicalManifest struct {
	Entries        []ManifestEntry `json:"entries"`
	ManifestHash   string          `json:"manifest_hash"`
	EntryTotal     uint64          `json:"entry_total"`
	ByteTotal      uint64          `json:"byte_total"`
	MaxContentSize uint64          `json:"max_content_size"`
	MetadataBytes  uint64          `json:"metadata_bytes"`
}

type entryHashInput struct {
	RelativePath           string    `json:"relative_path"`
	Type                   EntryType `json:"type"`
	Mode                   uint32    `json:"mode"`
	MtimeNS                int64     `json:"mtime_ns"`
	SymlinkTarget          string    `json:"symlink_target"`
	ExpectedSizeBytes      uint64    `json:"expected_size_bytes"`
	ExpectedChecksumSHA256 string    `json:"expected_checksum_sha256"`
}

// CanonicalizeManifest normalizes entry paths, computes entry hashes, sorts by
// canonical relative path, and validates the complete tree. It is intended for
// the local coordinator before it sends a canonical manifest to PlanImport.
func CanonicalizeManifest(entries []ManifestEntry, limits ManifestLimits) (*CanonicalManifest, error) {
	normalized := make([]ManifestEntry, len(entries))
	for i := range entries {
		entry := entries[i]
		path, err := CanonicalRelativePath(entry.RelativePath)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d path: %v", ErrInvalidManifest, i, err)
		}
		entry.RelativePath = path
		entry.EntryHash = ""
		normalized[i] = entry
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].RelativePath < normalized[j].RelativePath
	})
	for i := range normalized {
		hash, err := hashEntry(normalized[i])
		if err != nil {
			return nil, err
		}
		normalized[i].EntryHash = hash
	}
	return validateManifest(normalized, limits, true)
}

// ValidateCanonicalManifest validates the exact manifest bytes accepted by the
// server. It deliberately rejects non-canonical paths, ordering, and hashes
// instead of silently rewriting them, so PlanImport and CreateImport bind the
// same representation.
func ValidateCanonicalManifest(entries []ManifestEntry, limits ManifestLimits) (*CanonicalManifest, error) {
	return validateManifest(entries, limits, false)
}

func validateManifest(entries []ManifestEntry, limits ManifestLimits, alreadyNormalized bool) (*CanonicalManifest, error) {
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	if uint64(len(entries)) > limits.MaxEntries {
		return nil, fmt.Errorf("%w: entries %d exceed %d", ErrManifestLimitExceeded, len(entries), limits.MaxEntries)
	}

	canonical := make([]ManifestEntry, len(entries))
	seen := make(map[string]EntryType, len(entries))
	var byteTotal, maxContent uint64
	for i := range entries {
		entry := entries[i]
		path, err := CanonicalRelativePath(entry.RelativePath)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d path: %v", ErrInvalidManifest, i, err)
		}
		if !alreadyNormalized && path != entry.RelativePath {
			return nil, fmt.Errorf("%w: entry %d path %q is not canonical", ErrInvalidManifest, i, entry.RelativePath)
		}
		entry.RelativePath = path
		if i > 0 && entries[i-1].RelativePath >= entry.RelativePath {
			return nil, fmt.Errorf("%w: entries are not in strict canonical path order", ErrInvalidManifest)
		}
		if _, ok := seen[path]; ok {
			return nil, fmt.Errorf("%w: duplicate path %q", ErrInvalidManifest, path)
		}
		if uint64(len(path)) > limits.MaxPathBytes {
			return nil, fmt.Errorf("%w: path %q exceeds %d bytes", ErrManifestLimitExceeded, path, limits.MaxPathBytes)
		}
		if depth := uint64(strings.Count(path, "/") + 1); depth > limits.MaxDepth {
			return nil, fmt.Errorf("%w: path %q depth %d exceeds %d", ErrManifestLimitExceeded, path, depth, limits.MaxDepth)
		}
		if entry.Mode&^uint32(0o7777) != 0 {
			return nil, fmt.Errorf("%w: path %q mode %#o contains file-type bits", ErrInvalidManifest, path, entry.Mode)
		}
		if err := validateEntryTypeFields(entry, limits); err != nil {
			return nil, err
		}
		if parent := relativeParent(path); parent != "" {
			parentType, ok := seen[parent]
			if !ok {
				return nil, fmt.Errorf("%w: path %q has missing parent %q", ErrInvalidManifest, path, parent)
			}
			if parentType != EntryTypeDirectory {
				return nil, fmt.Errorf("%w: path %q parent %q is not a directory", ErrInvalidManifest, path, parent)
			}
		}
		expectedHash, err := hashEntry(entry)
		if err != nil {
			return nil, err
		}
		if !alreadyNormalized && entry.EntryHash != expectedHash {
			return nil, fmt.Errorf("%w: path %q entry hash mismatch", ErrInvalidManifest, path)
		}
		entry.EntryHash = expectedHash
		canonical[i] = entry
		seen[path] = entry.Type
		if entry.Type == EntryTypeFile {
			if ^uint64(0)-byteTotal < entry.ExpectedSizeBytes {
				return nil, fmt.Errorf("%w: byte total overflows", ErrManifestLimitExceeded)
			}
			byteTotal += entry.ExpectedSizeBytes
			if entry.ExpectedSizeBytes > maxContent {
				maxContent = entry.ExpectedSizeBytes
			}
		}
	}
	if byteTotal > limits.MaxTotalInlineBytes {
		return nil, fmt.Errorf("%w: inline bytes %d exceed %d", ErrManifestLimitExceeded, byteTotal, limits.MaxTotalInlineBytes)
	}

	raw, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical manifest: %w", err)
	}
	if uint64(len(raw)) > limits.MaxMetadataBytes {
		return nil, fmt.Errorf("%w: metadata bytes %d exceed %d", ErrManifestLimitExceeded, len(raw), limits.MaxMetadataBytes)
	}
	return &CanonicalManifest{
		Entries:        canonical,
		ManifestHash:   hashHex(raw),
		EntryTotal:     uint64(len(canonical)),
		ByteTotal:      byteTotal,
		MaxContentSize: maxContent,
		MetadataBytes:  uint64(len(raw)),
	}, nil
}

func validateLimits(limits ManifestLimits) error {
	if limits.MaxEntries == 0 || limits.MaxMetadataBytes == 0 || limits.MaxTotalInlineBytes == 0 ||
		limits.MaxPathBytes == 0 || limits.MaxSymlinkBytes == 0 || limits.MaxDepth == 0 || limits.InlineThreshold == 0 {
		return fmt.Errorf("%w: all limits must be positive", ErrInvalidPlan)
	}
	return nil
}

func validateEntryTypeFields(entry ManifestEntry, limits ManifestLimits) error {
	switch entry.Type {
	case EntryTypeFile:
		if entry.SymlinkTarget != "" {
			return fmt.Errorf("%w: file %q has symlink target", ErrInvalidManifest, entry.RelativePath)
		}
		if entry.ExpectedSizeBytes >= limits.InlineThreshold {
			return fmt.Errorf("%w: file %q size %d requires external storage", ErrStorageBackendUnsupported, entry.RelativePath, entry.ExpectedSizeBytes)
		}
		if !validSHA256(entry.ExpectedChecksumSHA256) {
			return fmt.Errorf("%w: file %q has invalid SHA-256", ErrInvalidManifest, entry.RelativePath)
		}
	case EntryTypeDirectory:
		if entry.SymlinkTarget != "" || entry.ExpectedSizeBytes != 0 || entry.ExpectedChecksumSHA256 != "" {
			return fmt.Errorf("%w: directory %q has content fields", ErrInvalidManifest, entry.RelativePath)
		}
	case EntryTypeSymlink:
		if entry.ExpectedSizeBytes != 0 || entry.ExpectedChecksumSHA256 != "" {
			return fmt.Errorf("%w: symlink %q has file content fields", ErrInvalidManifest, entry.RelativePath)
		}
		if entry.SymlinkTarget == "" {
			return fmt.Errorf("%w: symlink %q has empty target", ErrInvalidManifest, entry.RelativePath)
		}
		if !utf8.ValidString(entry.SymlinkTarget) || strings.IndexByte(entry.SymlinkTarget, 0) >= 0 {
			return fmt.Errorf("%w: symlink %q target is not valid text", ErrInvalidManifest, entry.RelativePath)
		}
		if uint64(len(entry.SymlinkTarget)) > limits.MaxSymlinkBytes {
			return fmt.Errorf("%w: symlink %q target exceeds %d bytes", ErrManifestLimitExceeded, entry.RelativePath, limits.MaxSymlinkBytes)
		}
	default:
		return fmt.Errorf("%w: path %q has unknown type %q", ErrInvalidManifest, entry.RelativePath, entry.Type)
	}
	return nil
}

// CanonicalRelativePath applies the relative-path grammar shared by manifest
// creation and datastore continuation APIs.
func CanonicalRelativePath(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("relative path is empty")
	}
	if strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("relative path is absolute")
	}
	canonical, err := pathutil.Canonicalize("/" + raw)
	if err != nil {
		return "", err
	}
	if canonical == "/" {
		return "", fmt.Errorf("relative path resolves to root")
	}
	return strings.TrimPrefix(canonical, "/"), nil
}

func relativeParent(path string) string {
	idx := strings.LastIndexByte(path, '/')
	if idx < 0 {
		return ""
	}
	return path[:idx]
}

func hashEntry(entry ManifestEntry) (string, error) {
	raw, err := json.Marshal(entryHashInput{
		RelativePath:           entry.RelativePath,
		Type:                   entry.Type,
		Mode:                   entry.Mode,
		MtimeNS:                entry.MtimeNS,
		SymlinkTarget:          entry.SymlinkTarget,
		ExpectedSizeBytes:      entry.ExpectedSizeBytes,
		ExpectedChecksumSHA256: entry.ExpectedChecksumSHA256,
	})
	if err != nil {
		return "", fmt.Errorf("marshal manifest entry: %w", err)
	}
	return hashHex(raw), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func hashHex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
