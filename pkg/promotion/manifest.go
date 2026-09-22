// Package promotion defines the bounded atomic import used by synchronous
// local-to-remote rename.
package promotion

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const (
	EntryDirectory = "directory"
	EntryFile      = "file"

	MaxEntries    = 4096
	MaxTotalBytes = 64 << 20
	MaxPathBytes  = 4096
	MaxDepth      = 128
	// MaxRequestBodyBytes allows for base64 expansion plus bounded metadata.
	MaxRequestBodyBytes = 96 << 20
)

var (
	ErrInvalidRequest = errors.New("invalid promotion request")
	ErrLimitExceeded  = errors.New("promotion limit exceeded")
)

// Entry is one directory or DB-inline regular file in an atomic import.
// RelativePath is "." for the root and slash-separated for descendants.
type Entry struct {
	RelativePath   string `json:"relative_path"`
	Type           string `json:"type"`
	Mode           uint32 `json:"mode"`
	MtimeUnixNano  int64  `json:"mtime_unix_nano"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	Data           []byte `json:"data,omitempty"`
}

// PublishRequest asks the server to create a complete absent target in one
// transaction. OperationID makes a lost response retryable.
type PublishRequest struct {
	OperationID    string  `json:"operation_id"`
	Target         string  `json:"target"`
	ManifestSHA256 string  `json:"manifest_sha256"`
	Entries        []Entry `json:"entries"`
}

// Result is the immutable success result stored with the published tree.
type Result struct {
	OperationID    string `json:"operation_id"`
	Target         string `json:"target"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Committed      bool   `json:"committed"`
}

// Validate checks the bounded canonical request and verifies every file body.
func Validate(req PublishRequest, maxFileBytes int64) error {
	if err := ValidateOperationID(req.OperationID); err != nil {
		return err
	}
	if err := ValidateTarget(req.Target); err != nil {
		return err
	}
	if len(req.Entries) == 0 || len(req.Entries) > MaxEntries {
		return fmt.Errorf("%w: entry count", ErrLimitExceeded)
	}
	if req.Entries[0].RelativePath != "." || req.Entries[0].Type != EntryDirectory {
		return fmt.Errorf("%w: root entry", ErrInvalidRequest)
	}

	seen := make(map[string]string, len(req.Entries))
	var total int64
	previous := ""
	for i, entry := range req.Entries {
		if err := validateEntry(entry, maxFileBytes); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if err := validateFinalPath(req.Target, entry); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		// The canonical wire order fixes the root entry first, then sorts only
		// descendants. Valid Unix names such as "#cache" and "-backup" sort
		// before "." bytewise and must not make an otherwise valid tree fail.
		if i > 1 && entry.RelativePath <= previous {
			return fmt.Errorf("%w: entries are not strictly sorted", ErrInvalidRequest)
		}
		previous = entry.RelativePath
		if _, exists := seen[entry.RelativePath]; exists {
			return fmt.Errorf("%w: duplicate path %q", ErrInvalidRequest, entry.RelativePath)
		}
		if entry.RelativePath != "." {
			parent := path.Dir(entry.RelativePath)
			if parent == "." {
				parent = "."
			}
			if seen[parent] != EntryDirectory {
				return fmt.Errorf("%w: missing directory parent for %q", ErrInvalidRequest, entry.RelativePath)
			}
		}
		seen[entry.RelativePath] = entry.Type
		if entry.Type == EntryFile {
			if entry.SizeBytes > MaxTotalBytes-total {
				return fmt.Errorf("%w: total bytes", ErrLimitExceeded)
			}
			total += entry.SizeBytes
		}
	}

	digest := ManifestSHA256(req.Entries)
	if ValidateManifestSHA256(req.ManifestSHA256) != nil || req.ManifestSHA256 != digest {
		return fmt.Errorf("%w: manifest digest", ErrInvalidRequest)
	}
	return nil
}

// ValidateTarget checks the canonical absent-directory target shared by POST,
// outcome lookup, and acknowledgement routes.
func ValidateTarget(target string) error {
	canonicalTarget, targetErr := pathutil.CanonicalizeDir(target)
	if targetErr != nil || target == "/" || canonicalTarget != target {
		return fmt.Errorf("%w: target", ErrInvalidRequest)
	}
	// Phase one serializes publication with ordinary namespace creates by
	// locking the existing target-parent dentry. The root dentry is implicit,
	// so root-level targets remain outside this bounded preview.
	if pathutil.ParentPath(target) == "/" {
		return fmt.Errorf("%w: target parent", ErrInvalidRequest)
	}
	if len(target) > MaxPathBytes {
		return fmt.Errorf("%w: target path", ErrLimitExceeded)
	}
	return nil
}

// ValidateOperationID accepts only the canonical receipt key shape used by
// POST, GET, and DELETE. In particular, surrounding whitespace is never
// normalized because doing so would make a committed POST impossible to
// recover through the lookup routes.
func ValidateOperationID(operationID string) error {
	if len(operationID) < 16 || len(operationID) > 64 || strings.TrimSpace(operationID) != operationID {
		return fmt.Errorf("%w: operation_id", ErrInvalidRequest)
	}
	return nil
}

// ValidateManifestSHA256 requires the canonical lowercase wire encoding used
// by ManifestSHA256 and by the immutable receipt tuple.
func ValidateManifestSHA256(digest string) error {
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return fmt.Errorf("%w: manifest digest", ErrInvalidRequest)
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size {
		return fmt.Errorf("%w: manifest digest", ErrInvalidRequest)
	}
	return nil
}

func validateFinalPath(target string, entry Entry) error {
	if entry.RelativePath == "." {
		return nil
	}
	raw := strings.TrimSuffix(target, "/") + "/" + entry.RelativePath
	var (
		canonical string
		err       error
	)
	if entry.Type == EntryDirectory {
		canonical, err = pathutil.CanonicalizeDir(raw)
		raw += "/"
	} else {
		canonical, err = pathutil.Canonicalize(raw)
	}
	if len(raw) > MaxPathBytes {
		return fmt.Errorf("%w: final path %q", ErrLimitExceeded, entry.RelativePath)
	}
	if err != nil || canonical != raw {
		return fmt.Errorf("%w: non-canonical path %q", ErrInvalidRequest, entry.RelativePath)
	}
	return nil
}

func validateEntry(entry Entry, maxFileBytes int64) error {
	rel := entry.RelativePath
	if rel == "" || strings.HasPrefix(rel, "/") || path.Clean(rel) != rel || rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("%w: relative path %q", ErrInvalidRequest, rel)
	}
	if len(rel) > MaxPathBytes || pathDepth(rel) > MaxDepth {
		return fmt.Errorf("%w: path %q", ErrLimitExceeded, rel)
	}
	if entry.Mode&^uint32(0o777) != 0 {
		return fmt.Errorf("%w: mode", ErrInvalidRequest)
	}
	switch entry.Type {
	case EntryDirectory:
		if entry.SizeBytes != 0 || entry.ChecksumSHA256 != "" || len(entry.Data) != 0 {
			return fmt.Errorf("%w: directory payload", ErrInvalidRequest)
		}
	case EntryFile:
		if entry.SizeBytes < 0 || int64(len(entry.Data)) != entry.SizeBytes {
			return fmt.Errorf("%w: file size", ErrInvalidRequest)
		}
		if maxFileBytes > 0 && entry.SizeBytes >= maxFileBytes {
			return fmt.Errorf("%w: file exceeds inline limit", ErrLimitExceeded)
		}
		digest := sha256.Sum256(entry.Data)
		if entry.ChecksumSHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("%w: file checksum", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: entry type %q", ErrInvalidRequest, entry.Type)
	}
	return nil
}

func pathDepth(rel string) int {
	if rel == "." {
		return 0
	}
	return strings.Count(rel, "/") + 1
}

// ManifestSHA256 returns a deterministic digest of entry metadata and file
// checksums. File bodies are covered by their verified checksum.
func ManifestSHA256(entries []Entry) string {
	ordered := append([]Entry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool {
		return canonicalEntryPathLess(ordered[i].RelativePath, ordered[j].RelativePath)
	})
	h := sha256.New()
	for _, entry := range ordered {
		writeDigestField(h, entry.RelativePath)
		writeDigestField(h, entry.Type)
		writeDigestField(h, strconv.FormatUint(uint64(entry.Mode), 10))
		writeDigestField(h, strconv.FormatInt(entry.MtimeUnixNano, 10))
		writeDigestField(h, strconv.FormatInt(entry.SizeBytes, 10))
		writeDigestField(h, entry.ChecksumSHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func canonicalEntryPathLess(left, right string) bool {
	if left == "." {
		return right != "."
	}
	if right == "." {
		return false
	}
	return left < right
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestField(w digestWriter, value string) {
	_, _ = w.Write([]byte(strconv.Itoa(len(value))))
	_, _ = w.Write([]byte{':'})
	_, _ = w.Write([]byte(value))
}
