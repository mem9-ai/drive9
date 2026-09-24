package promotion

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestValidateCanonicalManifest(t *testing.T) {
	body := []byte("hello")
	sum := sha256.Sum256(body)
	entries := []Entry{
		{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "assets", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "assets/index.js", Type: EntryFile, Mode: 0o644, SizeBytes: int64(len(body)), ChecksumSHA256: hex.EncodeToString(sum[:]), Data: body},
	}
	req := PublishRequest{
		OperationID:    "0123456789abcdef",
		Target:         "/project/site/",
		ManifestSHA256: ManifestSHA256(entries),
		Entries:        entries,
	}
	if err := Validate(req, 50_000); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateCanonicalManifestKeepsRootBeforeLexicallyEarlierNames(t *testing.T) {
	entries := []Entry{
		{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "#cache", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "-backup", Type: EntryDirectory, Mode: 0o755},
	}
	req := PublishRequest{
		OperationID:    "0123456789abcdef",
		Target:         "/project/site/",
		ManifestSHA256: ManifestSHA256(entries),
		Entries:        entries,
	}
	if err := Validate(req, 50_000); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateRejectsMissingParentAndTamperedBody(t *testing.T) {
	body := []byte("hello")
	sum := sha256.Sum256(body)
	entries := []Entry{
		{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "assets/index.js", Type: EntryFile, Mode: 0o644, SizeBytes: int64(len(body)), ChecksumSHA256: hex.EncodeToString(sum[:]), Data: body},
	}
	req := PublishRequest{OperationID: "0123456789abcdef", Target: "/project/site/", Entries: entries}
	req.ManifestSHA256 = ManifestSHA256(entries)
	err := Validate(req, 50_000)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Validate missing parent error = %v, want %v", err, ErrInvalidRequest)
	}

	entries = []Entry{
		{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "index.js", Type: EntryFile, Mode: 0o644, SizeBytes: int64(len(body)), ChecksumSHA256: hex.EncodeToString(sum[:]), Data: []byte("jello")},
	}
	req.Entries = entries
	req.ManifestSHA256 = ManifestSHA256(entries)
	err = Validate(req, 50_000)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Validate tampered body error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestValidateRejectsBounds(t *testing.T) {
	body := []byte("hello")
	sum := sha256.Sum256(body)
	entries := []Entry{
		{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
		{RelativePath: "index.js", Type: EntryFile, Mode: 0o644, SizeBytes: int64(len(body)), ChecksumSHA256: hex.EncodeToString(sum[:]), Data: body},
	}
	req := PublishRequest{OperationID: "0123456789abcdef", Target: "/project/site/", Entries: entries}
	req.ManifestSHA256 = ManifestSHA256(entries)
	err := Validate(req, int64(len(body)))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Validate limit error = %v, want %v", err, ErrLimitExceeded)
	}
}

func TestValidateRejectsPathSegmentsLongerThanDatabaseNameColumn(t *testing.T) {
	for _, tt := range []struct {
		name    string
		target  string
		entries []Entry
	}{
		{
			name:    "target basename",
			target:  "/project/" + strings.Repeat("x", MaxPathSegmentRunes+1) + "/",
			entries: []Entry{{RelativePath: ".", Type: EntryDirectory, Mode: 0o755}},
		},
		{
			name:   "entry basename",
			target: "/project/site/",
			entries: []Entry{
				{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
				{RelativePath: strings.Repeat("界", MaxPathSegmentRunes+1), Type: EntryDirectory, Mode: 0o755},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := PublishRequest{
				OperationID: "0123456789abcdef", Target: tt.target, Entries: tt.entries,
				ManifestSHA256: ManifestSHA256(tt.entries),
			}
			if err := Validate(req, 50_000); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("Validate error = %v, want %v", err, ErrLimitExceeded)
			}
		})
	}
}

func TestValidateRejectsSpecialPermissionBits(t *testing.T) {
	entries := []Entry{{RelativePath: ".", Type: EntryDirectory, Mode: 0o1755}}
	req := PublishRequest{
		OperationID:    "0123456789abcdef",
		Target:         "/project/site/",
		ManifestSHA256: ManifestSHA256(entries),
		Entries:        entries,
	}
	if err := Validate(req, 50_000); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Validate special mode error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestValidateRejectsNonCanonicalReceiptTuple(t *testing.T) {
	entries := []Entry{{RelativePath: ".", Type: EntryDirectory, Mode: 0o755}}
	digest := ManifestSHA256(entries)
	for _, req := range []PublishRequest{
		{OperationID: " 0123456789abcdef", Target: "/project/site/", ManifestSHA256: digest, Entries: entries},
		{OperationID: "0123456789abcdef ", Target: "/project/site/", ManifestSHA256: digest, Entries: entries},
		{OperationID: "0123456789abcdef", Target: "/project/site/", ManifestSHA256: strings.ToUpper(digest), Entries: entries},
	} {
		if err := Validate(req, 50_000); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("Validate(%q, %q) error = %v, want %v", req.OperationID, req.ManifestSHA256, err, ErrInvalidRequest)
		}
	}
}

func TestValidateRejectsImplicitRootTargetParent(t *testing.T) {
	entries := []Entry{{RelativePath: ".", Type: EntryDirectory, Mode: 0o755}}
	req := PublishRequest{
		OperationID:    "0123456789abcdef",
		Target:         "/site/",
		ManifestSHA256: ManifestSHA256(entries),
		Entries:        entries,
	}
	if err := Validate(req, 50_000); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Validate root target error = %v, want %v", err, ErrInvalidRequest)
	}
}

func TestValidateRejectsNonCanonicalEntryPaths(t *testing.T) {
	body := []byte("hello")
	sum := sha256.Sum256(body)
	for _, relativePath := range []string{
		`assets/a\b.js`,
		"assets/cafe\u0301.js",
	} {
		t.Run(relativePath, func(t *testing.T) {
			entries := []Entry{
				{RelativePath: ".", Type: EntryDirectory, Mode: 0o755},
				{RelativePath: "assets", Type: EntryDirectory, Mode: 0o755},
				{RelativePath: relativePath, Type: EntryFile, Mode: 0o644, SizeBytes: int64(len(body)), ChecksumSHA256: hex.EncodeToString(sum[:]), Data: body},
			}
			req := PublishRequest{
				OperationID:    "0123456789abcdef",
				Target:         "/project/site/",
				ManifestSHA256: ManifestSHA256(entries),
				Entries:        entries,
			}
			err := Validate(req, 50_000)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("Validate(%q) error = %v, want %v", relativePath, err, ErrInvalidRequest)
			}
		})
	}
}
