package promotion

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func testManifestLimits() ManifestLimits {
	return ManifestLimits{
		MaxEntries:          32,
		MaxMetadataBytes:    32 << 10,
		MaxTotalInlineBytes: 8 << 10,
		MaxPathBytes:        256,
		MaxSymlinkBytes:     256,
		MaxDepth:            8,
		InlineThreshold:     1024,
	}
}

func checksum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func TestCanonicalizeManifestProducesStrictCompleteTree(t *testing.T) {
	manifest, err := CanonicalizeManifest([]ManifestEntry{
		{RelativePath: "dir/file.txt", Type: EntryTypeFile, Mode: 0o644, MtimeNS: 3, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: checksum("hello")},
		{RelativePath: "link", Type: EntryTypeSymlink, Mode: 0o777, MtimeNS: 4, SymlinkTarget: "../outside"},
		{RelativePath: "dir/", Type: EntryTypeDirectory, Mode: 0o755, MtimeNS: 2},
	}, testManifestLimits())
	if err != nil {
		t.Fatalf("CanonicalizeManifest: %v", err)
	}
	if got, want := manifest.EntryTotal, uint64(3); got != want {
		t.Fatalf("EntryTotal = %d, want %d", got, want)
	}
	if got, want := manifest.ByteTotal, uint64(5); got != want {
		t.Fatalf("ByteTotal = %d, want %d", got, want)
	}
	if got, want := manifest.Entries[0].RelativePath, "dir"; got != want {
		t.Fatalf("first path = %q, want %q", got, want)
	}
	if manifest.Entries[0].EntryHash == "" || manifest.ManifestHash == "" {
		t.Fatal("canonical hashes must be populated")
	}
	if _, err := ValidateCanonicalManifest(manifest.Entries, testManifestLimits()); err != nil {
		t.Fatalf("ValidateCanonicalManifest(canonical): %v", err)
	}
}

func TestValidateCanonicalManifestRejectsReorderedTuple(t *testing.T) {
	manifest, err := CanonicalizeManifest([]ManifestEntry{
		{RelativePath: "a", Type: EntryTypeDirectory, Mode: 0o755},
		{RelativePath: "b", Type: EntryTypeDirectory, Mode: 0o755},
	}, testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	manifest.Entries[0], manifest.Entries[1] = manifest.Entries[1], manifest.Entries[0]
	if _, err := ValidateCanonicalManifest(manifest.Entries, testManifestLimits()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("reordered manifest error = %v, want ErrInvalidManifest", err)
	}
}

func TestValidateCanonicalManifestRejectsMissingOrNonDirectoryParent(t *testing.T) {
	cases := []struct {
		name    string
		entries []ManifestEntry
	}{
		{
			name: "missing",
			entries: []ManifestEntry{{
				RelativePath: "dir/file", Type: EntryTypeFile, Mode: 0o644,
				ExpectedSizeBytes: 1, ExpectedChecksumSHA256: checksum("x"),
			}},
		},
		{
			name: "file parent",
			entries: []ManifestEntry{
				{RelativePath: "dir", Type: EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 1, ExpectedChecksumSHA256: checksum("x")},
				{RelativePath: "dir/file", Type: EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 1, ExpectedChecksumSHA256: checksum("y")},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			canonical := make([]ManifestEntry, len(tc.entries))
			copy(canonical, tc.entries)
			for i := range canonical {
				path, err := CanonicalRelativePath(canonical[i].RelativePath)
				if err != nil {
					t.Fatal(err)
				}
				canonical[i].RelativePath = path
				hash, err := hashEntry(canonical[i])
				if err != nil {
					t.Fatal(err)
				}
				canonical[i].EntryHash = hash
			}
			if _, err := ValidateCanonicalManifest(canonical, testManifestLimits()); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("error = %v, want ErrInvalidManifest", err)
			}
		})
	}
}

func TestValidateCanonicalManifestStorageBoundary(t *testing.T) {
	limits := testManifestLimits()
	for _, tc := range []struct {
		name string
		size uint64
		ok   bool
	}{
		{name: "threshold minus one", size: limits.InlineThreshold - 1, ok: true},
		{name: "threshold", size: limits.InlineThreshold, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := ManifestEntry{RelativePath: "file", Type: EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: tc.size, ExpectedChecksumSHA256: checksum("payload")}
			_, err := CanonicalizeManifest([]ManifestEntry{entry}, limits)
			if tc.ok && err != nil {
				t.Fatalf("CanonicalizeManifest: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrStorageBackendUnsupported) {
				t.Fatalf("error = %v, want ErrStorageBackendUnsupported", err)
			}
		})
	}
}

func TestValidateCanonicalManifestEmptyTree(t *testing.T) {
	manifest, err := ValidateCanonicalManifest(nil, testManifestLimits())
	if err != nil {
		t.Fatalf("ValidateCanonicalManifest: %v", err)
	}
	if manifest.EntryTotal != 0 || manifest.ByteTotal != 0 || manifest.ManifestHash == "" {
		t.Fatalf("unexpected empty manifest summary: %+v", manifest)
	}
}

func TestCanonicalManifestBindsOptionalRootMetadata(t *testing.T) {
	manifest, err := CanonicalizeManifest([]ManifestEntry{
		{RelativePath: ".", Type: EntryTypeDirectory, Mode: 0o710, MtimeNS: 1234},
		{RelativePath: "child", Type: EntryTypeFile, Mode: 0o640, MtimeNS: 5678, ExpectedChecksumSHA256: checksum("payload")},
	}, testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Entries) != 2 || manifest.Entries[0].RelativePath != "." ||
		manifest.Entries[0].Mode != 0o710 || manifest.Entries[0].MtimeNS != 1234 {
		t.Fatalf("canonical root metadata = %+v", manifest.Entries)
	}
	mutated := append([]ManifestEntry(nil), manifest.Entries...)
	mutated[0].Mode = 0o755
	if _, err := ValidateCanonicalManifest(mutated, testManifestLimits()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("root metadata mutation error = %v, want ErrInvalidManifest", err)
	}
}

func TestCanonicalManifestRejectsRootContentEntry(t *testing.T) {
	for _, entryType := range []EntryType{EntryTypeFile, EntryTypeSymlink} {
		t.Run(string(entryType), func(t *testing.T) {
			entry := ManifestEntry{RelativePath: ".", Type: entryType, Mode: 0o600}
			if entryType == EntryTypeFile {
				entry.ExpectedChecksumSHA256 = checksum("")
			} else {
				entry.SymlinkTarget = "target"
			}
			if _, err := CanonicalizeManifest([]ManifestEntry{entry}, testManifestLimits()); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("CanonicalizeManifest root %s error = %v, want ErrInvalidManifest", entryType, err)
			}
		})
	}
}

func TestValidateCanonicalManifestEntryHashGatesMetadata(t *testing.T) {
	manifest, err := CanonicalizeManifest([]ManifestEntry{{RelativePath: "dir", Type: EntryTypeDirectory, Mode: 0o755, MtimeNS: 7}}, testManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	manifest.Entries[0].Mode = 0o700
	if _, err := ValidateCanonicalManifest(manifest.Entries, testManifestLimits()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("metadata mutation error = %v, want ErrInvalidManifest", err)
	}
}
