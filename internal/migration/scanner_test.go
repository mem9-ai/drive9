package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
	"golang.org/x/text/unicode/norm"
)

func TestScannerPreservesSupportedFilesystemFactsWithoutFollowingLinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o751); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(root, "file")
	if err := os.WriteFile(filePath, []byte("content"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filePath, filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "must-not-scan"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(context.Background())
	if err != nil || !result.Complete {
		t.Fatalf("scan complete=%v err=%v", result.Complete, err)
	}
	if len(result.Entries) != 4 || result.DirectoryCount != 1 || result.LogicalBytes != int64(len("content"))*2 {
		t.Fatalf("scan summary=%+v", result)
	}
	if entry := result.Entries["/empty"]; entry.Kind != EntryDirectory || entry.Mode != 0o751 {
		t.Fatalf("empty directory=%+v", entry)
	}
	link := result.Entries["/link"]
	if link.Kind != EntrySymlink || link.LinkTarget != outside {
		t.Fatalf("symlink=%+v", link)
	}
	if _, ok := result.Entries["/link/must-not-scan"]; ok {
		t.Fatal("scanner followed symlink")
	}
	if !hasFinding(result.Findings, FindingSymlinkTarget) {
		t.Fatalf("external symlink warning missing: %+v", result.Findings)
	}
	primary, alias := result.Entries["/alias"], result.Entries["/file"]
	if primary.HardlinkKey == "" || primary.HardlinkKey != alias.HardlinkKey || primary.HardlinkPrimary == alias.HardlinkPrimary {
		t.Fatalf("hardlinks primary=%+v alias=%+v", primary, alias)
	}
	if primary.Version.Device == 0 || primary.Version.Inode == 0 || primary.Version.Kind != EntryRegular || primary.Version.Mode&0o777 != 0o640 {
		t.Fatalf("source token=%+v", primary.Version)
	}
}

func TestScannerWarnsForNormalizedExternalAndDanglingSymlinkTargets(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "target"), []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{
		"nested-external": "child/../../outside",
		"dangling":        "missing",
		"external-hop":    "hop-outside",
		"hop-outside":     outside,
		"cycle-a":         "cycle-b",
		"cycle-b":         "cycle-a",
		"non-directory":   "target/child",
		"internal":        "target",
		"internal-hop":    "internal",
	} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("../target", filepath.Join(root, "child", "parent-internal")); err != nil {
		t.Fatal(err)
	}

	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(context.Background())
	if err != nil || !result.Complete {
		t.Fatalf("scan complete=%v err=%v", result.Complete, err)
	}
	for _, path := range []string{
		"/nested-external", "/dangling", "/external-hop", "/hop-outside",
		"/cycle-a", "/cycle-b", "/non-directory",
	} {
		if !hasFindingAt(result.Findings, path, FindingSymlinkTarget) {
			t.Fatalf("symlink warning missing at %s: %+v", path, result.Findings)
		}
	}
	for _, path := range []string{"/internal", "/internal-hop", "/child/parent-internal"} {
		if hasFindingAt(result.Findings, path, FindingSymlinkTarget) {
			t.Fatalf("valid internal target warned at %s: %+v", path, result.Findings)
		}
	}
}

func TestScannerResolvesSymlinkTargetComponentsBeforeDotDot(t *testing.T) {
	root := t.TempDir()
	outsideParent := t.TempDir()
	outsideHop := filepath.Join(outsideParent, "hop")
	outsideSafe := filepath.Join(outsideParent, "safe")
	if err := os.Mkdir(outsideHop, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outsideSafe, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "safe"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideHop, filepath.Join(root, "hop")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("hop/../safe", filepath.Join(root, "external-after-hop")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "regular"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "valid"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("regular/../valid", filepath.Join(root, "dangling-after-file")); err != nil {
		t.Fatal(err)
	}

	resolved, err := filepath.EvalSymlinks(filepath.Join(root, "external-after-hop"))
	if err != nil {
		t.Fatalf("external symlink resolved=%q err=%v", resolved, err)
	}
	expected, err := filepath.EvalSymlinks(outsideSafe)
	if err != nil || resolved != expected {
		t.Fatalf("external symlink resolved=%q expected=%q err=%v", resolved, expected, err)
	}
	if _, err := filepath.EvalSymlinks(filepath.Join(root, "dangling-after-file")); err == nil {
		t.Fatal("regular-file/../valid unexpectedly resolved")
	}

	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(context.Background())
	if err != nil || !result.Complete {
		t.Fatalf("scan complete=%v err=%v", result.Complete, err)
	}
	for _, path := range []string{"/external-after-hop", "/dangling-after-file"} {
		if !hasFindingAt(result.Findings, path, FindingSymlinkTarget) {
			t.Errorf("symlink warning missing at %s: %+v", path, result.Findings)
		}
	}
}

func TestScannerStableReadHashesWithBoundedBuffer(t *testing.T) {
	root := t.TempDir()
	data := strings.Repeat("abcdef", 1000)
	if err := os.WriteFile(filepath.Join(root, "file"), []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	if scanner.bufferSize <= 0 || scanner.bufferSize > MaxSourceReadBufferBytes {
		t.Fatalf("buffer size=%d", scanner.bufferSize)
	}
	scanner.bufferSize = 17
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	deep, err := scanner.ReadStable(context.Background(), "/file", manifest.Entries["/file"].Version)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(data))
	if deep.ChecksumSHA256 != hex.EncodeToString(sum[:]) || deep.Size != int64(len(data)) {
		t.Fatalf("deep read=%+v", deep)
	}
}

func TestScannerStableReadRejectsTokenChangeWithUnchangedMtime(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "file")
	if err := os.WriteFile(filePath, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	original := manifest.Entries["/file"].Version
	var called atomic.Bool
	scanner.afterRead = func(string) {
		if !called.CompareAndSwap(false, true) {
			return
		}
		if err := os.WriteFile(filePath, []byte("after!"), 0o600); err != nil {
			t.Error(err)
		}
		mtime := time.Unix(0, original.MtimeNS)
		if err := os.Chtimes(filePath, mtime, mtime); err != nil {
			t.Error(err)
		}
	}
	if _, err := scanner.ReadStable(context.Background(), "/file", original); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("stable read error=%v", err)
	}
}

func TestScannerStableReadFailureClasses(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "file")
	if err := os.WriteFile(filePath, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	version := manifest.Entries["/file"].Version
	unsupported := version
	unsupported.Kind = EntryDirectory
	if _, err := scanner.ReadStable(context.Background(), "/file", unsupported); !errors.Is(err, ErrUnsupportedSource) {
		t.Fatalf("unsupported error=%v", err)
	}
	changed := version
	changed.Size++
	if _, err := scanner.ReadStable(context.Background(), "/file", changed); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("changed error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scanner.ReadStable(canceled, "/file", version); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error=%v", err)
	}
	scanner.afterRead = func(string) { _ = os.Remove(filePath) }
	if _, err := scanner.ReadStable(context.Background(), "/file", version); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("removed-after-read error=%v", err)
	}
}

func TestScannerNormalizesScanAndReadDisappearance(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "file")
	if err := os.WriteFile(filePath, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	removed := false
	scanner.beforeEntry = func(name string) {
		if !removed && name == filePath {
			removed = true
			_ = os.Remove(name)
		}
	}
	result, err := scanner.Scan(context.Background())
	if !errors.Is(err, ErrSourceChanged) || result.Complete {
		t.Fatalf("scan disappearance result=%+v err=%v", result, err)
	}

	scanner.beforeEntry = nil
	if err := os.WriteFile(filePath, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err = scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	version := result.Entries["/file"].Version
	if err := os.Remove(filePath); err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.ReadStable(context.Background(), "/file", version); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("read disappearance error=%v", err)
	}
}

func TestScannerRejectsDirectoryMutationAfterEnumeration(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "directory")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(directory, "marker")
	if err := os.WriteFile(marker, []byte("marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	created := false
	scanner.beforeEntry = func(name string) {
		if name == marker && !created {
			created = true
			if err := os.WriteFile(filepath.Join(directory, "late"), []byte("late"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	result, err := scanner.Scan(context.Background())
	if !errors.Is(err, ErrSourceChanged) || result.Complete {
		t.Fatalf("directory mutation result=%+v err=%v", result, err)
	}
}

func TestScannerReadStableRejectsAncestorSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "directory")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "file"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(t.TempDir(), "moved")
	if err := os.Rename(directory, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(moved, directory); err != nil {
		t.Fatal(err)
	}

	if _, err := scanner.ReadStableEntry(context.Background(), manifest.Entries["/directory/file"]); err == nil {
		t.Fatal("stable read followed an ancestor symlink outside the source root")
	}
}

func TestScannerReportsUnsupportedAndUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalidName := string([]byte{'b', 'a', 'd', 0xff})
	if utf8.ValidString(invalidName) {
		t.Fatal("fixture unexpectedly valid UTF-8")
	}
	if _, ok := canonicalSourcePath(invalidName); ok {
		t.Fatal("invalid UTF-8 path was canonicalized")
	}
	wantInvalidFinding := os.WriteFile(filepath.Join(root, invalidName), nil, 0o600) == nil
	if os.Symlink(string([]byte{'x', 0xff}), filepath.Join(root, "bad-link")) == nil {
		wantInvalidFinding = true
	}
	sparsePath := filepath.Join(root, "sparse")
	file, err := os.Create(sparsePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(1<<20, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(context.Background())
	if err != nil || !result.Complete {
		t.Fatalf("scan complete=%v err=%v", result.Complete, err)
	}
	wanted := []FindingKind{FindingSpecialFile, FindingSparseFile}
	if wantInvalidFinding {
		wanted = append(wanted, FindingInvalidUTF8)
	}
	for _, want := range wanted {
		if !hasFinding(result.Findings, want) {
			t.Fatalf("findings=%+v, missing %s", result.Findings, want)
		}
	}
}

func TestScannerDetectsNFCCollisionNestedMountAndIdentityFailure(t *testing.T) {
	canonical := norm.NFC.String("/e\u0301")
	accumulator := newScanAccumulator()
	accumulator.add(canonical, "/e\u0301", SourceEntry{Path: canonical})
	accumulator.add(canonical, "/é", SourceEntry{Path: canonical})
	if !hasFinding(accumulator.findings, FindingNFCCollision) {
		t.Fatalf("NFC findings=%+v", accumulator.findings)
	}

	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nested", "hidden"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	defaultIdentity := scanner.identity
	scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		identity, err := defaultIdentity(name, info)
		if strings.HasSuffix(name, "nested") {
			identity.version.Device++
		}
		return identity, err
	}
	result, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(result.Findings, FindingNestedMount) {
		t.Fatalf("nested findings=%+v", result.Findings)
	}
	if _, ok := result.Entries["/nested/hidden"]; ok {
		t.Fatal("scanner crossed nested mount")
	}

	scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		if strings.HasSuffix(name, "nested") {
			return fileIdentity{}, errors.New("identity unavailable")
		}
		return defaultIdentity(name, info)
	}
	result, err = scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasFinding(result.Findings, FindingSourceIdentity) {
		t.Fatalf("identity findings=%+v", result.Findings)
	}
}

func TestScannerRetainsRawLocalSpellingForNormalizedLogicalPath(t *testing.T) {
	root := t.TempDir()
	rawName := "e\u0301"
	canonical := norm.NFC.String("/" + rawName)
	if err := os.WriteFile(filepath.Join(root, rawName), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, exists := result.Entries[canonical]
	if !exists || entry.Path != canonical || entry.LocalPath == canonical {
		t.Fatalf("normalized entry=%+v entries=%+v", entry, result.Entries)
	}
	var readName string
	identity := scanner.identity
	scanner.identity = func(name string, info os.FileInfo) (fileIdentity, error) {
		readName = name
		return identity(name, info)
	}
	if _, err := scanner.ReadStableEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	if readName != filepath.Join(root, rawName) {
		t.Fatalf("read used %q, want raw local spelling %q", readName, filepath.Join(root, rawName))
	}
	for _, invalid := range []string{"bad\\name", "bad\x01name"} {
		if _, ok := canonicalSourcePath(invalid); ok {
			t.Fatalf("invalid Drive9 path %q was accepted", invalid)
		}
	}
}

func TestScannerFailureIsIncompleteAndPathsCannotEscape(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := scanner.Scan(canceled)
	if !errors.Is(err, context.Canceled) || result.Complete {
		t.Fatalf("canceled result=%+v err=%v", result, err)
	}
	for _, unsafe := range []string{"../file", "/../file", "relative", "/a/../../file"} {
		if _, err := scanner.ReadStable(context.Background(), unsafe, SourceVersion{}); !errors.Is(err, ErrUnsafeSourcePath) {
			t.Fatalf("unsafe path %q error=%v", unsafe, err)
		}
	}
}

func TestScannerRejectsUnsafeRootAndRootIdentityFailure(t *testing.T) {
	if _, err := NewScanner("relative"); !errors.Is(err, ErrUnsafeSourcePath) {
		t.Fatalf("relative root error=%v", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(file)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := scanner.Scan(context.Background()); err == nil || result.Complete {
		t.Fatalf("file root result=%+v err=%v", result, err)
	}
	root := t.TempDir()
	scanner, err = NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	scanner.identity = func(string, os.FileInfo) (fileIdentity, error) {
		return fileIdentity{}, errors.New("identity unavailable")
	}
	if result, err := scanner.Scan(context.Background()); err == nil || result.Complete {
		t.Fatalf("identity result=%+v err=%v", result, err)
	}
}

func TestScannerManifestCapacityExpectation(t *testing.T) {
	entries := make(map[string]SourceEntry, 100_000)
	for i := range 100_000 {
		path := fmt.Sprintf("/directory-%06d", i)
		entries[path] = SourceEntry{Path: path, Kind: EntryDirectory}
	}
	if len(entries) != 100_000 {
		t.Fatalf("manifest entries=%d", len(entries))
	}
}

func hasFinding(findings []Finding, kind FindingKind) bool {
	for _, finding := range findings {
		if finding.Kind == kind {
			return true
		}
	}
	return false
}

func hasFindingAt(findings []Finding, path string, kind FindingKind) bool {
	for _, finding := range findings {
		if finding.Path == path && finding.Kind == kind {
			return true
		}
	}
	return false
}
