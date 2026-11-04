package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestPackArchiveRoundTripCodingAgentDefaults(t *testing.T) {
	srcLocalRoot := t.TempDir()
	repoRoot := filepath.Join(srcLocalRoot, "overlay", "repo")
	mustWriteFile(t, filepath.Join(repoRoot, ".git", "config"), []byte("[core]\n\trepositoryformatversion = 0\n"), 0o600)
	mustWriteFile(t, filepath.Join(repoRoot, ".git", "HEAD"), []byte("ref: refs/heads/main\n"), 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "dist", "app.js"), []byte("console.log('packed')\n"), 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "src", "main.go"), []byte("package main\n"), 0o644)
	if runtime.GOOS != "windows" {
		if err := os.Symlink("config", filepath.Join(repoRoot, ".git", "config.link")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
	}

	var buf bytes.Buffer
	manifest, err := writePackArchive(context.Background(), &buf, packOptions{
		LocalRoot:        srcLocalRoot,
		RemoteRoot:       "/remote/root",
		LocalPrefix:      "repo",
		Profile:          "coding-agent",
		ProfilePackPaths: []string{".git", "dist", "build", "target"},
	})
	if err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}
	wantPaths := []string{"/repo/.git", "/repo/dist"}
	if !reflect.DeepEqual(manifest.Paths, wantPaths) {
		t.Fatalf("manifest paths = %v, want %v", manifest.Paths, wantPaths)
	}

	dstLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "stale"), []byte("old"), 0o644)
	gotManifest, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	})
	if err != nil {
		t.Fatalf("extractPackArchive: %v", err)
	}
	if !reflect.DeepEqual(gotManifest.Paths, wantPaths) {
		t.Fatalf("unpacked manifest paths = %v, want %v", gotManifest.Paths, wantPaths)
	}
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "config"), "[core]\n\trepositoryformatversion = 0\n")
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", "dist", "app.js"), "console.log('packed')\n")
	if _, err := os.Lstat(filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "stale")); !os.IsNotExist(err) {
		t.Fatalf("stale file still exists after replace: err=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(dstLocalRoot, "overlay", "repo", "src", "main.go")); !os.IsNotExist(err) {
		t.Fatalf("non-profile source file was unexpectedly packed: err=%v", err)
	}
	if runtime.GOOS != "windows" {
		link, err := os.Readlink(filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "config.link"))
		if err != nil {
			t.Fatalf("read restored symlink: %v", err)
		}
		if link != "config" {
			t.Fatalf("restored symlink = %q, want config", link)
		}
	}
}

func TestPackArchivePortablePacksExistingLocalOverlayOnly(t *testing.T) {
	srcLocalRoot := t.TempDir()
	repoRoot := filepath.Join(srcLocalRoot, "overlay", "repo")
	mustWriteFile(t, filepath.Join(repoRoot, ".git", "config"), []byte("git\n"), 0o600)
	mustWriteFile(t, filepath.Join(repoRoot, "dist", "app.js"), []byte("bundle\n"), 0o644)
	mustWriteFile(t, filepath.Join(repoRoot, "src", "main.go"), []byte("package main\n"), 0o644)
	mustWriteFile(t, filepath.Join(srcLocalRoot, "cache", "remote-managed.txt"), []byte("not overlay\n"), 0o644)

	var buf bytes.Buffer
	manifest, err := writePackArchive(context.Background(), &buf, packOptions{
		LocalRoot:        srcLocalRoot,
		RemoteRoot:       "/remote/root",
		Profile:          "portable",
		ProfilePackPaths: []string{"/"},
	})
	if err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}
	wantPaths := []string{"/repo"}
	if !reflect.DeepEqual(manifest.Paths, wantPaths) {
		t.Fatalf("manifest paths = %v, want %v", manifest.Paths, wantPaths)
	}
	if !reflect.DeepEqual(manifest.ReplacePaths, wantPaths) {
		t.Fatalf("manifest replace_paths = %v, want %v", manifest.ReplacePaths, wantPaths)
	}
	entries := map[string]bool{}
	for _, entry := range manifest.Entries {
		entries[entry.Path] = true
		if strings.Contains(entry.Path, "remote-managed") || strings.HasPrefix(entry.Path, "/cache/") {
			t.Fatalf("portable packed non-overlay entry: %#v", entry)
		}
	}
	for _, want := range []string{"/repo/.git/config", "/repo/dist/app.js", "/repo/src/main.go"} {
		if !entries[want] {
			t.Fatalf("portable archive missing local overlay entry %s; entries=%v", want, manifest.Entries)
		}
	}

	dstLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(dstLocalRoot, "overlay", "repo", "stale.txt"), []byte("stale\n"), 0o644)
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	}); err != nil {
		t.Fatalf("extractPackArchive: %v", err)
	}
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "config"), "git\n")
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", "dist", "app.js"), "bundle\n")
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", "src", "main.go"), "package main\n")
	if _, err := os.Lstat(filepath.Join(dstLocalRoot, "overlay", "repo", "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale portable file still exists after replace: err=%v", err)
	}
}

func TestPackArchiveDefaultReplacePathsRemoveMissingSiblings(t *testing.T) {
	srcLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(srcLocalRoot, "overlay", "repo", ".git", "config"), []byte("git\n"), 0o644)

	var buf bytes.Buffer
	manifest, err := writePackArchive(context.Background(), &buf, packOptions{
		LocalRoot:        srcLocalRoot,
		RemoteRoot:       "/remote/root",
		LocalPrefix:      "repo",
		Profile:          "coding-agent",
		ProfilePackPaths: []string{".git", "dist", "build", "target"},
	})
	if err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}
	wantReplacePaths := []string{"/repo/.git", "/repo/build", "/repo/dist", "/repo/target"}
	if !reflect.DeepEqual(manifest.ReplacePaths, wantReplacePaths) {
		t.Fatalf("manifest replace_paths = %v, want %v", manifest.ReplacePaths, wantReplacePaths)
	}

	dstLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(dstLocalRoot, "overlay", "repo", "dist", "app.js"), []byte("stale\n"), 0o644)
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	}); err != nil {
		t.Fatalf("extractPackArchive: %v", err)
	}
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "config"), "git\n")
	if _, err := os.Lstat(filepath.Join(dstLocalRoot, "overlay", "repo", "dist")); !os.IsNotExist(err) {
		t.Fatalf("stale dist still exists after replace_paths tombstone: err=%v", err)
	}
}

func TestPackArchiveRequiresPathsWhenProfileHasNoPackPaths(t *testing.T) {
	var buf bytes.Buffer
	_, err := writePackArchive(context.Background(), &buf, packOptions{
		LocalRoot:  t.TempDir(),
		RemoteRoot: "/remote/root",
		Profile:    "coding-agent",
	})
	if err == nil || !strings.Contains(err.Error(), "[pack] paths") {
		t.Fatalf("writePackArchive error = %v, want missing pack paths error", err)
	}
}

func TestPackRemoteArchivePreservesReplacePathsWhenDefaultRootsAllDeleted(t *testing.T) {
	remoteRoot := "/workspace"
	defaultArchive, err := defaultPackArchivePath(remoteRoot, "coding-agent")
	if err != nil {
		t.Fatalf("defaultPackArchivePath: %v", err)
	}

	var stored []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/fs" + defaultArchive:
			if len(stored) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(stored)
		case "PUT /v1/fs" + defaultArchive:
			var err error
			stored, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "sk-test")
	c.SetSmallFileThresholdForTests(1 << 30)

	archiveA := t.TempDir()
	mustWriteFile(t, filepath.Join(archiveA, "overlay", "repo", ".git", "config"), []byte("git\n"), 0o644)
	if err := packRemoteArchive(context.Background(), c, defaultArchive, packOptions{
		LocalRoot:        archiveA,
		RemoteRoot:       remoteRoot,
		Profile:          "coding-agent",
		ProfilePackPaths: []string{".git", "dist", "build", "target"},
	}); err != nil {
		t.Fatalf("pack archive A: %v", err)
	}

	archiveB := t.TempDir()
	mustWriteFile(t, filepath.Join(archiveB, "overlay", "repo", "src", "main.go"), []byte("package main\n"), 0o644)
	if err := packRemoteArchive(context.Background(), c, defaultArchive, packOptions{
		LocalRoot:        archiveB,
		RemoteRoot:       remoteRoot,
		Profile:          "coding-agent",
		ProfilePackPaths: []string{".git", "dist", "build", "target"},
	}); err != nil {
		t.Fatalf("pack archive B: %v", err)
	}

	manifest, err := readPackArchiveManifest(context.Background(), bytes.NewReader(stored))
	if err != nil {
		t.Fatalf("read uploaded manifest: %v", err)
	}
	if len(manifest.Paths) != 0 {
		t.Fatalf("archive B paths = %v, want empty after all default roots deleted", manifest.Paths)
	}
	if len(manifest.Entries) != 0 {
		t.Fatalf("archive B entries = %v, want empty after all default roots deleted", manifest.Entries)
	}
	wantReplacePaths := []string{"/repo/.git", "/repo/build", "/repo/dist", "/repo/target"}
	if !reflect.DeepEqual(manifest.ReplacePaths, wantReplacePaths) {
		t.Fatalf("archive B replace_paths = %v, want %v", manifest.ReplacePaths, wantReplacePaths)
	}

	dstLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(dstLocalRoot, "overlay", "repo", ".git", "config"), []byte("stale git\n"), 0o644)
	mustWriteFile(t, filepath.Join(dstLocalRoot, "overlay", "repo", "src", "main.go"), []byte("keep source\n"), 0o644)
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(stored), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	}); err != nil {
		t.Fatalf("extract archive B: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dstLocalRoot, "overlay", "repo", ".git")); !os.IsNotExist(err) {
		t.Fatalf("stale .git still exists after replace tombstone: err=%v", err)
	}
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", "src", "main.go"), "keep source\n")
}

func TestResolvePackSourcesMapsRemoteRootToLocalOverlay(t *testing.T) {
	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", "repo", ".git", "config"), []byte("git"), 0o644)

	sources, err := resolvePackSources(packOptions{
		LocalRoot:  localRoot,
		RemoteRoot: "/remote/root",
		Paths:      []string{"/remote/root/repo/.git"},
	})
	if err != nil {
		t.Fatalf("resolvePackSources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("sources = %v, want one", sources)
	}
	if sources[0].ArchivePath != "/repo/.git" {
		t.Fatalf("ArchivePath = %q, want /repo/.git", sources[0].ArchivePath)
	}
	if sources[0].RemotePath != "/remote/root/repo/.git" {
		t.Fatalf("RemotePath = %q, want /remote/root/repo/.git", sources[0].RemotePath)
	}
	wantLocal := filepath.Join(localRoot, "overlay", "repo", ".git")
	if sources[0].LocalPath != wantLocal {
		t.Fatalf("LocalPath = %q, want %q", sources[0].LocalPath, wantLocal)
	}
}

func TestExtractPackArchiveRejectsUnsafeEntry(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo/.git"]}`)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	data := []byte("escape")
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatalf("unsafe header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("unsafe write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: t.TempDir(),
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe pack entry path") {
		t.Fatalf("extractPackArchive error = %v, want unsafe path error", err)
	}
}

func TestExtractPackArchiveDoesNotReplaceUntilArchiveValidated(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo/.git"]}`)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	data := []byte("escape")
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatalf("unsafe header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("unsafe write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", "repo", ".git", "config"), []byte("old\n"), 0o644)
	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe pack entry path") {
		t.Fatalf("extractPackArchive error = %v, want unsafe path error", err)
	}
	assertFileContent(t, filepath.Join(localRoot, "overlay", "repo", ".git", "config"), "old\n")
}

// A symlink target is resolved relative to the link's own directory, so a
// nested relative target inside the overlay is restored while an absolute or
// climbing one is refused: the archive is tenant-writable input and a link to
// /etc/passwd (or to the user's home) would be followed by later local tooling.
func TestExtractPackArchiveSymlinkTargetConfinement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	build := func(t *testing.T, links map[string]string) []byte {
		t.Helper()
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo"]}`)
		if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
			t.Fatalf("manifest header: %v", err)
		}
		if _, err := tw.Write(manifest); err != nil {
			t.Fatalf("manifest write: %v", err)
		}
		for name, target := range links {
			if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777}); err != nil {
				t.Fatalf("symlink header %s: %v", name, err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatalf("tar close: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
		return buf.Bytes()
	}

	for _, tc := range []struct {
		name   string
		links  map[string]string
		wantOK bool
	}{
		{name: "absolute target", links: map[string]string{"repo/absolute": "/opt/tool/bin/python"}},
		{name: "parent escape", links: map[string]string{"repo/pkg-link": "../../../outside/pkg"}},
		{name: "sibling escape", links: map[string]string{"repo/deep/pkg-link": "../../../../store/pkg"}},
		{name: "safe nested relative", links: map[string]string{"repo/pkg-link": "../store/pkg"}, wantOK: true},
		{name: "safe sibling relative", links: map[string]string{"repo/deep/pkg-link": "../store/pkg"}, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			localRoot := t.TempDir()
			_, err := extractPackArchive(context.Background(), bytes.NewReader(build(t, tc.links)), unpackOptions{
				LocalRoot: localRoot,
				Replace:   true,
			})
			if tc.wantOK {
				if err != nil {
					t.Fatalf("extractPackArchive: %v", err)
				}
				for name, want := range tc.links {
					got, readErr := os.Readlink(filepath.Join(localRoot, "overlay", filepath.FromSlash(name)))
					if readErr != nil {
						t.Fatalf("readlink %s: %v", name, readErr)
					}
					if got != want {
						t.Fatalf("symlink %s = %q, want %q", name, got, want)
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("extractPackArchive accepted an escaping symlink target")
			}
			if !strings.Contains(err.Error(), "pack symlink") {
				t.Fatalf("error = %v, want a pack symlink confinement error", err)
			}
			// Nothing may be materialized outside the overlay by the failed unpack.
			if _, statErr := os.Lstat(filepath.Join(localRoot, "outside")); statErr == nil {
				t.Fatal("unpack created a path outside the overlay")
			}
		})
	}
}

func TestExtractPackArchiveRejectsInvalidSymlinkTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo/link"]}`)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "repo/link", Typeflag: tar.TypeSymlink, Linkname: "", Mode: 0o777}); err != nil {
		t.Fatalf("symlink header: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: t.TempDir(),
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe pack symlink target") {
		t.Fatalf("extractPackArchive error = %v, want unsafe symlink target error", err)
	}
}

func TestExtractPackArchiveRejectsSymlinkAncestor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo"]}`)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "repo", Typeflag: tar.TypeSymlink, Linkname: "safe", Mode: 0o777}); err != nil {
		t.Fatalf("symlink header: %v", err)
	}
	data := []byte("content")
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "repo/file.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatalf("file header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("file write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: t.TempDir(),
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to unpack through symlink") {
		t.Fatalf("extractPackArchive error = %v, want symlink ancestor error", err)
	}
}

func TestExtractPackArchiveRejectsSymlinkOverlayRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	srcLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(srcLocalRoot, "overlay", "repo", ".git", "config"), []byte("git\n"), 0o644)
	var buf bytes.Buffer
	if _, err := writePackArchive(context.Background(), &buf, packOptions{
		LocalRoot:  srcLocalRoot,
		RemoteRoot: "/remote/root",
		Paths:      []string{"repo/.git"},
	}); err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}

	dstLocalRoot := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dstLocalRoot, "overlay")); err != nil {
		t.Fatalf("symlink overlay: %v", err)
	}
	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to unpack through symlink") {
		t.Fatalf("extractPackArchive error = %v, want symlink overlay error", err)
	}
}

func TestExtractPackArchiveRequiresManifest(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	data := []byte("content")
	if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + "repo/.git/config", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatalf("entry header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("entry write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	_, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: t.TempDir(),
		Replace:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "missing leading") {
		t.Fatalf("extractPackArchive error = %v, want missing manifest error", err)
	}
}

func TestPackRemoteArchiveUploadsPackFile(t *testing.T) {
	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", "repo", "dist", "app.js"), []byte("bundle\n"), 0o644)

	var uploaded []byte
	var gotTags []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /v1/fs/packs/archive.tar.gz":
			gotTags = append([]string(nil), r.Header.Values("X-Dat9-Tag")...)
			var err error
			uploaded, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "sk-test")
	c.SetSmallFileThresholdForTests(1 << 30)
	if err := packRemoteArchive(context.Background(), c, "/packs/archive.tar.gz", packOptions{
		LocalRoot:  localRoot,
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		Paths:      []string{"repo/dist"},
	}); err != nil {
		t.Fatalf("packRemoteArchive: %v", err)
	}
	if len(uploaded) == 0 {
		t.Fatal("server did not receive archive bytes")
	}
	wantTags := []string{"drive9.pack.format=drive9.pack.v1", "drive9.pack.profile=coding-agent"}
	if !reflect.DeepEqual(gotTags, wantTags) {
		t.Fatalf("X-Dat9-Tag = %v, want %v", gotTags, wantTags)
	}

	dstLocalRoot := t.TempDir()
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(uploaded), unpackOptions{
		LocalRoot: dstLocalRoot,
		Replace:   true,
	}); err != nil {
		t.Fatalf("extract uploaded archive: %v", err)
	}
	assertFileContent(t, filepath.Join(dstLocalRoot, "overlay", "repo", "dist", "app.js"), "bundle\n")
}

func TestDefaultPackArchivePath(t *testing.T) {
	got, err := defaultPackArchivePath("/remote/root", "coding-agent")
	if err != nil {
		t.Fatalf("defaultPackArchivePath: %v", err)
	}
	if !strings.HasPrefix(got, defaultPackRoot+"/root-") || !strings.HasSuffix(got, ".tar.gz") {
		t.Fatalf("default archive path = %q, want hidden pack path", got)
	}
	if strings.Contains(got, "coding-agent") {
		t.Fatalf("default archive path = %q, should not include profile name", got)
	}
	custom, err := defaultPackArchivePath("/remote/root", "with-pack")
	if err != nil {
		t.Fatalf("defaultPackArchivePath custom: %v", err)
	}
	if !strings.HasPrefix(custom, defaultPackRoot+"/root-") || !strings.HasSuffix(custom, ".tar.gz") {
		t.Fatalf("custom default archive path = %q, want flat hidden pack path", custom)
	}
	if strings.Contains(custom, "with-pack") {
		t.Fatalf("custom default archive path = %q, should not include profile name", custom)
	}
	if custom == got {
		t.Fatalf("custom default archive path = %q, want profile-isolated path distinct from %q", custom, got)
	}
}

func TestDefaultPackArchivePathSeparatesProfilesForSameRemoteRoot(t *testing.T) {
	remoteRoot := "/remote/root"
	codingArchive, err := defaultPackArchivePath(remoteRoot, "coding-agent")
	if err != nil {
		t.Fatalf("defaultPackArchivePath coding-agent: %v", err)
	}
	customArchive, err := defaultPackArchivePath(remoteRoot, "with-pack")
	if err != nil {
		t.Fatalf("defaultPackArchivePath custom: %v", err)
	}
	if codingArchive == customArchive {
		t.Fatalf("default archive path collision for same remote root: %q", codingArchive)
	}

	stored := map[string][]byte{}
	var readErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.HasPrefix(r.URL.Path, "/v1/fs"+defaultPackRoot+"/") {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			readErr = err
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stored[strings.TrimPrefix(r.URL.Path, "/v1/fs")] = append([]byte(nil), body...)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"revision":1}`))
	}))
	defer srv.Close()

	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", ".git", "config"), []byte("git\n"), 0o644)
	mustWriteFile(t, filepath.Join(localRoot, "overlay", "dist", "app.js"), []byte("bundle\n"), 0o644)
	c := client.New(srv.URL, "sk-test")
	c.SetSmallFileThresholdForTests(1 << 30)

	if err := packRemoteArchive(context.Background(), c, codingArchive, packOptions{
		LocalRoot:        localRoot,
		RemoteRoot:       remoteRoot,
		Profile:          "coding-agent",
		ProfilePackPaths: []string{".git"},
	}); err != nil {
		t.Fatalf("pack coding-agent archive: %v", err)
	}
	if err := packRemoteArchive(context.Background(), c, customArchive, packOptions{
		LocalRoot:        localRoot,
		RemoteRoot:       remoteRoot,
		Profile:          "with-pack",
		ProfilePackPaths: []string{"dist"},
	}); err != nil {
		t.Fatalf("pack custom archive: %v", err)
	}
	if readErr != nil {
		t.Fatalf("read upload body: %v", readErr)
	}
	if len(stored) != 2 {
		t.Fatalf("stored archive count = %d, want 2; paths=%v", len(stored), reflect.ValueOf(stored).MapKeys())
	}

	codingManifest, err := readPackArchiveManifest(context.Background(), bytes.NewReader(stored[codingArchive]))
	if err != nil {
		t.Fatalf("read coding-agent manifest: %v", err)
	}
	if codingManifest.Profile != "coding-agent" || !reflect.DeepEqual(codingManifest.Paths, []string{"/.git"}) {
		t.Fatalf("coding-agent manifest profile/paths = %q/%v, want coding-agent/[/.git]", codingManifest.Profile, codingManifest.Paths)
	}
	customManifest, err := readPackArchiveManifest(context.Background(), bytes.NewReader(stored[customArchive]))
	if err != nil {
		t.Fatalf("read custom manifest: %v", err)
	}
	if customManifest.Profile != "with-pack" || !reflect.DeepEqual(customManifest.Paths, []string{"/dist"}) {
		t.Fatalf("custom manifest profile/paths = %q/%v, want with-pack/[/dist]", customManifest.Profile, customManifest.Paths)
	}
}

func TestPackMountProfileOverrideUsesEffectiveProfilePackPaths(t *testing.T) {
	writeTestProfile(t, "with-pack", "[pack]\ndist\n")

	mountPoint := t.TempDir()
	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", ".git", "config"), []byte("git\n"), 0o644)
	mustWriteFile(t, filepath.Join(localRoot, "overlay", "dist", "app.js"), []byte("bundle\n"), 0o644)
	_, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID:        os.Getpid(),
		MountPoint: mountPoint,
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  localRoot,
		PackPaths:  []string{".git"},
	})
	if err != nil {
		t.Fatalf("WriteProcessState: %v", err)
	}

	var uploaded []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/fs/packs/custom.tar.gz":
			http.NotFound(w, r)
		case "PUT /v1/fs/packs/custom.tar.gz":
			var err error
			uploaded, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := client.New(srv.URL, "sk-test")
	c.SetSmallFileThresholdForTests(1 << 30)

	if err := Pack(c, []string{"--mount", mountPoint, "--profile", "with-pack", ":/packs/custom.tar.gz"}); err != nil {
		t.Fatalf("Pack: %v", err)
	}
	manifest, err := readPackArchiveManifest(context.Background(), bytes.NewReader(uploaded))
	if err != nil {
		t.Fatalf("read uploaded manifest: %v", err)
	}
	if manifest.Profile != "with-pack" {
		t.Fatalf("manifest profile = %q, want with-pack", manifest.Profile)
	}
	if !reflect.DeepEqual(manifest.Paths, []string{"/dist"}) {
		t.Fatalf("manifest paths = %v, want custom profile pack path only", manifest.Paths)
	}
	for _, entry := range manifest.Entries {
		if strings.HasPrefix(entry.Path, "/.git") {
			t.Fatalf("mounted coding-agent pack path leaked into custom profile archive: %#v", entry)
		}
	}
}

func TestPackMountProfileNoneDoesNotUseMountPackPaths(t *testing.T) {
	mountPoint := t.TempDir()
	localRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(localRoot, "overlay", ".git", "config"), []byte("git\n"), 0o644)
	_, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID:        os.Getpid(),
		MountPoint: mountPoint,
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  localRoot,
		PackPaths:  []string{".git"},
	})
	if err != nil {
		t.Fatalf("WriteProcessState: %v", err)
	}

	uploadCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			uploadCalled = true
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := client.New(srv.URL, "sk-test")
	c.SetSmallFileThresholdForTests(1 << 30)

	err = Pack(c, []string{"--mount", mountPoint, "--profile", "none", ":/packs/none.tar.gz"})
	if err == nil || !strings.Contains(err.Error(), "[pack] paths") {
		t.Fatalf("Pack error = %v, want missing pack paths error", err)
	}
	if uploadCalled {
		t.Fatal("pack uploaded archive using mount-state pack paths despite --profile=none")
	}
}

func TestRunUmountPacksAfterUnmount(t *testing.T) {
	state := mountstate.ProcessState{
		PID:        1234,
		MountPoint: "/mnt/drive9",
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  filepath.Join(t.TempDir(), "local"),
		PackPaths:  []string{".git"},
	}
	var calls []string
	var gotPaths []string
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run: func(argv []string) error {
			calls = append(calls, "run")
			return nil
		},
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("readProcessState mountPoint = %q", mountPoint)
			}
			return state, "/tmp/drive9.pid", nil
		},
		readPID: func(mountPoint string) (int, string, error) {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("readPID mountPoint = %q", mountPoint)
			}
			return state.PID, "/tmp/drive9.pid", nil
		},
		pidAlive: func(pid int) bool {
			if pid != state.PID {
				t.Fatalf("pidAlive pid = %d", pid)
			}
			return false
		},
		packAfterUnmount: func(ctx context.Context, gotState mountstate.ProcessState, archives []string, paths []string) error {
			calls = append(calls, "pack")
			if !reflect.DeepEqual(gotState, state) {
				t.Fatalf("pack state = %#v, want %#v", gotState, state)
			}
			defaultArchive, err := defaultPackArchivePath(state.RemoteRoot, state.Profile)
			if err != nil {
				t.Fatal(err)
			}
			wantArchives := []string{":" + defaultArchive, ":/packs/archive.tar.gz"}
			if !reflect.DeepEqual(archives, wantArchives) {
				t.Fatalf("archives = %v, want %v", archives, wantArchives)
			}
			gotPaths = append([]string(nil), paths...)
			return nil
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}
	if err := runUmount([]string{"--pack", ":/packs/archive.tar.gz", "--pack-path", ".git", "/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if !reflect.DeepEqual(calls, []string{"run", "pack"}) {
		t.Fatalf("calls = %v, want run then pack", calls)
	}
	if !reflect.DeepEqual(gotPaths, []string{".git"}) {
		t.Fatalf("pack paths = %v, want [.git]", gotPaths)
	}
}

func TestRunUmountAutoPacksCodingAgentMount(t *testing.T) {
	state := mountstate.ProcessState{
		PID:        1234,
		MountPoint: "/mnt/drive9",
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  filepath.Join(t.TempDir(), "local"),
		PackPaths:  []string{".git"},
	}
	var gotArchives []string
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func(argv []string) error { return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return state, "/tmp/drive9.pid", nil
		},
		readPID: func(string) (int, string, error) {
			return state.PID, "/tmp/drive9.pid", nil
		},
		pidAlive: func(int) bool { return false },
		packAfterUnmount: func(ctx context.Context, gotState mountstate.ProcessState, archives []string, paths []string) error {
			gotArchives = append([]string(nil), archives...)
			if len(paths) != 0 {
				t.Fatalf("auto pack paths = %v, want profile defaults", paths)
			}
			return nil
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}
	if err := runUmount([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	defaultArchive, err := defaultPackArchivePath(state.RemoteRoot, state.Profile)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotArchives, []string{":" + defaultArchive}) {
		t.Fatalf("archives = %v, want default archive", gotArchives)
	}
}

func TestRunUmountDoesNotAutoPackWhenProfileHasNoPackPaths(t *testing.T) {
	state := mountstate.ProcessState{
		PID:        1234,
		MountPoint: "/mnt/drive9",
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  filepath.Join(t.TempDir(), "local"),
	}
	packCalled := false
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func(argv []string) error { return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return state, "/tmp/drive9.pid", nil
		},
		readPID: func(string) (int, string, error) {
			return state.PID, "/tmp/drive9.pid", nil
		},
		pidAlive: func(int) bool { return false },
		packAfterUnmount: func(context.Context, mountstate.ProcessState, []string, []string) error {
			packCalled = true
			return nil
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}
	if err := runUmount([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if packCalled {
		t.Fatal("packAfterUnmount was called for a profile with no pack paths")
	}
}

func TestPackAuthFromMountStateUsesCredentialSnapshot(t *testing.T) {
	state := mountstate.ProcessState{
		Server:         "https://mounted.example",
		CredentialKind: mountstate.CredentialKindToken,
		Token:          "mounted-token",
	}
	got, err := packAuthFromMountState(state)
	if err != nil {
		t.Fatalf("packAuthFromMountState: %v", err)
	}
	want := mountPackAuth{Server: "https://mounted.example", Token: "mounted-token"}
	if got != want {
		t.Fatalf("pack auth = %#v, want %#v", got, want)
	}
}

func TestMountCmdUnpacksBeforeFuseMount(t *testing.T) {
	stubMountProfileAppendLogProbe(t)
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	srcLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(srcLocalRoot, "overlay", "repo", ".git", "config"), []byte("restored\n"), 0o644)
	var archive bytes.Buffer
	if _, err := writePackArchive(context.Background(), &archive, packOptions{
		LocalRoot:  srcLocalRoot,
		RemoteRoot: "/remote",
		Paths:      []string{"repo/.git"},
	}); err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/fs/packs/archive.tar.gz":
			_, _ = w.Write(archive.Bytes())
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localRoot := t.TempDir()
	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	if err := MountCmd([]string{
		"--foreground",
		"--mode", "fuse",
		"--server", srv.URL,
		"--api-key", "sk-test",
		"--profile", "coding-agent",
		"--local-root", localRoot,
		"--unpack", ":/packs/archive.tar.gz",
		t.TempDir(),
	}); err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	assertFileContent(t, filepath.Join(localRoot, "overlay", "repo", ".git", "config"), "restored\n")
}

func TestMountCmdAutoUnpacksCodingAgentPackBeforeFuseMount(t *testing.T) {
	stubMountProfileAppendLogProbe(t)
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })
	writeTestProfile(t, "with-pack", "[pack]\n.git\n")

	srcLocalRoot := t.TempDir()
	mustWriteFile(t, filepath.Join(srcLocalRoot, "overlay", "repo", ".git", "config"), []byte("auto-restored\n"), 0o644)
	var archive bytes.Buffer
	if _, err := writePackArchive(context.Background(), &archive, packOptions{
		LocalRoot:  srcLocalRoot,
		RemoteRoot: "/remote",
		Paths:      []string{"repo/.git"},
	}); err != nil {
		t.Fatalf("writePackArchive: %v", err)
	}
	defaultArchive, err := defaultPackArchivePath("/remote", "with-pack")
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/fs" + defaultArchive:
			_, _ = w.Write(archive.Bytes())
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	localRoot := t.TempDir()
	mountFuse = func(opts *mountFuseOptions) error { return nil }

	if err := MountCmd([]string{
		"--foreground",
		"--mode", "fuse",
		"--server", srv.URL,
		"--api-key", "sk-test",
		"--profile", "with-pack",
		"--local-root", localRoot,
		":/remote",
		t.TempDir(),
	}); err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	assertFileContent(t, filepath.Join(localRoot, "overlay", "repo", ".git", "config"), "auto-restored\n")
}

func writeTestProfile(t *testing.T, name string, body string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".drive9", "profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", dir, err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func assertFileContent(t *testing.T, path string, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("ReadFile(%s) = %q, want %q", path, got, want)
	}
}

// A link graph can compose into an escape even when every target is lexically
// inside the overlay: "repo/s -> .." and "repo/a -> s/../outside" cleans to
// overlay/repo/outside but resolves one level above the overlay once "s" is
// followed. The staged graph is validated with the links materialized.
func TestExtractPackArchiveRejectsComposedSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	build := func(t *testing.T, forward bool) []byte {
		t.Helper()
		type entry struct{ name, target string }
		entries := []entry{
			{"repo/s", ".."},
			{"repo/a", "s/../outside"},
		}
		if forward {
			entries = []entry{
				{"repo/a", "s/../outside"},
				{"repo/s", ".."},
			}
		}
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo"]}`)
		if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
			t.Fatalf("manifest header: %v", err)
		}
		if _, err := tw.Write(manifest); err != nil {
			t.Fatalf("manifest write: %v", err)
		}
		for _, e := range entries {
			if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + e.name, Typeflag: tar.TypeSymlink, Linkname: e.target, Mode: 0o777}); err != nil {
				t.Fatalf("symlink header %s: %v", e.name, err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatalf("tar close: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
		return buf.Bytes()
	}

	for _, forward := range []bool{false, true} {
		name := "backward reference"
		if forward {
			name = "forward reference"
		}
		t.Run(name, func(t *testing.T) {
			localRoot := t.TempDir()
			_, err := extractPackArchive(context.Background(), bytes.NewReader(build(t, forward)), unpackOptions{
				LocalRoot: localRoot,
				Replace:   true,
			})
			if err == nil {
				t.Fatal("extractPackArchive accepted a composed symlink escape")
			}
			if !strings.Contains(err.Error(), "resolves outside the local overlay root") {
				t.Fatalf("error = %v, want a composed escape error", err)
			}
			// Nothing was installed.
			if _, statErr := os.Lstat(filepath.Join(localRoot, "overlay", "repo")); statErr == nil {
				t.Fatal("rejected archive was installed anyway")
			}
		})
	}
}

// A composed chain that stays inside the overlay must still restore.
func TestExtractPackArchiveAllowsComposedSymlinkInsideOverlay(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	manifest := []byte(`{"format":"drive9.pack.v1","version":1,"paths":["/repo"]}`)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write(manifest); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	for _, e := range [][2]string{
		{"repo/dir", "."},
		{"repo/link", "dir/../dir"},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: packArchiveEntryPrefix + e[0], Typeflag: tar.TypeSymlink, Linkname: e[1], Mode: 0o777}); err != nil {
			t.Fatalf("symlink header %s: %v", e[0], err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	localRoot := t.TempDir()
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(buf.Bytes()), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	}); err != nil {
		t.Fatalf("extractPackArchive: %v", err)
	}
	if got, err := os.Readlink(filepath.Join(localRoot, "overlay", "repo", "link")); err != nil || got != "dir/../dir" {
		t.Fatalf("link = %q, %v", got, err)
	}
}

// The staged tree cannot see links the destination already keeps, so an archive
// whose only link composes with a retained one must be refused by validating the
// overlay that remains after installation.
func TestExtractPackArchiveRejectsEscapeViaRetainedLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	localRoot := t.TempDir()
	// A safe link already present in the destination.
	if err := os.MkdirAll(filepath.Join(localRoot, "overlay", "repo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", filepath.Join(localRoot, "overlay", "repo", "s")); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo/a"]}`,
		packTestEntry{path: "repo/a", link: "s/../outside"})
	_, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	})
	if err == nil {
		t.Fatal("extractPackArchive accepted a link that escapes through a retained link")
	}
	if !strings.Contains(err.Error(), "resolves outside the local overlay root") {
		t.Fatalf("error = %v, want a composed escape error", err)
	}
	if _, statErr := os.Lstat(filepath.Join(localRoot, "overlay", "repo", "a")); statErr == nil {
		t.Fatal("the escaping link stayed live after the rejection")
	}
	if got, readErr := os.Readlink(filepath.Join(localRoot, "overlay", "repo", "s")); readErr != nil || got != ".." {
		t.Fatalf("retained link changed: %q, %v", got, readErr)
	}
}

// packTestEntry describes one archive member: a symlink when link is set, a
// regular file otherwise.
type packTestEntry struct {
	path string
	link string
	body string
}

func buildPackTestArchive(t *testing.T, manifest string, entries ...packTestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: packManifestEntryName, Mode: 0o644, Size: int64(len(manifest))}); err != nil {
		t.Fatalf("manifest header: %v", err)
	}
	if _, err := tw.Write([]byte(manifest)); err != nil {
		t.Fatalf("manifest write: %v", err)
	}
	for _, e := range entries {
		hdr := &tar.Header{Name: packArchiveEntryPrefix + e.path, Mode: 0o644}
		if e.link != "" {
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = e.link
			hdr.Mode = 0o777
		} else {
			hdr.Typeflag = tar.TypeReg
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("archive header %s: %v", e.path, err)
		}
		if e.link == "" {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("archive write %s: %v", e.path, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// Validating only the links an archive carries misses a retained link that the
// archive re-targets: replacing a directory with "s -> .." changes what a
// pre-existing "a -> s/../outside" resolves to even though the archive never
// names "a".
func TestExtractPackArchiveRejectsEscapeFromRetargetedRetainedLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	localRoot := t.TempDir()
	repoDir := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(filepath.Join(repoDir, "s"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Safe while "s" is a directory: s/../outside resolves inside overlay/repo.
	if err := os.Symlink("s/../outside", filepath.Join(repoDir, "a")); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo/s"]}`,
		packTestEntry{path: "repo/s", link: ".."})
	_, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	})
	if err == nil {
		t.Fatal("extractPackArchive accepted an archive that re-targeted a retained link out of the overlay")
	}
	if !strings.Contains(err.Error(), "resolves outside the local overlay root") {
		t.Fatalf("error = %v, want a resolution error for the retained link", err)
	}
	if _, statErr := os.Lstat(filepath.Join(repoDir, "a")); statErr == nil {
		t.Fatal("the retained link the archive made unsafe stayed live")
	}
	if got, readErr := os.Readlink(filepath.Join(repoDir, "s")); readErr != nil || got != ".." {
		t.Fatalf("installed link = %q, %v", got, readErr)
	}
}

// Rejection must unwind every escaping link, not just the first one the walk
// happens to reach: the others are already installed and would stay live.
func TestExtractPackArchiveRemovesEveryEscapingLink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	localRoot := t.TempDir()
	repoDir := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", filepath.Join(repoDir, "s")); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo/a","/repo/b"]}`,
		packTestEntry{path: "repo/a", link: "s/../outside-a"},
		packTestEntry{path: "repo/b", link: "s/../outside-b"})
	_, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	})
	if err == nil {
		t.Fatal("extractPackArchive accepted two links that escape through a retained link")
	}
	if !strings.Contains(err.Error(), "2 symlink(s)") {
		t.Fatalf("error = %v, want both offenders reported", err)
	}
	for _, name := range []string{"a", "b"} {
		if _, statErr := os.Lstat(filepath.Join(repoDir, name)); statErr == nil {
			t.Fatalf("escaping link %s stayed live after the rejection", name)
		}
	}
	if got, readErr := os.Readlink(filepath.Join(repoDir, "s")); readErr != nil || got != ".." {
		t.Fatalf("retained link changed: %q, %v", got, readErr)
	}
}

// A failed installation has already written part of the archive, so the sweep
// has to run on that path too instead of returning the install error straight
// away.
func TestExtractPackArchiveSweepsEscapesAfterInstallFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	localRoot := t.TempDir()
	repoDir := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Retained link that makes the archive's link escape during installation.
	if err := os.Symlink("..", filepath.Join(repoDir, "s")); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}
	// A symlinked parent makes the second merge fail after the first path is
	// installed, which is exactly the window the sweep has to cover.
	if err := os.Symlink(".", filepath.Join(repoDir, "out")); err != nil {
		t.Fatalf("plant blocking link: %v", err)
	}

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo/a","/repo/out/x"]}`,
		packTestEntry{path: "repo/a", link: "s/../outside"},
		packTestEntry{path: "repo/out/x", body: "payload\n"})
	_, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
	})
	if err == nil {
		t.Fatal("extractPackArchive accepted a link that escapes through a retained link")
	}
	if !strings.Contains(err.Error(), "resolves outside the local overlay root") {
		t.Fatalf("error = %v, want the installed escape reported alongside the install failure", err)
	}
	if _, statErr := os.Lstat(filepath.Join(repoDir, "a")); statErr == nil {
		t.Fatal("a link installed before the failure stayed live after the rejection")
	}
	if got, readErr := os.Readlink(filepath.Join(repoDir, "out")); readErr != nil || got != "." {
		t.Fatalf("unrelated retained link changed: %q, %v", got, readErr)
	}
}

// The audit is scoped to what the unpack writes. An unrelated link that already
// pointed outside the overlay is local state this archive neither installs nor
// changes, so a valid unpack must neither delete it nor fail because of it.
func TestExtractPackArchiveLeavesUnrelatedRetainedLinksAlone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	for _, replace := range []bool{true, false} {
		name := "replace"
		if !replace {
			name = "merge"
		}
		t.Run(name, func(t *testing.T) {
			localRoot := t.TempDir()
			overlay := filepath.Join(localRoot, "overlay")
			if err := os.MkdirAll(filepath.Join(overlay, "other"), 0o755); err != nil {
				t.Fatal(err)
			}
			unrelated := filepath.Join(overlay, "other", "cache")
			if err := os.Symlink("/opt/shared-cache", unrelated); err != nil {
				t.Fatalf("plant unrelated link: %v", err)
			}

			archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo/new.txt"]}`,
				packTestEntry{path: "repo/new.txt", body: "hello\n"})
			if _, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
				LocalRoot: localRoot,
				Replace:   replace,
			}); err != nil {
				t.Fatalf("a valid unpack must not fail over an unrelated local link: %v", err)
			}
			if got, err := os.Readlink(unrelated); err != nil || got != "/opt/shared-cache" {
				t.Fatalf("unrelated retained link changed: %q, %v", got, err)
			}
			if body, err := os.ReadFile(filepath.Join(overlay, "repo", "new.txt")); err != nil || string(body) != "hello\n" {
				t.Fatalf("installed file = %q, %v", body, err)
			}
		})
	}
}

// A traversal error must not abort the audit: returning it would stop the walk
// and leave an escape that was installed earlier in it live.
func TestExtractPackArchiveAuditSurvivesUnreadableSubtree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores mode 000")
	}
	localRoot := t.TempDir()
	overlay := filepath.Join(localRoot, "overlay")
	if err := os.MkdirAll(filepath.Join(overlay, "z"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Retained link the archive's own link escapes through.
	if err := os.Symlink("..", filepath.Join(overlay, "z", "s")); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}
	// Sorts before "z", so the walk hits it first.
	private := filepath.Join(overlay, "a-private")
	if err := os.MkdirAll(private, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(private, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(private, 0o755) })

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/z/a"]}`,
		packTestEntry{path: "z/a", link: "s/../outside"})
	_, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
		Replace:   true,
	})
	if err == nil {
		t.Fatal("extractPackArchive accepted a link that escapes through a retained link")
	}
	if !strings.Contains(err.Error(), "resolves outside the local overlay root") {
		t.Fatalf("error = %v, want the installed escape reported alongside the scan error", err)
	}
	if _, statErr := os.Lstat(filepath.Join(overlay, "z", "a")); statErr == nil {
		t.Fatal("the installed escape stayed live after the walk hit an unreadable directory")
	}
}

// Selecting a directory does not make its untouched descendants the archive's
// business: merging /repo into an existing directory must preserve a retained
// child link the archive never names.
func TestExtractPackArchivePreservesUntouchedDescendantOnDirectoryMerge(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	localRoot := t.TempDir()
	repoDir := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	retained := filepath.Join(repoDir, "cache")
	if err := os.Symlink("/opt/shared-cache", retained); err != nil {
		t.Fatalf("plant retained link: %v", err)
	}

	archive := buildPackTestArchive(t, `{"format":"drive9.pack.v1","version":1,"paths":["/repo"]}`,
		packTestEntry{path: "repo/new.txt", body: "hello\n"})
	if _, err := extractPackArchive(context.Background(), bytes.NewReader(archive), unpackOptions{
		LocalRoot: localRoot,
	}); err != nil {
		t.Fatalf("merging into a directory must not fail over an untouched child link: %v", err)
	}
	if got, err := os.Readlink(retained); err != nil || got != "/opt/shared-cache" {
		t.Fatalf("untouched descendant link changed: %q, %v", got, err)
	}
	if body, err := os.ReadFile(filepath.Join(repoDir, "new.txt")); err != nil || string(body) != "hello\n" {
		t.Fatalf("installed file = %q, %v", body, err)
	}
}

// Scope matching must follow the destination filesystem's own name semantics:
// on a case-insensitive filesystem the installed entry can be reached through a
// spelling the archive never wrote, and a literal comparison would classify the
// archive's own link as unrelated and leave it live.
func TestPackPathWithinUsesFilesystemIdentity(t *testing.T) {
	dir := t.TempDir()
	realDir := filepath.Join(dir, "Repo")
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(realDir, "a")
	if err := os.WriteFile(installed, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// An aliased ancestor is the portable stand-in for the case-insensitive
	// lookup: same directory entry, different spelling.
	aliasDir := filepath.Join(dir, "repo")
	if err := os.Symlink(realDir, aliasDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	aliased := filepath.Join(aliasDir, "a")
	if aliased == installed {
		t.Fatal("test setup produced identical spellings")
	}
	if !packPathWithin(aliased, []string{installed}) {
		t.Fatal("a differently spelled alias of an installed path was treated as unrelated")
	}
	if !packPathWithin(installed, []string{aliased}) {
		t.Fatal("scope entry reached through an alias was not matched")
	}

	unrelated := filepath.Join(dir, "other")
	if err := os.WriteFile(unrelated, []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	if packPathWithin(unrelated, []string{installed}) {
		t.Fatal("distinct filesystem objects compared equal")
	}
	if packPathWithin(filepath.Join(realDir, "missing"), []string{installed}) {
		t.Fatal("a path that does not exist matched an unrelated scope entry")
	}
	if !packPathUnder(filepath.Join(installed, "child"), installed) {
		t.Fatal("descendant of a scope entry was not matched")
	}
}
