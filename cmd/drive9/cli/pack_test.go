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

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountstate"
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
		LocalRoot:   srcLocalRoot,
		RemoteRoot:  "/remote/root",
		LocalPrefix: "repo",
		Profile:     "coding-agent",
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
		Paths:      []string{"repo/dist"},
	}); err != nil {
		t.Fatalf("packRemoteArchive: %v", err)
	}
	if len(uploaded) == 0 {
		t.Fatal("server did not receive archive bytes")
	}
	if !reflect.DeepEqual(gotTags, []string{"drive9.pack.format=drive9.pack.v1"}) {
		t.Fatalf("X-Dat9-Tag = %v, want pack format tag", gotTags)
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

func TestRunUmountPacksAfterUnmount(t *testing.T) {
	state := mountstate.ProcessState{
		PID:        1234,
		MountPoint: "/mnt/drive9",
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  filepath.Join(t.TempDir(), "local"),
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
			if !reflect.DeepEqual(archives, []string{":/packs/archive.tar.gz"}) {
				t.Fatalf("archives = %v", archives)
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

func TestMountCmdUnpacksBeforeFuseMount(t *testing.T) {
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
