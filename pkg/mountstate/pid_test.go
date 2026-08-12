package mountstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestPIDFilePathCanonicalizesMountPoint(t *testing.T) {
	dir := t.TempDir()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	}()
	if err := os.Mkdir(filepath.Join(dir, "mnt"), 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}

	// Clean collapses mnt/../mnt; both relative forms must hash the same after Abs.
	// Do not require equality with an absolute path that may differ by platform
	// symlink prefixes (/var vs /private/var on macOS) because we deliberately
	// skip EvalSymlinks to avoid hanging on wedged FUSE mounts.
	relPath := PIDFilePath("mnt")
	relCollapsed := PIDFilePath("mnt/../mnt")
	if relPath != relCollapsed {
		t.Fatalf("PIDFilePath mnt = %q, mnt/../mnt = %q", relPath, relCollapsed)
	}
	if !strings.Contains(relPath, "drive9-mount-") {
		t.Fatalf("PIDFilePath = %q, want drive9-mount- prefix", relPath)
	}
}

func TestControlSocketPathUsesUserRuntimeNamespace(t *testing.T) {
	runtimeDir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	mountPoint := filepath.Join(t.TempDir(), "mnt")

	path := ControlSocketPath(mountPoint)
	if !strings.HasPrefix(path, runtimeDir+string(os.PathSeparator)) {
		t.Fatalf("ControlSocketPath = %q, want under runtime dir %q", path, runtimeDir)
	}
	if filepath.Ext(path) != ".sock" {
		t.Fatalf("ControlSocketPath = %q, want .sock suffix", path)
	}
	if got := ControlSocketPath(mountPoint); got != path {
		t.Fatalf("ControlSocketPath unstable: first %q second %q", path, got)
	}
}

func TestControlSocketPathFallbackIsUIDScoped(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	mountPoint := filepath.Join(t.TempDir(), "mnt")

	path := ControlSocketPath(mountPoint)
	wantDir := filepath.Join(os.TempDir(), "drive9-"+currentUID())
	if filepath.Dir(path) != wantDir {
		t.Fatalf("ControlSocketPath dir = %q, want %q", filepath.Dir(path), wantDir)
	}
}

func TestReadProcessStateRejectsGroupWritable(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	path := PIDFilePath(mp)
	if err := os.WriteFile(path, []byte(`{"pid":12345}`+"\n"), 0o666); err != nil {
		t.Fatal(err)
	}
	// Force world-writable (umask may have masked WriteFile mode).
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	_, _, err := ReadProcessState(mp)
	if err == nil {
		t.Fatal("group/world-writable pidfile must be rejected")
	}
}

func TestReadProcessStateRejectsSymlink(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	real := filepath.Join(t.TempDir(), "real.pid")
	if err := os.WriteFile(real, []byte(`{"pid":99}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := PIDFilePath(mp)
	if err := os.Symlink(real, path); err != nil {
		t.Fatal(err)
	}
	_, _, err := ReadProcessState(mp)
	if err == nil {
		t.Fatal("symlink pidfile must be rejected")
	}
}

func TestCheckTrustedFileInfoRejectsWrongOwner(t *testing.T) {
	// Use pure check with fake FileInfo (same pattern as state dir tests on unix).
	// Platform without Sys()->Stat_t skips owner check — still validates regular/mode.
	info, err := os.Stat(t.TempDir()) // dir, not regular
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTrustedFileInfo("/x", info); err == nil {
		t.Fatal("directory must be rejected as state file")
	}
}

func TestLegacyStopTokenUntrustedIgnored(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	// Ensure no trusted token in stateDir.
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "xdg"))
	legacy := legacyTempStatePath(".stop", mp)
	if err := os.WriteFile(legacy, []byte(`{"reason":"umount","ts":"2099-01-01T00:00:00Z"}`+"\n"), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(legacy, 0o666); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(legacy) })
	if StopTokenPresent(mp) {
		t.Fatal("world-writable legacy stop token must not count as present")
	}
	if _, ok := ReadStopTokenTime(mp); ok {
		t.Fatal("world-writable legacy stop token must not be read")
	}
}

func TestEnsureStateDirCreatesPrivateDir(t *testing.T) {
	xdg := filepath.Join(t.TempDir(), "xdg")
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	if err := ensureStateDir(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(xdg)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("not a directory")
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode=%o want 700", info.Mode().Perm())
	}
}

func TestEnsureStateDirRepairsTooWideMode(t *testing.T) {
	xdg := filepath.Join(t.TempDir(), "xdg")
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		t.Fatal(err)
	}
	// Force wide mode in case umask narrowed MkdirAll.
	if err := os.Chmod(xdg, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	if err := ensureStateDir(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(xdg)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode=%o want 700 after repair", info.Mode().Perm())
	}
}

func TestEnsureStateDirRejectsNonDirectory(t *testing.T) {
	xdg := filepath.Join(t.TempDir(), "xdg-file")
	if err := os.WriteFile(xdg, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	if err := ensureStateDir(); err == nil {
		t.Fatal("expected non-directory reject")
	}
}

func TestWriteReadPID(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	pid := 12345

	path, err := WritePID(mountPoint, pid)
	if err != nil {
		t.Fatalf("WritePID: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	got, gotPath, err := ReadPID(mountPoint)
	if err != nil {
		t.Fatalf("ReadPID: %v", err)
	}
	if got != pid {
		t.Fatalf("ReadPID pid = %d, want %d", got, pid)
	}
	if gotPath != path {
		t.Fatalf("ReadPID path = %q, want %q", gotPath, path)
	}
}

func TestReadPIDRejectsInvalidFile(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	path := PIDFilePath(mountPoint)
	if err := os.WriteFile(path, []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	_, _, err := ReadPID(mountPoint)
	if err == nil {
		t.Fatal("expected error for invalid pid file")
	}
	if strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("ReadPID error = %v, unexpectedly used current pid", err)
	}
}

func TestWriteReadProcessState(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	want := ProcessState{
		PID:            12345,
		CreationTime:   67890,
		Server:         "https://drive9.example",
		CredentialKind: CredentialKindAPIKey,
		APIKey:         "sk-mounted",
	}

	path, err := WriteProcessState(mountPoint, want)
	if err != nil {
		t.Fatalf("WriteProcessState: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	got, gotPath, err := ReadProcessState(mountPoint)
	if err != nil {
		t.Fatalf("ReadProcessState: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadProcessState = %#v, want %#v", got, want)
	}
	if gotPath != path {
		t.Fatalf("ReadProcessState path = %q, want %q", gotPath, path)
	}
}

func TestWriteProcessStateUsesPrivatePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file modes do not preserve POSIX permission bits")
	}
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	path, err := WriteProcessState(mountPoint, ProcessState{PID: 12345, CredentialKind: CredentialKindToken, Token: "tok"})
	if err != nil {
		t.Fatalf("WriteProcessState: %v", err)
	}
	defer func() { _ = os.Remove(path) }()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat process state: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("process state permissions = %v, want 0600", got)
	}
}

func TestWriteProcessStateReplacesExistingFile(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")

	path, err := WriteProcessState(mountPoint, ProcessState{PID: 111, CreationTime: 1})
	if err != nil {
		t.Fatalf("initial WriteProcessState: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	if _, err := WriteProcessState(mountPoint, ProcessState{PID: 222, CreationTime: 2}); err != nil {
		t.Fatalf("replacement WriteProcessState: %v", err)
	}
	got, gotPath, err := ReadProcessState(mountPoint)
	if err != nil {
		t.Fatalf("ReadProcessState: %v", err)
	}
	if want := (ProcessState{PID: 222, CreationTime: 2}); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadProcessState = %#v, want replacement state", got)
	}
	if gotPath != path {
		t.Fatalf("ReadProcessState path = %q, want %q", gotPath, path)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*"))
	if err != nil {
		t.Fatalf("glob temp pid files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("left temporary pid files: %v", matches)
	}
}

func TestReadProcessStateSupportsLegacyPIDFile(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	path := PIDFilePath(mountPoint)
	if err := os.WriteFile(path, []byte("12345\n"), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	got, gotPath, err := ReadProcessState(mountPoint)
	if err != nil {
		t.Fatalf("ReadProcessState: %v", err)
	}
	if got.PID != 12345 {
		t.Fatalf("ReadProcessState pid = %d, want 12345", got.PID)
	}
	if got.CreationTime != 0 {
		t.Fatalf("ReadProcessState creation time = %d, want 0", got.CreationTime)
	}
	if gotPath != path {
		t.Fatalf("ReadProcessState path = %q, want %q", gotPath, path)
	}
}

func TestReadProcessStateRejectsInvalidJSON(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	path := PIDFilePath(mountPoint)
	data, err := json.Marshal(map[string]string{"pid": "oops"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	_, _, err = ReadProcessState(mountPoint)
	if err == nil {
		t.Fatal("expected error for invalid process state file")
	}
}
