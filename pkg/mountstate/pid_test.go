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

	relPath := PIDFilePath("mnt/../mnt")
	absPath := PIDFilePath(filepath.Join(dir, "mnt"))
	if relPath != absPath {
		t.Fatalf("PIDFilePath relative = %q, absolute = %q", relPath, absPath)
	}
	if !strings.HasPrefix(relPath, os.TempDir()) {
		t.Fatalf("PIDFilePath = %q, want temp-dir path", relPath)
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
