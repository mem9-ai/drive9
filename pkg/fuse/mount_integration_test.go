package fuse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

var execLookPathForTest = exec.LookPath

var execCommandForTest = exec.Command

func newMountTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if serveMetadataCreate(w, r) {
			return
		}
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				_, _ = io.WriteString(w, `{"entries":[]}`)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
}

func runMountScenario(t *testing.T, relDir, relFile string, want []byte) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux FUSE mount")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse not available")
	}
	if _, err := os.Stat("/bin/fusermount3"); err != nil {
		if _, err2 := os.Stat("/usr/bin/fusermount3"); err2 != nil {
			t.Skip("fusermount3 not available")
		}
	}

	ts := newMountTestServer()
	serverClosed := false
	t.Cleanup(func() {
		if !serverClosed {
			ts.Close()
		}
	})

	mountPoint := t.TempDir()
	cacheDir := t.TempDir()
	opts := &MountOptions{
		Server:        ts.URL,
		APIKey:        "test-key",
		MountPoint:    mountPoint,
		CacheDir:      cacheDir,
		FlushDebounce: 0,
		SyncMode:      SyncStrict,
	}

	done := make(chan error, 1)
	go func() {
		done <- Mount(opts)
	}()
	t.Cleanup(func() {
		_ = UnmountForTest(mountPoint)
		select {
		case err := <-done:
			if err != nil {
				t.Logf("mount exited with error during cleanup: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("mount did not exit within cleanup timeout")
		}
		ts.Close()
		serverClosed = true
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mount did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}

	rootDir := filepath.Join(mountPoint, relDir)
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	filePath := filepath.Join(rootDir, relFile)
	if err := os.WriteFile(filePath, want, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("ReadFile = %q, want %q", string(got), string(want))
	}
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := UnmountForTest(mountPoint); err != nil {
		t.Fatalf("UnmountForTest: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Mount returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mount did not exit after unmount")
	}
	ts.Close()
	serverClosed = true
}

func TestMountCreateWriteReadPendingNew(t *testing.T) {
	runMountScenario(t, "fuse-it", "pending.txt", []byte("integration pending data"))
}

func TestMountNestedCreateWriteReadPendingNew(t *testing.T) {
	runMountScenario(t, filepath.Join("fuse-it", "alpha"), "text.txt", []byte("nested integration pending data"))
}

func UnmountForTest(mountPoint string) error {
	argv := []string{"fusermount3", "-u", mountPoint}
	if runtime.GOOS == "darwin" {
		argv = []string{"umount", mountPoint}
	}
	cmd := execCommandForTest(argv[0], argv[1:]...)
	return cmd.Run()
}

func isMountedForTest(mountPoint string) bool {
	st1, err := os.Stat(mountPoint)
	if err != nil {
		return false
	}
	st2, err := os.Stat(filepath.Dir(mountPoint))
	if err != nil {
		return false
	}
	return st1.Sys() != nil && st2.Sys() != nil && st1.Sys().(*syscall.Stat_t).Dev != st2.Sys().(*syscall.Stat_t).Dev
}
