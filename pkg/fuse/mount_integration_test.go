package fuse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var execLookPathForTest = exec.LookPath

var execCommandForTest = exec.Command

type mountTestNode struct {
	isDir bool
	data  []byte
	rev   int64
	mtime time.Time
}

func fsPathFromRequest(r *http.Request) string {
	p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return pathpkg.Clean(p)
}

func parentDirPath(p string) string {
	d := pathpkg.Dir(p)
	if d == "." {
		return "/"
	}
	return d
}

func listEntriesForDir(dir string, nodes map[string]mountTestNode) []map[string]any {
	seen := make(map[string]struct{})
	entries := make([]map[string]any, 0)

	for p, node := range nodes {
		if p == "/" || p == "/.go-fuse-epoll-hack" {
			continue
		}

		var rest string
		var childPath string
		if dir == "/" {
			if !strings.HasPrefix(p, "/") {
				continue
			}
			rest = strings.TrimPrefix(p, "/")
			if rest == "" {
				continue
			}
			parts := strings.SplitN(rest, "/", 2)
			childPath = "/" + parts[0]
		} else {
			prefix := dir + "/"
			if !strings.HasPrefix(p, prefix) {
				continue
			}
			rest = strings.TrimPrefix(p, prefix)
			if rest == "" {
				continue
			}
			parts := strings.SplitN(rest, "/", 2)
			childPath = dir + "/" + parts[0]
		}

		if _, ok := seen[childPath]; ok {
			continue
		}
		seen[childPath] = struct{}{}

		child, ok := nodes[childPath]
		if !ok {
			// Child directory may be implied by descendants.
			child = mountTestNode{isDir: true, rev: 1, mtime: node.mtime}
		}
		name := strings.TrimPrefix(childPath, dir+"/")
		if dir == "/" {
			name = strings.TrimPrefix(childPath, "/")
		}
		entries = append(entries, map[string]any{
			"name":  name,
			"size":  int64(len(child.data)),
			"isDir": child.isDir,
			"mtime": child.mtime.Unix(),
		})
	}

	return entries
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func newMountTestServer() *httptest.Server {
	var mu sync.Mutex
	nodes := map[string]mountTestNode{
		"/": {
			isDir: true,
			rev:   1,
			mtime: time.Now(),
		},
		"/.go-fuse-epoll-hack": {
			isDir: false,
			rev:   1,
			mtime: time.Now(),
		},
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := fsPathFromRequest(r)

		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			n, ok := nodes[p]
			mu.Unlock()
			if !ok {
				writeErr(w, http.StatusNotFound, "not found")
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(n.data)))
			if n.isDir {
				w.Header().Set("X-Dat9-IsDir", "true")
			} else {
				w.Header().Set("X-Dat9-IsDir", "false")
			}
			w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", n.rev))
			w.Header().Set("X-Dat9-Mtime", fmt.Sprintf("%d", n.mtime.Unix()))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.RawQuery == "list=1" {
				mu.Lock()
				n, ok := nodes[p]
				if !ok {
					mu.Unlock()
					writeErr(w, http.StatusNotFound, "not found")
					return
				}
				if !n.isDir {
					mu.Unlock()
					writeErr(w, http.StatusBadRequest, "not a directory")
					return
				}
				entries := listEntriesForDir(p, nodes)
				mu.Unlock()
				writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
				return
			}
			mu.Lock()
			n, ok := nodes[p]
			mu.Unlock()
			if !ok {
				writeErr(w, http.StatusNotFound, "not found")
				return
			}
			if n.isDir {
				writeErr(w, http.StatusBadRequest, "cannot read directory")
				return
			}
			_, _ = w.Write(n.data)
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			expectedStr := r.Header.Get("X-Dat9-Expected-Revision")

			mu.Lock()
			defer mu.Unlock()

			if parent, ok := nodes[parentDirPath(p)]; !ok || !parent.isDir {
				writeErr(w, http.StatusNotFound, "parent not found")
				return
			}

			cur, exists := nodes[p]
			if exists && cur.isDir {
				writeErr(w, http.StatusBadRequest, "path is a directory")
				return
			}

			if expectedStr != "" {
				expected, err := strconv.ParseInt(expectedStr, 10, 64)
				if err != nil {
					writeErr(w, http.StatusBadRequest, "invalid expected revision")
					return
				}
				switch {
				case expected == 0 && exists:
					writeErr(w, http.StatusConflict, "revision conflict")
					return
				case expected > 0 && (!exists || cur.rev != expected):
					writeErr(w, http.StatusConflict, "revision conflict")
					return
				}
			}

			nextRev := int64(1)
			if exists {
				nextRev = cur.rev + 1
			}
			nodes[p] = mountTestNode{isDir: false, data: append([]byte(nil), data...), rev: nextRev, mtime: time.Now()}
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			switch {
			case r.URL.Query().Has("create"):
				mu.Lock()
				defer mu.Unlock()
				if parent, ok := nodes[parentDirPath(p)]; !ok || !parent.isDir {
					writeErr(w, http.StatusNotFound, "parent not found")
					return
				}
				if _, exists := nodes[p]; exists {
					writeErr(w, http.StatusConflict, "already exists")
					return
				}
				now := time.Now()
				nodes[p] = mountTestNode{isDir: false, rev: 1, mtime: now}
				writeJSON(w, http.StatusOK, map[string]any{
					"path":     p,
					"revision": int64(1),
					"size":     int64(0),
					"status":   "CONFIRMED",
					"mtime":    now.Unix(),
				})
			case r.URL.Query().Has("mkdir"):
				mu.Lock()
				defer mu.Unlock()
				if parent, ok := nodes[parentDirPath(p)]; !ok || !parent.isDir {
					writeErr(w, http.StatusNotFound, "parent not found")
					return
				}
				if existing, ok := nodes[p]; ok {
					if existing.isDir {
						w.WriteHeader(http.StatusOK)
						return
					}
					writeErr(w, http.StatusConflict, "already exists")
					return
				}
				nodes[p] = mountTestNode{isDir: true, rev: 1, mtime: time.Now()}
				w.WriteHeader(http.StatusOK)
			case r.URL.Query().Has("rename"):
				src := r.Header.Get("X-Dat9-Rename-Source")
				if src == "" {
					writeErr(w, http.StatusBadRequest, "missing rename source")
					return
				}
				src = pathpkg.Clean(src)

				mu.Lock()
				defer mu.Unlock()
				n, ok := nodes[src]
				if !ok {
					writeErr(w, http.StatusNotFound, "source not found")
					return
				}
				if _, exists := nodes[p]; exists {
					writeErr(w, http.StatusConflict, "target exists")
					return
				}
				delete(nodes, src)
				nodes[p] = n
				writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
		case http.MethodDelete:
			mu.Lock()
			defer mu.Unlock()
			n, ok := nodes[p]
			if !ok {
				writeErr(w, http.StatusNotFound, "not found")
				return
			}
			if n.isDir {
				prefix := p + "/"
				for child := range nodes {
					if strings.HasPrefix(child, prefix) {
						writeErr(w, http.StatusConflict, "directory not empty")
						return
					}
				}
			}
			delete(nodes, p)
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
	closeServer := func() {
		if serverClosed {
			return
		}
		ts.Close()
		serverClosed = true
	}

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

	var stopOnce sync.Once
	var stopErr error
	mountDone := false
	var mountErr error
	joinErr := func(base error, next error) error {
		if next == nil {
			return base
		}
		if base == nil {
			return next
		}
		return fmt.Errorf("%v; %v", base, next)
	}
	stopMount := func(timeout time.Duration) error {
		stopOnce.Do(func() {
			if isMountedForTest(mountPoint) {
				if err := UnmountForTest(mountPoint); err != nil {
					stopErr = joinErr(stopErr, fmt.Errorf("UnmountForTest: %w", err))
				}
			}

			if !mountDone {
				select {
				case err := <-done:
					mountDone = true
					mountErr = err
				case <-time.After(timeout):
					stopErr = joinErr(stopErr, fmt.Errorf("mount did not exit within cleanup timeout"))
				}
			}

			if mountDone && mountErr != nil {
				stopErr = joinErr(stopErr, fmt.Errorf("mount exited with error: %w", mountErr))
			}

			if isMountedForTest(mountPoint) {
				stopErr = joinErr(stopErr, fmt.Errorf("mount still active after stop sequence"))
			}

			closeServer()
		})
		return stopErr
	}
	t.Cleanup(func() {
		if err := stopMount(10 * time.Second); err != nil {
			t.Logf("cleanup stop mount: %v", err)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		if isMountedForTest(mountPoint) {
			break
		}
		if !mountDone {
			select {
			case err := <-done:
				mountDone = true
				mountErr = err
				closeServer()
				if err != nil {
					t.Fatalf("Mount returned before ready: %v", err)
				}
				t.Fatal("Mount returned before ready")
			default:
			}
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
	if err := stopMount(10 * time.Second); err != nil {
		t.Fatalf("stopMount: %v", err)
	}
}

func TestMountCreateWriteReadPendingNew(t *testing.T) {
	runMountScenario(t, "fuse-it", "pending.txt", []byte("integration pending data"))
}

func TestMountNestedCreateWriteReadPendingNew(t *testing.T) {
	runMountScenario(t, filepath.Join("fuse-it", "alpha"), "text.txt", []byte("nested integration pending data"))
}

func UnmountForTest(mountPoint string) error {
	if !isMountedForTest(mountPoint) {
		return nil
	}

	commands := make([][]string, 0, 6)
	if runtime.GOOS == "darwin" {
		commands = append(commands, []string{"umount", mountPoint})
		if _, err := execLookPathForTest("diskutil"); err == nil {
			commands = append(commands, []string{"diskutil", "unmount", "force", mountPoint})
		}
	} else {
		if _, err := execLookPathForTest("fusermount3"); err == nil {
			commands = append(commands,
				[]string{"fusermount3", "-u", mountPoint},
				[]string{"fusermount3", "-uz", mountPoint},
			)
		}
		if _, err := execLookPathForTest("fusermount"); err == nil {
			commands = append(commands,
				[]string{"fusermount", "-u", mountPoint},
				[]string{"fusermount", "-uz", mountPoint},
			)
		}
		if _, err := execLookPathForTest("umount"); err == nil {
			commands = append(commands,
				[]string{"umount", mountPoint},
				[]string{"umount", "-l", mountPoint},
			)
		}
	}

	if len(commands) == 0 {
		return fmt.Errorf("no unmount command available")
	}

	var errs []string
	for _, argv := range commands {
		cmd := execCommandForTest(argv[0], argv[1:]...)
		if err := cmd.Run(); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", strings.Join(argv, " "), err))
		}
		if !isMountedForTest(mountPoint) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !isMountedForTest(mountPoint) {
		return nil
	}
	return fmt.Errorf("unmount failed: %s", strings.Join(errs, "; "))
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
