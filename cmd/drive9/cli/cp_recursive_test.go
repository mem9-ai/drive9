package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

// mockTreeServer is a small httptest harness that captures the
// PUT/MKDIR/COPY/LIST/BATCH-STAT side effects of a `drive9 fs cp -r`
// invocation. Tests can read the captured state to assert the
// recursive plan executed as expected without re-implementing the
// full drive9 server contract.
type mockTreeServer struct {
	mu sync.Mutex
	// existing reports paths that pre-exist on the "remote". Used by
	// BatchStat preflight tests to make the preflight fail. Maps
	// absolute path → isDir.
	existing map[string]bool
	// listEntries supplies the directory listing returned by ListCtx
	// for each dir path. Test setup populates this for remote-source
	// trees.
	listEntries map[string][]client.FileInfo
	// statResults supplies StatCtx responses for explicit paths
	// (typically the source root in remote→{local,remote} flows).
	statResults map[string]struct {
		exists bool
		isDir  bool
		size   int64
	}
	// fileBodies stores the bytes a HEAD/GET should return for
	// remote-source files (remote→local downloads use these).
	fileBodies map[string][]byte
	// recordedPuts captures the body of every PUT to /v1/fs/<path>.
	recordedPuts map[string][]byte
	// recordedMkdirs captures every successful MKDIR.
	recordedMkdirs []string
	// recordedCopies captures every server-side copy as (src, dst).
	recordedCopies [][2]string
	// failPutPath, when non-empty, makes that single PUT return 500.
	// Used to test partial-failure runtime semantics.
	failPutPath string
}

func newMockTreeServer() *mockTreeServer {
	return &mockTreeServer{
		existing:    map[string]bool{},
		listEntries: map[string][]client.FileInfo{},
		statResults: map[string]struct {
			exists, isDir bool
			size          int64
		}{},
		fileBodies:   map[string][]byte{},
		recordedPuts: map[string][]byte{},
	}
}

func (m *mockTreeServer) httpServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			var req struct {
				Paths []string `json:"paths"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &req)
			results := make([]map[string]any, len(req.Paths))
			for i, p := range req.Paths {
				isDir, exists := m.existing[p]
				if !exists {
					results[i] = map[string]any{"path": p, "status": 404, "isDir": false, "hasMode": false}
				} else {
					results[i] = map[string]any{"path": p, "status": 200, "isDir": isDir, "hasMode": false}
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			// Per-leaf upload. Optionally fail to test partial-failure.
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			if m.failPutPath != "" && path == m.failPutPath {
				http.Error(w, "injected put failure", http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			m.recordedPuts[path] = body
			// Mark as existing so subsequent BatchStat sees it.
			m.existing[path] = false
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.URL.Query().Has("mkdir"):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			m.recordedMkdirs = append(m.recordedMkdirs, path)
			m.existing[path] = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.URL.Query().Has("copy"):
			dst := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			src := r.Header.Get("X-Dat9-Copy-Source")
			m.recordedCopies = append(m.recordedCopies, [2]string{src, dst})
			m.existing[dst] = false
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/") && r.URL.Query().Has("list"):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			entries := m.listEntries[path]
			out := make([]map[string]any, len(entries))
			for i, e := range entries {
				out[i] = map[string]any{
					"name":    e.Name,
					"size":    e.Size,
					"isDir":   e.IsDir,
					"mode":    e.Mode,
					"hasMode": e.HasMode,
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			// Prefer the explicit statResults override (used for dst-
			// exists-as-file and remote-source tests). Fall back to
			// m.existing so dst-root preflight HEADs see the same
			// state as BatchStat preflights — without this the dst
			// root's "exists as dir" flag in m.existing was invisible
			// to remotePathStatus, which uses StatCtx (HEAD).
			if s, ok := m.statResults[path]; ok && s.exists {
				if s.isDir {
					w.Header().Set("X-Dat9-IsDir", "true")
				} else {
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("Content-Length", fmt.Sprintf("%d", s.size))
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			if isDir, exists := m.existing[path]; exists {
				if isDir {
					w.Header().Set("X-Dat9-IsDir", "true")
				} else {
					w.Header().Set("X-Dat9-IsDir", "false")
				}
				w.WriteHeader(http.StatusOK)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			if body, ok := m.fileBodies[path]; ok {
				_, _ = w.Write(body)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
}

// ───────────────────────── Constraint #1: destination semantics ─────────────────────────

// TestCpRecursiveLocalToRemote_DstDoesNotExist asserts the locked
// destination semantics from @adversary-1 msg 72d6eb06: when dst
// doesn't exist, copy source CONTENTS into dst/ (not dst/basename(src)/).
func TestCpRecursiveLocalToRemote_DstDoesNotExist(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{
		"a.txt":     "alpha",
		"b/c.txt":   "charlie",
		"b/d/e.txt": "echo",
	})

	mock := newMockTreeServer()
	// dst does NOT pre-exist anywhere.
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	if err := Cp(c, []string{"-r", srcDir, ":/dst"}); err != nil {
		t.Fatalf("Cp(-r): %v", err)
	}

	// All file leaves under SOURCE landed at dst/<relative path>,
	// NOT at dst/<basename(srcDir)>/<relative path>.
	wantPuts := map[string]string{
		"/dst/a.txt":     "alpha",
		"/dst/b/c.txt":   "charlie",
		"/dst/b/d/e.txt": "echo",
	}
	for path, body := range wantPuts {
		got, ok := mock.recordedPuts[path]
		if !ok {
			t.Errorf("expected PUT %q, missing (recorded: %v)", path, sortedKeys(mock.recordedPuts))
			continue
		}
		if string(got) != body {
			t.Errorf("PUT %q body = %q, want %q", path, got, body)
		}
	}

	// Destination root + intermediate dirs were created in parent-
	// first order.
	wantDirs := []string{"/dst", "/dst/b", "/dst/b/d"}
	if !equalAsSets(mock.recordedMkdirs, wantDirs) {
		t.Errorf("MKDIR set = %v, want %v", mock.recordedMkdirs, wantDirs)
	}
}

// TestCpRecursiveLocalToRemote_DstExistsAsDir confirms the locked
// rule from #drive9:72bf030c thread (msgs 72d6eb06 + 5f25d0a0):
// when dst already exists as a directory, source CONTENTS land at
// dst/<rel>, NOT at dst/<basename(src)>/<rel>, and the existing
// dst directory is ACCEPTED (no preflight rejection).
//
// Regression note: round-1 of this PR incorrectly rejected dst-
// exists-as-dir, treating any preexisting destination as a
// conflict. @adversary-2 caught this in PR review.
func TestCpRecursiveLocalToRemote_DstExistsAsDir(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{
		"a.txt":   "alpha",
		"b/c.txt": "charlie",
	})

	mock := newMockTreeServer()
	// dst pre-exists as a directory. Common case: agent-B already
	// has its workspace dir; agent-A wants to drop a scratch tree
	// into it.
	mock.existing["/dst"] = true
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	if err := Cp(c, []string{"-r", srcDir, ":/dst"}); err != nil {
		t.Fatalf("Cp(-r) with dst-exists-as-dir must accept, got: %v", err)
	}

	// Contents landed under dst/, not under dst/<basename(srcDir)>/.
	wantPuts := map[string]string{
		"/dst/a.txt":   "alpha",
		"/dst/b/c.txt": "charlie",
	}
	for path, body := range wantPuts {
		got, ok := mock.recordedPuts[path]
		if !ok {
			t.Errorf("expected PUT %q, missing (recorded: %v)", path, sortedKeys(mock.recordedPuts))
			continue
		}
		if string(got) != body {
			t.Errorf("PUT %q body = %q, want %q", path, got, body)
		}
	}

	// The dst root itself MUST NOT be re-mkdir'd (it already exists).
	// Only descendant dirs (/dst/b) should be created.
	wantDirs := []string{"/dst/b"}
	if !equalAsSets(mock.recordedMkdirs, wantDirs) {
		t.Errorf("MKDIR set = %v, want %v (must not re-mkdir existing dst root)", mock.recordedMkdirs, wantDirs)
	}
}

// TestCpRecursiveLocalToRemote_DstExistsAsFileRejects confirms that
// dst-exists-as-FILE (not dir) is rejected with a clear message —
// we refuse to overwrite a regular file with a directory tree, since
// that would be a destructive surprise the user almost certainly
// didn't intend.
func TestCpRecursiveLocalToRemote_DstExistsAsFileRejects(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{"a.txt": "alpha"})

	mock := newMockTreeServer()
	// dst pre-exists, but it's a FILE (not a dir).
	mock.statResults["/dst"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 5}
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	err := Cp(c, []string{"-r", srcDir, ":/dst"})
	if err == nil {
		t.Fatalf("expected reject when dst exists as file, got nil; puts=%v mkdirs=%v",
			sortedKeys(mock.recordedPuts), mock.recordedMkdirs)
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("error = %v, want 'not a directory' message", err)
	}
	if len(mock.recordedPuts) > 0 {
		t.Errorf("file-conflict must abort before any PUT, got %v", sortedKeys(mock.recordedPuts))
	}
	if len(mock.recordedMkdirs) > 0 {
		t.Errorf("file-conflict must abort before any MKDIR, got %v", mock.recordedMkdirs)
	}
}

// TestCpRecursiveLocalToRemote_DescendantConflictAbortsPreflight
// confirms preflight rejects when a DESCENDANT path (not dst root)
// already exists. Even when dst-exists-as-dir is accepted, descendant
// conflicts still abort before any transfer — locked by @adversary-1
// design rule #3.
func TestCpRecursiveLocalToRemote_DescendantConflictAbortsPreflight(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{
		"a.txt": "alpha",
		"b.txt": "bravo",
	})

	mock := newMockTreeServer()
	// dst is accepted-as-dir, but one descendant file already exists.
	mock.existing["/dst"] = true
	mock.existing["/dst/a.txt"] = false
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	err := Cp(c, []string{"-r", srcDir, ":/dst"})
	if err == nil {
		t.Fatalf("expected descendant-conflict preflight error, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want 'already exists' message", err)
	}
	if len(mock.recordedPuts) > 0 {
		t.Errorf("descendant preflight conflict must abort before any PUT, got %v", sortedKeys(mock.recordedPuts))
	}
}

// ───────────────────────── Constraint #2: symlink reject ─────────────────────────

// TestCpRecursiveLocalToRemote_RejectsSymlinkInTree asserts that
// recursive copy refuses to silently follow OR skip symlinks (per
// @adversary-1 msg b8603b41).
func TestCpRecursiveLocalToRemote_RejectsSymlinkInTree(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{"a.txt": "alpha"})
	// Add a symlink that would, if followed, copy /etc/passwd or
	// loop back into the source.
	if err := os.Symlink("/dev/null", filepath.Join(srcDir, "danger.lnk")); err != nil {
		t.Fatalf("create symlink fixture: %v", err)
	}

	mock := newMockTreeServer()
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", srcDir, ":/dst"})
	if err == nil {
		t.Fatalf("expected symlink reject error, got nil")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error = %v, want 'symlink' message", err)
	}
	// No partial transfer should have happened.
	if len(mock.recordedPuts) > 0 {
		t.Errorf("expected zero PUT before symlink reject, got %v", sortedKeys(mock.recordedPuts))
	}
}

func TestCpRecursiveRemoteToLocal_RejectsSymlinkInTree(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "target.txt", Size: 5, IsDir: false},
		{Name: "link", Size: 10, IsDir: false, HasMode: true, Mode: uint32(syscall.S_IFLNK) | 0o777},
	}
	mock.fileBodies["/src/target.txt"] = []byte("alpha")

	srv := mock.httpServer(t)
	defer srv.Close()

	dstLocal := filepath.Join(t.TempDir(), "dst")
	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", ":/src", dstLocal})
	if err == nil {
		t.Fatal("expected remote symlink reject error, got nil")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error = %v, want 'symlink' message", err)
	}
	if _, statErr := os.Lstat(dstLocal); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("local destination should not be created after symlink reject, stat err=%v", statErr)
	}
}

func TestCpRecursiveRemoteToRemote_RejectsSymlinkInTree(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "link", Size: 10, IsDir: false, HasMode: true, Mode: uint32(syscall.S_IFLNK) | 0o777},
	}

	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", ":/src", ":/dst"})
	if err == nil {
		t.Fatal("expected remote symlink reject error, got nil")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error = %v, want 'symlink' message", err)
	}
	if len(mock.recordedCopies) > 0 {
		t.Fatalf("expected no remote copies before symlink reject, got %v", mock.recordedCopies)
	}
	if len(mock.recordedMkdirs) > 0 {
		t.Fatalf("expected no remote mkdirs before symlink reject, got %v", mock.recordedMkdirs)
	}
}

// ───────────────────────── Constraint #3: preflight then runtime semantics ─────────────────────────

// TestCpRecursiveLocalToRemote_RuntimePartialFailureSurfacesError
// asserts the runtime rule (sibling transfers continue, but the
// overall return is non-nil if any leaf fails). Distinguished from
// preflight conflict which would abort BEFORE any transfer.
func TestCpRecursiveLocalToRemote_RuntimePartialFailureSurfacesError(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{
		"a.txt": "alpha",
		"b.txt": "bravo",
		"c.txt": "charlie",
	})

	mock := newMockTreeServer()
	mock.failPutPath = "/dst/b.txt" // 1 leaf fails at runtime, not at preflight.
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	err := Cp(c, []string{"-r", srcDir, ":/dst"})
	if err == nil {
		t.Fatalf("expected runtime failure error, got nil")
	}

	// Siblings should have still been attempted (and succeeded).
	if _, ok := mock.recordedPuts["/dst/a.txt"]; !ok {
		t.Errorf("/dst/a.txt should have been PUT despite sibling failure")
	}
	if _, ok := mock.recordedPuts["/dst/c.txt"]; !ok {
		t.Errorf("/dst/c.txt should have been PUT despite sibling failure")
	}
	// The failing leaf should NOT be in recordedPuts.
	if _, ok := mock.recordedPuts["/dst/b.txt"]; ok {
		t.Errorf("/dst/b.txt should NOT have been recorded (injected failure)")
	}
}

// ───────────────────────── Constraint #4: path safety ─────────────────────────

// TestJoinRemoteSafe_RejectsTraversal asserts the path-safety helper
// rejects `..` traversal segments that would otherwise let a
// malicious symlink target (or naive caller) write outside the dst
// base.
func TestJoinRemoteSafe_RejectsTraversal(t *testing.T) {
	cases := []struct {
		name string
		base string
		rel  string
	}{
		{"parent traversal", "/dst", "../escape.txt"},
		{"nested traversal", "/dst", "child/../../escape.txt"},
		{"deep traversal", "/dst", "a/b/c/../../../../escape.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := joinRemoteSafe(tc.base, tc.rel)
			if err == nil {
				t.Fatalf("expected error for traversal %q (got %q)", tc.rel, got)
			}
			if !strings.Contains(err.Error(), "..") {
				t.Errorf("error = %v, want '..' rejection", err)
			}
		})
	}
}

// TestJoinRemoteSafe_BoundaryNotPrefix asserts that joinRemoteSafe
// won't let a sibling-prefixed path masquerade as a child via
// boundary confusion (e.g. base "/foo" + something that lands at
// "/foobar" — Drive9 paths should be segment-anchored).
func TestJoinRemoteSafe_BoundaryNotPrefix(t *testing.T) {
	got, err := joinRemoteSafe("/foo", "bar")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "/foo/bar" {
		t.Errorf("got %q, want /foo/bar (must be segment-anchored, not prefix-anchored)", got)
	}
}

// TestJoinRemoteSafe_NormalizesTrailingSlash asserts the base-trailing
// slash is collapsed so `:/dst` and `:/dst/` produce identical leaf
// paths.
func TestJoinRemoteSafe_NormalizesTrailingSlash(t *testing.T) {
	without, _ := joinRemoteSafe("/dst", "a.txt")
	with, _ := joinRemoteSafe("/dst/", "a.txt")
	if without != with {
		t.Errorf("trailing-slash drift: %q vs %q", without, with)
	}
}

// ───────────────────────── Constraint #5: dir creation order + empty dir preservation ─────────────────────────

// TestCpRecursiveLocalToRemote_EmptyDirPreserved asserts that an
// empty directory in the source tree results in an explicit MKDIR
// for the corresponding remote path — empty dirs must not silently
// vanish (per design lock #5).
func TestCpRecursiveLocalToRemote_EmptyDirPreserved(t *testing.T) {
	srcDir := t.TempDir()
	// One file + one empty directory.
	writeTreeFiles(t, srcDir, map[string]string{"a.txt": "alpha"})
	if err := os.MkdirAll(filepath.Join(srcDir, "empty"), 0o755); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}

	mock := newMockTreeServer()
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	if err := Cp(c, []string{"-r", srcDir, ":/dst"}); err != nil {
		t.Fatalf("Cp(-r): %v", err)
	}

	wantDirs := []string{"/dst", "/dst/empty"}
	if !equalAsSets(mock.recordedMkdirs, wantDirs) {
		t.Errorf("MKDIR set = %v, want %v (empty dir must be preserved)", mock.recordedMkdirs, wantDirs)
	}
}

// TestCpRecursiveLocalToRemote_MkdirParentBeforeChild asserts the
// observed MKDIR order is parent-before-child by path length.
func TestCpRecursiveLocalToRemote_MkdirParentBeforeChild(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{
		"deep/nested/leaf.txt": "leaf",
	})

	mock := newMockTreeServer()
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	if err := Cp(c, []string{"-r", srcDir, ":/dst"}); err != nil {
		t.Fatalf("Cp(-r): %v", err)
	}

	// MKDIR must visit shorter paths before longer ones (parent
	// before child) so the server can succeed each call.
	for i := 1; i < len(mock.recordedMkdirs); i++ {
		if len(mock.recordedMkdirs[i-1]) > len(mock.recordedMkdirs[i]) {
			t.Errorf("MKDIR order broken at index %d: %q (len %d) before %q (len %d)",
				i, mock.recordedMkdirs[i-1], len(mock.recordedMkdirs[i-1]),
				mock.recordedMkdirs[i], len(mock.recordedMkdirs[i]))
		}
	}
}

// ───────────────────────── Flag composition ─────────────────────────

// TestCpRecursive_RejectsIncompatibleFlags ensures -r refuses to
// compose with single-file ergonomics flags whose semantics don't
// extend cleanly to trees.
func TestCpRecursive_RejectsIncompatibleFlags(t *testing.T) {
	srcDir := t.TempDir()
	cases := [][]string{
		{"-r", "--append", srcDir, ":/dst"},
		{"-r", "--resume", srcDir, ":/dst"},
		{"-r", "--tag", "k=v", srcDir, ":/dst"},
		{"-r", "--description", "x", srcDir, ":/dst"},
	}
	c := client.New("http://unused", "")
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			err := Cp(c, args)
			if err == nil {
				t.Fatalf("expected rejection for incompatible flags, got nil")
			}
			if !strings.Contains(err.Error(), "-r/--recursive cannot be combined") {
				t.Fatalf("error = %v, want incompatible-flags message", err)
			}
		})
	}
}

// TestCpRecursive_RejectsStdinOrStdout ensures -r refuses pipe
// endpoints (Unix `cp -r` doesn't support pipes as endpoints).
func TestCpRecursive_RejectsStdinOrStdout(t *testing.T) {
	c := client.New("http://unused", "")
	if err := Cp(c, []string{"-r", "-", ":/dst"}); err == nil || !strings.Contains(err.Error(), "stdin/stdout") {
		t.Fatalf("stdin: err = %v, want stdin/stdout reject", err)
	}
	if err := Cp(c, []string{"-r", ":/src", "-"}); err == nil || !strings.Contains(err.Error(), "stdin/stdout") {
		t.Fatalf("stdout: err = %v, want stdin/stdout reject", err)
	}
}

// TestCpRecursive_RequiresAtLeastOneRemote ensures local→local
// recursive copy is out of scope (Drive9 CLI only handles paths
// where at least one side is remote).
func TestCpRecursive_RequiresAtLeastOneRemote(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	c := client.New("http://unused", "")
	err := Cp(c, []string{"-r", dir1, dir2})
	if err == nil {
		t.Fatalf("expected remote-path requirement error, got nil")
	}
}

// TestCpRecursive_RejectsFileSource ensures `cp -r <file> ...` fails
// with a clear message redirecting to the non-recursive form.
func TestCpRecursive_RejectsFileSource(t *testing.T) {
	srcFile := filepath.Join(t.TempDir(), "single.txt")
	if err := os.WriteFile(srcFile, []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	mock := newMockTreeServer()
	srv := mock.httpServer(t)
	defer srv.Close()
	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", srcFile, ":/dst"})
	if err == nil {
		t.Fatalf("expected file-source reject, got nil")
	}
	if !strings.Contains(err.Error(), "requires a directory source") {
		t.Fatalf("error = %v, want directory-source guidance", err)
	}
}

// ───────────────────────── Remote→Remote (server-side zero-copy) ─────────────────────────

// TestCpRecursiveRemoteToRemote_UsesServerSideCopyPerLeaf asserts the
// remote→remote tree copy uses Client.Copy (server-side zero-copy)
// for every leaf — content bytes never cross the client.
func TestCpRecursiveRemoteToRemote_UsesServerSideCopyPerLeaf(t *testing.T) {
	mock := newMockTreeServer()
	// Source tree on the remote: /src is a dir with two files.
	mock.statResults["/src"] = struct {
		exists, isDir bool
		size          int64
	}{exists: true, isDir: true}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "a.txt", Size: 5, IsDir: false},
		{Name: "sub", Size: 0, IsDir: true},
	}
	mock.listEntries["/src/sub"] = []client.FileInfo{
		{Name: "b.txt", Size: 7, IsDir: false},
	}
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := Cp(c, []string{"-r", ":/src", ":/dst"}); err != nil {
		t.Fatalf("Cp(-r remote→remote): %v", err)
	}

	// Two leaf files → two COPY calls.
	wantCopies := [][2]string{
		{"/src/a.txt", "/dst/a.txt"},
		{"/src/sub/b.txt", "/dst/sub/b.txt"},
	}
	if !equalCopyPairs(mock.recordedCopies, wantCopies) {
		t.Errorf("recorded copies = %v, want %v", mock.recordedCopies, wantCopies)
	}
	// No PUTs — content never round-tripped through the client.
	if len(mock.recordedPuts) != 0 {
		t.Errorf("remote→remote must not PUT (content bytes round-trip), got %v", sortedKeys(mock.recordedPuts))
	}
	// Dir hierarchy was created.
	wantDirs := []string{"/dst", "/dst/sub"}
	if !equalAsSets(mock.recordedMkdirs, wantDirs) {
		t.Errorf("recorded MKDIR = %v, want %v", mock.recordedMkdirs, wantDirs)
	}
}

// ───────────────────────── Remote→Local (B3 + B4) ─────────────────────────

// TestCpRecursiveRemoteToLocal_HappyPath asserts the basic remote→local
// flow works: a directory tree on the remote lands as a matching local
// tree under dstLocal. Verifies all leaf files arrive byte-identical.
//
// Closes B3 coverage gap from @adversary-1 secondary review msg c7eb6852.
func TestCpRecursiveRemoteToLocal_HappyPath(t *testing.T) {
	mock := newMockTreeServer()
	// Remote tree: /src is a dir; /src/a.txt and /src/sub/b.txt are
	// files; /src/sub is a subdirectory.
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.statResults["/src/a.txt"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 5}
	mock.statResults["/src/sub/b.txt"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 7}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "a.txt", Size: 5, IsDir: false},
		{Name: "sub", Size: 0, IsDir: true},
	}
	mock.listEntries["/src/sub"] = []client.FileInfo{
		{Name: "b.txt", Size: 7, IsDir: false},
	}
	mock.fileBodies["/src/a.txt"] = []byte("alpha")
	mock.fileBodies["/src/sub/b.txt"] = []byte("bravooo")

	srv := mock.httpServer(t)
	defer srv.Close()

	dstLocal := t.TempDir()
	// Empty dstLocal — must be accepted, and contents land directly
	// inside it (not inside dstLocal/src/).
	c := client.New(srv.URL, "")
	if err := Cp(c, []string{"-r", ":/src", dstLocal}); err != nil {
		t.Fatalf("Cp(-r remote→local): %v", err)
	}

	// Verify each leaf landed at the expected local path with the
	// expected bytes.
	wantFiles := map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "bravooo",
	}
	for rel, body := range wantFiles {
		got, err := os.ReadFile(filepath.Join(dstLocal, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("read %s: %v", rel, err)
			continue
		}
		if string(got) != body {
			t.Errorf("%s = %q, want %q", rel, got, body)
		}
	}
	// Verify the descendant subdir exists.
	if info, err := os.Stat(filepath.Join(dstLocal, "sub")); err != nil || !info.IsDir() {
		t.Errorf("expected dst/sub to exist as dir: err=%v info=%v", err, info)
	}
}

// TestCpRecursiveRemoteToLocal_DstExistsAsFileRejects mirrors the
// local→remote dst-as-file rejection for the remote→local direction.
func TestCpRecursiveRemoteToLocal_DstExistsAsFileRejects(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "a.txt", Size: 5, IsDir: false},
	}
	mock.fileBodies["/src/a.txt"] = []byte("alpha")

	srv := mock.httpServer(t)
	defer srv.Close()

	// Pre-create dst as a regular file so it should be rejected.
	tmp := t.TempDir()
	dstLocal := filepath.Join(tmp, "dst-as-file")
	if err := os.WriteFile(dstLocal, []byte("preexisting"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", ":/src", dstLocal})
	if err == nil {
		t.Fatalf("expected reject when local dst exists as file, got nil")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("error = %v, want 'not a directory' message", err)
	}
	// The original file must NOT have been overwritten.
	body, _ := os.ReadFile(dstLocal)
	if string(body) != "preexisting" {
		t.Errorf("preexisting file content was modified: %q", body)
	}
}

// TestCpRecursiveRemoteToLocal_DescendantLeafConflictRejects asserts
// the remote→local direction also preflights DESCENDANT paths and
// rejects before any download starts. Without this, DownloadToFile-
// WithSummary's os.Create would silently truncate a pre-existing
// local leaf, violating the locked "no overwrite in P0" rule.
func TestCpRecursiveRemoteToLocal_DescendantLeafConflictRejects(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.statResults["/src/a.txt"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 5}
	mock.statResults["/src/b.txt"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 5}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "a.txt", Size: 5, IsDir: false},
		{Name: "b.txt", Size: 5, IsDir: false},
	}
	mock.fileBodies["/src/a.txt"] = []byte("alpha")
	mock.fileBodies["/src/b.txt"] = []byte("bravo")

	srv := mock.httpServer(t)
	defer srv.Close()

	dstLocal := t.TempDir()
	// dst-as-dir is accepted (mirrors remote semantics), but a
	// pre-existing descendant leaf MUST cause preflight to abort.
	preexisting := filepath.Join(dstLocal, "a.txt")
	if err := os.WriteFile(preexisting, []byte("DO NOT OVERWRITE"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", ":/src", dstLocal})
	if err == nil {
		t.Fatalf("expected descendant-conflict reject, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want 'already exists' message", err)
	}
	// The pre-existing file MUST NOT have been truncated/overwritten.
	body, _ := os.ReadFile(preexisting)
	if string(body) != "DO NOT OVERWRITE" {
		t.Errorf("preexisting descendant was overwritten: %q", body)
	}
	// b.txt must NOT have been downloaded either — preflight aborts
	// the whole batch.
	if _, err := os.Stat(filepath.Join(dstLocal, "b.txt")); err == nil {
		t.Errorf("/dst/b.txt was downloaded despite preflight conflict on sibling")
	}
}

// TestCpRecursiveRemoteToLocal_EmptyDirPreserved asserts an empty
// directory in the remote tree results in a corresponding local
// directory — matches local→remote empty-dir behavior.
func TestCpRecursiveRemoteToLocal_EmptyDirPreserved(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "empty", Size: 0, IsDir: true},
		{Name: "a.txt", Size: 5, IsDir: false},
	}
	mock.listEntries["/src/empty"] = nil // empty subdir
	mock.statResults["/src/a.txt"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: false, size: 5}
	mock.fileBodies["/src/a.txt"] = []byte("alpha")

	srv := mock.httpServer(t)
	defer srv.Close()

	dstLocal := t.TempDir()
	c := client.New(srv.URL, "")
	if err := Cp(c, []string{"-r", ":/src", dstLocal}); err != nil {
		t.Fatalf("Cp(-r): %v", err)
	}

	info, err := os.Stat(filepath.Join(dstLocal, "empty"))
	if err != nil {
		t.Fatalf("expected dst/empty to exist: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("dst/empty should be a directory")
	}
}

// TestCpRecursiveRemoteToLocal_RejectsPathEscape asserts that a
// malformed remote list entry (e.g. "../escape.txt") is rejected
// before any local file is written. Closes B4 coverage gap from
// @adversary-1 secondary review msg c7eb6852.
func TestCpRecursiveRemoteToLocal_RejectsPathEscape(t *testing.T) {
	mock := newMockTreeServer()
	mock.statResults["/src"] = struct {
		exists bool
		isDir  bool
		size   int64
	}{exists: true, isDir: true}
	// Malicious list response: name starts with ".." which would
	// escape dstLocal if naively joined.
	mock.listEntries["/src"] = []client.FileInfo{
		{Name: "..", Size: 0, IsDir: true},
	}
	mock.listEntries["/src/.."] = []client.FileInfo{
		{Name: "escape.txt", Size: 5, IsDir: false},
	}

	srv := mock.httpServer(t)
	defer srv.Close()

	tmpRoot := t.TempDir()
	// Sentinel file in the PARENT of dstLocal that, if path-escape
	// worked, would get overwritten by an "escape.txt" leaf.
	sentinelDir := filepath.Join(tmpRoot, "sibling")
	if err := os.MkdirAll(sentinelDir, 0o755); err != nil {
		t.Fatalf("mkdir sentinel: %v", err)
	}
	dstLocal := filepath.Join(tmpRoot, "dst")
	if err := os.MkdirAll(dstLocal, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}

	c := client.New(srv.URL, "")
	err := Cp(c, []string{"-r", ":/src", dstLocal})
	if err == nil {
		t.Fatalf("expected path-escape reject, got nil")
	}
	if !strings.Contains(err.Error(), "..") {
		t.Fatalf("error = %v, want '..' rejection", err)
	}
	// Most important: nothing was written outside dstLocal.
	if _, statErr := os.Stat(filepath.Join(tmpRoot, "escape.txt")); statErr == nil {
		t.Errorf("path escape wrote outside dstLocal: %s", filepath.Join(tmpRoot, "escape.txt"))
	}
}

// TestJoinLocalSafe_RejectsTraversal asserts the local path-safety
// helper rejects `..` traversal segments just like joinRemoteSafe.
func TestJoinLocalSafe_RejectsTraversal(t *testing.T) {
	tmp := t.TempDir()
	cases := []struct {
		name string
		rel  string
	}{
		{"parent traversal", "../escape.txt"},
		{"nested traversal", "child/../../escape.txt"},
		{"deep traversal", "a/b/c/../../../../escape.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := joinLocalSafe(tmp, tc.rel)
			if err == nil {
				t.Fatalf("expected error for traversal %q (got %q)", tc.rel, got)
			}
			if !strings.Contains(err.Error(), "..") {
				t.Errorf("error = %v, want '..' rejection", err)
			}
		})
	}
}

// TestJoinLocalSafe_RejectsAbsoluteRel asserts an absolute `rel` is
// rejected even without `..` segments — a remote-supplied "/etc/passwd"
// must not be treated as relative to dstLocal and then silently
// absolute-resolved.
func TestJoinLocalSafe_RejectsAbsoluteRel(t *testing.T) {
	tmp := t.TempDir()
	if _, err := joinLocalSafe(tmp, "/etc/passwd"); err == nil {
		t.Fatalf("expected reject for absolute rel, got nil")
	}
}

// TestJoinLocalSafe_HappyPath asserts the trivial case still resolves
// to base/rel.
func TestJoinLocalSafe_HappyPath(t *testing.T) {
	tmp := t.TempDir()
	got, err := joinLocalSafe(tmp, "sub/leaf.txt")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := filepath.Join(tmp, "sub", "leaf.txt")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ───────────────────────── B5: cancellation regressions ─────────────────────────

// TestParallelTransfer_AlreadyCancelledCtxStopsLoop is the "ctx
// cancelled BEFORE the loop even starts" regression. Verifies the
// labeled break exits the for loop and no transfer ops are launched.
func TestParallelTransfer_AlreadyCancelledCtxStopsLoop(t *testing.T) {
	ctx, cancel := contextWithCancel()
	cancel() // cancel BEFORE parallelTransfer runs

	var ops atomicCounter
	items := []int{1, 2, 3, 4, 5}
	err := parallelTransfer(ctx, items, func(_ context.Context, _ int) error {
		ops.inc()
		return nil
	})
	if err == nil {
		t.Fatalf("expected ctx.Err(), got nil")
	}
	if ops.get() != 0 {
		t.Errorf("expected 0 ops when ctx pre-cancelled, got %d", ops.get())
	}
}

// TestParallelTransfer_CancelDuringSemaphoreAcquireStopsLoop is the
// "ctx cancelled while waiting for a semaphore slot" regression. It
// saturates the worker pool with blocking ops, then cancels, and
// asserts the launcher stops promptly rather than waiting for a
// worker to finish.
//
// This is the case @adversary-1 flagged in B5: a top-of-loop ctx
// check alone is not enough — the `sem <- struct{}{}` send must also
// be ctx-aware.
func TestParallelTransfer_CancelDuringSemaphoreAcquireStopsLoop(t *testing.T) {
	// More items than worker slots so the launcher hits the
	// semaphore-full case for items beyond recursiveCopyConcurrency.
	const nItems = recursiveCopyConcurrency * 3
	items := make([]int, nItems)
	for i := range items {
		items[i] = i
	}

	ctx, cancel := contextWithCancel()
	// release blocks ops in the workers so the pool stays saturated
	// until we cancel.
	release := make(chan struct{})
	var (
		started atomicCounter
		opCalls atomicCounter
	)
	go func() {
		// Wait until the worker pool is fully saturated, then cancel.
		// This forces the launcher to be blocked on `sem <- struct{}{}`
		// for the (nItems - recursiveCopyConcurrency) remaining items
		// at the moment ctx is cancelled.
		for started.get() < recursiveCopyConcurrency {
		}
		cancel()
		close(release)
	}()

	err := parallelTransfer(ctx, items, func(_ context.Context, _ int) error {
		opCalls.inc()
		started.inc()
		<-release // block until cancellation is signalled
		return nil
	})
	if err == nil {
		t.Fatalf("expected ctx.Err(), got nil")
	}
	// At most `recursiveCopyConcurrency` ops should ever start —
	// the launcher must NOT slip another transfer in after cancel.
	if got := opCalls.get(); got > recursiveCopyConcurrency {
		t.Errorf("expected ≤ %d ops to start, got %d (launcher slipped past sem cancel)",
			recursiveCopyConcurrency, got)
	}
}

// contextWithCancel is a tiny shim so test code reads naturally. We
// keep it local to avoid an import in production code paths.
func contextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// atomicCounter is a minimal sync.Mutex-protected int used only by
// the cancellation regression tests above.
type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (a *atomicCounter) inc() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
}

func (a *atomicCounter) get() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

// ───────────────────────── Test helpers ─────────────────────────

func writeTreeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		fullPath := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", rel, err)
		}
		if err := os.WriteFile(fullPath, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
}

func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func equalAsSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, g := range got {
		gotSet[g] = struct{}{}
	}
	for _, w := range want {
		if _, ok := gotSet[w]; !ok {
			return false
		}
	}
	return true
}

func equalCopyPairs(got, want [][2]string) bool {
	if len(got) != len(want) {
		return false
	}
	gotSet := make(map[[2]string]struct{}, len(got))
	for _, g := range got {
		gotSet[g] = struct{}{}
	}
	for _, w := range want {
		if _, ok := gotSet[w]; !ok {
			return false
		}
	}
	return true
}
