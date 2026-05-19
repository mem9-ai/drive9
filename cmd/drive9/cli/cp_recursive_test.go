package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
		existing:     map[string]bool{},
		listEntries:  map[string][]client.FileInfo{},
		statResults:  map[string]struct{ exists, isDir bool; size int64 }{},
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
					"hasMode": false,
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
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

// TestCpRecursiveLocalToRemote_DstExistsAsDir confirms the same
// locked rule: when dst already exists as a directory, source
// CONTENTS land at dst/<rel>, NOT at dst/<basename(src)>/<rel>.
func TestCpRecursiveLocalToRemote_DstExistsAsDir(t *testing.T) {
	srcDir := t.TempDir()
	writeTreeFiles(t, srcDir, map[string]string{"a.txt": "alpha"})

	mock := newMockTreeServer()
	// dst pre-exists as a directory → preflight must catch this and
	// abort (we refuse to overwrite per design lock #3).
	mock.existing["/dst"] = true
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	err := Cp(c, []string{"-r", srcDir, ":/dst"})
	if err == nil {
		t.Fatalf("expected preflight error when dst exists, got nil; puts=%v mkdirs=%v",
			sortedKeys(mock.recordedPuts), mock.recordedMkdirs)
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error = %v, want 'already exists' message", err)
	}
	// Critical: NO PUT/MKDIR happened before the preflight failure.
	if len(mock.recordedPuts) > 0 {
		t.Errorf("preflight should have prevented any PUT, but got %v", sortedKeys(mock.recordedPuts))
	}
	if len(mock.recordedMkdirs) > 0 {
		t.Errorf("preflight should have prevented any MKDIR, but got %v", mock.recordedMkdirs)
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
	mock.statResults["/src"] = struct{ exists, isDir bool; size int64 }{exists: true, isDir: true}
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
