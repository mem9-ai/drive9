package client

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDownloadDir_HappyPath seeds a multi-level remote tree (files,
// nested dirs, and an empty dir) and downloads it to a temp dir with
// DownloadDirCtx, then verifies every leaf's content and every dir's
// existence locally.
func TestDownloadDir_HappyPath(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	// Seed remote tree:
	//   /tree/
	//     a.txt          "alpha"
	//     b.txt          "bravo"
	//     sub/
	//       c.txt        "charlie"
	//       deep/
	//         d.txt      "delta"
	//     empty/
	if err := c.Mkdir("/tree"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/tree/a.txt", []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/tree/b.txt", []byte("bravo")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/tree/sub"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/tree/sub/c.txt", []byte("charlie")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/tree/sub/deep"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/tree/sub/deep/d.txt", []byte("delta")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/tree/empty"); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := c.DownloadDir("/tree", dst); err != nil {
		t.Fatalf("DownloadDir: %v", err)
	}

	// Verify files.
	wantFiles := map[string]string{
		filepath.Join(dst, "a.txt"):                "alpha",
		filepath.Join(dst, "b.txt"):                "bravo",
		filepath.Join(dst, "sub", "c.txt"):         "charlie",
		filepath.Join(dst, "sub", "deep", "d.txt"): "delta",
	}
	for localPath, wantContent := range wantFiles {
		got, err := os.ReadFile(localPath)
		if err != nil {
			t.Fatalf("read %s: %v", localPath, err)
		}
		if string(got) != wantContent {
			t.Errorf("%s = %q, want %q", localPath, got, wantContent)
		}
	}

	// Verify directories exist (including empty dir).
	wantDirs := []string{
		dst,
		filepath.Join(dst, "sub"),
		filepath.Join(dst, "sub", "deep"),
		filepath.Join(dst, "empty"),
	}
	for _, d := range wantDirs {
		info, err := os.Stat(d)
		if err != nil {
			t.Fatalf("stat dir %s: %v", d, err)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", d)
		}
	}
}

// TestDownloadDir_EmptyDir verifies that a remote directory containing
// only an empty subdirectory preserves the empty subdirectory locally.
func TestDownloadDir_EmptyDir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/onlyempty"); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/onlyempty/inner"); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := c.DownloadDir("/onlyempty", dst); err != nil {
		t.Fatalf("DownloadDir: %v", err)
	}

	inner := filepath.Join(dst, "inner")
	info, err := os.Stat(inner)
	if err != nil {
		t.Fatalf("stat empty dir %s: %v", inner, err)
	}
	if !info.IsDir() {
		t.Errorf("%s is not a directory", inner)
	}
}

// TestDownloadDir_DstExistsAsDir verifies that downloading into a
// pre-existing local directory works (contents are merged into it).
func TestDownloadDir_DstExistsAsDir(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/merge"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/merge/x.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := c.DownloadDir("/merge", dst); err != nil {
		t.Fatalf("DownloadDir: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dst, "x.txt"))
	if err != nil {
		t.Fatalf("read x.txt: %v", err)
	}
	if string(got) != "x" {
		t.Errorf("x.txt = %q, want %q", got, "x")
	}
}

// TestDownloadDir_SourceIsFile verifies that DownloadDir rejects a file
// source with an error mentioning it requires a directory.
func TestDownloadDir_SourceIsFile(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Write("/file.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	err := c.DownloadDir("/file.txt", dst)
	if err == nil {
		t.Fatal("expected error for file source, got nil")
	}
	if !strings.Contains(err.Error(), "is a file") {
		t.Errorf("error = %v, want 'is a file'", err)
	}
}

// TestDownloadDir_DstExistsAsFile verifies that a pre-existing file at
// the local destination is rejected.
func TestDownloadDir_DstExistsAsFile(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/srcdir"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/srcdir/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	// Create a file at dst.
	dstFile := filepath.Join(dst, "target")
	if err := os.WriteFile(dstFile, []byte("preexisting"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := c.DownloadDir("/srcdir", dstFile)
	if err == nil {
		t.Fatal("expected error for file-as-dst, got nil")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("error = %v, want 'not a directory'", err)
	}
}

// TestDownloadDir_DescendantConflictRejects verifies that a
// pre-existing descendant file aborts the download before any file is
// written (no truncation of the existing file).
func TestDownloadDir_DescendantConflictRejects(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/conflict"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/conflict/a.txt", []byte("remote-content")); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	// Pre-create the descendant file with different content.
	localFile := filepath.Join(dst, "a.txt")
	if err := os.WriteFile(localFile, []byte("local-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := c.DownloadDir("/conflict", dst)
	if err == nil {
		t.Fatal("expected error for descendant conflict, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %v, want 'already exists'", err)
	}

	// The pre-existing file must NOT have been truncated.
	got, err := os.ReadFile(localFile)
	if err != nil {
		t.Fatalf("read local file: %v", err)
	}
	if string(got) != "local-content" {
		t.Errorf("local file was modified: got %q, want %q", got, "local-content")
	}
}

// TestDownloadDir_ContextCancelled verifies that a cancelled context
// aborts the download promptly.
func TestDownloadDir_ContextCancelled(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/cancel"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		path := "/cancel/f" + string(rune('0'+i)) + ".txt"
		if err := c.Write(path, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before starting

	dst := t.TempDir()
	err := c.DownloadDirCtx(ctx, "/cancel", dst)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// TestJoinLocalSafe_RejectsTraversal asserts the local path-safety
// helper rejects `..` traversal segments.
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
// rejected.
func TestJoinLocalSafe_RejectsAbsoluteRel(t *testing.T) {
	tmp := t.TempDir()
	if _, err := joinLocalSafe(tmp, "/etc/passwd"); err == nil {
		t.Fatal("expected reject for absolute rel, got nil")
	}
}

// TestJoinLocalSafe_HappyPath asserts the trivial case resolves to
// base/rel.
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

// TestWalkRemoteTreeBFS_HappyPath verifies the BFS walk visits all
// entries under a remote root and returns slash-relative paths.
func TestWalkRemoteTreeBFS_HappyPath(t *testing.T) {
	c, cleanup := newTestClient(t)
	defer cleanup()

	if err := c.Mkdir("/walk"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/walk/a.txt", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Mkdir("/walk/sub"); err != nil {
		t.Fatal(err)
	}
	if err := c.Write("/walk/sub/b.txt", []byte("b")); err != nil {
		t.Fatal(err)
	}

	var visited []string
	err := walkRemoteTreeBFS(context.Background(), c, "/walk", func(rel string, info FileInfo) error {
		visited = append(visited, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walkRemoteTreeBFS: %v", err)
	}

	want := map[string]bool{"a.txt": true, "sub": true, "sub/b.txt": true}
	if len(visited) != len(want) {
		t.Fatalf("visited %d entries, want %d: %v", len(visited), len(want), visited)
	}
	for _, rel := range visited {
		if !want[rel] {
			t.Errorf("unexpected entry %q", rel)
		}
	}
}

// TestParallelDownload_AlreadyCancelledCtxStopsLoop verifies that a
// pre-cancelled context launches zero workers.
func TestParallelDownload_AlreadyCancelledCtxStopsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var ops int
	items := []int{1, 2, 3, 4, 5}
	err := parallelDownload(ctx, items, func(_ context.Context, _ int) error {
		ops++
		return nil
	})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if ops != 0 {
		t.Errorf("launched %d ops, want 0", ops)
	}
}

// TestParallelDownload_CollectsMultipleErrors verifies that when
// multiple leaves fail, the returned error mentions the count.
func TestParallelDownload_CollectsMultipleErrors(t *testing.T) {
	items := []int{1, 2, 3, 4}
	err := parallelDownload(context.Background(), items, func(_ context.Context, n int) error {
		// Items 2 and 4 fail → two failures → count-prefixed message.
		if n%2 == 0 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "2 transfers failed") {
		t.Errorf("error = %v, want '2 transfers failed'", err)
	}
}

// TestParallelDownload_SingleFailureReturnsRawError verifies that a
// single leaf failure returns the raw error without a count prefix.
func TestParallelDownload_SingleFailureReturnsRawError(t *testing.T) {
	items := []int{1, 2, 3}
	err := parallelDownload(context.Background(), items, func(_ context.Context, n int) error {
		if n == 2 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "deadline exceeded") {
		t.Errorf("error = %v, want 'deadline exceeded'", err)
	}
	if strings.Contains(err.Error(), "transfers failed") {
		t.Errorf("single failure should not have count prefix, got %v", err)
	}
}
