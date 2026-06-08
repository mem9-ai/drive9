package gitcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitHubCodeloadURL(t *testing.T) {
	got := GitHubCodeloadURL(GitHubRepoRef{Owner: "mem9-ai", Repo: "drive9"}, "abc123")
	want := "https://codeload.github.com/mem9-ai/drive9/tar.gz/abc123"
	if got != want {
		t.Fatalf("GitHubCodeloadURL = %q, want %q", got, want)
	}
}

func TestSanitizeRepoURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "https user token",
			raw:  "https://secret@github.com/mem9-ai/drive9.git",
			want: "https://github.com/mem9-ai/drive9.git",
		},
		{
			name: "https password token and query",
			raw:  "https://x-access-token:secret@github.com/mem9-ai/drive9.git?token=hidden&depth=1",
			want: "https://github.com/mem9-ai/drive9.git?depth=1",
		},
		{
			name: "ssh keeps git username",
			raw:  "ssh://git@github.com/mem9-ai/drive9.git",
			want: "ssh://git@github.com/mem9-ai/drive9.git",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SanitizeRepoURL(tt.raw); got != tt.want {
				t.Fatalf("SanitizeRepoURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestSanitizeGitConfigCredentials(t *testing.T) {
	config := "[remote \"origin\"]\n\turl = https://secret@github.com/mem9-ai/drive9.git\n"
	got := string(SanitizeGitConfigCredentials([]byte(config)))
	if strings.Contains(got, "secret") {
		t.Fatalf("SanitizeGitConfigCredentials leaked secret: %q", got)
	}
	if !strings.Contains(got, "url = https://github.com/mem9-ai/drive9.git") {
		t.Fatalf("SanitizeGitConfigCredentials = %q, want sanitized GitHub URL", got)
	}
}

func TestWorkspaceDeletedMarkerLifecycle(t *testing.T) {
	localRoot := t.TempDir()
	ctx := context.Background()

	if WorkspaceDeleted(ctx, localRoot, "ws1") {
		t.Fatal("WorkspaceDeleted before mark = true, want false")
	}
	if err := MarkWorkspaceDeleted(ctx, localRoot, "ws1"); err != nil {
		t.Fatalf("MarkWorkspaceDeleted: %v", err)
	}
	if !WorkspaceDeleted(ctx, localRoot, "ws1") {
		t.Fatal("WorkspaceDeleted after mark = false, want true")
	}
	if err := ClearWorkspaceDeleted(ctx, localRoot, "ws1"); err != nil {
		t.Fatalf("ClearWorkspaceDeleted: %v", err)
	}
	if WorkspaceDeleted(ctx, localRoot, "ws1") {
		t.Fatal("WorkspaceDeleted after clear = true, want false")
	}
	if _, ok := WorkspaceRefreshMarkerTime(ctx, localRoot, "ws1"); !ok {
		t.Fatal("WorkspaceRefreshMarkerTime after clear ok = false, want true")
	}
}

func TestWorkspaceDeletedMarkerHonorsCanceledContext(t *testing.T) {
	localRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := MarkWorkspaceDeleted(ctx, localRoot, "ws1"); err == nil {
		t.Fatal("MarkWorkspaceDeleted with canceled context err = nil, want error")
	}
	if WorkspaceDeleted(context.Background(), localRoot, "ws1") {
		t.Fatal("WorkspaceDeleted after canceled mark = true, want false")
	}
}

func TestExtractCodeloadTarRejectsTraversal(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/../evil.txt", Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()

	_, _, err = extractCodeloadTar(zr, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "invalid relative path") {
		t.Fatalf("extractCodeloadTar err = %v, want invalid relative path", err)
	}
}

func TestExtractCodeloadTarRejectsSymlinkTraversal(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/target", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/link", Typeflag: tar.TypeSymlink, Linkname: "target"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/link/file.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len("hello"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()

	_, _, err = extractCodeloadTar(zr, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "traverses symlink") {
		t.Fatalf("extractCodeloadTar err = %v, want symlink traversal error", err)
	}
}

func TestExtractCodeloadTarSkipsRootDirectory(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "repo-abc/README.md", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len("hello\n"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zr.Close() }()

	dst := t.TempDir()
	files, bytesWritten, err := extractCodeloadTar(zr, dst)
	if err != nil {
		t.Fatalf("extractCodeloadTar: %v", err)
	}
	if files != 1 || bytesWritten != int64(len("hello\n")) {
		t.Fatalf("extractCodeloadTar stats = files %d bytes %d, want 1/%d", files, bytesWritten, len("hello\n"))
	}
	got, err := os.ReadFile(filepath.Join(dst, "README.md"))
	if err != nil {
		t.Fatalf("read extracted README: %v", err)
	}
	if string(got) != "hello\n" {
		t.Fatalf("README content = %q, want hello", got)
	}
}

func TestReadTreeFileReadsSymlinkTarget(t *testing.T) {
	localRoot := t.TempDir()
	root := TreeRoot(localRoot, "ws1", "commit1")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, "link")), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	got, hit, err := ReadTreeFile(context.Background(), localRoot, "ws1", "commit1", "link", 0, -1)
	if err != nil {
		t.Fatalf("ReadTreeFile: %v", err)
	}
	if !hit {
		t.Fatal("ReadTreeFile hit = false, want true")
	}
	if string(got) != "target.txt" {
		t.Fatalf("ReadTreeFile = %q, want target.txt", got)
	}
}

func TestStatTreeFileReturnsRegularAndSymlinkSizes(t *testing.T) {
	localRoot := t.TempDir()
	root := TreeRoot(localRoot, "ws1", "commit1")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("README.md", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	size, hit, err := StatTreeFile(context.Background(), localRoot, "ws1", "commit1", "README.md")
	if err != nil {
		t.Fatalf("StatTreeFile regular: %v", err)
	}
	if !hit || size != int64(len("hello\n")) {
		t.Fatalf("StatTreeFile regular = size %d hit %t, want %d true", size, hit, len("hello\n"))
	}

	size, hit, err = StatTreeFile(context.Background(), localRoot, "ws1", "commit1", "link")
	if err != nil {
		t.Fatalf("StatTreeFile symlink: %v", err)
	}
	if !hit || size != int64(len("README.md")) {
		t.Fatalf("StatTreeFile symlink = size %d hit %t, want %d true", size, hit, len("README.md"))
	}

	size, hit, err = StatTreeFile(context.Background(), localRoot, "ws1", "commit1", "missing")
	if err != nil {
		t.Fatalf("StatTreeFile missing: %v", err)
	}
	if hit || size != 0 {
		t.Fatalf("StatTreeFile missing = size %d hit %t, want 0 false", size, hit)
	}
}

func TestStatBlobReturnsCachedBlobSize(t *testing.T) {
	localRoot := t.TempDir()
	data := []byte("cached blob\n")
	sha := gitBlobSHA(data)
	if err := WriteBlob(context.Background(), localRoot, "ws1", "commit1", sha, data); err != nil {
		t.Fatal(err)
	}

	size, hit, err := StatBlob(context.Background(), localRoot, "ws1", "commit1", sha)
	if err != nil {
		t.Fatalf("StatBlob: %v", err)
	}
	if !hit || size != int64(len(data)) {
		t.Fatalf("StatBlob = size %d hit %t, want %d true", size, hit, len(data))
	}

	size, hit, err = StatBlob(context.Background(), localRoot, "ws1", "commit1", "2222222222222222222222222222222222222222")
	if err != nil {
		t.Fatalf("StatBlob missing: %v", err)
	}
	if hit || size != 0 {
		t.Fatalf("StatBlob missing = size %d hit %t, want 0 false", size, hit)
	}
}

func TestHydrateWritesCleanTreeObjectsToGitObjectDB(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	localRoot := t.TempDir()
	workspaceID := "ws1"
	commit := "abcdef"
	treeRoot := TreeRoot(localRoot, workspaceID, commit)
	if err := os.MkdirAll(filepath.Join(treeRoot, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	readme := []byte("hello from hydrate\n")
	if err := os.WriteFile(filepath.Join(treeRoot, "README.md"), readme, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../README.md", filepath.Join(treeRoot, "docs", "readme-link")); err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(t.TempDir(), "repo.git")
	runGitCacheTest(t, "", "init", "--bare", gitDir)

	result, err := Hydrate(context.Background(), HydrateOptions{
		LocalRoot:   localRoot,
		WorkspaceID: workspaceID,
		Commit:      commit,
		RepoURL:     "https://github.com/mem9-ai/drive9.git",
		GitDir:      gitDir,
		TreeEntries: []HydrateTreeEntry{
			{Path: "README.md", Kind: "file", Mode: "100644", ObjectSHA: gitBlobSHA(readme)},
			{Path: "docs/readme-link", Kind: "symlink", Mode: "120000", ObjectSHA: gitBlobSHA([]byte("../README.md"))},
			{Path: "docs", Kind: "dir", Mode: "040000", ObjectSHA: "0000"},
			{Path: "vendor/lib", Kind: "submodule", Mode: "160000", ObjectSHA: "1111"},
		},
	})
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if result.Provider != "cache" {
		t.Fatalf("Provider = %q, want cache", result.Provider)
	}
	if result.ObjectStatus != hydrateStatusSuccess {
		t.Fatalf("ObjectStatus = %q, want success", result.ObjectStatus)
	}
	if result.Objects != 2 {
		t.Fatalf("Objects = %d, want 2", result.Objects)
	}
	if result.ObjectSkipped != 2 {
		t.Fatalf("ObjectSkipped = %d, want 2", result.ObjectSkipped)
	}

	got := gitCacheOutput(t, gitDir, "cat-file", "blob", gitBlobSHA(readme))
	if string(got) != string(readme) {
		t.Fatalf("cat-file README = %q, want %q", got, readme)
	}
	got = gitCacheOutput(t, gitDir, "cat-file", "blob", gitBlobSHA([]byte("../README.md")))
	if string(got) != "../README.md" {
		t.Fatalf("cat-file symlink = %q, want target", got)
	}
	meta, err := os.ReadFile(filepath.Join(CacheRoot(localRoot, workspaceID, commit), "hydrate.json"))
	if err != nil {
		t.Fatalf("read hydrate metadata: %v", err)
	}
	for _, want := range []string{`"object_status": "success"`, `"objects": 2`, `"object_skipped": 2`} {
		if !strings.Contains(string(meta), want) {
			t.Fatalf("hydrate metadata missing %s:\n%s", want, meta)
		}
	}
}

func TestHydrateObjectReadySkipsRepeatedObjectWrites(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	localRoot := t.TempDir()
	workspaceID := "ws1"
	commit := "abcdef"
	treeRoot := TreeRoot(localRoot, workspaceID, commit)
	if err := os.MkdirAll(treeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("cached\n")
	if err := os.WriteFile(filepath.Join(treeRoot, "cached.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(t.TempDir(), "repo.git")
	runGitCacheTest(t, "", "init", "--bare", gitDir)
	opts := HydrateOptions{
		LocalRoot:   localRoot,
		WorkspaceID: workspaceID,
		Commit:      commit,
		RepoURL:     "https://github.com/mem9-ai/drive9.git",
		GitDir:      gitDir,
		TreeEntries: []HydrateTreeEntry{{Path: "cached.txt", Kind: "file", Mode: "100644", ObjectSHA: gitBlobSHA(content)}},
	}
	if _, err := Hydrate(context.Background(), opts); err != nil {
		t.Fatalf("first Hydrate: %v", err)
	}
	result, err := Hydrate(context.Background(), opts)
	if err != nil {
		t.Fatalf("second Hydrate: %v", err)
	}
	if result.ObjectStatus != "cache" {
		t.Fatalf("ObjectStatus = %q, want cache", result.ObjectStatus)
	}
}

func gitBlobSHA(data []byte) string {
	payload := append([]byte(fmt.Sprintf("blob %d\x00", len(data))), data...)
	sum := sha1.Sum(payload)
	return fmt.Sprintf("%x", sum[:])
}

func runGitCacheTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func gitCacheOutput(t *testing.T, gitDir string, args ...string) []byte {
	t.Helper()
	full := append([]string{"--git-dir", gitDir}, args...)
	cmd := exec.Command("git", full...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", full, err, out)
	}
	return out
}
