package gitcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
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

func TestReadTreeFileReadsSymlinkTarget(t *testing.T) {
	localRoot := t.TempDir()
	root := TreeRoot(localRoot, "ws1", "commit1")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, "link")), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	got, hit, err := ReadTreeFile(localRoot, "ws1", "commit1", "link", 0, -1)
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
