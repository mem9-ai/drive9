package gitcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
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
