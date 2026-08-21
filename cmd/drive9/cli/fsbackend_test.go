package cli

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLsDoesNotSendRawS3ToClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "s3:") {
			t.Errorf("client saw object URI path %s", r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := client.New(srv.URL, "k")
	err := Ls(c, []string{"s3://bucket/key"})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), srv.URL) {
		t.Fatalf("Ls hit drive9 client: %v", err)
	}
}

func TestCpDoesNotCreateLocalFileNamedS3(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	c := client.New("http://127.0.0.1:1", "k")
	_ = Cp(c, []string{":/a.txt", "s3://bucket/a.txt"})
	if _, err := os.Stat("s3://bucket/a.txt"); err == nil {
		t.Fatal("created local file named s3://bucket/a.txt")
	}
}

func TestMvBarePathsStayOnClient(t *testing.T) {
	var renamed string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "rename") || r.Header.Get("X-Dat9-Rename-To") != "" || r.Method == "MOVE" {
			renamed = r.URL.Path
		}
		if r.URL.RawQuery != "" && strings.Contains(r.URL.RawQuery, "to=") {
			renamed = r.URL.Path
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := client.New(srv.URL, "k")
	// Even if rename wire format differs, we must not create local files.
	_ = Mv(c, []string{"/a", "/b"})
	if _, err := os.Stat("a"); err == nil {
		t.Fatal("Mv(/a,/b) touched local file a")
	}
	if _, err := os.Stat("b"); err == nil {
		t.Fatal("Mv(/a,/b) touched local file b")
	}
	_ = renamed
}

func TestFsClientForRemoteArgObjectNeverOKFalse(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "k")
	_, _, _, isRemote, err := fsClientForRemoteArg(c, "s3://bucket/k")
	if !isRemote {
		t.Fatal("s3:// must not classify as non-remote")
	}
	if err == nil {
		t.Fatal("expected object scheme error")
	}
}

func TestFsHandleForArgObjectDoesNotUseClient(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "k")
	h, err := fsHandleForArg(c, "s3://bucket/k")
	if err != nil {
		if strings.Contains(err.Error(), c.BaseURL()) {
			t.Fatalf("object open hit drive9 client: %v", err)
		}
		return
	}
	if h.Client != nil {
		t.Fatal("object handle must not carry a drive9 client")
	}
	if h.Loc.Kind != KindObject || h.Loc.Bucket != "bucket" {
		t.Fatalf("handle loc = %+v", h.Loc)
	}
}

func TestFsHandleRelativeUsesRawDrive9Path(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "k")
	h, err := fsHandleForArg(c, "./foo")
	if err != nil {
		t.Fatal(err)
	}
	if h.Loc.Path != "./foo" {
		t.Fatalf("leftover relative loc.Path=%q, want ./foo so ls/cat hit the client", h.Loc.Path)
	}
}

func TestFsHandleForArgRejectsStdin(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "k")
	_, err := fsHandleForArg(c, "-")
	if err == nil || !strings.Contains(err.Error(), "fs cp") {
		t.Fatalf("err=%v", err)
	}
	h, err := fsHandleForArgOpts(c, "-", true)
	if err != nil {
		t.Fatal(err)
	}
	if h.Loc.Kind != KindStdin {
		t.Fatalf("kind=%v", h.Loc.Kind)
	}
}

func TestWalkChildUsesPathNotRootLocal(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	a := newLocalFS()
	root := Location{Local: dir, Path: ""}
	page, err := a.List(context.Background(), root, ListOpts{Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Name != "a.txt" {
		t.Fatalf("list = %+v", page.Entries)
	}
	child := Location{Local: dir, Path: page.Entries[0].Path}
	rc, err := a.OpenRead(context.Background(), child)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(body) != "hi" {
		t.Fatalf("read %q err=%v", body, err)
	}
}
