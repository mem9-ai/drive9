//go:build darwin

package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestSynchronousPromotionRejectsDarwinACLBeforePublish(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
			publishCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	filePath, err := fs.localOverlay.abs("/project/dist/.site-next/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("chmod", "+a", "everyone deny delete", filePath).CombinedOutput(); err != nil {
		t.Skipf("cannot create ACL fixture: %v: %s", err, output)
	}
	t.Cleanup(func() {
		_ = exec.Command("chmod", "-a#", "0", filePath).Run()
	})

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promotion status = %v, want EXDEV", status)
	}
	if publishCalls.Load() != 0 {
		t.Fatalf("publish calls = %d, want 0", publishCalls.Load())
	}
	if _, err := os.Stat(filepath.Clean(filePath)); err != nil {
		t.Fatalf("source changed after ACL rejection: %v", err)
	}
}
