package fuse

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

func TestUnlinkHardlinkKernel(t *testing.T) {
	backend := testbackend.NewBackend()
	backend.Seed("/keep", []byte("survivor"))
	backend.SeedDir("/other", 0755)
	ts := httptest.NewServer(backend)
	t.Cleanup(ts.Close)
	stack := mountPendingKernelStack(t, ts.URL, "strict")
	keep, alias := filepath.Join(stack.mp, "keep"), filepath.Join(stack.mp, "other", "alias")
	if err := os.Link(keep, alias); err != nil {
		t.Fatal(err)
	}
	stack.f.Ctx.DirCache.(*dircache.DirCache).Put("/", []fs.DirEntryInfo{{Name: "keep", Size: 8, Nlink: 2}})
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	// Force a fresh LOOKUP rather than accept the kernel's cached dentry.
	if st := stack.server.EntryNotify(1, "keep"); st != gofuse.OK {
		t.Fatal(st)
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "survivor" {
		t.Fatalf("surviving hardlink = %q, %v", got, err)
	}
	if _, err := os.Stat(alias); !os.IsNotExist(err) {
		t.Fatalf("removed alias stat = %v", err)
	}
}

func TestUnlinkRecreateKernel(t *testing.T) {
	backend := testbackend.NewBackend()
	backend.Seed("/same", []byte("old"))
	entered, proceed := make(chan struct{}), make(chan struct{})
	var enteredOnce, proceedOnce sync.Once
	unblock := func() { proceedOnce.Do(func() { close(proceed) }) }
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/same" {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-proceed:
			case <-r.Context().Done():
				return
			}
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	stack := mountPendingKernelStack(t, ts.URL, "strict")
	// Unblock before mount cleanup if a test assertion fails.
	t.Cleanup(unblock)
	name := filepath.Join(stack.mp, "same")
	old, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(name); err != nil {
		_ = old.Close()
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- old.Close() }()
	awaitUnlinkTest(t, entered)
	created := make(chan error, 1)
	go func() {
		file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			_, err = file.Write([]byte("replacement"))
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
		created <- err
	}()
	select {
	case err := <-created:
		t.Fatalf("recreate completed while old delete was blocked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	if err := awaitUnlinkTest(t, closed); err != nil {
		t.Fatal(err)
	}
	if err := awaitUnlinkTest(t, created); err != nil {
		t.Fatal(err)
	}
	stack.drain(t)
	if got, err := os.ReadFile(name); err != nil || string(got) != "replacement" {
		t.Fatalf("replacement = %q, %v", got, err)
	}
}
