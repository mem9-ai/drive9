//go:build !windows

package fuse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestValidationRuntimeRejectsMutationsBeforeLocalState(t *testing.T) {
	for _, policy := range []string{"writeback", "write-sync", "close-sync"} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			t.Setenv("XDG_RUNTIME_DIR", "")
			main, layer := testbackend.NewBackend(), testbackend.NewBackend()
			layer.Seed("/file", []byte("sealed"))
			layer.SeedDir("/repo", 0755)
			var mutations atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, viewprotocol.RoutePrefix+"a/") {
					binding, err := viewprotocol.Parse(r.Header)
					if err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					if binding.Validation && r.Method != http.MethodGet && r.Method != http.MethodHead {
						mutations.Add(1)
						w.WriteHeader(403)
						return
					}
					clone := r.Clone(r.Context())
					clone.URL.Path = strings.TrimPrefix(r.URL.Path, viewprotocol.RoutePrefix+"a")
					layer.ServeHTTP(w, clone)
					return
				}
				if r.URL.Path == "/v1/status" {
					_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","inline_threshold":1048576}`))
					return
				}
				main.ServeHTTP(w, r)
			}))
			defer server.Close()
			cfg := MountConfig{Server: server.URL, APIKey: "test-key", MountPoint: t.TempDir(), CacheDir: t.TempDir(), LocalRoot: t.TempDir(), Profile: "coding-agent", Extensions: []string{"core", "writeback", "localize", "xattr"}, WritePolicy: policy, SyncMode: "strict"}
			cl := client.New(server.URL, "test-key")
			cl.Warm(t.Context())
			v, err := prepareMountView(t.Context(), cfg, cl, "actor")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if v != nil {
					_ = v.close(context.Background())
				}
			})
			prepareSwitch := func() {
				v.server.Store(&gofuse.Server{})
				v.ready.Store(true)
				v.switcher.notifier = &viewCacheProbe{}
				v.switcher.fence = func(context.Context) error { return nil }
			}
			prepareSwitch()
			switchTo := func(binding viewprotocol.Binding) {
				t.Helper()
				snapshot, err := v.switcher.journal.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				result := v.switcher.apply(t.Context(), mountcontrol.SwitchRequest{MountID: snapshot.MountID, OperationID: fmt.Sprintf("switch-%d", snapshot.Generation), ExpectedGeneration: snapshot.Generation, Target: binding})
				if result.Error != "" || result.Receipt == nil || result.Receipt.State != mountstate.SwitchDone {
					t.Fatalf("switch: %+v", result)
				}
			}
			validation := viewprotocol.Binding{LayerID: "a", WorkID: "validation", Fence: 2, Validation: true}
			localRoot, err := mountstate.ViewLocalRoot(cfg.LocalRoot, validation)
			if err != nil {
				t.Fatal(err)
			}
			localPath := filepath.Join(localRoot, "overlay/repo/node_modules/keep")
			if err := os.MkdirAll(filepath.Dir(localPath), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(localPath, []byte("local sealed"), 0600); err != nil {
				t.Fatal(err)
			}
			switchTo(validation)
			for _, phase := range []string{"switched", "reopened"} {
				if phase == "reopened" {
					snapshot, err := v.switcher.journal.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					cfg.MountID = snapshot.MountID
					if err := v.close(t.Context()); err != nil {
						t.Fatal(err)
					}
					v = nil
					v, err = prepareMountView(t.Context(), cfg, cl, "actor")
					if err != nil {
						t.Fatal(err)
					}
					prepareSwitch()
				}
				t.Run(phase, func(t *testing.T) {
					rt := v.currentRuntime()
					if rt.writeback != nil || rt.staging != nil || !rt.fs.validationReadOnly {
						t.Fatal("validation constructed writable data plane")
					}
					f := rt.fs
					for _, parent := range []string{"/repo", "/repo/node_modules"} {
						f.EnsureInodeForTest(parent, true, 0, time.Now())
					}
					for _, path := range []string{"/file", "/repo/node_modules/keep"} {
						ino := f.EnsureInodeForTest(path, false, 6, time.Now())
						input := gofuse.InHeader{NodeId: ino}
						for _, flags := range []uint32{syscall.O_WRONLY, syscall.O_RDWR, syscall.O_TRUNC, syscall.O_APPEND} {
							if st := f.Open(nil, &gofuse.OpenIn{InHeader: input, Flags: flags}, &gofuse.OpenOut{}); st != gofuse.Status(syscall.EROFS) {
								t.Fatalf("write open %s: %v", path, st)
							}
						}
						var opened gofuse.OpenOut
						if st := f.Open(nil, &gofuse.OpenIn{InHeader: input}, &opened); st != gofuse.OK {
							t.Fatalf("read open %s: %v", path, st)
						}
						if _, st := f.Write(nil, &gofuse.WriteIn{InHeader: input, Fh: opened.Fh}, []byte("changed")); st != gofuse.Status(syscall.EROFS) {
							t.Fatalf("handle write: %v", st)
						}
						if st := f.Access(nil, &gofuse.AccessIn{InHeader: input, Mask: 2}); st != gofuse.Status(syscall.EROFS) {
							t.Fatalf("write access: %v", st)
						}
						before, _ := f.inodes.GetEntry(ino)
						set := &gofuse.SetAttrIn{}
						set.InHeader, set.Valid, set.Size, set.Mtime = input, gofuse.FATTR_SIZE|gofuse.FATTR_MTIME, 0, 1
						if st := f.SetAttr(nil, set, &gofuse.AttrOut{}); st != gofuse.Status(syscall.EROFS) {
							t.Fatalf("setattr: %v", st)
						}
						after, _ := f.inodes.GetEntry(ino)
						if before.Size != after.Size || before.Mtime != after.Mtime {
							t.Fatal("rejected setattr changed local inode")
						}
						if path == "/file" {
							if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file"); st != gofuse.Status(syscall.EROFS) {
								t.Fatalf("open unlink: %v", st)
							}
							if _, ok := f.inodes.GetInode(path); !ok {
								t.Fatal("rejected unlink hid the sealed name")
							}
						}
						buf := make([]byte, 32)
						result, st := f.Read(nil, &gofuse.ReadIn{InHeader: input, Fh: opened.Fh, Size: uint32(len(buf))}, buf)
						if st != gofuse.OK {
							t.Fatalf("read: %v", st)
						}
						data, st := result.Bytes(buf)
						want := "sealed"
						if strings.Contains(path, "node_modules") {
							want = "local sealed"
						}
						if st != gofuse.OK || string(data) != want {
							t.Fatalf("sealed read=%q %v", data, st)
						}
						result.Done()
						f.Release(nil, &gofuse.ReleaseIn{InHeader: input, Fh: opened.Fh})
					}
					// FUSE CREATE must reject even O_RDONLY: the callback itself creates.
					if st := f.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}}, "new", &gofuse.CreateOut{}); st != gofuse.Status(syscall.EROFS) {
						t.Fatalf("create: %v", st)
					}
					for _, kind := range []fs.OpKind{fs.OpCreate, fs.OpMkdir, fs.OpUnlink, fs.OpRmdir, fs.OpRename, fs.OpLink, fs.OpSymlink, fs.OpMknod, fs.OpSetXattr, fs.OpRemoveXattr, fs.OpFallocate, fs.OpCopyFileRange} {
						_, err := f.dispatchErr(t.Context(), &fs.Op{Kind: kind, Path: "/repo/node_modules/new", Flags: syscall.O_CREAT | syscall.O_RDWR})
						if !errors.Is(err, syscall.EROFS) {
							t.Fatalf("local mutation %v: %v", kind, err)
						}
					}
					if _, err := os.Stat(filepath.Join(filepath.Dir(localPath), "new")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("local creation: %v", err)
					}
					if data, err := os.ReadFile(localPath); err != nil || string(data) != "local sealed" {
						t.Fatalf("local data: %q %v", data, err)
					}
					drain := f.Drain(t.Context())
					if !drain.OK {
						t.Fatalf("validation dirty after rejected writes: %+v", drain)
					}
					if mutations.Load() != 0 {
						t.Fatalf("dispatched %d validation mutations", mutations.Load())
					}
				})
			}
			switchTo(viewprotocol.Binding{})
			if v.currentRuntime().fs.validationReadOnly || v.currentRuntime().writeback == nil {
				t.Fatal("return to main lost writable runtime")
			}
		})
	}
}
