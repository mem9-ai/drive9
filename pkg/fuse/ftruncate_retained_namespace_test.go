package fuse

import (
	"context"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateRetainedReleaseNamespace(t *testing.T) {
	for _, operation := range []string{"rename", "unlink-reuse"} {
		t.Run(operation, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			fs.opts.LegacyInterruptibleMutations = true
			var moved, deleted atomic.Bool
			interceptRetiredReadHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPost && r.URL.Query().Has("rename") {
					moved.Store(true)
					w.WriteHeader(http.StatusOK)
					return true
				}
				if r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/file.bin" {
					deleted.Store(true)
					w.WriteHeader(http.StatusNoContent)
					return true
				}
				if r.URL.Path == "/v1/fs/moved.bin" {
					if !moved.Load() {
						http.NotFound(w, r)
						return true
					}
					r.URL.Path = "/v1/fs/file.bin"
				} else if r.URL.Path == "/v1/fs/file.bin" && (moved.Load() || deleted.Load()) {
					http.NotFound(w, r)
					return true
				}
				return false
			})
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 5)
			cancel := make(chan struct{})
			close(cancel)
			sibling, _ := fs.fileHandles.Get(b)
			sibling.Lock() // force the canceled Release to retain before queue handoff
			fs.Release(cancel, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			sibling.Unlock()
			if _, ok := fs.fileHandles.Get(a); !ok {
				t.Fatal("retained owner prerequisite missing")
			}
			var shadow, pending uint64
			if operation == "rename" {
				if st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "file.bin", "moved.bin"); st != gofuse.OK {
					t.Fatal(st)
				}
			} else {
				if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "file.bin"); st != gofuse.OK {
					t.Fatal(st)
				}
				fs.inodes.LookupWithIdentity("/file.bin", "replacement", 1, false, 3, time.Now())
				if err := fs.shadowStore.WriteFull("/file.bin", []byte("NEW"), 0); err != nil {
					t.Fatal(err)
				}
				shadow = fs.shadowStore.ActiveGeneration("/file.bin")
				var err error
				pending, err = fs.pendingIndex.PutWithBaseRev("/file.bin", 3, PendingNew, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			if response := fs.Drain(ctx); !response.OK {
				t.Fatalf("namespace drain=%+v", response)
			}
			if _, ok := fs.fileHandles.Get(a); ok {
				t.Fatal("closed owner not finalized")
			}
			if operation == "rename" {
				if got, _, _ := remote.snapshot(); string(got) != "hello" {
					t.Fatalf("renamed content=%q", got)
				}
				retiredReadWant(t, fs, ino, b, "hello")
			} else if fs.shadowStore.ActiveGeneration("/file.bin") != shadow || fs.pendingIndex.Generation("/file.bin") != pending {
				t.Fatal("old anonymous close consumed replacement staging")
			}
		})
	}
}
