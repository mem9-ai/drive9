package fuse

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestRetiredReadDelayedSSERename(t *testing.T) {
	for _, tc := range []struct {
		name           string
		alias, refresh bool
	}{
		{"same", false, false},
		{"hardlink", true, false},
		{"getattr-same-control", false, true},
		{"getattr-hardlink-control", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			target, err := url.Parse(fs.client.BaseURL())
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			var renamed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Query().Has("rename") {
					if r.URL.Path != "/v1/fs/moved.bin" || r.Header.Get("X-Dat9-Rename-Source") != "/file.bin" {
						t.Errorf("unexpected rename %s source=%q", r.URL, r.Header.Get("X-Dat9-Rename-Source"))
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					renamed.Store(true)
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.URL.Path == "/v1/fs/moved.bin" {
					if !renamed.Load() {
						http.NotFound(w, r)
						return
					}
					r.URL.Path = "/v1/fs/file.bin"
				} else if r.URL.Path == "/v1/fs/file.bin" && renamed.Load() {
					http.NotFound(w, r)
					return
				}
				proxy.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			fs.client = newTestClient(server.URL)
			fs.client.SetSmallFileThresholdForTests(32 << 20)
			fs.commitQueue.client = fs.client
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			if tc.alias {
				addTruncateHandoffAlias(t, fs, ino, remote)
			}
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			external := newTestClient(server.URL)
			external.SetSmallFileThresholdForTests(32 << 20)
			put := func(want string) {
				_, rev, _ := remote.snapshot()
				actual, err := external.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte(want), rev)
				if err != nil {
					t.Fatal(err)
				}
				data, gotRev, _ := remote.snapshot()
				if actual != rev+1 || gotRev != actual || string(data) != want {
					t.Fatal("external PUT prerequisite")
				}
			}
			watcher := &SSEWatcher{fs: fs, actor: "local-mount"}
			put("world")
			watcher.handleEvent(&client.ChangeEvent{Seq: 10, Path: "/file.bin", Op: "write", Actor: "external"}, nil)
			got, st := truncateHandoffRead(fs, ino, b, 0, 64)
			if st != gofuse.OK || string(got) != "world" || truncateHandoffEvent(fs, b) != nil {
				t.Fatalf("settled prerequisite got=%q/%v", got, st)
			}
			gotA, stA := truncateHandoffRead(fs, ino, a, 0, 64)
			if stA != gofuse.OK || string(gotA) != "world" || truncateHandoffEvent(fs, a) != nil {
				t.Fatalf("writer retirement prerequisite got=%q/%v event=%v", gotA, stA, truncateHandoffEvent(fs, a))
			}
			put("earth")
			if st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "file.bin", "moved.bin"); st != gofuse.OK {
				t.Fatal(st)
			}
			if _, exists := fs.inodes.GetInode("/file.bin"); exists || !renamed.Load() {
				t.Fatal("rename prerequisite")
			}
			if gotIno, exists := fs.inodes.GetInode("/moved.bin"); !exists || gotIno != ino {
				t.Fatal("new identity prerequisite")
			}
			h, _ := fs.fileHandles.Get(b)
			h.Lock()
			path := h.Path
			h.Unlock()
			wantPath := "/moved.bin"
			if tc.alias {
				wantPath = "/alias.bin"
			}
			if path != wantPath {
				t.Fatalf("reader path=%s want=%s", path, wantPath)
			}
			watcher.handleEvent(&client.ChangeEvent{Seq: 11, Path: "/file.bin", Op: "write", Actor: "external"}, nil)
			if tc.refresh {
				var out gofuse.AttrOut
				if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			for attempt := 0; attempt < 3; attempt++ {
				got, st = truncateHandoffRead(fs, ino, b, 0, 64)
				if st != gofuse.OK || string(got) != "earth" {
					t.Errorf("delayed old-path SSE attempt=%d reader=%s got=%q/%v want=earth", attempt+1, path, got, st)
				}
			}
		})
	}
}
