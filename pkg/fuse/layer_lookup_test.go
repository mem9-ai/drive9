package fuse

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestLayerLookupRetriesViewRefresh(t *testing.T) {
	for _, exists := range []bool{false, true} {
		name := "absent_create_target"
		if exists {
			name = "existing_file"
		}
		t.Run(name, func(t *testing.T) {
			started, resume := make(chan struct{}), make(chan struct{})
			var stats atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					_, _ = w.Write([]byte(`{"entries":[]}`))
					return
				}
				if stats.Add(1) == 1 {
					close(started)
					<-resume
				}
				if exists {
					w.Header().Set("Content-Length", "7")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "2")
				} else {
					w.WriteHeader(404)
				}
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			done := make(chan gofuse.Status, 1)
			go func() { done <- fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "new.txt", &gofuse.EntryOut{}) }()
			<-started
			fs.resetMountView()
			close(resume)
			want := gofuse.ENOENT
			if exists {
				want = gofuse.OK
			}
			if st := <-done; st != want {
				t.Fatalf("lookup overlapping Layer refresh=%v, want %v", st, want)
			}
			if stats.Load() != 2 {
				t.Fatalf("stat calls=%d, want a fresh lookup", stats.Load())
			}
		})
	}
}
