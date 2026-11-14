//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestMountListOnlyRootWithLocalDirectories(t *testing.T) {
	for _, root := range []string{"/", "/team"} {
		t.Run(root, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			t.Setenv("XDG_RUNTIME_DIR", "")
			backend := testbackend.NewBackend()
			backend.Seed("/team/child", []byte("private"))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(map[string]string{"tenant_id": "tenant-a"})
					return
				}
				if r.Method == http.MethodHead {
					http.Error(w, "forbidden", http.StatusForbidden)
					return
				}
				backend.ServeHTTP(w, r)
			}))
			defer server.Close()
			cfg := MountConfig{RemoteRoot: root, Server: server.URL, APIKey: "test-key", MountPoint: t.TempDir(), CacheDir: t.TempDir(), LocalRoot: t.TempDir(), LocalOnlyPatterns: []string{"**/.git/**"}, Extensions: []string{"core", "localize"}, WritePolicy: "write-sync", SyncMode: "strict"}
			v, err := prepareMountView(t.Context(), cfg, client.New(server.URL, cfg.APIKey), "actor")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = v.close(context.Background()) }()
			if v.currentRuntime().fs.localDirAttrs {
				t.Fatal("test bypassed the overlay attribute chain")
			}
			var out gofuse.AttrOut
			if code := v.switcher.adapter.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, &out); code != gofuse.OK {
				t.Fatalf("list-authorized root getattr: %v", code)
			}
		})
	}
}
