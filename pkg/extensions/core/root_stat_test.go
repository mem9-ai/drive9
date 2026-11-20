package core

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMainRootStatListAuthorization(t *testing.T) {
	for _, root := range []string{"/", "/team"} {
		t.Run(root, func(t *testing.T) {
			var lists atomic.Int32
			var denied atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" || r.URL.Path == viewprotocol.RoutePrefix+"layer/v1/status" {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				if r.Method == http.MethodGet && r.URL.Query().Has("list") {
					lists.Add(1)
					if r.URL.Path != "/v1/fs"+root {
						t.Errorf("listed outside mount root: %s", r.URL.Path)
					}
					if !denied.Load() {
						_, _ = w.Write([]byte(`{"entries":[]}`))
						return
					}
				}
				w.WriteHeader(http.StatusForbidden)
			}))
			defer srv.Close()
			c, err := New(client.New(srv.URL, "test-key"), Options{RemoteRoot: root})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Do(t.Context(), &fs.Op{Kind: fs.OpGetAttr, Path: "/"}, nil)
			if err != nil || got.Attr == nil || got.Attr.Kind != fs.KindDir || got.Attr.Mode != 0o755 || got.Attr.Rev != 0 || got.Attr.ResourceID != "" {
				t.Fatalf("root: %+v %v", got.Attr, err)
			}
			if lists.Load() != 1 {
				t.Fatalf("lists=%d", lists.Load())
			}
			for _, op := range []*fs.Op{
				{Kind: fs.OpGetAttr, Path: "/child"},
				{Kind: fs.OpRead, Path: "/child", Limit: 1},
				{Kind: fs.OpWrite, Path: "/child", Data: []byte("x"), Mode: 0o644},
				{Kind: fs.OpSetAttr, Path: "/", HasMode: true, Mode: 0o777},
			} {
				if _, err := c.Do(t.Context(), op, nil); fs.StatusOf(err) != gofuse.EACCES {
					t.Fatalf("operation %v acquired permission: %v", op.Kind, err)
				}
			}
			// The root chmod revalidates the root but still fails at its own API.
			if lists.Load() != 2 {
				t.Fatalf("child borrowed root listing: lists=%d", lists.Load())
			}
			denied.Store(true)
			if _, err := c.Do(t.Context(), &fs.Op{Kind: fs.OpGetAttr, Path: "/"}, nil); fs.StatusOf(err) != gofuse.EACCES {
				t.Fatalf("cached root masked revocation: %v", err)
			}
			if lists.Load() != 3 {
				t.Fatalf("revocation did not query list: %d", lists.Load())
			}
		})
	}
}

func TestRootStatListFallbackBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		head, list int
		body       string
		fixed      bool
		mode       uint32
		wantLists  int32
		wantOK     bool
	}{
		{name: "real_zero_mode", head: 200, mode: 0, wantOK: true},
		{name: "real_mode", head: 200, mode: 0o700, wantOK: true},
		{name: "list_forbidden", head: 403, list: 403, wantLists: 1},
		{name: "list_server_error", head: 403, list: 503, wantLists: 1},
		{name: "list_invalid_response", head: 403, list: 200, body: `{`, wantLists: 1},
		{name: "head_unauthorized", head: 401},
		{name: "head_missing", head: 404},
		{name: "head_server_error", head: 500},
		{name: "fixed_forbidden", head: 403, fixed: true},
		{name: "fixed_real_mode", head: 200, fixed: true, mode: 0o700, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lists atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" || r.URL.Path == viewprotocol.RoutePrefix+"layer/v1/status" {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				if tc.fixed && !strings.HasPrefix(r.URL.Path, viewprotocol.RoutePrefix+"layer/") {
					t.Errorf("fixed request escaped view: %s", r.URL.Path)
				}
				if r.Method == http.MethodHead {
					w.Header().Set("X-Dat9-IsDir", "true")
					w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(tc.mode), 10))
					w.Header().Set("X-Dat9-Revision", "7")
					w.Header().Set("X-Dat9-Resource-ID", "root-id")
					w.WriteHeader(tc.head)
					return
				}
				if !r.URL.Query().Has("list") || tc.list == 0 {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)
					return
				}
				lists.Add(1)
				w.WriteHeader(tc.list)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			cl := client.New(srv.URL, "test-key")
			if tc.fixed {
				var err error
				cl, err = cl.WithView(viewprotocol.Binding{LayerID: "layer", WorkID: "work", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
			}
			c, err := New(cl, Options{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Do(t.Context(), &fs.Op{Kind: fs.OpGetAttr, Path: "/"}, nil)
			if (err == nil) != tc.wantOK {
				t.Fatalf("result=%+v err=%v", got, err)
			}
			if tc.wantOK && (got.Attr == nil || got.Attr.Mode != tc.mode || got.Attr.Rev != 7 || got.Attr.ResourceID != "root-id") {
				t.Fatalf("HEAD attributes changed: %+v", got.Attr)
			}
			if lists.Load() != tc.wantLists {
				t.Fatalf("lists=%d want=%d", lists.Load(), tc.wantLists)
			}
		})
	}
}
