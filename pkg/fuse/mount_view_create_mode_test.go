//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountViewNegotiatesCreateModeOnInitialAndReturningMain(t *testing.T) {
	for _, returningStatus := range []string{"supported", "omitted", "failed"} {
		t.Run(returningStatus, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			t.Setenv("XDG_RUNTIME_DIR", "")
			main, layer := testbackend.NewBackend(), testbackend.NewBackend()
			var mainStatus, batches, creates, chmods atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if prefix := viewprotocol.RoutePrefix + "a"; strings.HasPrefix(r.URL.Path, prefix+"/") {
					binding, err := viewprotocol.Parse(r.Header)
					if err != nil || binding != (viewprotocol.Binding{LayerID: "a", WorkID: "w", Fence: 1}) {
						t.Errorf("wrong Layer binding: %+v %v", binding, err)
						w.WriteHeader(403)
						return
					}
					clone := r.Clone(r.Context())
					clone.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
					layer.ServeHTTP(w, clone)
					return
				}
				if r.URL.Path == "/v1/status" {
					call := mainStatus.Add(1)
					if call >= 3 && returningStatus == "failed" {
						w.WriteHeader(503)
						return
					}
					if call >= 3 && returningStatus == "omitted" {
						_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","inline_threshold":1048576}`))
						return
					}
					_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","inline_threshold":1048576,"storage_capabilities":{"batch_write_mode_v1":true}}`))
					return
				}
				if r.URL.Path == "/v1/fs:batch-write" {
					batches.Add(1)
					var body struct {
						Items []client.BatchWriteItem `json:"items"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Items) != 1 {
						t.Errorf("invalid create: %+v %v", body, err)
						w.WriteHeader(400)
						return
					}
					item := body.Items[0]
					if !item.HasMode || item.Mode != 0o600 || item.ExpectedRevision != 0 || len(item.Data) != 0 {
						t.Errorf("lost atomic create contract: %+v", item)
					}
					main.Seed(item.Path, nil)
					main.SetNodeMode(item.Path, item.Mode)
					_ = json.NewEncoder(w).Encode(map[string]any{"results": []client.BatchWriteResult{{Path: item.Path, Status: 200, Revision: 1}}})
					return
				}
				if r.URL.Query().Has("create") {
					creates.Add(1)
				}
				if r.URL.Query().Has("chmod") {
					chmods.Add(1)
				}
				main.ServeHTTP(w, r)
			}))
			defer server.Close()
			cl := client.New(server.URL, "test-key")
			cl.Warm(t.Context())
			cfg := MountConfig{Server: server.URL, APIKey: "test-key", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core"}, WritePolicy: "write-sync", SyncMode: "strict"}
			v, err := prepareMountView(t.Context(), cfg, cl, "actor")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = v.close(context.Background()) })
			create := func(path string) {
				t.Helper()
				res, err := v.currentRuntime().fs.dispatchErr(t.Context(), &fs.Op{Kind: fs.OpCreate, Path: path, Mode: 0o600, HasMode: true, Flags: syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR})
				if err != nil || res.Handle == nil || res.Attr == nil || res.Attr.Rev <= 0 {
					t.Fatalf("runtime create: %+v %v", res, err)
				}
				if err := res.Handle.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			create("/initial")
			if mainStatus.Load() != 2 || batches.Load() != 1 || creates.Load() != 0 || chmods.Load() != 0 {
				t.Fatalf("initial negotiation: status=%d batch=%d create=%d chmod=%d", mainStatus.Load(), batches.Load(), creates.Load(), chmods.Load())
			}
			// Real journal, target validation, runtime construction and install;
			// only kernel notifications are replaced. No kernel mount is started.
			v.server.Store(&gofuse.Server{})
			v.ready.Store(true)
			v.switcher.notifier = &viewCacheProbe{}
			v.switcher.fence = func(context.Context) error { return nil }
			snapshot, err := v.switcher.journal.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for i, target := range []viewprotocol.Binding{{LayerID: "a", WorkID: "w", Fence: 1}, {}} {
				result := v.switcher.apply(t.Context(), mountcontrol.SwitchRequest{MountID: snapshot.MountID, OperationID: fmt.Sprintf("switch-%d", i), ExpectedGeneration: uint64(i + 1), Target: target})
				if result.Error != "" || result.Receipt == nil || result.Receipt.State != mountstate.SwitchDone {
					t.Fatalf("switch: %+v", result)
				}
			}
			create("/returned")
			wantBatches, wantLegacy := int64(2), int64(0)
			if returningStatus != "supported" {
				// Preserve the existing old/failed-status fallback. This case
				// proves the new runtime cannot reuse the previous main's grant;
				// it does not claim to fix the old create+chmod version handoff.
				wantBatches, wantLegacy = 1, 1
			}
			if mainStatus.Load() != 3 || batches.Load() != wantBatches || creates.Load() != wantLegacy || chmods.Load() != wantLegacy {
				t.Fatalf("return negotiation: status=%d batch=%d create=%d chmod=%d", mainStatus.Load(), batches.Load(), creates.Load(), chmods.Load())
			}
			if !cl.CachedBatchWriteModeSupported() {
				t.Fatal("new runtime changed the original client's cache")
			}
		})
	}
}
