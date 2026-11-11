package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountRuntimeCacheScope(t *testing.T) {
	cfg := MountConfig{Server: "https://example.invalid", MountPoint: "/mnt/test", RemoteRoot: "/", APIKey: "test-credential"}
	base := viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 1}
	generation := mountRuntimeGeneration{MountID: "mount-1", Generation: 1}
	write, read, err := mountRuntimeCacheHashes(cfg, base, generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		binding    viewprotocol.Binding
		generation mountRuntimeGeneration
	}{
		{"main", viewprotocol.Binding{}, generation},
		{"layer", viewprotocol.Binding{LayerID: "B", WorkID: "work-A", Fence: 1}, generation},
		{"work", viewprotocol.Binding{LayerID: "A", WorkID: "work-B", Fence: 1}, generation},
		{"fence", viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 2}, generation},
		{"validation", viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 1, Validation: true}, generation},
		{"generation", base, mountRuntimeGeneration{MountID: "mount-1", Generation: 2}},
		{"mount", base, mountRuntimeGeneration{MountID: "mount-2", Generation: 1}},
		{"unscoped-fixed", base, mountRuntimeGeneration{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, r, err := mountRuntimeCacheHashes(cfg, tc.binding, tc.generation)
			if err != nil || w == write || r == read {
				t.Fatalf("cache namespace aliases another binding/generation: %q %q %v", w, r, err)
			}
		})
	}
	w, r, err := mountRuntimeCacheHashes(cfg, base, generation)
	if err != nil || w != write || r != read {
		t.Fatalf("same identity cannot recover its cache: %q %q %v", w, r, err)
	}
	cfg.APIKey = "different-test-credential"
	_, r, err = mountRuntimeCacheHashes(cfg, base, generation)
	if err != nil || r == read {
		t.Fatal("read cache lost credential isolation")
	}
	w, r, err = mountRuntimeCacheHashes(cfg, viewprotocol.Binding{}, mountRuntimeGeneration{})
	if err != nil || w != MountHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot) || r != MountReadCacheHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot, "api_key", cfg.APIKey) {
		t.Fatal("ordinary main cache namespace changed")
	}
}

func TestMountRuntimeCacheRejectsInvalidScopeBeforeCreatingFiles(t *testing.T) {
	for _, tc := range []struct {
		name       string
		generation mountRuntimeGeneration
		layer      string
		checkpoint string
	}{
		{"missing-generation", mountRuntimeGeneration{MountID: "mount-1"}, "", ""},
		{"missing-mount", mountRuntimeGeneration{Generation: 1}, "", ""},
		{"invalid-mount", mountRuntimeGeneration{MountID: "../other", Generation: 1}, "", ""},
		{"legacy-layer", mountRuntimeGeneration{}, "old-layer", ""},
		{"legacy-checkpoint", mountRuntimeGeneration{}, "", "old-checkpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := MountConfig{CacheDir: t.TempDir(), LayerRef: tc.layer, CheckpointRef: tc.checkpoint, Extensions: []string{"core", "writeback"}}
			cl, err := client.New("http://example.invalid", "").WithView(client.ViewBinding{LayerID: "A", WorkID: "work-A", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			if rt, err := buildMountRuntimeGeneration(cfg, cl, "/", "actor", tc.generation); err == nil || rt != nil {
				t.Fatalf("runtime=%v error=%v", rt, err)
			}
			entries, err := os.ReadDir(cfg.CacheDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid identity touched cache: %v %v", entries, err)
			}
		})
	}
}

func TestMountRuntimeGenerationDoesNotRecoverAnotherViewsPendingWrites(t *testing.T) {
	cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "writeback"}, WritePolicy: "writeback", SyncMode: "strict", WriteCacheFreeRatio: -1}
	base := client.New(cfg.Server, "")
	bound, err := base.WithView(client.ViewBinding{LayerID: "A", WorkID: "work-A", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := mountRuntimeGeneration{MountID: "mount-1", Generation: 1}
	old, err := buildMountRuntimeGeneration(cfg, base, "/", "actor", first)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(old.closeStaging)
	if err := old.staging.Shadow.WriteFull("/a", []byte("old pending"), 7); err != nil {
		t.Fatal(err)
	}
	if err := old.staging.Shadow.Sync("/a"); err != nil {
		t.Fatal(err)
	}
	if err := old.staging.Journal.Append(staging.JournalEntry{Op: staging.JournalFsync, Path: "/a", Length: 11, BaseRev: 7}); err != nil {
		t.Fatal(err)
	}
	if err := old.staging.Journal.Fsync(); err != nil {
		t.Fatal(err)
	}
	oldDir := old.staging.Pending.Dir()
	// Keep the old runtime alive while preparing a target, just like a switch.
	for _, tc := range []struct {
		name       string
		client     *client.Client
		generation mountRuntimeGeneration
	}{
		{"layer", bound, mountRuntimeGeneration{MountID: "mount-1", Generation: 2}},
		{"return-to-main", base, mountRuntimeGeneration{MountID: "mount-1", Generation: 3}},
		{"other-mountpoint", base, mountRuntimeGeneration{MountID: "mount-2", Generation: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targetConfig := cfg
			if tc.name == "other-mountpoint" {
				targetConfig.MountPoint = t.TempDir()
			}
			rt, err := buildMountRuntimeGeneration(targetConfig, tc.client, "/", "actor", tc.generation)
			if err != nil {
				t.Fatal(err)
			}
			defer rt.closeStaging()
			if rt.staging.Pending.Dir() == oldDir || rt.staging.Pending.HasPending("/a") {
				t.Fatal("target recovered old pending write")
			}
			if _, exists := rt.staging.Shadow.ContentSize("/a"); exists {
				t.Fatal("target recovered old shadow")
			}
			if err := rt.staging.Journal.Append(staging.JournalEntry{Op: staging.JournalCommit, Path: "/a"}); err != nil {
				t.Fatal(err)
			}
		})
	}
	old.closeStaging()
	// Recovering the ORIGINAL generation must still find its original bytes
	// and revision. Neither another runtime's Compact nor its commit marker
	// may erase the pending work.
	recovered, err := buildMountRuntimeGeneration(cfg, base, "/", "actor", first)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.closeStaging()
	meta, exists := recovered.staging.Pending.GetMeta("/a")
	if !exists || meta.BaseRev != 7 || meta.Size != 11 {
		t.Fatalf("pending=%+v exists=%v", meta, exists)
	}
	data := make([]byte, 11)
	if n, err := recovered.staging.Shadow.ReadAt("/a", 0, data); err != nil || n != 11 || string(data) != "old pending" {
		t.Fatalf("recovery read=%q n=%d err=%v", data, n, err)
	}
	if filepath.Dir(recovered.staging.Pending.Dir()) != filepath.Dir(oldDir) {
		t.Fatal("original recovery changed namespace")
	}
}

func TestMountRuntimeGenerationDiskReadsDoNotCrossViews(t *testing.T) {
	mainBackend, layerBackend := testbackend.NewBackend(), testbackend.NewBackend()
	mainBackend.Seed("/a", []byte("main-one"))
	layerBackend.Seed("/a", []byte("layer-aa"))
	binding := client.ViewBinding{LayerID: "A", WorkID: "work-A", Fence: 1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := viewprotocol.RoutePrefix + binding.LayerID
		if strings.HasPrefix(r.URL.Path, prefix+"/") {
			clone := r.Clone(r.Context())
			clone.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
			layerBackend.ServeHTTP(w, clone)
		} else {
			mainBackend.ServeHTTP(w, r)
		}
	}))
	defer server.Close()
	base := client.New(server.URL, "")
	bound, err := base.WithView(binding)
	if err != nil {
		t.Fatal(err)
	}
	cfg := MountConfig{Server: server.URL, MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "cache"}, SyncMode: "strict", ReadCacheMaxFileBytes: 1, DiskReadCacheFreeRatio: -1}
	// All fixtures deliberately have the SAME pathname, revision and size.
	// Their bytes differ, so only correct view/generation isolation can help.
	for i, cl := range []*client.Client{base, bound, base} {
		if i == 2 {
			mainBackend.Seed("/a", []byte("main-two"))
		}
		rt, err := buildMountRuntimeGeneration(cfg, cl, "/", "actor", mountRuntimeGeneration{MountID: "mount-1", Generation: uint64(i + 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(rt.closeStaging)
		rt.fs.Init2(context.Background())
		t.Cleanup(func() { _ = rt.fs.chain.Stop(context.Background()) })
		if err := rt.fs.StartErr(); err != nil {
			t.Fatal(err)
		}
		var entry gofuse.EntryOut
		if st := rt.fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &entry); st != gofuse.OK {
			t.Fatal(st)
		}
		// Writable open disables sequential read-ahead, exercising the disk range tier deterministically.
		var opened gofuse.OpenOut
		if st := rt.fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Flags: syscall.O_RDWR}, &opened); st != gofuse.OK {
			t.Fatal(st)
		}
		result, st := rt.fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: opened.Fh, Size: 8}, make([]byte, 8))
		if st != gofuse.OK {
			t.Fatal(st)
		}
		data, status := result.Bytes(make([]byte, 8))
		result.Done()
		want := []string{"main-one", "layer-aa", "main-two"}[i]
		if status != gofuse.OK || string(data) != want {
			t.Fatalf("generation %d read=%q status=%v want=%q", i+1, data, status, want)
		}
		rt.fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: opened.Fh})
		if err := rt.fs.chain.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Ensure this exercised persistent disk entries, not just memory caches.
		count := 0
		err = filepath.WalkDir(cfg.CacheDir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && strings.HasSuffix(path, ".drc") {
				count++
			}
			return nil
		})
		if err != nil || count != i+1 {
			t.Fatalf("disk entries=%d want=%d err=%v", count, i+1, err)
		}
	}
}
