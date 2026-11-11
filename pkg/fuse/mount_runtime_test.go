package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountRuntimeConstructionUsesIndependentBoundClients(t *testing.T) {
	mainBackend, layerBackend := testbackend.NewBackend(), testbackend.NewBackend()
	mainBackend.Seed("/a", []byte("main"))
	layerBackend.Seed("/a", []byte("private-layer"))
	binding := client.ViewBinding{LayerID: "A", WorkID: "work-A", Fence: 1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := viewprotocol.RoutePrefix + binding.LayerID
		if strings.HasPrefix(r.URL.Path, prefix+"/") {
			got, err := viewprotocol.Parse(r.Header)
			if err != nil || got != binding {
				t.Errorf("binding=%+v err=%v", got, err)
				http.Error(w, "binding", 400)
				return
			}
			clone := r.Clone(r.Context())
			clone.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
			layerBackend.ServeHTTP(w, clone)
		} else {
			if r.Header.Get(viewprotocol.ViewHeader) != "" {
				t.Error("main received Layer binding")
			}
			mainBackend.ServeHTTP(w, r)
		}
	}))
	defer server.Close()
	base := client.New(server.URL, "")
	bound, err := base.WithView(binding)
	if err != nil {
		t.Fatal(err)
	}
	cfg := MountConfig{Server: server.URL, MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core"}, WritePolicy: "write-sync", SyncMode: "strict"}
	var runtimes []*mountRuntime
	for _, cl := range []*client.Client{base, bound} {
		runtime, err := buildMountRuntime(cfg, cl, "/", "actor")
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime)
		t.Cleanup(runtime.closeStaging)
		if runtime.fs.pluginsStarted() {
			t.Fatal("constructor started background chain")
		}
		runtime.fs.Init2(context.Background())
		if err := runtime.fs.StartErr(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtime.fs.chain.Stop(context.Background()) })
	}
	if runtimes[0].fs.Ctx.DirCache == runtimes[1].fs.Ctx.DirCache || runtimes[0].fs.inodes == runtimes[1].fs.inodes || runtimes[0].fs.events == runtimes[1].fs.events {
		t.Fatal("runtime reused mutable view state")
	}
	for _, index := range []int{0, 1, 0} {
		var out gofuse.EntryOut
		if status := runtimes[index].fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &out); status != gofuse.OK {
			t.Fatal(status)
		}
		want := uint64(4)
		if index == 1 {
			want = 13
		}
		if out.Size != want {
			t.Fatalf("runtime %d size=%d want=%d", index, out.Size, want)
		}
	}
	// The main path still uses the real common terminal for creation and writes.
	var created gofuse.CreateOut
	if status := runtimes[0].fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0600}, "out", &created); status != gofuse.OK {
		t.Fatal(status)
	}
	n, status := runtimes[0].fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}, []byte("saved"))
	if status != gofuse.OK || n != 5 {
		t.Fatalf("write %d %v", n, status)
	}
	runtimes[0].fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	node, ok := mainBackend.Node("/out")
	if !ok || string(node.Data) != "saved" {
		t.Fatalf("main write=%+v", node)
	}
	if _, ok := layerBackend.Node("/out"); ok {
		t.Fatal("main write entered Layer")
	}
}

func TestMountRuntimeRetainsStagingUntilOwnerCloses(t *testing.T) {
	cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "writeback"}, WritePolicy: "writeback", SyncMode: "strict"}
	runtime, err := buildMountRuntime(cfg, client.New(cfg.Server, ""), "/", "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.closeStaging)
	if runtime.staging == nil {
		t.Fatal("missing staging")
	}
	entry := staging.JournalEntry{Op: staging.JournalCommit, Path: "/a"}
	if err := runtime.staging.Journal.Append(entry); err != nil {
		t.Fatalf("constructor closed live journal: %v", err)
	}
	runtime.closeStaging()
	if err := runtime.staging.Journal.Append(entry); err == nil {
		t.Fatal("journal remained open after owner closed")
	}
}

func TestMountRuntimeConstructionRejectsInvalidOptions(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MountConfig)
	}{
		{"extension", func(c *MountConfig) { c.Extensions = []string{"unknown"} }},
		{"write-policy", func(c *MountConfig) { c.WritePolicy = "unknown" }},
		{"sync-mode", func(c *MountConfig) { c.SyncMode = "unknown" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core"}, SyncMode: "strict"}
			tc.mutate(&cfg)
			if runtime, err := buildMountRuntime(cfg, client.New(cfg.Server, ""), "/", "actor"); err == nil || runtime != nil {
				t.Fatalf("runtime=%v err=%v", runtime, err)
			}
		})
	}
}

func TestMountRuntimeKeepsKernelPermissionContractAcrossViews(t *testing.T) {
	for _, allowOther := range []bool{false, true} {
		for _, binding := range []client.ViewBinding{{}, {LayerID: "a", WorkID: "w", Fence: 1}} {
			cl, err := client.New("http://127.0.0.1:1", "").WithView(binding)
			if err != nil {
				t.Fatal(err)
			}
			cfg := MountConfig{Server: "http://127.0.0.1:1", MountPoint: t.TempDir(), CacheDir: t.TempDir(),
				Extensions: []string{"core"}, WritePolicy: "write-sync", SyncMode: "strict", AllowOther: allowOther}
			r, err := buildMountRuntime(cfg, cl, "/", "permission-test")
			if err != nil {
				t.Fatal(err)
			}
			defer r.closeStaging()
			kernelChecks := slices.Contains(newGoFuseMountOptions(cfg).Options, "default_permissions")
			if r.fs.defaultPermissions != kernelChecks {
				t.Fatalf("view=%+v allow-other=%t daemon skips checks=%t kernel checks=%t", binding, allowOther, r.fs.defaultPermissions, kernelChecks)
			}
		}
	}
}
