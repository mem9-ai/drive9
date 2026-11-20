//go:build !windows

package fuse

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/git"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitcache"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountRuntimeLocalDataReturnsWithoutCopying(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws-1", "/repo")
	ts := httptest.NewServer(b)
	defer ts.Close()
	cfg := MountConfig{Server: ts.URL, LocalRoot: t.TempDir(), CacheDir: t.TempDir(), MountPoint: t.TempDir(), RemoteRoot: "/", Profile: "coding-agent", EnableGitWorkspaces: true, Extensions: []string{"core", "git", "localize"}}
	if err := gitcache.MarkWorkspacePending(t.Context(), cfg.LocalRoot, "/repo"); err != nil {
		t.Fatal(err)
	}
	base := client.New(ts.URL, "")
	main := viewprotocol.Binding{}
	a := viewprotocol.Binding{LayerID: "A", WorkID: "work-A", Fence: 1}
	bindingB := viewprotocol.Binding{LayerID: "B", WorkID: "work-B", Fence: 1}
	path := "/repo/node_modules/keep"
	gitPath := "/repo/.git/config"
	s, _, _, _ := viewSwitchFixture(t)
	s.build = func(ctx context.Context, binding viewprotocol.Binding, generation mountRuntimeGeneration) (*mountRuntime, error) {
		cl, err := base.WithView(binding)
		if err != nil {
			return nil, err
		}
		return buildMountRuntimeGeneration(cfg, cl, "/", "actor", generation)
	}
	first, err := s.build(t.Context(), main, mountRuntimeGeneration{MountID: "mount-1", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	first.fs.Init2(t.Context())
	if err := first.fs.StartErr(); err != nil {
		t.Fatal(err)
	}
	s.current, s.adapter = first, &viewRequestFS{inner: first.fs}
	t.Cleanup(func() { _ = s.current.stop(context.Background()) })
	for i, binding := range []viewprotocol.Binding{main, a, bindingB, a, main} {
		if i > 0 {
			resp := s.apply(t.Context(), mountcontrol.SwitchRequest{MountID: "mount-1", OperationID: fmt.Sprintf("switch-local-%d", i), ExpectedGeneration: uint64(i), Target: binding})
			if resp.Error != "" || resp.Receipt == nil || resp.Receipt.State != mountstate.SwitchDone || resp.Receipt.Target != binding {
				t.Fatalf("switch to %q failed: %+v", binding.LayerID, resp)
			}
		}
		rt := s.current
		// Prime main's actual workspace discovery, so .git routing follows the
		// existing workspace contract. Fixed views have no main Git API client.
		for _, h := range rt.fs.chain.Handlers() {
			if g, ok := h.(*git.Git); ok {
				if binding.LayerID != "" {
					t.Fatal("fixed runtime constructed main-only Git workspace extension")
				}
				g.Reload(t.Context())
				g.Reload(t.Context())
			}
		}
		for _, p := range []string{path, gitPath} {
			res, err := rt.fs.chain.Dispatch(t.Context(), &fs.Op{Kind: fs.OpGetAttr, Path: p})
			if i < 3 {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("view %q inherited %s: %+v %v", binding.LayerID, p, res, err)
				}
				created, err := rt.fs.chain.Dispatch(t.Context(), &fs.Op{Kind: fs.OpCreate, Path: p, Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0600})
				if err != nil {
					t.Fatal(err)
				}
				data := []byte("main-local")
				if binding.LayerID != "" {
					data = []byte(binding.LayerID + "-local")
				}
				if err := created.Handle.Write(t.Context(), 0, data); err != nil {
					t.Fatal(err)
				}
				if err := created.Handle.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else if err != nil || res.Attr == nil {
				t.Fatalf("return lost %s: %+v %v", p, res, err)
			}
			root, err := mountstate.ViewLocalRoot(cfg.LocalRoot, binding)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, "overlay", p))
			want := "main-local"
			if binding.LayerID != "" {
				want = binding.LayerID + "-local"
			}
			if err != nil || string(data) != want {
				t.Fatalf("local content = %q %v, want %q", data, err, want)
			}
		}
		if rt.fs.ReexecPreflight().OK {
			t.Fatal("process re-exec became allowed")
		}
		rt.fs.viewLocalState = false
		if rt.fs.viewSwitchPreflight().OK {
			t.Fatal("unscoped runtime relaxed local-state restrictions")
		}
		rt.fs.viewLocalState = true
	}
}
