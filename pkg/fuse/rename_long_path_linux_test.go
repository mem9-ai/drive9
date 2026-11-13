package fuse

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Exercise rename/02's 4095-byte relative argument through the Linux kernel,
// then read and remove the returned name to catch stale facade/kernel state.
func TestRenameLongPathKernel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy writeback.Policy
	}{{"fsync", writeback.PolicyWriteBack}, {"close-sync", writeback.PolicyCloseSync}} {
		t.Run(tc.name, func(t *testing.T) {
			backend := testbackend.NewBackend()
			backend.Seed("/short", []byte("payload"))
			long := strings.TrimSuffix(strings.Repeat(strings.Repeat("a", 127)+"/", 32), "/")
			for dir := path.Dir("/" + long); dir != "/"; dir = path.Dir(dir) {
				backend.SeedDir(dir, 0755)
			}
			ts := httptest.NewServer(backend)
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			local, err := localize.New(localize.Options{LocalRoot: t.TempDir(), Profile: "coding-agent", ApplyBuiltinDefaults: true})
			if err != nil {
				t.Fatal(err)
			}
			wb, err := writeback.New(writeback.Options{Client: cl, Policy: tc.policy})
			if err != nil {
				t.Fatal(err)
			}
			f, err := New(Options{Extensions: []fs.Handler{local, wb, terminal}})
			if err != nil {
				t.Fatal(err)
			}
			if dc, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
				terminal.Attach(dc)
			}
			mp := t.TempDir()
			server, err := gofuse.NewServer(f, mp, &gofuse.MountOptions{FsName: "drive9", Name: "drive9", DirectMountStrict: os.Geteuid() == 0})
			if err != nil {
				if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
					t.Fatal(err)
				}
				t.Skipf("cannot mount Linux FUSE: %v", err)
			}
			f.Init2(context.Background())
			go server.Serve()
			t.Cleanup(func() {
				if err := server.Unmount(); err != nil {
					t.Error(err)
				}
			})
			if err := server.WaitMount(); err != nil {
				t.Fatal(err)
			}
			dir, err := os.Open(mp)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dir.Close() }()
			fd := int(dir.Fd())
			if err := unix.Renameat(fd, "short", fd, long); err != nil {
				t.Fatalf("rename short to long: %v", err)
			}
			if err := unix.Renameat(fd, long, fd, "short"); err != nil {
				t.Fatalf("rename long back to short: %v", err)
			}
			if data, err := os.ReadFile(filepath.Join(mp, "short")); err != nil || string(data) != "payload" {
				t.Fatalf("returned name read = %q, %v", data, err)
			}
			if node, ok := backend.Node("/short"); !ok || string(node.Data) != "payload" {
				t.Fatal("remote short name does not match the mounted view")
			}
			var st unix.Stat_t
			if err := unix.Fstatat(fd, long, &st, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
				t.Fatalf("old long name stat = %v", err)
			}
			if err := unix.Unlinkat(fd, "short", 0); err != nil {
				t.Fatalf("unlink returned short name: %v", err)
			}
			if err := unix.Fstatat(fd, "short", &st, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
				t.Fatalf("unlinked short name stat = %v", err)
			}
		})
	}
}
