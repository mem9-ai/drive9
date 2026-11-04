package writeback

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 remoteOpenFlagsForHandle replied DIRECT_IO for a large base only on a
// buffer-backed (writable) handle; a read-only open of a large file kept the
// kernel page cache. The port applied the size rule to every open, so a
// kernel-warm re-read of a 1 GiB file went through FUSE. O_TRUNC alone does
// not select DIRECT_IO (unchanged from the port).
func TestOpenDirectIOGatesSizeRuleOnWritableOpens(t *testing.T) {
	w, err := New(Options{Policy: PolicyWriteBack, Client: client.New("http://127.0.0.1:1", "")})
	if err != nil {
		t.Fatal(err)
	}
	const big = int64(smallFileShadowThreshold)
	cases := []struct {
		name  string
		flags uint32
		size  int64
		want  bool
	}{
		{"read-only large", uint32(syscall.O_RDONLY), big, false},
		{"read-only small", uint32(syscall.O_RDONLY), 4096, false},
		{"read-only with O_TRUNC bit, large", uint32(syscall.O_RDONLY | syscall.O_TRUNC), big, false},
		{"rdwr large", uint32(syscall.O_RDWR), big, true},
		{"wronly large", uint32(syscall.O_WRONLY), big, true},
		{"wronly trunc large", uint32(syscall.O_WRONLY | syscall.O_TRUNC), big, true},
		{"rdwr small", uint32(syscall.O_RDWR), 4096, false},
		{"wronly trunc small", uint32(syscall.O_WRONLY | syscall.O_TRUNC), 4096, false},
	}
	for _, tc := range cases {
		if got := w.openDirectIO("/f", tc.flags, tc.size); got != tc.want {
			t.Errorf("%s: openDirectIO = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Through the chain: a read-only open of a base at the shadow threshold
// replies KEEP_CACHE-compatible flags (no DIRECT_IO); the same base opened
// for writing replies DIRECT_IO as before.
func TestOpenReadOnlyLargeBaseKeepsKernelCache(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/huge", make([]byte, smallFileShadowThreshold))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/huge", Flags: uint32(syscall.O_RDONLY)})
	if err != nil || ro.Handle == nil {
		t.Fatalf("open read-only = %v", err)
	}
	t.Cleanup(func() { _ = ro.Handle.Release(ctx) })
	if ro.OpenFlags&gofuse.FOPEN_DIRECT_IO != 0 {
		t.Fatal("read-only open of a large base replied FOPEN_DIRECT_IO, dropping the page cache")
	}

	rw, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/huge", Flags: uint32(syscall.O_RDWR)})
	if err != nil || rw.Handle == nil {
		t.Fatalf("open rdwr = %v", err)
	}
	t.Cleanup(func() { _ = rw.Handle.Release(ctx) })
	if rw.OpenFlags&gofuse.FOPEN_DIRECT_IO == 0 {
		t.Fatal("writable open of a large base lost its FOPEN_DIRECT_IO reply")
	}
}
