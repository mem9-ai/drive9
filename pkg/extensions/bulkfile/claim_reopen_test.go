package bulkfile

import (
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A read-only re-open of a live claim serves the claim's pending image,
// never the not-yet-pushed remote bytes (the tier-transition overwrite
// read a mixed old/new image).
func TestClaimReopenServesPendingImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/t.bin", []byte(strings.Repeat("A", 1<<20)))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bf, err := New(Options{Client: cl, Policy: "writeback"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(bf, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/t.bin",
		Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte(strings.Repeat("B", 1<<20))); err != nil {
		t.Fatal(err)
	}
	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/t.bin", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err := ro.Handle.Read(ctx, 512<<10, got); err != nil {
		t.Fatal(err)
	}
	if string(got[:32]) != strings.Repeat("B", 32) {
		t.Fatalf("re-open over live claim = %q, want the pending B image", got[:16])
	}
}
