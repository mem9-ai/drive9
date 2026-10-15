package layer

// mashenjun's row: a read extending past EOF of a base-inherited file
// returns a short body with ErrUnexpectedEOF — those bytes are the real
// tail, not an EIO.

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortBaseFileShortTailRead(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	n, err := res.Handle.Read(ctx, 0, got)
	if err != nil {
		t.Fatalf("short tail read: %v", err)
	}
	if string(got[:n]) != "hello" {
		t.Fatalf("short tail read = %q, want hello", string(got[:n]))
	}
}
