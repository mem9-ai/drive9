package cache

// qiffang seventeenth round: a cached handle's path migration is
// synchronized with concurrent reads — one stable identity snapshot per
// cache operation, no bare path reads racing SetPath.

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newTestServer wraps the backend in an httptest server.
func newTestServer(t *testing.T, b *testbackend.Backend) string {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestPortRetargetPathRacesStayClean(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ts := newTestServer(t, b)
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts, c)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			if flip {
				c.RetargetPath("/f", "/g")
			} else {
				c.RetargetPath("/g", "/f")
			}
			flip = !flip
		}
	}()
	buf := make([]byte, 11)
	for i := 0; i < 500; i++ {
		if _, err := h.Read(ctx, 0, buf); err != nil {
			t.Fatalf("read during retarget: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
