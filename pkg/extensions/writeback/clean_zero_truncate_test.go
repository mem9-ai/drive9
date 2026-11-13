package writeback

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestCleanZeroTruncateRequiresSingleLayout(t *testing.T) {
	for _, layout := range []string{"", "unknown", "append_log", "extent"} {
		t.Run(fmt.Sprintf("layout-%s", layout), func(t *testing.T) {
			b := testbackend.NewBackend()
			b.SeedWithLayout("/file", []byte("seed"), layout)
			var puts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					puts.Add(1)
					http.Error(w, "layout rejects ordinary PUT", http.StatusBadRequest)
					return
				}
				b.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			cl := client.New(server.URL, "")
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			chain, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}
			h := open(t, chain, "/file")
			op := &fs.Op{Kind: fs.OpSetAttr, Path: "/file", Size: 0}
			if _, err, handled := wb.truncateCleanMainToZero(context.Background(), op); handled || err != nil {
				t.Fatalf("layout entered direct PUT path: handled=%t err=%v", handled, err)
			}
			// The ordinary dispatch still adopts the truncate locally. No
			// remote publication is added for these layouts by this fix.
			if _, err := chain.Dispatch(context.Background(), op); err != nil {
				t.Fatal(err)
			}
			if n, err := h.Read(context.Background(), 0, make([]byte, 4)); err != nil || n != 0 {
				t.Fatalf("local truncate: n=%d err=%v", n, err)
			}
			if n, _ := b.Node("/file"); string(n.Data) != "seed" || puts.Load() != 0 {
				t.Fatalf("unexpected publication: body=%q PUTs=%d", n.Data, puts.Load())
			}
		})
	}
}
