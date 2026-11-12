package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

func TestFixedViewKeepsWholeImageForBoundPublication(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(503)
	}))
	defer server.Close()
	for _, validation := range []bool{false, true} {
		bound, err := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1, Validation: validation})
		if err != nil {
			t.Fatal(err)
		}
		w := &Writeback{opts: Options{Client: bound, Policy: PolicyCloseSync}, preload: 1}
		h := &bufferingHandle{w: w, path: "/large", baseSize: 16 << 20, baseRev: 2, storageClass: client.StorageTypeS3, buf: writebuffer.NewWriteBuffer("/large", 32<<20, 0)}
		if _, err := h.buf.Write(0, []byte("dirty")); err != nil {
			t.Fatal(err)
		}
		if err := h.maybeArmStreaming(); err != nil || h.stream != nil || h.buf.OnPartFull != nil {
			t.Fatal(err)
		}
		if _, attempted, err := h.commitPatched(context.Background()); err != nil || attempted {
			t.Fatal(attempted, err)
		}
		if string(h.buf.Bytes()) != "dirty" || !h.buf.HasDirtyParts() {
			t.Fatal("dispatch discarded dirty image")
		}
	}
	if requests.Load() != 0 {
		t.Fatal("unbound protocol was probed", requests.Load())
	}
}
