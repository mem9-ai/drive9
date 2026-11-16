package writeback

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// Exercise the kernel CREATE -> writeback O_EXCL -> core path, including the
// first buffered write. A mock POST create would hide the production 501.
func TestFixedKernelExclusiveCreate(t *testing.T) {
	for _, mode := range []uint32{0, 0o600, 0o644, 0o755} {
		for _, scenario := range []string{"normal", "existing", "lost-receipt", "foreign-write"} {
			t.Run(fmt.Sprintf("mode_%o/%s", mode, scenario), func(t *testing.T) {
				var mu sync.Mutex
				var revision int64
				var stored string
				creates, puts, chmods := 0, 0, 0
				if scenario == "existing" {
					revision, stored = 3, "existing"
				}
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch {
					case r.URL.Path == "/v1/views/a/v1/status":
						_, _ = io.WriteString(w, `{"inline_threshold":1024}`)
					case r.Method == http.MethodHead:
						w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
						w.Header().Set("X-Dat9-Size", strconv.Itoa(len(stored)))
						w.Header().Set("X-Dat9-Resource-ID", "file")
						w.WriteHeader(200)
					case r.Method == http.MethodPut && r.URL.RawQuery == "":
						id := r.Header.Get(viewprotocol.OperationHeader)
						if r.Header.Get("X-Dat9-Expected-Revision") == "0" {
							creates++
							if id != "exclusive-create" || r.Header.Get("X-Dat9-Mode") != strconv.FormatUint(uint64(mode), 10) || r.ContentLength != 0 {
								t.Error("changed creation request")
							}
							if scenario == "existing" {
								w.WriteHeader(409)
								return
							}
							if creates == 1 {
								revision = 1
								if scenario == "foreign-write" {
									revision, stored = 2, "foreign"
								}
								if scenario == "lost-receipt" {
									w.Header().Set("Content-Length", "99999")
									return
								}
							}
							_, _ = fmt.Fprintf(w, `{"status":"ok","operation_id":%q,"view":{"layer_id":"a","fence":1,"generation":1},"resource_id":"file","revision":1,"size_bytes":0}`, id)
							return
						}
						puts++
						if r.Header.Get("X-Dat9-Expected-Revision") != "1" {
							t.Error("adopted a different revision")
						}
						if revision != 1 {
							w.WriteHeader(409)
							return
						}
						body, _ := io.ReadAll(r.Body)
						stored, revision = string(body), 2
						_, _ = io.WriteString(w, `{"revision":2}`)
					default:
						if r.URL.Query().Has("chmod") {
							chmods++
						}
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
						w.WriteHeader(501)
					}
				}))
				defer srv.Close()
				cl, err := client.New(srv.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				wb, err := New(Options{Client: cl, Policy: PolicyCloseSync})
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
				defer func() { _ = chain.Stop(context.Background()) }()
				ctx, _ := client.WithViewOperationID(t.Context(), "exclusive-create")
				op := &fs.Op{Kind: fs.OpCreate, Path: "/file", Mode: mode, HasMode: true, Flags: syscall.O_CREAT | syscall.O_EXCL | syscall.O_WRONLY, Kernel: &gofuse.CreateIn{}}
				res, err := chain.Dispatch(ctx, op)
				if scenario == "existing" {
					if !errors.Is(err, syscall.EEXIST) || res.Handle != nil {
						t.Fatalf("existing: %+v %v", res, err)
					}
					mu.Lock()
					defer mu.Unlock()
					if stored != "existing" || revision != 3 || puts != 0 {
						t.Fatal("changed existing file")
					}
					return
				}
				if scenario == "lost-receipt" {
					if err == nil || res.Handle != nil {
						t.Fatal("accepted incomplete receipt")
					}
					// The identical request reconciles its original receipt, without stat.
					res, err = chain.Dispatch(ctx, op)
				}
				if err != nil || res.Handle == nil || res.Attr.Rev != 1 || res.Attr.Mode != mode {
					t.Fatalf("create: %+v %v", res, err)
				}
				h := writebackHandleOf(res.Handle)
				if h.baseRev != 1 || h.hasPendingMode {
					t.Fatal("lost exact creation revision or deferred initial mode")
				}
				if err := h.Write(t.Context(), 0, []byte("ours")); err != nil {
					t.Fatal(err)
				}
				err = h.Flush(t.Context())
				if scenario == "foreign-write" {
					if err == nil {
						t.Fatal("accepted stale write")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				defer mu.Unlock()
				want := "ours"
				if scenario == "foreign-write" {
					want = "foreign"
				}
				if stored != want || puts != 1 || chmods != 0 {
					t.Fatalf("stored=%q puts=%d chmods=%d", stored, puts, chmods)
				}
				wantCreates := 1
				if scenario == "lost-receipt" {
					wantCreates = 2
				}
				if creates != wantCreates {
					t.Fatal("unexpected create retry", creates)
				}
			})
		}
	}
}
