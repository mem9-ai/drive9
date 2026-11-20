package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestBoundClientIsolationAndRequestOwnership(t *testing.T) {
	type call struct {
		path, query string
		view        ViewBinding
	}
	calls := make(chan call, 3)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := viewprotocol.Parse(r.Header)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if v.LayerID != "" {
			// Reverse proxies remove an optional /api base path before dispatch.
			copy := r.Clone(r.Context())
			copy.URL.Path = strings.TrimPrefix(copy.URL.Path, "/api")
			copy.URL.RawPath = strings.TrimPrefix(copy.URL.RawPath, "/api")
			plain, parsed, err := viewprotocol.ParseRoute(copy)
			if err != nil || parsed != v || plain.URL.EscapedPath() != "/v1/fs/a%2Fb%20c" {
				t.Errorf("server binding: %+v %v", parsed, err)
			}
		}
		calls <- call{r.URL.EscapedPath(), r.URL.RawQuery, v}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	root := New(srv.URL+"/api", "test-token")
	root.SetActor("mount")
	bindings := []ViewBinding{{}, {LayerID: "a", WorkID: "wa", Fence: 2}, {LayerID: "b", WorkID: "wb", Fence: 3, Validation: true}}
	var wg sync.WaitGroup
	for _, binding := range bindings {
		c, err := root.WithView(binding)
		if err != nil {
			t.Fatal(err)
		}
		if c == root || c.httpClient == root.httpClient || c.statusBody.Load() != nil {
			t.Fatal("shared mutable client state")
		}
		wg.Add(1)
		go func(c *Client, b ViewBinding) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, srv.URL+"/api/v1/fs/a%2Fb%20c?offset=7", strings.NewReader("x"))
			if err != nil {
				t.Error(err)
				return
			}
			original := req.URL.String()
			resp, err := c.do(req)
			if err != nil {
				t.Error(err)
				return
			}
			_ = resp.Body.Close()
			if req.URL.String() != original || req.Header.Get(viewprotocol.ViewHeader) != "" {
				t.Error("changed caller request binding")
			}
		}(c, binding)
	}
	wg.Wait()
	close(calls)
	seen := map[string]bool{}
	for got := range calls {
		want := "/api/v1/fs/a%2Fb%20c"
		if got.view.LayerID != "" {
			want = "/api/v1/views/" + got.view.LayerID + "/v1/fs/a%2Fb%20c"
		}
		if got.path != want || got.query != "offset=7" {
			t.Fatalf("rewritten route %+v want %s", got, want)
		}
		seen[got.view.LayerID] = true
	}
	if len(seen) != 3 || root.view != (ViewBinding{}) {
		t.Fatalf("isolation %v", seen)
	}
}

func TestBoundClientCannotFallBackToMain(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(fmt.Sprint(redirect), func(t *testing.T) {
			mainWrites := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/v1/views/") {
					if redirect {
						http.Redirect(w, r, "/v1/fs/file", http.StatusTemporaryRedirect)
					} else {
						http.NotFound(w, r)
					}
					return
				}
				mainWrites++
				w.WriteHeader(204)
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.WriteCtx(context.Background(), "/file", []byte("private")); err == nil {
				t.Fatal("unsupported/redirected view write succeeded")
			}
			if mainWrites != 0 {
				t.Fatalf("main touched %d times", mainWrites)
			}
		})
	}
}

func TestBoundClientValidationAndRepeatedRequest(t *testing.T) {
	root := New("http://127.0.0.1:1", "test-token")
	if _, err := root.WithView(ViewBinding{LayerID: "a"}); err == nil {
		t.Fatal("incomplete binding accepted")
	}
	c, err := root.WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"http://elsewhere.invalid/v1/fs/a", "http://127.0.0.1:1/health", "http://127.0.0.1:1/v10/fs/a"} {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		if _, err := c.boundRequest(req); err == nil {
			t.Fatalf("invalid target accepted %s", target)
		}
	}
	req, _ := http.NewRequest(http.MethodPut, "http://127.0.0.1:1/v1/fs/a", strings.NewReader("x"))
	for i := 0; i < 2; i++ {
		out, err := c.boundRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		if out.URL.Path != "/v1/views/a/v1/fs/a" || req.URL.Path != "/v1/fs/a" {
			t.Fatal("binding accumulated across retries")
		}
		if i == 0 {
			data, err := io.ReadAll(out.Body)
			if err != nil || string(data) != "x" {
				t.Fatalf("body %q %v", data, err)
			}
		}
	}
	v2, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:1/v2/uploads/u/complete", nil)
	v2out, err := c.boundRequest(v2)
	if err != nil || v2out.URL.Path != "/v1/views/a/v2/uploads/u/complete" {
		t.Fatalf("v2 binding %v", err)
	}
	// Returning to main allocates another client and leaves old Layer requests bound.
	main, err := c.WithView(ViewBinding{})
	if err != nil || main.view != (ViewBinding{}) || c.view.LayerID != "a" {
		t.Fatalf("main reset %v", err)
	}
}

func TestBoundEventStreamUsesSameView(t *testing.T) {
	for _, binding := range []ViewBinding{{}, {LayerID: "a", WorkID: "w", Fence: 2}} {
		t.Run(binding.LayerID, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := "/v1/events"
				if binding.LayerID != "" {
					want = "/v1/views/a/v1/events"
				}
				got, err := viewprotocol.Parse(r.Header)
				if err != nil || got != binding || r.URL.Path != want || r.URL.Query().Get("since") != "17" || r.Header.Get("X-Dat9-Actor") != "watch-actor" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("event binding: %s %v %+v %v", r.URL, r.Header, got, err)
				}
				_, _ = io.WriteString(w, "event: file_changed\ndata: {\"seq\":18,\"path\":\"/a\",\"op\":\"write\"}\n\nevent: heartbeat\ndata: {\"seq\":19}\n\n")
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(binding)
			if err != nil {
				t.Fatal(err)
			}
			c.SetActor("default-actor")
			var changed, current uint64
			err = c.streamEvents(context.Background(), 17, "watch-actor", func(ev *ChangeEvent, _ *ResetEvent) { changed = ev.Seq }, func(seq uint64) { current = seq })
			if err != nil || changed != 18 || current != 19 {
				t.Fatalf("events %d %d %v", changed, current, err)
			}
		})
	}
}

func TestBoundReadAndEventsRejectRedirect(t *testing.T) {
	for _, status := range []int{302, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var followups int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/v1/views/") {
					w.Header().Set("Location", "http://"+r.Host+"/v1/fs/main")
					w.WriteHeader(status)
					return
				}
				followups++
				_, _ = io.WriteString(w, "wrong view")
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.streamEvents(context.Background(), 0, "actor", func(*ChangeEvent, *ResetEvent) { t.Error("delivered redirected event") }, nil); err == nil {
				t.Fatal("redirected SSE succeeded")
			}
			if body, err := c.ReadStream(context.Background(), "/a"); err == nil {
				_ = body.Close()
				t.Fatal("redirected stream succeeded")
			}
			if body, err := c.ReadStreamRange(context.Background(), "/a", 1, 2); err == nil {
				_ = body.Close()
				t.Fatal("redirected range succeeded")
			}
			if _, err := c.ResolveReadTarget(context.Background(), "/a"); err == nil {
				t.Fatal("redirected target escaped")
			}
			if followups != 0 {
				t.Fatalf("left view route %d times", followups)
			}
		})
	}
}

func TestExtentPlanKeepsFixedViewJSONAsBytes(t *testing.T) {
	body := `{"size":0,"blocks":[]}`
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Dat9-Extent-Read-Plan") != "" || r.URL.Path != "/v1/views/a/v1/fs/file" {
			t.Error("incorrect fixed route or negotiation", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
			w.WriteHeader(206)
		} else {
			w.WriteHeader(200)
		}
		_, _ = io.WriteString(w, body)
	}))
	defer api.Close()
	c, err := New(api.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ReadAtCtx(context.Background(), "/file", 0, int64(len(body)))
	if err != nil || string(got) != body {
		t.Fatalf("got=%q err=%v", got, err)
	}
	full, err := c.ReadCtx(context.Background(), "/file")
	if err != nil || string(full) != body {
		t.Fatalf("full=%q err=%v", full, err)
	}
	if _, err := c.ResolveReadTarget(context.Background(), "/file"); !IsReadTargetNoRedirect(err) {
		t.Fatalf("resolve target err=%v", err)
	}
}

func TestFixedViewSmallWriteIgnoresInheritedExtentTargetLayout(t *testing.T) {
	var puts, patches atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch:
			patches.Add(1)
			w.WriteHeader(http.StatusMethodNotAllowed)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/views/a/v1/fs/inherited.bin":
			puts.Add(1)
			opID := r.Header.Get(viewprotocol.OperationHeader)
			w.Header().Set(viewprotocol.OperationHeader, opID)
			_, _ = fmt.Fprintf(w, `{"status":"ok","operation_id":%q,"revision":2}`, opID)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	c, err := New(api.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	zeroSummary, err := c.WriteStreamWithSummary(context.Background(), "/inherited.bin", strings.NewReader(""), 0, nil, WithTargetLayout(ContentLayoutExtent))
	if err != nil {
		t.Fatalf("fixed zero-byte overwrite: %v", err)
	}
	if zeroSummary.Mode != "direct_put" || puts.Load() != 1 || patches.Load() != 0 {
		t.Fatalf("zeroSummary=%+v puts=%d patches=%d", zeroSummary, puts.Load(), patches.Load())
	}
	c.SetSmallFileThresholdForTests(65536)
	summary, err := c.WriteStreamWithSummary(context.Background(), "/inherited.bin", strings.NewReader("small"), 5, nil, WithTargetLayout(ContentLayoutExtent))
	if err != nil {
		t.Fatalf("fixed small overwrite: %v", err)
	}
	if summary.Mode != "direct_put" || puts.Load() != 2 || patches.Load() != 0 {
		t.Fatalf("summary=%+v puts=%d patches=%d", summary, puts.Load(), patches.Load())
	}
	if _, err := c.WriteStreamWithSummary(context.Background(), "/inherited.bin", strings.NewReader("small"), 5, nil, WithContentLayout(ContentLayoutSingle)); err == nil {
		t.Fatal("accepted explicit single layout on fixed view")
	}
}
