package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedViewControlRoutesAndBodies(t *testing.T) {
	work := ViewWork{LayerID: "a", ID: "w", Fence: 2, LeaseUntil: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	state := FixedViewState{LayerID: "a", Phase: "READY", Generation: 2, Fence: 2, AdmissionClosed: true}
	outcome := FixedViewOutcome{State: "DONE", Layer: state}
	type call struct {
		method, target string
		body           map[string]any
		response       any
	}
	calls := []call{
		{"POST", "/v1/views", map[string]any{"operation_id": "create", "layer_id": "a", "parent_layer_id": "parent", "name": "demo"}, state},
		{"GET", "/v1/views?after=a&limit=2", nil, FixedViewPage{Layers: []FixedViewInfo{{LayerID: "b"}}, NextAfter: "b"}},
		{"GET", "/v1/views/a", nil, FixedViewInfo{LayerID: "a", Phase: "OPEN"}},
		{"POST", "/v1/views/a/work", map[string]any{"operation_id": "begin", "ttl_ms": float64(60000), "validation": false}, ViewWorkClaim{Work: work, Launch: true}},
		{"POST", "/v1/views/a/renew", nil, work},
		{"POST", "/v1/views/a/seal", nil, outcome},
		{"POST", "/v1/views/a/cancel", map[string]any{"operation_id": "cancel"}, outcome},
		{"DELETE", "/v1/views/a", map[string]any{"operation_id": "delete"}, outcome},
		{"GET", "/v1/views/a/operations/main", nil, FixedViewOperation{OperationID: "main", LayerID: "a", Kind: "seal", State: "DONE"}},
	}
	index := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if index >= len(calls) {
			t.Error("unexpected extra request")
			w.WriteHeader(500)
			return
		}
		want := calls[index]
		index++
		if r.Method != want.method || r.URL.RequestURI() != want.target || r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("X-Dat9-Actor") != "mount" || r.Header.Get(viewprotocol.ViewHeader) != "" {
			t.Errorf("control route: %s %s %+v", r.Method, r.URL, r.Header)
		}
		var body map[string]any
		if r.Method != "GET" {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("missing JSON type")
			}
		}
		if want.body != nil && !reflect.DeepEqual(body, want.body) {
			t.Errorf("body %+v want %+v", body, want.body)
		}
		if strings.HasSuffix(want.target, "/renew") || strings.HasSuffix(want.target, "/seal") {
			encoded, _ := json.Marshal(body["work"])
			var got ViewWork
			if err := json.Unmarshal(encoded, &got); err != nil || got != work {
				t.Errorf("work %+v %v", got, err)
			}
			if strings.HasSuffix(want.target, "/renew") && body["ttl_ms"] != float64(1) {
				t.Error("wrong renewal ttl")
			}
			if strings.HasSuffix(want.target, "/seal") && body["operation_id"] != "seal" {
				t.Error("wrong seal operation")
			}
		}
		_ = json.NewEncoder(w).Encode(want.response)
	}))
	defer srv.Close()
	root := New(srv.URL, "test-token")
	root.SetActor("mount")
	c, err := root.WithView(ViewBinding{LayerID: "different", WorkID: "old", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if got, err := c.CreateFixedView(ctx, FixedViewCreate{OperationID: "create", LayerID: "a", ParentLayerID: "parent", Name: "demo"}); err != nil || got != state {
		t.Fatalf("create %+v %v", got, err)
	}
	if got, err := c.ListFixedViews(ctx, "a", 2); err != nil || len(got.Layers) != 1 || got.NextAfter != "b" {
		t.Fatalf("list %+v %v", got, err)
	}
	if got, err := c.GetFixedView(ctx, "a"); err != nil || got.Phase != "OPEN" {
		t.Fatalf("get %+v %v", got, err)
	}
	if got, err := c.BeginViewWork(ctx, "a", "begin", time.Minute, false); err != nil || !got.Launch || got.Work != work {
		t.Fatalf("work %+v %v", got, err)
	}
	if got, err := c.RenewViewWork(ctx, work, time.Millisecond); err != nil || got != work {
		t.Fatalf("renew %+v %v", got, err)
	}
	if got, err := c.SealFixedView(ctx, work, "seal"); err != nil || got != outcome {
		t.Fatalf("seal %+v %v", got, err)
	}
	if got, err := c.EndFixedView(ctx, "a", "cancel", false); err != nil || got != outcome {
		t.Fatalf("cancel %+v %v", got, err)
	}
	if got, err := c.EndFixedView(ctx, "a", "delete", true); err != nil || got != outcome {
		t.Fatalf("delete %+v %v", got, err)
	}
	if got, err := c.GetFixedViewOperation(ctx, "a", "main"); err != nil || got.State != "DONE" {
		t.Fatalf("receipt %+v %v", got, err)
	}
	if index != len(calls) || c.view.LayerID != "different" {
		t.Fatalf("calls=%d binding=%+v", index, c.view)
	}
}

func TestFixedViewControlDoesNotRetryOrGrantMissingLaunch(t *testing.T) {
	for _, status := range []int{200, 302, 409, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Location", "/v1/views/a/work")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"work":{"layer_id":"a","work_id":"w","fence":2,"lease_until":"2026-01-01T00:00:00Z"},"launch":false,"error":"query original operation"}`))
			}))
			defer srv.Close()
			c := New(srv.URL, "test-token")
			claim, err := c.BeginViewWork(context.Background(), "a", "begin", time.Minute, false)
			if (err != nil) != (status != 200) || claim.Launch || requests != 1 {
				t.Fatalf("result %+v %v requests=%d", claim, err, requests)
			}
		})
	}
}

func TestFixedViewControlRejectsInvalidInputBeforeHTTP(t *testing.T) {
	c := New("http://127.0.0.1:1", "test-token")
	c.httpClient.Transport = viewControlRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid input reached transport")
		return nil, nil
	})
	ctx := context.Background()
	for _, id := range []string{"", "main", "MAIN", "../a", "a/b", strings.Repeat("x", 65)} {
		if _, err := c.GetFixedView(ctx, id); err == nil {
			t.Errorf("accepted %q", id)
		}
		if _, err := c.CreateFixedView(ctx, FixedViewCreate{LayerID: id, OperationID: "create"}); err == nil {
			t.Errorf("created %q", id)
		}
	}
	for _, ttl := range []time.Duration{0, time.Microsecond, time.Hour + 1} {
		if _, err := c.BeginViewWork(ctx, "a", "begin", ttl, false); err == nil {
			t.Errorf("accepted ttl %v", ttl)
		}
	}
	for _, limit := range []int{0, 257} {
		if _, err := c.ListFixedViews(ctx, "", limit); err == nil {
			t.Errorf("accepted limit %d", limit)
		}
	}
	w := ViewWork{LayerID: "a"}
	if _, err := c.RenewViewWork(ctx, w, time.Minute); err == nil {
		t.Fatal("renewed incomplete work")
	}
	if _, err := c.SealFixedView(ctx, w, "seal"); err == nil {
		t.Fatal("sealed incomplete work")
	}
}

type viewControlRoundTripper func(*http.Request) (*http.Response, error)

func (f viewControlRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestViewClaimRejectsIncompleteOrMismatchedReceipt(t *testing.T) {
	for _, response := range []string{
		`{"launch":true}`,
		`{"work":{"layer_id":"a","work_id":"w","fence":1,"lease_until":"2026-01-01T00:00:00Z"}}`,
		`{"launch":true,"work":{"layer_id":"b","work_id":"w","fence":1,"lease_until":"2026-01-01T00:00:00Z"}}`,
		`{"launch":true,"work":{"layer_id":"a","work_id":"w","fence":1,"lease_until":"2026-01-01T00:00:00Z","validation":true}}`,
		`{"launch":true,"work":{"layer_id":"a","work_id":"w","fence":0,"lease_until":"2026-01-01T00:00:00Z"}}`,
		`{"launch":true,"work":{"layer_id":"a","work_id":"w","fence":1}}`,
		`{"launch":true,"work":`,
	} {
		t.Run(response, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(response)) }))
			defer srv.Close()
			got, err := New(srv.URL, "test-token").BeginViewWork(context.Background(), "a", "begin", time.Minute, false)
			if err == nil || got != (ViewWorkClaim{}) {
				t.Fatalf("unsafe receipt %+v %v", got, err)
			}
		})
	}
}

func TestFixedControlCompleteResponseBeforeLaunchOrSuccess(t *testing.T) {
	const receipt = `{"layer_id":"a","phase":"OPEN","generation":1,"work":{"layer_id":"a","work_id":"w","fence":1,"lease_until":"2026-01-01T00:00:00Z"},"launch":true,"state":"DONE","layer":{"layer_id":"a","phase":"READY","generation":2}}`
	for _, action := range []string{"create", "begin", "seal", "cancel", "delete"} {
		for _, response := range []string{"complete", "short-body", "trailing", "oversize", "empty"} {
			t.Run(action+"/"+response, func(t *testing.T) {
				var requests atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					body := receipt
					switch response {
					case "short-body":
						// The entire JSON prefix is present, but the HTTP response
						// ends early. Decoder.Decode alone would grant launch.
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+32))
					case "trailing":
						body += ` {"launch":false}`
					case "oversize":
						body += strings.Repeat(" ", 4<<20)
					case "empty":
						body = ""
					}
					_, _ = w.Write([]byte(body))
				}))
				defer srv.Close()
				c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "other", WorkID: "old", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				var claim ViewWorkClaim
				switch action {
				case "create":
					_, err = c.CreateFixedView(ctx, FixedViewCreate{LayerID: "a", OperationID: "op"})
				case "begin":
					claim, err = c.BeginViewWork(ctx, "a", "op", time.Minute, false)
				case "seal":
					_, err = c.SealFixedView(ctx, ViewWork{LayerID: "a", ID: "w", Fence: 1}, "op")
				case "cancel", "delete":
					_, err = c.EndFixedView(ctx, "a", "op", action == "delete")
				}
				if requests.Load() != 1 {
					t.Fatal("control retried", requests.Load())
				}
				if response == "complete" {
					if err != nil || action == "begin" && !claim.Launch {
						t.Fatal(claim, err)
					}
					return
				}
				var identified *ViewOperationError
				if err == nil || !errors.As(err, &identified) || identified.LayerID != "a" || identified.OperationID != "op" || claim.Launch {
					t.Fatal("incomplete response accepted or original identity lost", claim, err)
				}
			})
		}
	}
}

func TestFixedControlOperationIDValidatedBeforeTransport(t *testing.T) {
	c := New("http://127.0.0.1:1", "test-token")
	c.httpClient.Transport = viewControlRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid operation reached transport")
		return nil, nil
	})
	ctx := context.Background()
	for _, id := range []string{"", "../invalid", strings.Repeat("x", 129)} {
		if _, err := c.CreateFixedView(ctx, FixedViewCreate{LayerID: "a", OperationID: id}); err == nil {
			t.Fatal(id)
		}
		if _, err := c.BeginViewWork(ctx, "a", id, time.Minute, false); err == nil {
			t.Fatal(id)
		}
		if _, err := c.SealFixedView(ctx, ViewWork{LayerID: "a", ID: "w", Fence: 1}, id); err == nil {
			t.Fatal(id)
		}
		if _, err := c.EndFixedView(ctx, "a", id, true); err == nil {
			t.Fatal(id)
		}
		if _, err := c.GetFixedViewOperation(ctx, "a", id); err == nil {
			t.Fatal(id)
		}
	}
}

func TestFixedRenewalKeepsWorkIdentity(t *testing.T) {
	original := ViewWork{LayerID: "a", ID: "w", Fence: 7, Validation: true,
		LeaseUntil: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, test := range []string{"valid", "layer", "work", "fence", "validation", "expiry", "null", "truncated", "trailing", "status"} {
		t.Run(test, func(t *testing.T) {
			renewed := original
			renewed.LeaseUntil = renewed.LeaseUntil.Add(time.Minute)
			switch test {
			case "layer":
				renewed.LayerID = "b"
			case "work":
				renewed.ID = "next-work"
			case "fence":
				renewed.Fence++
			case "validation":
				renewed.Validation = false
			case "expiry":
				renewed.LeaseUntil = time.Time{}
			}
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v1/views/a/renew" {
					t.Error(r.Method, r.URL)
				}
				body, _ := json.Marshal(renewed)
				switch test {
				case "null":
					body = []byte("null")
				case "truncated":
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
				case "trailing":
					body = append(body, []byte(" {}")...)
				case "status":
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				_, _ = w.Write(body)
			}))
			defer srv.Close()
			got, err := New(srv.URL, "test-token").RenewViewWork(context.Background(), original, time.Minute)
			if calls.Load() != 1 {
				t.Fatal("renewal retried", calls.Load())
			}
			if test == "valid" {
				if err != nil || got != renewed {
					t.Fatal(got, err)
				}
			} else if err == nil || got != (ViewWork{}) {
				t.Fatal("accepted a mismatched or partial work generation", got, err)
			}
		})
	}
}

func TestFixedOperationQueryChecksReceiptIdentity(t *testing.T) {
	for _, test := range []string{"RUNNING", "DONE", "FAILED", "layer", "operation", "kind", "state", "null", "truncated", "trailing", "status"} {
		t.Run(test, func(t *testing.T) {
			receipt := FixedViewOperation{LayerID: "a", OperationID: "original", Kind: "mutation", State: "DONE"}
			switch test {
			case "RUNNING", "DONE", "FAILED":
				receipt.State = test
			case "layer":
				receipt.LayerID = "b"
			case "operation":
				receipt.OperationID = "another"
			case "kind":
				receipt.Kind = ""
			case "state":
				receipt.State = "UNKNOWN"
			}
			if receipt.State == "FAILED" {
				receipt.Code = "AUTHORIZATION_FAILED"
			}
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.Path != "/v1/views/a/operations/original" || r.Header.Get(viewprotocol.ViewHeader) != "" {
					t.Error(r.Method, r.URL, r.Header)
				}
				body, _ := json.Marshal(receipt)
				switch test {
				case "null":
					body = []byte("null")
				case "truncated":
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
				case "trailing":
					body = append(body, []byte(" {}")...)
				case "status":
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				_, _ = w.Write(body)
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "different", WorkID: "other", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.GetFixedViewOperation(context.Background(), "a", "original")
			if calls.Load() != 1 {
				t.Fatal("receipt query retried", calls.Load())
			}
			if test == "RUNNING" || test == "DONE" || test == "FAILED" {
				if err != nil || !reflect.DeepEqual(got, receipt) {
					t.Fatal(got, err)
				}
				return
			}
			var identified *ViewOperationError
			if !errors.As(err, &identified) || identified.LayerID != "a" || identified.OperationID != "original" || !reflect.DeepEqual(got, FixedViewOperation{}) {
				t.Fatal("query returned an unverified receipt or lost request identity", got, err)
			}
		})
	}
}
