package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func prepareViewRequest() FixedViewPrepareRequest {
	return FixedViewPrepareRequest{Create: FixedViewCreate{LayerID: "a", OperationID: "create", ParentLayerID: "parent"},
		WorkOperationID: "work", SealOperationID: "seal", ValidationOperationID: "validation", WorkTTL: time.Minute, ValidationTTL: time.Minute}
}

type prepareViewServer struct {
	mu       sync.Mutex
	calls    []string
	claimed  map[string]bool
	response map[string]any
	failAt   string
	failure  string
	fileOps  []ViewBinding
}

func newPrepareViewServer(t *testing.T) (*Client, *prepareViewServer) {
	t.Helper()
	p := &prepareViewServer{claimed: map[string]bool{}, response: map[string]any{
		// The actual server starts at generation zero, including derived layers.
		"create":     FixedViewState{LayerID: "a", Phase: "OPEN"},
		"work":       ViewWorkClaim{Launch: true, Work: ViewWork{LayerID: "a", ID: "w", Fence: 1, LeaseUntil: time.Now().Add(time.Hour)}},
		"seal":       FixedViewOutcome{State: "DONE", Layer: FixedViewState{LayerID: "a", Phase: "READY", Fence: 1, Generation: 1, AdmissionClosed: true}},
		"validation": ViewWorkClaim{Launch: true, Work: ViewWork{LayerID: "a", ID: "v", Fence: 2, Validation: true, LeaseUntil: time.Now().Add(time.Hour)}},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/v1/views/a/v1/fs/") {
			binding, err := viewprotocol.Parse(r.Header)
			if err != nil || binding.LayerID != "a" {
				t.Errorf("file binding=%+v err=%v", binding, err)
			}
			p.fileOps = append(p.fileOps, binding)
			if binding.Validation && r.Method != "HEAD" {
				http.Error(w, "validation is readonly", http.StatusForbidden)
				return
			}
			if r.Method == "PUT" {
				data, _ := io.ReadAll(r.Body)
				if string(data) != "result" || r.Header.Get(viewprotocol.OperationHeader) != "write-file" {
					t.Errorf("write %q identity %q", data, r.Header.Get(viewprotocol.OperationHeader))
				}
				_, _ = w.Write([]byte(`{"revision":1}`))
			} else {
				w.Header().Set("Content-Length", "6")
				w.Header().Set("X-Dat9-Revision", "1")
			}
			return
		}
		var req struct {
			OperationID string   `json:"operation_id"`
			Validation  bool     `json:"validation"`
			TTL         int64    `json:"ttl_ms"`
			Work        ViewWork `json:"work"`
			Parent      string   `json:"parent_layer_id"`
		}
		if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.Error(w, "unexpected", 500)
			return
		}
		stage := req.OperationID
		paths := map[string]string{"create": "/v1/views", "work": "/v1/views/a/work", "seal": "/v1/views/a/seal", "validation": "/v1/views/a/work"}
		if r.URL.Path != paths[stage] || r.Header.Get(viewprotocol.ViewHeader) != "" ||
			(stage == "work" || stage == "validation") && (req.Validation != (stage == "validation") || req.TTL != 60000) ||
			stage == "seal" && (req.Work.LayerID != "a" || req.Work.ID != "w" || req.Work.Fence != 1 || req.Work.Validation) ||
			stage == "create" && req.Parent != "parent" {
			t.Errorf("control request %s %s %+v", r.Method, r.URL, req)
		}
		p.calls = append(p.calls, stage)
		response := p.response[stage]
		if claim, ok := response.(ViewWorkClaim); ok {
			claim.Launch = claim.Launch && !p.claimed[stage]
			p.claimed[stage] = true
			response = claim
		}
		raw, err := json.Marshal(response)
		if err != nil {
			t.Error(err)
		}
		if stage == p.failAt {
			switch p.failure {
			case "503":
				w.WriteHeader(503)
			case "short-body":
				// The server has already consumed launch, even if a complete JSON
				// prefix arrives but the HTTP response ends prematurely.
				w.Header().Set("Content-Length", strconv.Itoa(len(raw)+1))
			case "malformed":
				raw = []byte("{")
			}
		}
		_, _ = w.Write(raw)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, ""), p
}

func (p *prepareViewServer) requests() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func TestPrepareFixedViewBindsBothCallbacksAndNeverPublishes(t *testing.T) {
	root, server := newPrepareViewServer(t)
	original := ViewBinding{LayerID: "other", WorkID: "old", Fence: 8}
	c, err := root.WithView(original)
	if err != nil {
		t.Fatal(err)
	}
	var workCtx, validationCtx context.Context
	out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), func(ctx context.Context, bound *Client, work ViewWork) error {
		workCtx = ctx
		if bound.ViewBinding() != work.Binding() || work.Validation || bound == c {
			t.Fatal("work received wrong binding")
		}
		writeCtx, err := WithViewOperationID(ctx, "write-file")
		if err != nil {
			return err
		}
		return bound.WriteCtx(writeCtx, "/a.txt", []byte("result"))
	}, func(ctx context.Context, bound *Client, work ViewWork) error {
		validationCtx = ctx
		if workCtx.Err() != context.Canceled || !work.Validation || bound.ViewBinding() != work.Binding() {
			t.Fatal("validation did not get a separate readonly work context")
		}
		stat, err := bound.StatCtx(ctx, "/a.txt")
		if err == nil && (stat.Size != 6 || stat.Revision != 1) {
			t.Fatalf("stat=%+v", stat)
		}
		return err
	})
	if err != nil || !out.Validated || out.Stage != "validation" || out.OperationID != "validation" || out.Validation.Fence != 2 || out.Work.Fence != 1 || out.Seal.State != "DONE" {
		t.Fatalf("result=%+v err=%v", out, err)
	}
	if validationCtx.Err() != context.Canceled || c.ViewBinding() != original {
		t.Fatal("callback lifetime or original binding changed")
	}
	if got := server.requests(); !reflect.DeepEqual(got, []string{"create", "work", "seal", "validation"}) {
		t.Fatalf("requests=%v", got)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if !reflect.DeepEqual(server.fileOps, []ViewBinding{out.Work.Binding(), out.Validation.Binding()}) {
		t.Fatalf("file requests=%+v", server.fileOps)
	}
}

func TestPrepareFixedViewDuplicateRunsCallbacksAtMostOnce(t *testing.T) {
	c, server := newPrepareViewServer(t)
	var work, validate atomic.Int32
	workFn := func(context.Context, *Client, ViewWork) error { work.Add(1); return nil }
	validateFn := func(context.Context, *Client, ViewWork) error { validate.Add(1); return nil }
	var wg sync.WaitGroup
	var completed, refused atomic.Int32
	for range 2 {
		wg.Go(func() {
			out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), workFn, validateFn)
			if err == nil && out.Validated {
				completed.Add(1)
			} else if errors.Is(err, ErrViewLaunchUnavailable) && out.Stage == "work" {
				refused.Add(1)
			} else {
				t.Errorf("unexpected duplicate result=%+v err=%v", out, err)
			}
		})
	}
	wg.Wait()
	if work.Load() != 1 || validate.Load() != 1 || completed.Load() != 1 || refused.Load() != 1 || len(server.requests()) != 6 {
		t.Fatalf("work=%d validate=%d completed=%d refused=%d requests=%v", work.Load(), validate.Load(), completed.Load(), refused.Load(), server.requests())
	}
}

func TestPrepareFixedViewUnknownResultStopsAtOriginalOperation(t *testing.T) {
	stages := []string{"create", "work", "seal", "validation"}
	for index, stage := range stages {
		for _, failure := range []string{"503", "short-body", "malformed"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				c, server := newPrepareViewServer(t)
				server.failAt, server.failure = stage, failure
				work, validate := 0, 0
				out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), func(context.Context, *Client, ViewWork) error { work++; return nil }, func(context.Context, *Client, ViewWork) error { validate++; return nil })
				var operation *ViewOperationError
				if !errors.As(err, &operation) || operation.LayerID != "a" || operation.OperationID != stage || out.Stage != stage || out.Validated {
					t.Fatalf("out=%+v err=%v", out, err)
				}
				wantWork := 0
				if index >= 2 {
					wantWork = 1
				}
				if work != wantWork || validate != 0 || !reflect.DeepEqual(server.requests(), stages[:index+1]) {
					t.Fatalf("callbacks=%d,%d requests=%v", work, validate, server.requests())
				}
				if stage == "work" {
					server.mu.Lock()
					server.failAt = ""
					server.mu.Unlock()
					_, err = c.PrepareFixedView(context.Background(), prepareViewRequest(), func(context.Context, *Client, ViewWork) error { t.Error("lost launch was reused"); return nil }, func(context.Context, *Client, ViewWork) error { t.Error("unexpected validation"); return nil })
					if !errors.Is(err, ErrViewLaunchUnavailable) {
						t.Fatalf("replay=%v", err)
					}
				}
			})
		}
	}
}

func TestPrepareFixedViewStopsOnCallbackFailure(t *testing.T) {
	for _, validation := range []bool{false, true} {
		t.Run(fmt.Sprint(validation), func(t *testing.T) {
			c, server := newPrepareViewServer(t)
			failure := errors.New("tool failed")
			var callbackCtx context.Context
			fn := func(ctx context.Context, _ *Client, work ViewWork) error {
				if work.Validation == validation {
					callbackCtx = ctx
					return failure
				}
				return nil
			}
			out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), fn, fn)
			wantCalls, wantStage := 2, "work"
			if validation {
				wantCalls, wantStage = 4, "validation"
			}
			if !errors.Is(err, failure) || out.Validated || out.Stage != wantStage || len(server.requests()) != wantCalls || callbackCtx.Err() != context.Canceled {
				t.Fatalf("out=%+v err=%v calls=%v", out, err, server.requests())
			}
		})
	}
}

func TestPrepareFixedViewRequiresConfirmedSealBeforeValidation(t *testing.T) {
	for _, kind := range []string{"running", "upload-pending", "wrong-layer", "wrong-fence", "open", "admission-open", "negative-generation"} {
		t.Run(kind, func(t *testing.T) {
			c, server := newPrepareViewServer(t)
			seal := server.response["seal"].(FixedViewOutcome)
			switch kind {
			case "running":
				seal.State, seal.Layer.Phase = "RUNNING", "FREEZING"
			case "upload-pending":
				seal.State, seal.Code, seal.Layer.Phase = "FAILED", "UPLOAD_PENDING", "OPEN"
			case "wrong-layer":
				seal.Layer.LayerID = "other"
			case "wrong-fence":
				seal.Layer.Fence++
			case "open":
				seal.Layer.Phase = "OPEN"
			case "admission-open":
				seal.Layer.AdmissionClosed = false
			case "negative-generation":
				seal.Layer.Generation = -1
			}
			server.response["seal"] = seal
			out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), func(context.Context, *Client, ViewWork) error { return nil }, func(context.Context, *Client, ViewWork) error { t.Error("validation started without seal"); return nil })
			var operation *ViewOperationError
			if !errors.As(err, &operation) || operation.OperationID != "seal" || out.Seal != seal || out.Validated || len(server.requests()) != 3 {
				t.Fatalf("out=%+v err=%v calls=%v", out, err, server.requests())
			}
		})
	}
}

func TestPrepareFixedViewRejectsInvalidInputBeforeCreating(t *testing.T) {
	for _, kind := range []string{"create-id", "work-id", "seal-id", "validation-id", "duplicate-id", "layer", "parent", "work-ttl", "validation-ttl", "work-callback", "validation-callback", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			c, server := newPrepareViewServer(t)
			req := prepareViewRequest()
			fn := func(context.Context, *Client, ViewWork) error { t.Error("callback ran"); return nil }
			work, validate := ViewWorkFunc(fn), ViewWorkFunc(fn)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "create-id":
				req.Create.OperationID = ""
			case "work-id":
				req.WorkOperationID = "../w"
			case "seal-id":
				req.SealOperationID = ""
			case "validation-id":
				req.ValidationOperationID = ""
			case "duplicate-id":
				req.SealOperationID = req.WorkOperationID
			case "layer":
				req.Create.LayerID = "main"
			case "parent":
				req.Create.ParentLayerID = "../p"
			case "work-ttl":
				req.WorkTTL = 0
			case "validation-ttl":
				req.ValidationTTL = time.Hour + 1
			case "work-callback":
				work = nil
			case "validation-callback":
				validate = nil
			case "canceled":
				cancel()
			}
			out, err := c.PrepareFixedView(ctx, req, work, validate)
			if err == nil || out.Validated || len(server.requests()) != 0 {
				t.Fatalf("out=%+v err=%v calls=%v", out, err, server.requests())
			}
		})
	}
}

func TestPrepareFixedViewRequiresValidCreateAndFreshGeneration(t *testing.T) {
	for _, kind := range []string{"wrong-create-layer", "closed-create", "wrong-create-phase", "negative-create-generation", "negative-create-fence", "replayed-work", "replayed-validation", "old-validation-fence"} {
		t.Run(kind, func(t *testing.T) {
			c, server := newPrepareViewServer(t)
			created := server.response["create"].(FixedViewState)
			stage, wantWork, wantCalls := "create", 0, 1
			switch kind {
			case "wrong-create-layer":
				created.LayerID = "other"
			case "closed-create":
				created.AdmissionClosed = true
			case "wrong-create-phase":
				created.Phase = "READY"
			case "negative-create-generation":
				created.Generation = -1
			case "negative-create-fence":
				created.Fence = -1
			case "replayed-work":
				server.claimed["work"] = true
				stage, wantCalls = "work", 2
			case "replayed-validation":
				server.claimed["validation"] = true
				stage, wantWork, wantCalls = "validation", 1, 4
			case "old-validation-fence":
				claim := server.response["validation"].(ViewWorkClaim)
				claim.Work.Fence = 1
				server.response["validation"] = claim
				stage, wantWork, wantCalls = "validation", 1, 4
			}
			server.response["create"] = created
			work := 0
			out, err := c.PrepareFixedView(context.Background(), prepareViewRequest(), func(context.Context, *Client, ViewWork) error { work++; return nil }, func(context.Context, *Client, ViewWork) error { t.Error("invalid validation grant"); return nil })
			var operation *ViewOperationError
			if !errors.As(err, &operation) || operation.OperationID != stage || out.Stage != stage || out.Validated || work != wantWork || len(server.requests()) != wantCalls {
				t.Fatalf("out=%+v err=%v work=%d calls=%v", out, err, work, server.requests())
			}
			if strings.HasPrefix(kind, "replayed-") && !errors.Is(err, ErrViewLaunchUnavailable) {
				t.Fatalf("replay error=%v", err)
			}
		})
	}
}

func TestPrepareFixedViewCancellationAfterClaimDoesNotRunCallback(t *testing.T) {
	for _, validation := range []bool{false, true} {
		t.Run(fmt.Sprint(validation), func(t *testing.T) {
			c, server := newPrepareViewServer(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base := c.httpClient.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			c.httpClient.Transport = viewControlRoundTripper(func(r *http.Request) (*http.Response, error) {
				resp, err := base.RoundTrip(r)
				if err != nil || !strings.HasSuffix(r.URL.Path, "/work") {
					return resp, err
				}
				body, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if readErr != nil {
					return nil, readErr
				}
				resp.Body = io.NopCloser(strings.NewReader(string(body)))
				var claim ViewWorkClaim
				if err := json.Unmarshal(body, &claim); err != nil {
					return nil, err
				}
				if claim.Work.Validation == validation {
					cancel()
				}
				return resp, nil
			})
			work := 0
			out, err := c.PrepareFixedView(ctx, prepareViewRequest(), func(context.Context, *Client, ViewWork) error { work++; return nil }, func(context.Context, *Client, ViewWork) error { t.Error("canceled validation ran"); return nil })
			wantWork, wantCalls, stage := 0, 2, "work"
			if validation {
				wantWork, wantCalls, stage = 1, 4, "validation"
			}
			if !errors.Is(err, context.Canceled) || out.Validated || out.Stage != stage || work != wantWork || len(server.requests()) != wantCalls {
				t.Fatalf("out=%+v err=%v work=%d calls=%v", out, err, work, server.requests())
			}
		})
	}
}

func TestPrepareFixedViewCallbackPanicCancelsContextWithoutCleanupOrPublish(t *testing.T) {
	c, server := newPrepareViewServer(t)
	var callbackCtx context.Context
	func() {
		defer func() {
			if got := recover(); got != "callback panic" {
				t.Errorf("panic=%v", got)
			}
		}()
		_, _ = c.PrepareFixedView(context.Background(), prepareViewRequest(), func(ctx context.Context, _ *Client, _ ViewWork) error {
			callbackCtx = ctx
			panic("callback panic")
		}, func(context.Context, *Client, ViewWork) error { t.Error("unexpected validation"); return nil })
	}()
	if callbackCtx == nil || callbackCtx.Err() != context.Canceled || !reflect.DeepEqual(server.requests(), []string{"create", "work"}) {
		t.Fatalf("context=%v requests=%v", callbackCtx, server.requests())
	}
}
