package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestBatchWriteCreateModeAndCreateOnlyCAS(t *testing.T) {
	for _, mode := range []uint32{0o755, 0} {
		t.Run(fmt.Sprintf("%o", mode), func(t *testing.T) {
			s := newTestServer(t)
			ts := httptest.NewServer(s)
			t.Cleanup(ts.Close)
			c := client.New(ts.URL, "")
			// Literal wire fields avoid validating a shared client/server tag
			// mistake by encoding the request with either implementation's type.
			payload := fmt.Sprintf(`{"items":[{"path":"/mode.txt","data":"aGVsbG8=","expected_revision":0,"mode":%d,"hasMode":true}]}`, mode)
			for attempt, wantStatus := range []int{http.StatusOK, http.StatusConflict} {
				resp, err := http.Post(ts.URL+"/v1/fs:batch-write", "application/json", strings.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				var out batchWriteResponse
				err = json.NewDecoder(resp.Body).Decode(&out)
				_ = resp.Body.Close()
				if err != nil || resp.StatusCode != http.StatusOK || len(out.Results) != 1 || out.Results[0].Status != wantStatus {
					t.Fatalf("attempt %d: HTTP=%d results=%+v err=%v", attempt, resp.StatusCode, out.Results, err)
				}
				if attempt == 0 && out.Results[0].Revision != 1 {
					t.Fatalf("committed revision=%d", out.Results[0].Revision)
				}
			}
			stat, err := c.StatCtx(context.Background(), "/mode.txt")
			if err != nil {
				t.Fatal(err)
			}
			if !stat.HasMode || stat.Mode&0o777 != mode || stat.Revision != 1 || stat.Size != 5 {
				t.Fatalf("committed mode/content metadata: %+v", stat)
			}
			resp, err := http.Get(ts.URL + "/v1/fs/mode.txt")
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || string(body) != "hello" {
				t.Fatalf("committed bytes=%q HTTP=%d err=%v", body, resp.StatusCode, err)
			}
		})
	}
}

func TestBatchWriteScopedModeDeniedBeforeMutation(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "")
	ctx := context.Background()
	seed, err := c.BatchWriteCtx(ctx, []client.BatchWriteItem{{
		Path: "/existing.txt", Data: []byte("original"), ExpectedRevision: 0, Mode: 0o600, HasMode: true,
	}})
	if err != nil || len(seed) != 1 || !seed[0].OK() {
		t.Fatalf("seed=%+v err=%v", seed, err)
	}
	scope := &TenantScope{
		IsScoped: true, Backend: s.fallback,
		FSScopes: []FSScope{{Prefix: "/", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/fs:batch-write", strings.NewReader(`{"items":[
		{"path":"/existing.txt","data":"Y2hhbmdlZA==","expected_revision":1,"mode":511,"hasMode":true},
		{"path":"/new-mode.txt","data":"bmV3","expected_revision":0,"mode":493,"hasMode":true},
		{"path":"/plain.txt","data":"cGxhaW4=","expected_revision":0}
	]}`))
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	s.handleBatchWrite(w, r)
	var out batchWriteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != http.StatusOK || len(out.Results) != 3 {
		t.Fatalf("batch response HTTP=%d body=%s err=%v", w.Code, w.Body.String(), err)
	}
	for i, want := range []int{http.StatusForbidden, http.StatusForbidden, http.StatusOK} {
		if out.Results[i].Status != want {
			t.Fatalf("result %d=%+v, want status %d", i, out.Results[i], want)
		}
	}
	stat, err := c.StatCtx(ctx, "/existing.txt")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode&0o777 != 0o600 || stat.Revision != 1 || stat.Size != 8 {
		t.Fatalf("forbidden item mutated original: %+v", stat)
	}
	if _, err := c.StatCtx(ctx, "/new-mode.txt"); !client.IsNotFound(err) {
		t.Fatalf("forbidden create exists: %v", err)
	}
	stat, err = c.StatCtx(ctx, "/plain.txt")
	if err != nil || stat.Size != 5 {
		t.Fatalf("ordinary scoped content write: stat=%+v err=%v", stat, err)
	}
}
