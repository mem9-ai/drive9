package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPrepareExtentKeepsDuplicateFileOff(t *testing.T) {
	t.Parallel()
	var got []int64
	putOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(putOK.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "prepare-blocks=1") {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Ranges []struct {
				FileOff int64 `json:"file_off"`
				Len     int64 `json:"len"`
			} `json:"ranges"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err)
			http.Error(w, "bad", 400)
			return
		}
		blocks := make([]map[string]any, 0, len(body.Ranges))
		for i, rng := range body.Ranges {
			got = append(got, rng.FileOff)
			blocks = append(blocks, map[string]any{
				"file_off":  rng.FileOff,
				"len":       rng.Len,
				"block_key": "blocks/" + strings.Repeat("x", i+1),
				"put_url":   putOK.URL,
				"headers":   map[string]string{},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	landed, err := c.prepareExtent(context.Background(), "/dup.bin", []ExtentPayload{
		{FileOff: 0, Data: []byte("aaaa")},
		{FileOff: 0, Data: []byte("bbbb")},
	})
	if err != nil {
		t.Fatalf("prepareExtent: %v", err)
	}
	if len(landed) != 2 {
		t.Fatalf("landed=%d want 2", len(landed))
	}
	if string(landed[0].data) != "aaaa" || string(landed[1].data) != "bbbb" {
		t.Fatalf("data=%q %q", landed[0].data, landed[1].data)
	}
	if len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("prepare ranges=%v", got)
	}
}

func TestFetchExtentPlanEmptyPartsReturnsZeros(t *testing.T) {
	t.Parallel()
	c := New("http://127.0.0.1:1", "")
	got, err := c.FetchExtentPlan(context.Background(), &ExtentReadPlan{SizeBytes: 16, Parts: nil}, 4, 8)
	if err != nil {
		t.Fatalf("FetchExtentPlan: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("len=%d want 8 (hole window, not short read)", len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("got[%d]=%d want 0", i, b)
		}
	}
}

func TestSplitExtentRange(t *testing.T) {
	t.Parallel()
	parts := SplitExtentRange(0, ExtentMaxBlockSize+1)
	if len(parts) != 2 {
		t.Fatalf("parts=%d want 2", len(parts))
	}
	if parts[0].Len != ExtentMaxBlockSize || parts[1].FileOff != ExtentMaxBlockSize || parts[1].Len != 1 {
		t.Fatalf("parts=%+v", parts)
	}
	chunkParts := SplitExtentRange(ExtentChunkSize-1, 2)
	if len(chunkParts) != 2 {
		t.Fatalf("chunk split parts=%d want 2", len(chunkParts))
	}
}

func TestFlushExtentRetriesCASConflict(t *testing.T) {
	t.Parallel()
	var commits atomic.Int32
	putOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(putOK.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.RawQuery, "prepare-blocks=1"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off":  0,
					"len":       4,
					"block_key": "blocks/inode/1",
					"put_url":   putOK.URL,
					"headers":   map[string]string{},
				}},
			})
		case strings.Contains(r.URL.RawQuery, "commit-slices=1"):
			n := commits.Add(1)
			if n == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": "revision conflict", "code": "cas_conflict",
					"revision": 3, "generation": 2, "size_bytes": 0,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(CommitSlicesResult{Revision: 4, Generation: 3, SizeBytes: 4})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	result, err := c.FlushExtent(context.Background(), FlushExtentRequest{
		Path:             "/app.db-wal",
		ExpectedRevision: 1,
		Payloads:         []ExtentPayload{{FileOff: 0, Data: []byte("abcd")}},
	})
	if err != nil {
		t.Fatalf("FlushExtent: %v", err)
	}
	if result.Revision != 4 || commits.Load() != 2 {
		t.Fatalf("result=%+v commits=%d", result, commits.Load())
	}
}

func TestFlushExtentCASRetryAfterLandedDropsTruncateTo(t *testing.T) {
	t.Parallel()
	var commits atomic.Int32
	var landed []int
	putOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(putOK.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.RawQuery, "prepare-blocks=1"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off":  0,
					"len":       4,
					"block_key": "blocks/inode/1",
					"put_url":   putOK.URL,
					"headers":   map[string]string{},
				}},
			})
		case strings.Contains(r.URL.RawQuery, "commit-slices=1"):
			var body struct {
				TruncateTo *int64 `json:"truncate_to"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			n := commits.Add(1)
			if n == 1 {
				if body.TruncateTo == nil || *body.TruncateTo != 4 {
					t.Errorf("first commit truncate_to=%v want 4", body.TruncateTo)
				}
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": "revision conflict", "code": "cas_conflict",
					"revision": 5, "generation": 2, "size_bytes": 16,
				})
				return
			}
			if body.TruncateTo != nil {
				t.Errorf("retry must drop truncate_to, got %v", *body.TruncateTo)
			}
			_ = json.NewEncoder(w).Encode(CommitSlicesResult{Revision: 6, Generation: 3, SizeBytes: 16})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	to := int64(4)
	result, err := c.FlushExtent(context.Background(), FlushExtentRequest{
		Path:             "/wal",
		ExpectedRevision: 1,
		Payloads:         []ExtentPayload{{FileOff: 0, Data: []byte("abcd")}},
		TruncateTo:       &to,
		AfterLanded: func(_ string, _ []SliceOp, _, _ int64, truncateTo *int64) error {
			if truncateTo == nil {
				landed = append(landed, -1)
			} else {
				landed = append(landed, int(*truncateTo))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("FlushExtent: %v", err)
	}
	if result.Revision != 6 || commits.Load() != 2 {
		t.Fatalf("result=%+v commits=%d", result, commits.Load())
	}
	if len(landed) < 2 || landed[0] != 4 || landed[len(landed)-1] != -1 {
		t.Fatalf("AfterLanded truncate_to sequence=%v want 4 then nil", landed)
	}
}

func TestCommitSlicesRetryDropsGrowTruncateOnConflict(t *testing.T) {
	t.Parallel()
	var commits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "commit-slices=1") {
			http.NotFound(w, r)
			return
		}
		commits.Add(1)
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "revision conflict", "code": "cas_conflict",
			"revision": 4, "generation": 2, "size_bytes": 32,
		})
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	to := int64(16)
	result, err := c.CommitSlicesRetry(context.Background(), "/grow.bin", 1, 0, nil, &to, false, false)
	if err != nil {
		t.Fatalf("CommitSlicesRetry: %v", err)
	}
	if result.SizeBytes != 32 || result.Revision != 4 {
		t.Fatalf("result=%+v want winner size 32", result)
	}
	if commits.Load() != 1 {
		t.Fatalf("commits=%d want 1 (no shrink retry)", commits.Load())
	}
}

func TestCommitSlicesRetryKeepsGrowTruncateWhenWinnerSmaller(t *testing.T) {
	t.Parallel()
	var commits atomic.Int32
	var secondHasTruncate atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "commit-slices=1") {
			http.NotFound(w, r)
			return
		}
		var body struct {
			TruncateTo *int64 `json:"truncate_to"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		n := commits.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "revision conflict", "code": "cas_conflict",
				"revision": 4, "generation": 2, "size_bytes": 8,
			})
			return
		}
		if body.TruncateTo != nil && *body.TruncateTo == 16 {
			secondHasTruncate.Store(true)
		}
		_ = json.NewEncoder(w).Encode(CommitSlicesResult{Revision: 5, Generation: 3, SizeBytes: 16})
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	to := int64(16)
	result, err := c.CommitSlicesRetry(context.Background(), "/grow.bin", 1, 0, nil, &to, false, false)
	if err != nil {
		t.Fatalf("CommitSlicesRetry: %v", err)
	}
	if result.SizeBytes != 16 || commits.Load() != 2 || !secondHasTruncate.Load() {
		t.Fatalf("result=%+v commits=%d kept truncate_to=%v", result, commits.Load(), secondHasTruncate.Load())
	}
}

func TestCommitSlicesRetryKeepsExactTruncate(t *testing.T) {
	t.Parallel()
	var commits atomic.Int32
	var sawTruncate atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "commit-slices=1") {
			http.NotFound(w, r)
			return
		}
		var body struct {
			TruncateTo *int64 `json:"truncate_to"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		n := commits.Add(1)
		if body.TruncateTo != nil && *body.TruncateTo == 16 {
			sawTruncate.Store(true)
		}
		if n == 1 {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "revision conflict", "code": "cas_conflict",
				"revision": 4, "generation": 2, "size_bytes": 32,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(CommitSlicesResult{Revision: 5, Generation: 3, SizeBytes: 16})
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	to := int64(16)
	result, err := c.CommitSlicesRetry(context.Background(), "/trunc.bin", 1, 0, nil, &to, false, true)
	if err != nil {
		t.Fatalf("CommitSlicesRetry: %v", err)
	}
	if result.SizeBytes != 16 || commits.Load() != 2 || !sawTruncate.Load() {
		t.Fatalf("result=%+v commits=%d sawTruncate=%v", result, commits.Load(), sawTruncate.Load())
	}
}

func TestFlushExtentRetriesPendingExpired(t *testing.T) {
	t.Parallel()
	var prepares atomic.Int32
	putOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(putOK.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.RawQuery, "prepare-blocks=1"):
			prepares.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off":  0,
					"len":       4,
					"block_key": "blocks/inode/1",
					"put_url":   putOK.URL,
					"headers":   map[string]string{},
				}},
			})
		case strings.Contains(r.URL.RawQuery, "commit-slices=1"):
			if prepares.Load() == 1 {
				w.WriteHeader(http.StatusGone)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "pending expired", "code": "pending_expired"})
				return
			}
			_ = json.NewEncoder(w).Encode(CommitSlicesResult{Revision: 2, Generation: 1, SizeBytes: 4})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, "")
	result, err := c.FlushExtent(context.Background(), FlushExtentRequest{
		Path:     "/events.jsonl",
		Payloads: []ExtentPayload{{FileOff: 0, Data: []byte("abcd")}},
	})
	if err != nil {
		t.Fatalf("FlushExtent: %v", err)
	}
	if result.Revision != 2 || prepares.Load() != 2 {
		t.Fatalf("result=%+v prepares=%d", result, prepares.Load())
	}
}

func TestFetchExtentPlanCopiesWindow(t *testing.T) {
	t.Parallel()
	payload := []byte("hello-extent-window")
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Range", "bytes 0-18/19")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(s3.Close)
	c := New("http://127.0.0.1:1", "")
	got, err := c.FetchExtentPlan(context.Background(), &ExtentReadPlan{
		SizeBytes: int64(len(payload)),
		Parts: []ExtentReadPart{{
			FileOff: 0, Len: int64(len(payload)), BlockKey: "blocks/x/y",
			GetURL: s3.URL, Headers: map[string]string{"Range": "bytes=0-18"},
		}},
	}, 6, 7)
	if err != nil {
		t.Fatalf("FetchExtentPlan: %v", err)
	}
	if !bytes.Equal(got, payload[6:13]) {
		t.Fatalf("got %q want %q", got, payload[6:13])
	}
}

func TestExtentPartPayloadSlicesFullObjectAtBlockOff(t *testing.T) {
	obj := []byte("AAAABBBBCCCC")
	got, err := ExtentPartPayload(ExtentReadPart{BlockKey: "blocks/c", BlockOff: 4, Len: 4}, obj)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "BBBB" {
		t.Fatalf("got %q want BBBB (JuiceFS ReadAt into the slice object)", got)
	}
	ranged, err := ExtentPartPayload(ExtentReadPart{BlockKey: "blocks/c", BlockOff: 4, Len: 4}, []byte("BBBB"))
	if err != nil {
		t.Fatal(err)
	}
	if string(ranged) != "BBBB" {
		t.Fatalf("ranged body %q want BBBB", ranged)
	}
}

func TestFetchExtentPlanUsesBlockOffWhenRangeIgnored(t *testing.T) {
	t.Parallel()
	obj := []byte("AAAABBBBCCCC")
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Range not honored
		_, _ = w.Write(obj)
	}))
	t.Cleanup(s3.Close)
	c := New("http://127.0.0.1:1", "")
	got, err := c.FetchExtentPlan(context.Background(), &ExtentReadPlan{
		SizeBytes: 12,
		Parts: []ExtentReadPart{{
			FileOff: 4, Len: 4, BlockKey: "blocks/c", BlockOff: 4,
			GetURL: s3.URL, Headers: map[string]string{"Range": "bytes=4-7"},
		}},
	}, 4, 4)
	if err != nil {
		t.Fatalf("FetchExtentPlan: %v", err)
	}
	if string(got) != "BBBB" {
		t.Fatalf("got %q want BBBB (must not serve object prefix as this page)", got)
	}
}
