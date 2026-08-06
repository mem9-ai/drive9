//go:build !integration

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMigrationStatChecksumCompatibility(t *testing.T) {
	emptyChecksum := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	for _, tc := range []struct {
		name       string
		header     string
		want       string
		wantAbsent bool
	}{
		{name: "persisted empty file checksum", header: emptyChecksum, want: emptyChecksum},
		{name: "old server omits checksum", wantAbsent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/file" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("X-Dat9-IsDir", "false")
				if tc.header != "" {
					w.Header().Set("X-Dat9-Checksum-SHA256", tc.header)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			stat, err := New(srv.URL, "").StatCtx(context.Background(), "/file")
			if err != nil {
				t.Fatal(err)
			}
			if stat.ChecksumSHA256 != tc.want {
				t.Fatalf("checksum = %q, want %q", stat.ChecksumSHA256, tc.want)
			}
			if tc.wantAbsent && stat.ChecksumSHA256 != "" {
				t.Fatalf("old-server checksum = %q, want unknown", stat.ChecksumSHA256)
			}
		})
	}
}

func TestMigrationBatchStatChecksumCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name         string
		include      bool
		wantField    bool
		response     string
		wantChecksum string
	}{
		{
			name:     "default request omits option",
			response: `{"results":[{"path":"/file","status":200}]}`,
		},
		{
			name:         "opt in requests and decodes checksum",
			include:      true,
			wantField:    true,
			response:     `{"results":[{"path":"/file","status":200,"checksum_sha256":"abc"}]}`,
			wantChecksum: "abc",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				_, hasField := request["include_checksum"]
				if hasField != tc.wantField {
					t.Fatalf("include_checksum presence = %v, want %v", hasField, tc.wantField)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			}))
			defer srv.Close()

			client := New(srv.URL, "")
			var (
				results []BatchStatResult
				err     error
			)
			if tc.include {
				results, err = client.BatchStatWithOptionsCtx(
					context.Background(),
					[]string{"/file"},
					BatchStatOptions{IncludeChecksum: true},
				)
			} else {
				results, err = client.BatchStatCtx(context.Background(), []string{"/file"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 1 || results[0].ChecksumSHA256 != tc.wantChecksum {
				t.Fatalf("results = %+v, want checksum %q", results, tc.wantChecksum)
			}
		})
	}
}

func TestMigrationCapabilityPreflightWarmsTransferLimits(t *testing.T) {
	const (
		maxUpload       = int64(1048576)
		inlineThreshold = int64(1024)
	)
	payload := []byte("reader-only payload")
	checksum := strings.Repeat("a", 64)
	var statusHits atomic.Int32
	var putBody []byte

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		statusHits.Add(1)
		_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":1024,"migration_capabilities":{"checksum_read":true,"checksum_complete":true,"conditional_create":true,"conditional_update":true,"event_ingest":true}}`))
	})
	mux.HandleFunc("/v1/fs/small.bin", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "0" {
				t.Errorf("expected revision header = %q, want 0", got)
			}
			putBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, "")
	caps, err := client.GetMigrationCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !caps.ChecksumRead || !caps.ChecksumComplete || !caps.ConditionalCreate || !caps.ConditionalUpdate || !caps.EventIngest {
		t.Fatalf("capabilities = %+v", caps)
	}
	if client.statusMax.Load() != maxUpload || client.CachedSmallFileThreshold() != inlineThreshold {
		t.Fatalf("cached max=%d inline=%d", client.statusMax.Load(), client.CachedSmallFileThreshold())
	}

	readerOnly := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
	stat, err := client.WriteStreamConditionalWithChecksum(
		context.Background(), "/small.bin", readerOnly, int64(len(payload)), nil, 0, checksum,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(putBody, payload) {
		t.Fatalf("uploaded body = %q, want %q", putBody, payload)
	}
	if stat.Revision != 1 || stat.ChecksumSHA256 != checksum {
		t.Fatalf("post-write stat = %+v", stat)
	}
	if statusHits.Load() != 1 {
		t.Fatalf("status hits = %d, want 1", statusHits.Load())
	}
}

func TestMigrationCapabilityOldServerIsRecognizablyUnsupported(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"status":"active","max_upload_bytes":10,"inline_threshold":5}`))
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	for range 2 {
		_, err := client.GetMigrationCapabilities(context.Background())
		if !errors.Is(err, ErrMigrationUnsupported) {
			t.Fatalf("error = %v, want ErrMigrationUnsupported", err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("successful old-server status hits = %d, want cached 1", hits.Load())
	}
}

func TestMigrationCapabilityStatusErrorsAreTypedAndRetryable(t *testing.T) {
	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusServiceUnavailable,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if hits.Add(1) == 1 {
					http.Error(w, "temporary", status)
					return
				}
				_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`))
			}))
			defer srv.Close()

			client := New(srv.URL, "")
			_, err := client.GetMigrationCapabilities(context.Background())
			var statusErr *StatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != status {
				t.Fatalf("error = %T %v, want StatusError %d", err, err, status)
			}
			if client.CachedSmallFileThreshold() != 0 {
				t.Fatal("failed response populated transfer cache")
			}
			caps, err := client.GetMigrationCapabilities(context.Background())
			if err != nil || !caps.ChecksumRead || client.CachedSmallFileThreshold() != 7 {
				t.Fatalf("retry caps=%+v threshold=%d err=%v", caps, client.CachedSmallFileThreshold(), err)
			}
		})
	}
}

func TestMigrationCapabilityNetworkContextAndJSONErrorsRemainDistinct(t *testing.T) {
	t.Run("network", func(t *testing.T) {
		networkErr := errors.New("network unavailable")
		var calls atomic.Int32
		client := New("http://migration.invalid", "")
		client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return nil, networkErr
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`,
				)),
			}, nil
		})}

		if _, err := client.GetMigrationCapabilities(context.Background()); !errors.Is(err, networkErr) {
			t.Fatalf("network error = %v", err)
		}
		caps, err := client.GetMigrationCapabilities(context.Background())
		if err != nil || !caps.ChecksumRead {
			t.Fatalf("network retry caps=%+v err=%v", caps, err)
		}
	})

	for _, tc := range []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		want    error
	}{
		{
			name: "canceled context",
			context: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			want: context.Canceled,
		},
		{
			name: "expired deadline",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			want: context.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.context()
			defer cancel()
			client := New("http://migration.invalid", "")
			if _, err := client.GetMigrationCapabilities(ctx); !errors.Is(err, tc.want) {
				t.Fatalf("context error = %v, want %v", err, tc.want)
			}
		})
	}

	t.Run("malformed JSON", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"migration_capabilities":!}`))
				return
			}
			_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`))
		}))
		defer srv.Close()

		client := New(srv.URL, "")
		_, err := client.GetMigrationCapabilities(context.Background())
		var syntaxErr *json.SyntaxError
		if !errors.As(err, &syntaxErr) {
			t.Fatalf("JSON error = %T %v", err, err)
		}
		caps, err := client.GetMigrationCapabilities(context.Background())
		if err != nil || !caps.ChecksumRead {
			t.Fatalf("JSON retry caps=%+v err=%v", caps, err)
		}
	})

	t.Run("trailing JSON", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}} {}`))
				return
			}
			_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`))
		}))
		defer srv.Close()

		client := New(srv.URL, "")
		if _, err := client.GetMigrationCapabilities(context.Background()); err == nil {
			t.Fatal("trailing JSON was accepted")
		}
		caps, err := client.GetMigrationCapabilities(context.Background())
		if err != nil || !caps.ChecksumRead || hits.Load() != 2 {
			t.Fatalf("trailing JSON retry caps=%+v hits=%d err=%v", caps, hits.Load(), err)
		}
	})
}

func TestMigrationCapabilityIncompleteLimitsDoNotPopulateCache(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"migration_capabilities":{"checksum_read":true}}`))
			return
		}
		_, _ = w.Write([]byte(`{"max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`))
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	if _, err := client.GetMigrationCapabilities(context.Background()); err == nil {
		t.Fatal("preflight without transfer limits succeeded")
	}
	if client.statusMax.Load() != 0 || client.CachedSmallFileThreshold() != 0 {
		t.Fatalf("incomplete response populated limits: max=%d inline=%d", client.statusMax.Load(), client.CachedSmallFileThreshold())
	}
	caps, err := client.GetMigrationCapabilities(context.Background())
	if err != nil || !caps.ChecksumRead || hits.Load() != 2 {
		t.Fatalf("incomplete status retry caps=%+v hits=%d err=%v", caps, hits.Load(), err)
	}
	if client.statusMax.Load() != 1048576 || client.CachedSmallFileThreshold() != 7 {
		t.Fatalf("successful preflight limits: max=%d inline=%d", client.statusMax.Load(), client.CachedSmallFileThreshold())
	}
}

func TestMigrationChecksumUploadPrefersV2AndReturnsCommittedStat(t *testing.T) {
	checksum := strings.Repeat("b", 64)
	for _, expectedRevision := range []int64{0, 9} {
		t.Run(fmt.Sprintf("revision_%d", expectedRevision), func(t *testing.T) {
			var sawV1 atomic.Bool
			var gotExpected *int64
			var completeChecksum string
			var headHits atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
					var req struct {
						ExpectedRevision *int64 `json:"expected_revision"`
					}
					_ = json.NewDecoder(r.Body).Decode(&req)
					gotExpected = req.ExpectedRevision
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "v2", PartSize: 8, TotalParts: 1})
				case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2/presign-batch":
					_ = json.NewEncoder(w).Encode(struct {
						Parts []presignedPart `json:"parts"`
					}{Parts: []presignedPart{{Number: 1, URL: fmt.Sprintf("http://%s/v2/part/1", r.Host), Size: 8, ExpiresAt: time.Now().Add(time.Minute)}}})
				case r.Method == http.MethodPut && r.URL.Path == "/v2/part/1":
					w.Header().Set("ETag", `"etag"`)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2/complete":
					var req struct {
						Checksum string `json:"checksum_sha256"`
					}
					_ = json.NewDecoder(r.Body).Decode(&req)
					completeChecksum = req.Checksum
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file":
					headHits.Add(1)
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "10")
					w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
					w.WriteHeader(http.StatusOK)
				case r.URL.Path == "/v1/uploads/initiate":
					sawV1.Store(true)
					http.Error(w, "unexpected V1", http.StatusInternalServerError)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			client := New(srv.URL, "")
			client.SetSmallFileThresholdForTests(1)
			stat, err := client.WriteStreamConditionalWithChecksum(
				context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, expectedRevision, checksum,
			)
			if err != nil {
				t.Fatal(err)
			}
			if gotExpected == nil || *gotExpected != expectedRevision {
				t.Fatalf("expected_revision = %v, want %d", gotExpected, expectedRevision)
			}
			if completeChecksum != checksum || stat.Revision != 10 || stat.ChecksumSHA256 != checksum || headHits.Load() != 1 {
				t.Fatalf("complete checksum=%q stat=%+v headHits=%d", completeChecksum, stat, headHits.Load())
			}
			if sawV1.Load() {
				t.Fatal("V2 success unexpectedly fell back to V1")
			}
		})
	}
}

func TestMigrationChecksumUploadRejectsMalformedChecksumWithoutLeaks(t *testing.T) {
	const (
		apiKey  = "owner-secret-key"
		content = "sensitive-file-content"
	)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := New(srv.URL, apiKey)
	client.SetSmallFileThresholdForTests(1)
	for _, checksum := range []string{"bad", strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		_, err := client.WriteStreamConditionalWithChecksum(
			context.Background(), "/file", bytes.NewReader([]byte(content)), int64(len(content)), nil, 0, checksum,
		)
		if err == nil || !strings.Contains(err.Error(), "checksum_sha256") {
			t.Fatalf("checksum %q error = %v", checksum, err)
		}
		if strings.Contains(err.Error(), apiKey) || strings.Contains(err.Error(), content) {
			t.Fatalf("validation error leaked credential or content: %v", err)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("malformed checksum issued %d requests", requests.Load())
	}
}

func TestMigrationChecksumUploadRejectsUnconditionalRevisionBeforeRequests(t *testing.T) {
	for _, threshold := range []int64{1, 1024} {
		t.Run(fmt.Sprintf("threshold_%d", threshold), func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer srv.Close()

			client := New(srv.URL, "")
			client.SetSmallFileThresholdForTests(threshold)
			_, err := client.WriteStreamConditionalWithChecksum(
				context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, -1, strings.Repeat("a", 64),
			)
			if err == nil || !strings.Contains(err.Error(), "expected revision") {
				t.Fatalf("error = %v, want invalid expected revision", err)
			}
			if requests.Load() != 0 {
				t.Fatalf("unconditional revision issued %d requests", requests.Load())
			}
		})
	}
}

func TestMigrationChecksumUploadFallsBackOnlyWhenV2IsUnavailable(t *testing.T) {
	checksum := strings.Repeat("c", 64)
	var v1Expected *int64
	var v1Checksum string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			var req struct {
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			v1Expected = req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(UploadPlan{UploadID: "v1", PartSize: 8, Parts: []PartURL{{Number: 1, URL: fmt.Sprintf("http://%s/v1/part/1", r.Host), Size: 8}}})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/part/1":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/v1/complete":
			var req struct {
				Checksum string `json:"checksum_sha256"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			v1Checksum = req.Checksum
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file":
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	client.SetSmallFileThresholdForTests(1)
	stat, err := client.WriteStreamConditionalWithChecksum(
		context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, 17, checksum,
	)
	if err != nil {
		t.Fatal(err)
	}
	if v1Expected == nil || *v1Expected != 17 || v1Checksum != checksum || stat.Revision != 2 {
		t.Fatalf("expected=%v checksum=%q stat=%+v", v1Expected, v1Checksum, stat)
	}

	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
	} {
		t.Run("no fallback on "+http.StatusText(status), func(t *testing.T) {
			var v1Hits atomic.Int32
			errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/uploads/initiate" {
					http.Error(w, "v2 error", status)
					return
				}
				if r.URL.Path == "/v1/uploads/initiate" {
					v1Hits.Add(1)
				}
				http.NotFound(w, r)
			}))
			defer errorServer.Close()

			errorClient := New(errorServer.URL, "")
			errorClient.SetSmallFileThresholdForTests(1)
			_, err := errorClient.WriteStreamConditionalWithChecksum(
				context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, 0, checksum,
			)
			var statusErr *StatusError
			if !errors.As(err, &statusErr) || statusErr.StatusCode != status {
				t.Fatalf("error = %T %v, want StatusError %d", err, err, status)
			}
			if v1Hits.Load() != 0 {
				t.Fatal("non-availability V2 error fell back to V1")
			}
		})
	}
}

func TestMigrationChecksumUploadRetryStartsFreshAtPartOne(t *testing.T) {
	checksum := strings.Repeat("d", 64)
	var initiateHits atomic.Int32
	var abortHits atomic.Int32
	var mu sync.Mutex
	partsByUpload := map[string]map[int]int{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			id := fmt.Sprintf("upload-%d", initiateHits.Add(1))
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: id, PartSize: 4, TotalParts: 2})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/presign-batch"):
			id := strings.Split(r.URL.Path, "/")[3]
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{Parts: []presignedPart{
				{Number: 1, URL: fmt.Sprintf("http://%s/%s/part/1", r.Host, id), Size: 4, ExpiresAt: time.Now().Add(time.Minute)},
				{Number: 2, URL: fmt.Sprintf("http://%s/%s/part/2", r.Host, id), Size: 4, ExpiresAt: time.Now().Add(time.Minute)},
			}})
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/part/"):
			segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
			var part int
			_, _ = fmt.Sscanf(segments[2], "%d", &part)
			mu.Lock()
			if partsByUpload[segments[0]] == nil {
				partsByUpload[segments[0]] = map[int]int{}
			}
			partsByUpload[segments[0]][part]++
			mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, part))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/upload-1/complete":
			http.Error(w, "complete failed", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/upload-1/abort":
			abortHits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/upload-2/complete":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file":
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
			w.WriteHeader(http.StatusOK)
		case strings.HasPrefix(r.URL.Path, "/v1/uploads"):
			t.Errorf("Migration attempted V1 query/resume path %s", r.URL.Path)
			http.Error(w, "resume forbidden", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	client.SetSmallFileThresholdForTests(1)
	data := []byte("12345678")
	if _, err := client.WriteStreamConditionalWithChecksum(context.Background(), "/file", bytes.NewReader(data), 8, nil, 0, checksum); err == nil {
		t.Fatal("first Complete unexpectedly succeeded")
	}
	if _, err := client.WriteStreamConditionalWithChecksum(context.Background(), "/file", bytes.NewReader(data), 8, nil, 0, checksum); err != nil {
		t.Fatalf("fresh retry: %v", err)
	}
	if initiateHits.Load() != 2 || abortHits.Load() != 1 {
		t.Fatalf("initiate=%d abort=%d", initiateHits.Load(), abortHits.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range []string{"upload-1", "upload-2"} {
		if partsByUpload[id][1] != 1 || partsByUpload[id][2] != 1 {
			t.Fatalf("%s parts = %+v, want fresh parts 1 and 2", id, partsByUpload[id])
		}
	}
}

func TestMigrationChecksumUploadV1RetryStartsFreshAtPartOne(t *testing.T) {
	checksum := strings.Repeat("e", 64)
	var initiateHits atomic.Int32
	var mu sync.Mutex
	partsByUpload := map[string]map[int]int{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			id := fmt.Sprintf("legacy-%d", initiateHits.Add(1))
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(UploadPlan{
				UploadID: id,
				PartSize: 4,
				Parts: []PartURL{
					{Number: 1, URL: fmt.Sprintf("http://%s/%s/part/1", r.Host, id), Size: 4},
					{Number: 2, URL: fmt.Sprintf("http://%s/%s/part/2", r.Host, id), Size: 4},
				},
			})
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/part/"):
			segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
			var part int
			_, _ = fmt.Sscanf(segments[2], "%d", &part)
			mu.Lock()
			if partsByUpload[segments[0]] == nil {
				partsByUpload[segments[0]] = map[int]int{}
			}
			partsByUpload[segments[0]][part]++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-1/complete":
			http.Error(w, "complete failed", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/legacy-2/complete":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/file":
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "resume"):
			t.Errorf("Migration attempted Resume path %s", r.URL.Path)
			http.Error(w, "resume forbidden", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	client.SetSmallFileThresholdForTests(1)
	data := []byte("12345678")
	if _, err := client.WriteStreamConditionalWithChecksum(context.Background(), "/file", bytes.NewReader(data), 8, nil, 0, checksum); err == nil {
		t.Fatal("first V1 Complete unexpectedly succeeded")
	}
	if _, err := client.WriteStreamConditionalWithChecksum(context.Background(), "/file", bytes.NewReader(data), 8, nil, 0, checksum); err != nil {
		t.Fatalf("fresh V1 retry: %v", err)
	}
	if initiateHits.Load() != 2 {
		t.Fatalf("V1 initiate=%d, want 2", initiateHits.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range []string{"legacy-1", "legacy-2"} {
		if partsByUpload[id][1] != 1 || partsByUpload[id][2] != 1 {
			t.Fatalf("%s parts = %+v, want fresh parts 1 and 2", id, partsByUpload[id])
		}
	}
}

func TestMigrationEventUsesClosedSourceVersionTokenDTO(t *testing.T) {
	event := MigrationEvent{
		EventID: "evt-1", EmittedAt: "2026-08-04T12:00:00Z", Phase: "DUAL_WRITE_REPAIRING", RoundID: "round-1", CASAttempt: 1,
		FirstSeenAt: "2026-08-04T11:59:00Z", GraceSeconds: 60, JobID: "job-1", VolumeID: "vol-1", NodeName: "node-1", PodName: "pod-1",
		TenantID: "tenant-1", SpaceID: "space-1", SourcePath: "/ebs/file", TargetPath: "/drive9/file", SourceVersionToken: "inode:1:2",
		Size: 7, Mtime: 123, SourceChecksumSHA256: strings.Repeat("a", 64), ResourceID: "resource-1", Revision: 2,
		Drive9ChecksumSHA256: strings.Repeat("b", 64), ExpectedRevision: 1, Operation: "update", Result: "success", ErrorClass: "none", LatencyMS: 12,
	}
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/migration/events" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer owner-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := New(srv.URL, "owner-key").PostMigrationEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if got["source_version_token"] != "inode:1:2" {
		t.Fatalf("source_version_token = %v", got["source_version_token"])
	}
	if _, exists := got["source_version"]; exists {
		t.Fatal("legacy source_version was sent")
	}
	if len(got) != 27 {
		t.Fatalf("event field count = %d, want closed 27-field DTO", len(got))
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "owner-key") {
		t.Fatal("event payload leaked API key")
	}
}

func TestMigrationEventErrorIsTypedAndDoesNotLeakCredential(t *testing.T) {
	const apiKey = "owner-secret-key"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "endpoint unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	err := New(srv.URL, apiKey).PostMigrationEvent(context.Background(), MigrationEvent{})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %T %v", err, err)
	}
	if strings.Contains(err.Error(), apiKey) {
		t.Fatal("event error leaked API key")
	}
}
