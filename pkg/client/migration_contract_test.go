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

func TestMigrationTenantIdentityContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		want        string
		unsupported bool
		wantError   bool
	}{
		{name: "present", body: `{"tenant_id":"tenant-a","unknown":"ignored"}`, want: "tenant-a"},
		{name: "missing", body: `{"status":"active"}`, unsupported: true},
		{name: "empty", body: `{"tenant_id":""}`, unsupported: true},
		{name: "whitespace padded", body: `{"tenant_id":" tenant-a "}`, unsupported: true},
		{name: "malformed", body: `{"tenant_id":42}`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			got, err := New(srv.URL, "").GetMigrationTenantID(context.Background())
			switch {
			case tc.unsupported && !errors.Is(err, ErrMigrationUnsupported):
				t.Fatalf("error = %v, want ErrMigrationUnsupported", err)
			case tc.wantError && err == nil:
				t.Fatal("error = nil, want decode error")
			case !tc.unsupported && !tc.wantError && err != nil:
				t.Fatal(err)
			case err == nil && got != tc.want:
				t.Fatalf("tenant ID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMigrationTenantIdentitySharesStatusCacheWithCapabilities(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"tenant_id":"tenant-a","max_upload_bytes":1048576,"inline_threshold":7,"migration_capabilities":{"checksum_read":true}}`))
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	tenantID, err := client.GetMigrationTenantID(context.Background())
	if err != nil || tenantID != "tenant-a" {
		t.Fatalf("tenant ID = %q, error = %v", tenantID, err)
	}
	caps, err := client.GetMigrationCapabilities(context.Background())
	if err != nil || !caps.ChecksumRead || hits.Load() != 1 {
		t.Fatalf("capabilities = %+v, hits = %d, error = %v", caps, hits.Load(), err)
	}
}

func TestMigrationTenantIdentityStatusFailureRetries(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"tenant_id":"tenant-a"}`))
	}))
	defer srv.Close()

	client := New(srv.URL, "")
	if _, err := client.GetMigrationTenantID(context.Background()); err == nil {
		t.Fatal("status failure error = nil")
	}
	tenantID, err := client.GetMigrationTenantID(context.Background())
	if err != nil || tenantID != "tenant-a" || hits.Load() != 2 {
		t.Fatalf("tenant ID = %q, hits = %d, error = %v", tenantID, hits.Load(), err)
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

func TestMigrationPreCompleteCheckPreventsDirectV1AndV2Commit(t *testing.T) {
	checkErr := errors.New("source changed before commit")
	for _, mode := range []string{"direct", "v1", "v2"} {
		t.Run(mode, func(t *testing.T) {
			var partHits, commitHits, abortHits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/file":
					commitHits.Add(1)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
					if mode == "v1" {
						http.NotFound(w, r)
						return
					}
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "v2", PartSize: 8, TotalParts: 1})
				case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/v2/presign-batch":
					_ = json.NewEncoder(w).Encode(struct {
						Parts []presignedPart `json:"parts"`
					}{Parts: []presignedPart{{Number: 1, URL: fmt.Sprintf("http://%s/part", r.Host), Size: 8, ExpiresAt: time.Now().Add(time.Minute)}}})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(UploadPlan{UploadID: "v1", PartSize: 8, Parts: []PartURL{{Number: 1, URL: fmt.Sprintf("http://%s/part", r.Host), Size: 8}}})
				case r.Method == http.MethodPut && r.URL.Path == "/part":
					_, _ = io.Copy(io.Discard, r.Body)
					partHits.Add(1)
					w.Header().Set("ETag", `"etag"`)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
					commitHits.Add(1)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/abort"):
					abortHits.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			api := New(srv.URL, "")
			if mode == "direct" {
				api.SetSmallFileThresholdForTests(1024)
			} else {
				api.SetSmallFileThresholdForTests(1)
			}
			checkHits := 0
			_, err := api.WriteStreamConditionalWithChecksumAndPreCompleteCheck(
				context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, 0, strings.Repeat("a", 64),
				func() error {
					checkHits++
					if mode != "direct" && partHits.Load() != 1 {
						t.Errorf("pre-Complete check ran before part upload: parts=%d", partHits.Load())
					}
					return checkErr
				},
			)
			if !errors.Is(err, checkErr) {
				t.Fatalf("error=%v, want pre-Complete check error", err)
			}
			if IsCommitAttempted(err) {
				t.Fatalf("pre-Complete error was marked as a commit attempt: %v", err)
			}
			if checkHits != 1 || commitHits.Load() != 0 {
				t.Fatalf("checks=%d commits=%d", checkHits, commitHits.Load())
			}
			wantAbort := int32(0)
			if mode == "v2" {
				wantAbort = 1
			}
			if abortHits.Load() != wantAbort {
				t.Fatalf("abort hits=%d, want %d", abortHits.Load(), wantAbort)
			}
		})
	}
}

func TestMigrationPreCompleteCheckIsRequiredBeforeRequests(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer srv.Close()

	api := New(srv.URL, "")
	api.SetSmallFileThresholdForTests(1)
	_, err := api.WriteStreamConditionalWithChecksumAndPreCompleteCheck(
		context.Background(), "/file", bytes.NewReader([]byte("12345678")), 8, nil, 0, strings.Repeat("a", 64), nil,
	)
	if err == nil || !strings.Contains(err.Error(), "pre-Complete check is required") || requests.Load() != 0 {
		t.Fatalf("error=%v requests=%d", err, requests.Load())
	}
}

func TestMigrationV2HungAbortIsBoundedAndPreservesCompleteError(t *testing.T) {
	abortStarted := make(chan struct{})
	var abortOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "hung", PartSize: 8, TotalParts: 1})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/hung/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{Parts: []presignedPart{{Number: 1, URL: fmt.Sprintf("http://%s/part", r.Host), Size: 8, ExpiresAt: time.Now().Add(time.Minute)}}})
		case r.Method == http.MethodPut && r.URL.Path == "/part":
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/hung/complete":
			http.Error(w, "original complete failure", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/hung/abort":
			abortOnce.Do(func() { close(abortStarted) })
			select {
			case <-r.Context().Done():
			case <-time.After(300 * time.Millisecond):
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	api := New(srv.URL, "")
	api.SetSmallFileThresholdForTests(1)
	api.multipartAbortTimeout = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := api.WriteStreamConditionalWithChecksum(
			ctx, "/file", bytes.NewReader([]byte("12345678")), 8, nil, 0, strings.Repeat("b", 64),
		)
		result <- err
	}()

	select {
	case <-abortStarted:
	case <-time.After(time.Second):
		t.Fatal("abort did not start")
	}
	cancel()
	select {
	case err := <-result:
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable || !strings.Contains(err.Error(), "original complete failure") {
			t.Fatalf("error=%T %v, want original 503", err, err)
		}
		if !IsCommitAttempted(err) {
			t.Fatalf("Complete error was not marked as a commit attempt: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("bounded abort did not return")
	}
}

func TestMultipartAbortContextIsIndependentAndBounded(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cleanup, cancelCleanup := newMultipartAbortContext(parent, time.Second)
	defer cancelCleanup()
	deadline, bounded := cleanup.Deadline()
	if !bounded || time.Until(deadline) <= 0 || time.Until(deadline) > time.Second {
		t.Fatalf("cleanup deadline=%v bounded=%v", deadline, bounded)
	}
	cancelParent()
	if !errors.Is(parent.Err(), context.Canceled) || cleanup.Err() != nil {
		t.Fatalf("parent error=%v cleanup error=%v", parent.Err(), cleanup.Err())
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
		SpaceID: "space-1", SourcePath: "/ebs/file", TargetPath: "/drive9/file", SourceVersionToken: "inode:1:2",
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
	if _, exists := got["tenant_id"]; exists {
		t.Fatal("tenant_id was sent instead of being derived from Owner authentication")
	}
	if len(got) != 26 {
		t.Fatalf("event field count = %d, want closed 26-field DTO", len(got))
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

func TestMigrationManifestPageContract(t *testing.T) {
	const apiKey = "owner-key"
	checksum := strings.Repeat("a", 64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/migration/manifest" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		query := r.URL.Query()
		if query.Get("prefix") != "/" || query.Get("cursor") != "cursor-0" || query.Get("limit") != "2" {
			t.Fatalf("query = %v", query)
		}
		_, _ = io.WriteString(w, `{
			"entries":[
				{"path":"/a.txt","type":"regular","metadata_complete":true,"identity_kind":"inode","mode":420,"size":7,"checksum_sha256":"`+checksum+`","revision":3,"resource_id":"inode-a","nlink":2},
				{"path":"/legacy/","type":"directory","metadata_complete":false,"identity_kind":"legacy_dentry","mode":null,"size":0,"checksum_sha256":null,"revision":null,"resource_id":"node-legacy","nlink":2}
			],
			"next_cursor":"cursor-1",
			"done":false
		}`)
	}))
	defer server.Close()

	page, err := New(server.URL, apiKey).ManifestPageCtx(context.Background(), "/", "cursor-0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if page.Done || page.NextCursor != "cursor-1" || len(page.Entries) != 2 || page.ResponseBytes == 0 {
		t.Fatalf("page = %+v", page)
	}
	regular := page.Entries[0]
	if regular.Type != ManifestEntryRegular || !regular.MetadataComplete || regular.IdentityKind != ManifestIdentityInode ||
		regular.Mode == nil || *regular.Mode != 0o644 || regular.ChecksumSHA256 == nil || *regular.ChecksumSHA256 != checksum ||
		regular.Revision == nil || *regular.Revision != 3 || regular.ResourceID != "inode-a" || regular.Nlink != 2 {
		t.Fatalf("regular = %+v", regular)
	}
	legacy := page.Entries[1]
	if legacy.Type != ManifestEntryDirectory || legacy.MetadataComplete || legacy.IdentityKind != ManifestIdentityLegacyDentry ||
		legacy.Mode != nil || legacy.ChecksumSHA256 != nil || legacy.Revision != nil || legacy.ResourceID != "node-legacy" {
		t.Fatalf("legacy = %+v", legacy)
	}
}

func TestMigrationManifestPageAcceptsTerminalNullCursor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"entries":[],"next_cursor":null,"done":true}`)
	}))
	defer server.Close()

	page, err := New(server.URL, "").ManifestPageCtx(context.Background(), "/", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Done || page.NextCursor != "" || len(page.Entries) != 0 {
		t.Fatalf("page = %+v", page)
	}
}

func TestMigrationManifestPageRejectsMalformedContracts(t *testing.T) {
	validEntry := `{"path":"/a.txt","type":"regular","metadata_complete":true,"identity_kind":"inode","mode":420,"size":1,"checksum_sha256":"` + strings.Repeat("a", 64) + `","revision":1,"resource_id":"inode-a","nlink":1}`
	for _, tc := range []struct {
		name   string
		body   string
		cursor string
		limit  int
	}{
		{name: "null entries", body: `{"entries":null,"next_cursor":"","done":true}`, limit: 1},
		{name: "missing done", body: `{"entries":[],"next_cursor":""}`, limit: 1},
		{name: "missing entry field", body: `{"entries":[{"path":"/a.txt","type":"regular","metadata_complete":true,"identity_kind":"inode","size":1,"checksum_sha256":null,"revision":1,"resource_id":"inode-a","nlink":1}],"next_cursor":"","done":true}`, limit: 1},
		{name: "invalid path", body: `{"entries":[{"path":"/a/../b","type":"regular","metadata_complete":false,"identity_kind":"inode","mode":420,"size":1,"checksum_sha256":null,"revision":1,"resource_id":"inode-a","nlink":1}],"next_cursor":"","done":true}`, limit: 1},
		{name: "non advancing cursor", body: `{"entries":[],"next_cursor":"same","done":false}`, cursor: "same", limit: 1},
		{name: "terminal cursor", body: `{"entries":[],"next_cursor":"unexpected","done":true}`, limit: 1},
		{name: "too many entries", body: `{"entries":[` + validEntry + `,` + validEntry + `],"next_cursor":"","done":true}`, limit: 1},
		{name: "trailing json", body: `{"entries":[],"next_cursor":"","done":true} {}`, limit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			if _, err := New(server.URL, "").ManifestPageCtx(context.Background(), "/", tc.cursor, tc.limit); err == nil {
				t.Fatal("malformed Manifest response accepted")
			}
		})
	}
}

func TestMigrationManifestPageRejectsInvalidRequestBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	c := New(server.URL, "")
	for _, tc := range []struct {
		prefix string
		limit  int
	}{
		{prefix: "relative", limit: 1},
		{prefix: "/not-a-dir", limit: 1},
		{prefix: "/", limit: MaxManifestPageEntries + 1},
		{prefix: "/", limit: -1},
	} {
		if _, err := c.ManifestPageCtx(context.Background(), tc.prefix, "", tc.limit); err == nil {
			t.Fatalf("request prefix=%q limit=%d accepted", tc.prefix, tc.limit)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid requests made %d network calls", calls.Load())
	}
}

func TestMigrationBatchMkdirContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs:batch-mkdir" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Items []BatchMkdirItem `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if len(request.Items) != 2 || request.Items[0].Path != "/a/" || request.Items[0].Mode != 0o755 || request.Items[1].Mode != 0 {
			t.Fatalf("request = %+v", request)
		}
		_, _ = io.WriteString(w, `{"results":[
			{"path":"/a/","status":201,"error":null,"created":true,"resource_id":"dir-a"},
			{"path":"/b/","status":409,"error":"path conflict","created":null,"resource_id":null}
		]}`)
	}))
	defer server.Close()

	results, err := New(server.URL, "").BatchMkdirCtx(context.Background(), []BatchMkdirItem{
		{Path: "/a/", Mode: 0o755},
		{Path: "/b/", Mode: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].OK() || results[0].Created == nil || !*results[0].Created ||
		results[0].ResourceID == nil || *results[0].ResourceID != "dir-a" {
		t.Fatalf("success = %+v", results)
	}
	if results[1].OK() || results[1].Error == nil || *results[1].Error != "path conflict" ||
		results[1].Created != nil || results[1].ResourceID != nil {
		t.Fatalf("conflict = %+v", results[1])
	}
}

func TestMigrationBatchChmodContract(t *testing.T) {
	revision := int64(7)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs:batch-chmod" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Items []map[string]json.RawMessage `json:"items"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if len(request.Items) != 2 {
			t.Fatalf("request = %+v", request)
		}
		if _, exists := request.Items[0]["expected_revision"]; !exists {
			t.Fatal("regular item omitted expected_revision")
		}
		if _, exists := request.Items[1]["expected_revision"]; exists {
			t.Fatal("directory item unexpectedly included expected_revision")
		}
		_, _ = io.WriteString(w, `{"results":[
			{"path":"/a.txt","status":200,"error":null,"resource_id":"inode-a","revision":7,"mode":384},
			{"path":"/dir/","status":404,"error":"not found","resource_id":null,"revision":null,"mode":null}
		]}`)
	}))
	defer server.Close()

	results, err := New(server.URL, "").BatchChmodCtx(context.Background(), []BatchChmodItem{
		{Path: "/a.txt", Mode: 0o600, ExpectedResourceID: "inode-a", ExpectedRevision: &revision},
		{Path: "/dir/", Mode: 0o700, ExpectedResourceID: "inode-dir"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].OK() || results[0].ResourceID == nil || *results[0].ResourceID != "inode-a" ||
		results[0].Revision == nil || *results[0].Revision != revision || results[0].Mode == nil || *results[0].Mode != 0o600 {
		t.Fatalf("success = %+v", results)
	}
	if results[1].OK() || results[1].Error == nil || results[1].ResourceID != nil || results[1].Revision != nil || results[1].Mode != nil {
		t.Fatalf("not found = %+v", results[1])
	}
}

func TestMigrationBatchMutationRejectsInvalidContracts(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"results":null}`)
	}))
	defer server.Close()
	c := New(server.URL, "")

	if _, err := c.BatchMkdirCtx(context.Background(), nil); err == nil {
		t.Fatal("empty BatchMkdir accepted")
	}
	if _, err := c.BatchMkdirCtx(context.Background(), make([]BatchMkdirItem, MaxBatchMkdirItems+1)); err == nil {
		t.Fatal("oversized BatchMkdir accepted")
	}
	if _, err := c.BatchMkdirCtx(context.Background(), []BatchMkdirItem{{Path: "/file", Mode: 0o755}}); err == nil {
		t.Fatal("non-directory BatchMkdir path accepted")
	}
	if _, err := c.BatchChmodCtx(context.Background(), []BatchChmodItem{{Path: "/a.txt", Mode: 0o600, ExpectedResourceID: "inode-a"}}); err == nil {
		t.Fatal("regular BatchChmod without revision accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid requests made %d network calls", calls.Load())
	}

	if _, err := c.BatchMkdirCtx(context.Background(), []BatchMkdirItem{{Path: "/a/", Mode: 0o755}}); err == nil {
		t.Fatal("null BatchMkdir results accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("valid request made %d network calls", calls.Load())
	}
}
