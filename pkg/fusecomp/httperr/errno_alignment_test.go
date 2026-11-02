package httperr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Ports of v1's errno_alignment_test.go (#1008) that lock the quota-message
// parsing matrix and the client SDK's error shapes across upload transports
// (plain PUT, V2 multipart initiate, batch-write results, nested and plain
// JSON bodies). The SDK (pkg/client) is shared with v1 unchanged, so these
// rows bind the httperr table to exactly the error strings the client emits.

var quotaErrnoTestCases = []struct {
	name    string
	message string
	want    gofuse.Status
}{
	{"storage_bare", "tenant storage quota exceeded", gofuse.Status(syscall.EDQUOT)},
	{"storage_server", "tenant storage quota exceeded: server limit=8 used=8 reserved=0 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
	{"storage_legacy_update", "tenant storage quota exceeded: limit=8 used=8 reserved=0 current_path=0 requested=1 delta=1", gofuse.Status(syscall.EDQUOT)},
	{"storage_legacy_create", "tenant storage quota exceeded: limit=8 used=8 reserved=0 requested=1 delta=1", gofuse.Status(syscall.EDQUOT)},
	{"count_bare", "tenant file count quota exceeded", gofuse.Status(syscall.EDQUOT)},
	{"count_server", "tenant file count quota exceeded: server limit=8 used=8 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
	{"size_bare", "tenant file size quota exceeded", gofuse.Status(syscall.EFBIG)},
	{"size_server", "tenant file size quota exceeded: server limit=8 requested=9", gofuse.Status(syscall.EFBIG)},
	{"signed_fields", "tenant storage quota exceeded: server limit=8 used=8 reserved=-1 pending=-2 delta=4", gofuse.Status(syscall.EDQUOT)},
	{"unknown_507", "insufficient storage", gofuse.EIO},
	{"empty", "", gofuse.EIO},
	{"media_quota", "tenant media LLM file quota exceeded", gofuse.EIO},
	{"embedded", "backend failure: tenant storage quota exceeded", gofuse.EIO},
	{"cause_suffix", "tenant storage quota exceeded unexpectedly", gofuse.EIO},
	{"storage_arbitrary_detail", "tenant storage quota exceeded: backend unavailable", gofuse.EIO},
	{"count_arbitrary_detail", "tenant file count quota exceeded: backend unavailable", gofuse.EIO},
	{"size_arbitrary_detail", "tenant file size quota exceeded: backend unavailable", gofuse.EIO},
	{"mixed_causes", "tenant storage quota exceeded: server limit=8 used=8 reserved=0 pending=0 delta=1; tenant file size quota exceeded", gofuse.EIO},
	{"newline", "tenant file size quota exceeded: server limit=8 requested=9\nbackend unavailable", gofuse.EIO},
	{"trailing_detail", "tenant file size quota exceeded: server limit=8 requested=9 extra", gofuse.EIO},
	{"invalid_number", "tenant file size quota exceeded: server limit=8 requested=x", gofuse.EIO},
	{"reordered_fields", "tenant file size quota exceeded: requested=9 server limit=8", gofuse.EIO},
	{"missing_field", "tenant file count quota exceeded: server limit=8 used=8 delta=1", gofuse.EIO},
	{"text_wrapped_cause", "reserve upload: tenant storage quota exceeded: server limit=8 used=8 reserved=0 pending=0 delta=1", gofuse.EIO},
}

func TestErrnoAlignmentQuotaMessages(t *testing.T) {
	for _, testCase := range quotaErrnoTestCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := error(&client.StatusError{StatusCode: 507, Message: testCase.message})
			// errors.As must see through the commit/upload wrapper chains the
			// engine builds around SDK failures.
			for _, variant := range []error{
				err,
				fmt.Errorf("upload: %w", err),
				fmt.Errorf("tenant storage quota exceeded: %w", fmt.Errorf("commit: %w", err)),
			} {
				if got := HTTPStatusOf(variant); got != testCase.want {
					t.Errorf("HTTPStatusOf(%v) = %v, want %v", variant, got, testCase.want)
				}
			}
		})
	}
}

// TestErrnoAlignmentNonQuotaStatusRows locks the status-code table for
// non-quota statuses even when the message body claims a quota cause: quota
// parsing only applies to 507 (v1's non_quota_status rows). Each row is
// wrapped once, as the engine wraps SDK errors.
func TestErrnoAlignmentNonQuotaStatusRows(t *testing.T) {
	for _, testCase := range []struct {
		code int
		want gofuse.Status
	}{
		{400, gofuse.Status(syscall.EINVAL)},
		{401, gofuse.EACCES},
		{403, gofuse.EACCES},
		{404, gofuse.ENOENT},
		{409, gofuse.Status(syscall.EEXIST)},
		{412, gofuse.Status(syscall.ESTALE)},
		{413, gofuse.Status(syscall.EFBIG)},
		{499, gofuse.Status(syscall.EAGAIN)},
		{500, gofuse.Status(syscall.EAGAIN)},
		{502, gofuse.Status(syscall.EAGAIN)},
		{503, gofuse.Status(syscall.EAGAIN)},
		{504, gofuse.Status(syscall.EAGAIN)},
	} {
		t.Run(fmt.Sprintf("non_quota_status_%d", testCase.code), func(t *testing.T) {
			err := fmt.Errorf("upload: %w", &client.StatusError{
				StatusCode: testCase.code,
				Message:    "tenant storage quota exceeded",
			})
			if got := HTTPStatusOf(err); got != testCase.want {
				t.Fatalf("status = %v, want %v", got, testCase.want)
			}
		})
	}
}

// timeoutNetError is a net.Error whose deadline expired: HTTPStatusOf maps
// it to retryable EAGAIN before any status/string matching.
type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "read tcp: i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

// idleNetError is a net.Error that has not timed out: it must fall through
// to the plain-error paths.
type idleNetError struct{}

func (idleNetError) Error() string   { return "connection refused" }
func (idleNetError) Timeout() bool   { return false }
func (idleNetError) Temporary() bool { return true }

// TestErrnoAlignmentUntypedErrors locks the rows for errors without a typed
// status: cancellation and net timeouts map to retryable EAGAIN, quota-ish
// strings without a 507 stay EIO, and nil is OK (v1's closing rows).
func TestErrnoAlignmentUntypedErrors(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if got := HTTPStatusOf(nil); got != gofuse.OK {
			t.Fatalf("HTTPStatusOf(nil) = %v, want OK", got)
		}
	})
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := HTTPStatusOf(err); got != gofuse.Status(syscall.EAGAIN) {
			t.Errorf("context error %v = %v, want EAGAIN", err, got)
		}
	}
	if got := HTTPStatusOf(timeoutNetError{}); got != gofuse.Status(syscall.EAGAIN) {
		t.Errorf("net timeout = %v, want EAGAIN", got)
	}
	if got := HTTPStatusOf(fmt.Errorf("upload: %w", context.Canceled)); got != gofuse.Status(syscall.EAGAIN) {
		t.Errorf("wrapped context cancellation = %v, want EAGAIN", got)
	}
	for _, err := range []error{
		errors.New("tenant storage quota exceeded"),
		errors.New("HTTP 507: tenant storage quota exceeded"),
		syscall.ENOSPC,
		idleNetError{},
	} {
		if got := HTTPStatusOf(err); got != gofuse.EIO {
			t.Errorf("untyped error %v = %v, want EIO", err, got)
		}
	}
}

// TestErrnoAlignmentStringFallbackRows locks the full string-matching
// fallback for errors that carry no typed status: the "HTTP <code>"
// prefixes the SDK used to emit, plus the not-found/not-empty/exists
// substrings.
func TestErrnoAlignmentStringFallbackRows(t *testing.T) {
	cases := []struct {
		msg  string
		want gofuse.Status
	}{
		{"GET failed: HTTP 404", gofuse.ENOENT},
		{"node not found", gofuse.ENOENT},
		{"cannot remove: directory not empty", gofuse.Status(syscall.ENOTEMPTY)},
		{"HTTP 409: conflict", gofuse.Status(syscall.EEXIST)},
		{"path already exists", gofuse.Status(syscall.EEXIST)},
		{"HTTP 401", gofuse.EACCES},
		{"HTTP 403", gofuse.EACCES},
		{"HTTP 413", gofuse.Status(syscall.EFBIG)},
		{"HTTP 412", gofuse.Status(syscall.ESTALE)},
		{"HTTP 400", gofuse.Status(syscall.EINVAL)},
		{"HTTP 499", gofuse.Status(syscall.EAGAIN)},
		{"HTTP 500", gofuse.Status(syscall.EAGAIN)},
		{"HTTP 502", gofuse.Status(syscall.EAGAIN)},
		{"HTTP 503", gofuse.Status(syscall.EAGAIN)},
		{"HTTP 504", gofuse.Status(syscall.EAGAIN)},
		// 507 has no string-fallback row: quota parsing is 507-only.
		{"HTTP 507: out of space", gofuse.EIO},
		{"mystery backend failure", gofuse.EIO},
	}
	for _, c := range cases {
		if got := HTTPStatusOf(errors.New(c.msg)); got != c.want {
			t.Errorf("%q: HTTPStatusOf = %v, want %v", c.msg, got, c.want)
		}
	}
}

func TestErrnoAlignmentQuotaTransports(t *testing.T) {
	for _, transport := range []string{"put", "multipart", "batch", "nested", "plain"} {
		for _, testCase := range quotaErrnoTestCases {
			t.Run(transport+"/"+testCase.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Type", "application/json")
					if transport == "batch" {
						if request.Method != http.MethodPost || request.URL.Path != "/v1/fs:batch-write" {
							t.Errorf("unexpected batch route: %s %s", request.Method, request.URL.Path)
						}
						_ = json.NewEncoder(writer).Encode(map[string]any{"results": []client.BatchWriteResult{{Path: "/quota.txt", Status: 507, Error: testCase.message}}})
						return
					}
					if transport == "multipart" {
						if request.Method != http.MethodPost || request.URL.Path != "/v2/uploads/initiate" {
							t.Errorf("unexpected multipart route: %s %s", request.Method, request.URL.Path)
						}
					} else if request.Method != http.MethodPut || request.URL.Path != "/v1/fs/quota.txt" {
						t.Errorf("unexpected PUT route: %s %s", request.Method, request.URL.Path)
					}
					writer.WriteHeader(http.StatusInsufficientStorage)
					switch transport {
					case "nested":
						_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]string{"message": testCase.message}})
					case "plain":
						_, _ = io.WriteString(writer, testCase.message)
					default:
						_ = json.NewEncoder(writer).Encode(map[string]string{"error": testCase.message})
					}
				}))
				t.Cleanup(server.Close)
				sdk := client.New(server.URL, "")
				var err error
				switch transport {
				case "multipart":
					err = sdk.WriteMultipartStreamConditional(context.Background(), "/quota.txt", bytes.NewReader([]byte("payload")), 7, nil, 0)
				case "batch":
					var results []client.BatchWriteResult
					results, err = sdk.BatchWriteCtx(context.Background(), []client.BatchWriteItem{{Path: "/quota.txt", Data: []byte("payload")}})
					if err != nil || len(results) != 1 {
						t.Fatalf("BatchWriteCtx = %v, %v", results, err)
					}
					err = &client.StatusError{StatusCode: results[0].Status, Message: results[0].Error}
				default:
					_, err = sdk.WriteCtxConditionalWithRevision(context.Background(), "/quota.txt", []byte("payload"), 0)
					if !client.IsCommitAttempted(err) {
						t.Fatalf("PUT error lacks commit-attempt wrapper: %v", err)
					}
				}
				want := testCase.want
				if transport == "plain" {
					want = gofuse.EIO
				}
				for _, variant := range []error{err, fmt.Errorf("commit: %w", fmt.Errorf("upload: %w", err))} {
					if got := HTTPStatusOf(variant); got != want {
						t.Errorf("SDK error %v = %v, want %v", variant, got, want)
					}
				}
			})
		}
	}
}

// TestErrnoAlignmentUploadFailureRows ports the per-operation rows of v1's
// TestErrnoAlignmentShadowUploadFailures that only need the client transport
// (the fsync/flush plumbing lives in pkg/extensions/writeback now).
func TestErrnoAlignmentUploadFailureRows(t *testing.T) {
	for _, transport := range []string{"put", "multipart"} {
		for _, testCase := range []struct {
			name    string
			code    int
			message string
			want    gofuse.Status
		}{
			{"storage", 507, "tenant storage quota exceeded: server limit=8 used=8 reserved=0 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
			{"count", 507, "tenant file count quota exceeded: server limit=8 used=8 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
			{"file_size", 507, "tenant file size quota exceeded: server limit=8 requested=9", gofuse.Status(syscall.EFBIG)},
			{"unknown_507", 507, "tenant storage quota exceeded: backend unavailable", gofuse.EIO},
			{"internal_error", 500, "backend unavailable", gofuse.Status(syscall.EAGAIN)},
			{"revision_conflict", 409, "revision conflict", gofuse.EIO},
		} {
			t.Run(transport+"/"+testCase.name, func(t *testing.T) {
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					requests.Add(1)
					if transport == "multipart" {
						if request.Method != http.MethodPost || request.URL.Path != "/v2/uploads/initiate" {
							t.Errorf("unexpected multipart route: %s %s", request.Method, request.URL.Path)
						}
					} else if request.Method != http.MethodPut || request.URL.Path != "/v1/fs/quota.bin" {
						t.Errorf("unexpected PUT route: %s %s", request.Method, request.URL.Path)
					}
					writer.Header().Set("Content-Type", "application/json")
					writer.WriteHeader(testCase.code)
					_ = json.NewEncoder(writer).Encode(map[string]string{"error": testCase.message})
				}))
				t.Cleanup(server.Close)
				sdk := client.New(server.URL, "")
				var err error
				if transport == "put" {
					sdk.SetSmallFileThresholdForTests(64)
					_, err = sdk.WriteCtxConditionalWithRevision(context.Background(), "/quota.bin", []byte("payload"), 0)
				} else {
					sdk.SetSmallFileThresholdForTests(1)
					err = sdk.WriteMultipartStreamConditional(context.Background(), "/quota.bin", bytes.NewReader([]byte("payload")), 7, nil, 0)
				}
				if err == nil {
					t.Fatal("expected upload failure")
				}
				if got := HTTPStatusOf(err); got != testCase.want {
					t.Errorf("%s upload error %v = %v, want %v", transport, err, got, testCase.want)
				}
			})
		}
	}
}
