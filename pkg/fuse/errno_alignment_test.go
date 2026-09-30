package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

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

func TestErrnoAlignmentLocalErrors(test *testing.T) {
	testCases := []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"nil", nil, gofuse.OK},
		{"eof", io.EOF, gofuse.OK},
		{"permission_sentinel", os.ErrPermission, gofuse.EACCES},
		{"exist_sentinel", os.ErrExist, gofuse.Status(syscall.EEXIST)},
		{"not_exist_sentinel", os.ErrNotExist, gofuse.ENOENT},
		{"unknown", errors.New("unknown local error"), gofuse.EIO},
	}
	for _, errno := range []syscall.Errno{syscall.ENOTEMPTY, syscall.EPERM, syscall.EACCES, syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EISDIR, syscall.EXDEV, syscall.EFBIG, syscall.EDQUOT, syscall.ENOSPC, syscall.EIO} {
		testCases = append(testCases, struct {
			name string
			err  error
			want gofuse.Status
		}{errno.Error(), errno, gofuse.Status(errno)})
	}
	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			variants := []error{testCase.err}
			if testCase.err != nil {
				variants = append(variants,
					&os.PathError{Op: "remove", Path: "/local", Err: testCase.err},
					&os.LinkError{Op: "rename", Old: "/old", New: "/new", Err: testCase.err},
					fmt.Errorf("outer: %w", &os.PathError{Op: "remove", Path: "/local", Err: testCase.err}),
				)
			}
			for _, variant := range variants {
				if got := localErrToFuseStatus(variant); got != testCase.want {
					test.Errorf("localErrToFuseStatus(%v) = %v, want %v", variant, got, testCase.want)
				}
			}
		})
	}
}

func TestErrnoAlignmentQuotaMessages(test *testing.T) {
	for _, testCase := range quotaErrnoTestCases {
		test.Run(testCase.name, func(test *testing.T) {
			err := &client.StatusError{StatusCode: http.StatusInsufficientStorage, Message: testCase.message}
			for _, variant := range []error{err, fmt.Errorf("upload: %w", err), fmt.Errorf("tenant storage quota exceeded: %w", fmt.Errorf("commit: %w", err))} {
				if got := httpToFuseStatus(variant); got != testCase.want {
					test.Errorf("httpToFuseStatus(%v) = %v, want %v", variant, got, testCase.want)
				}
			}
		})
	}
	for _, testCase := range []struct {
		code int
		want gofuse.Status
	}{
		{400, gofuse.EINVAL}, {401, gofuse.EACCES}, {403, gofuse.EACCES}, {404, gofuse.ENOENT},
		{409, gofuse.Status(syscall.EEXIST)}, {412, gofuse.Status(syscall.ESTALE)}, {413, gofuse.Status(syscall.EFBIG)},
		{499, gofuse.EAGAIN}, {500, gofuse.EAGAIN}, {502, gofuse.EAGAIN}, {503, gofuse.EAGAIN}, {504, gofuse.EAGAIN},
	} {
		test.Run(fmt.Sprintf("non_quota_status_%d", testCase.code), func(test *testing.T) {
			err := &client.StatusError{StatusCode: testCase.code, Message: "tenant storage quota exceeded"}
			if got := httpToFuseStatus(fmt.Errorf("upload: %w", err)); got != testCase.want {
				test.Fatalf("status = %v, want %v", got, testCase.want)
			}
		})
	}
	for _, err := range []error{errors.New("tenant storage quota exceeded"), errors.New("HTTP 507: tenant storage quota exceeded"), syscall.ENOSPC} {
		if got := httpToFuseStatus(err); got != gofuse.EIO {
			test.Errorf("untyped error %v = %v, want EIO", err, got)
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := httpToFuseStatus(err); got != gofuse.EAGAIN {
			test.Errorf("context error %v = %v, want EAGAIN", err, got)
		}
	}
}

func TestErrnoAlignmentQuotaTransports(test *testing.T) {
	for _, transport := range []string{"put", "multipart", "batch", "nested", "plain"} {
		for _, testCase := range quotaErrnoTestCases {
			test.Run(transport+"/"+testCase.name, func(test *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Type", "application/json")
					if transport == "batch" {
						if request.Method != http.MethodPost || request.URL.Path != "/v1/fs:batch-write" {
							test.Errorf("unexpected batch route: %s %s", request.Method, request.URL.Path)
						}
						_ = json.NewEncoder(writer).Encode(map[string]any{"results": []client.BatchWriteResult{{Path: "/quota.txt", Status: 507, Error: testCase.message}}})
						return
					}
					if transport == "multipart" {
						if request.Method != http.MethodPost || request.URL.Path != "/v2/uploads/initiate" {
							test.Errorf("unexpected multipart route: %s %s", request.Method, request.URL.Path)
						}
					} else if request.Method != http.MethodPut || request.URL.Path != "/v1/fs/quota.txt" {
						test.Errorf("unexpected PUT route: %s %s", request.Method, request.URL.Path)
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
				test.Cleanup(server.Close)
				sdk := newTestClient(server.URL)
				var err error
				switch transport {
				case "multipart":
					err = sdk.WriteMultipartStreamConditional(context.Background(), "/quota.txt", bytes.NewReader([]byte("payload")), 7, nil, 0)
				case "batch":
					var results []client.BatchWriteResult
					results, err = sdk.BatchWriteCtx(context.Background(), []client.BatchWriteItem{{Path: "/quota.txt", Data: []byte("payload")}})
					if err != nil || len(results) != 1 {
						test.Fatalf("BatchWriteCtx = %v, %v", results, err)
					}
					err = batchWriteResultError(results[0])
				default:
					_, err = sdk.WriteCtxConditionalWithRevision(context.Background(), "/quota.txt", []byte("payload"), 0)
					if !client.IsCommitAttempted(err) {
						test.Fatalf("PUT error lacks commit-attempt wrapper: %v", err)
					}
				}
				want := testCase.want
				if transport == "plain" {
					want = gofuse.EIO
				}
				for _, variant := range []error{err, fmt.Errorf("commit: %w", fmt.Errorf("upload: %w", err))} {
					if got := httpToFuseStatus(variant); got != want {
						test.Errorf("SDK error %v = %v, want %v", variant, got, want)
					}
				}
			})
		}
	}
}

func TestErrnoAlignmentShadowUploadFailures(test *testing.T) {
	for _, operation := range []string{"fsync", "flush_fallback", "flush_close_sync"} {
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
				{"internal_error", 500, "backend unavailable", gofuse.EIO},
				{"revision_conflict", 409, "revision conflict", gofuse.EIO},
			} {
				test.Run(operation+"/"+transport+"/"+testCase.name, func(test *testing.T) {
					var requests atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						requests.Add(1)
						if transport == "multipart" {
							if request.Method != http.MethodPost || request.URL.Path != "/v2/uploads/initiate" {
								test.Errorf("unexpected multipart route: %s %s", request.Method, request.URL.Path)
							}
						} else if request.Method != http.MethodPut || request.URL.Path != "/v1/fs/quota.bin" {
							test.Errorf("unexpected PUT route: %s %s", request.Method, request.URL.Path)
						}
						writer.Header().Set("Content-Type", "application/json")
						writer.WriteHeader(testCase.code)
						_ = json.NewEncoder(writer).Encode(map[string]string{"error": testCase.message})
					}))
					test.Cleanup(server.Close)
					options := &MountOptions{FlushDebounce: 0}
					options.setDefaults()
					sdk := newTestClient(server.URL)
					fileSize := int64(16)
					if operation == "flush_fallback" {
						fileSize = writeBackThreshold
					}
					if transport == "put" {
						sdk.SetSmallFileThresholdForTests(fileSize + 1)
					} else {
						sdk.SetSmallFileThresholdForTests(1)
					}
					fs := NewDat9FS(sdk, options)
					fs.syncMode = SyncStrict
					shadow, err := NewShadowStore(test.TempDir())
					if err != nil {
						test.Fatal(err)
					}
					test.Cleanup(shadow.Close)
					fs.shadowStore = shadow
					const filePath = "/quota.bin"
					if err := shadow.Truncate(filePath, fileSize, 0); err != nil {
						test.Fatal(err)
					}
					if _, err := shadow.WriteAt(filePath, fileSize-1, []byte("q"), 0); err != nil {
						test.Fatal(err)
					}
					inode := fs.inodes.Lookup(filePath, false, fileSize, time.Now())
					dirty := NewWriteBuffer(filePath, fileSize, 0)
					if _, err := dirty.Write(fileSize-1, []byte("q")); err != nil {
						test.Fatal(err)
					}
					fileHandle := &FileHandle{Ino: inode, Path: filePath, Dirty: dirty, DirtySeq: fs.markDirtySize(inode, fileSize), ShadowSpill: true, ShadowReady: true, IsNew: true}
					if operation == "flush_close_sync" {
						fileHandle.WritePolicy = WritePolicyCloseSync
					}
					dirtySequence := fileHandle.DirtySeq
					handleID := fs.fileHandles.Allocate(fileHandle)
					fs.openHandles.Add(fileHandle)
					var got gofuse.Status
					if operation == "fsync" {
						got = fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: inode}, Fh: handleID})
					} else {
						got = fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: inode}, Fh: handleID})
					}
					want := testCase.want
					if operation == "flush_close_sync" && testCase.code == 500 {
						want = gofuse.EAGAIN
					}
					if got != want {
						test.Errorf("%s = %v, want %v", operation, got, want)
					}
					if requests.Load() == 0 {
						test.Fatal("operation did not attempt remote upload")
					}
					if fileHandle.Dirty != dirty || !dirty.HasDirtyParts() || dirty.Size() != fileSize || fileHandle.DirtySeq != dirtySequence || !fileHandle.IsNew || fileHandle.BaseRev != 0 {
						test.Fatal("failed upload changed uncommitted handle state")
					}
					if !shadow.Has(filePath) || shadow.Size(filePath) != fileSize {
						test.Fatal("failed upload discarded shadow data")
					}
					retained := make([]byte, 1)
					if _, err := shadow.ReadAt(filePath, fileSize-1, retained); err != nil || retained[0] != 'q' {
						test.Fatalf("retained shadow byte = %q, %v, want q", retained, err)
					}
				})
			}
		}
	}
}
