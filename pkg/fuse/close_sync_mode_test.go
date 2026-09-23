package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
)

func readCloseSyncBatchItem(t *testing.T, r *http.Request) client.BatchWriteItem {
	t.Helper()
	var req struct {
		Items []client.BatchWriteItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Items) != 1 {
		t.Errorf("batch request: items=%d err=%v", len(req.Items), err)
		return client.BatchWriteItem{}
	}
	return req.Items[0]
}

func replyCloseSyncBatch(w http.ResponseWriter, item client.BatchWriteItem, status int, revision int64) {
	_ = json.NewEncoder(w).Encode(struct {
		Results []client.BatchWriteResult `json:"results"`
	}{[]client.BatchWriteResult{{Path: item.Path, Status: status, Revision: revision}}})
}

func TestCloseSyncCombinedModeCommitsContentAndMode(t *testing.T) {
	for _, mode := range []uint32{0o755, 0, 0o4755} {
		t.Run(fmt.Sprintf("%o", mode), func(t *testing.T) {
			var batches atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/fs:batch-write" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				batches.Add(1)
				item := readCloseSyncBatchItem(t, r)
				if item.Path != "/subtree/mode.txt" || item.ExpectedRevision != 0 || !item.HasMode || item.Mode != mode&0o777 || string(item.Data) != "close-sync content" {
					t.Errorf("unexpected batch item: %+v", item)
				}
				replyCloseSyncBatch(w, item, http.StatusOK, 1)
			})
			fs.opts.RemoteRoot = "/subtree"
			fh, input := createCloseSyncShadowTestFile(t, fs, "mode.txt", mode)
			if st := fs.Flush(nil, input); st != gofuse.OK {
				t.Fatalf("Flush: %v", st)
			}
			if batches.Load() != 1 || fh.HasPendingMode || fh.IsNew || fh.BaseRev != 1 || fh.Dirty.HasDirtyParts() || fs.shadowStore.Has(fh.Path) {
				t.Fatalf("combined commit did not finalize: batches=%d pending=%t new=%t revision=%d dirty=%t", batches.Load(), fh.HasPendingMode, fh.IsNew, fh.BaseRev, fh.Dirty.HasDirtyParts())
			}
			entry, _ := fs.inodes.GetEntry(fh.Ino)
			if entry.Mode&posixPermissionModeMask != mode {
				t.Fatalf("local mode = %o, want %o", entry.Mode, mode)
			}
			data, ok := fs.readCache.Get(fh.Path, 1)
			if !ok || string(data) != "close-sync content" {
				t.Fatalf("committed read cache = %q, found=%t", data, ok)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: input.InHeader, Fh: input.Fh})
			if batches.Load() != 1 {
				t.Fatal("Release reuploaded committed content")
			}
		})
	}
}

func TestCloseSyncCombinedModeFallsBackForUnsupportedEndpoint(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var batches, puts, chmods atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/fs:batch-write":
					batches.Add(1)
					w.WriteHeader(code)
				case r.Method == http.MethodPut:
					puts.Add(1)
					if r.Header.Get("X-Dat9-Expected-Revision") != "0" {
						t.Error("fallback lost create-only CAS")
					}
					_, _ = io.WriteString(w, `{"revision":1}`)
				case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
					chmods.Add(1)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
			for i := range 2 {
				fh, input := createCloseSyncShadowTestFile(t, fs, fmt.Sprintf("fallback-%d", i), 0o755)
				if st := fs.Flush(nil, input); st != gofuse.OK || fh.HasPendingMode {
					t.Fatalf("fallback: status=%v pending=%t", st, fh.HasPendingMode)
				}
			}
			if batches.Load() != 1 || puts.Load() != 2 || chmods.Load() != 2 {
				t.Fatalf("requests: batch=%d put=%d chmod=%d", batches.Load(), puts.Load(), chmods.Load())
			}
		})
	}
}

func TestCloseSyncCombinedModeForbiddenFallsBackWithoutBypassingPermissions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		putStatus  int
		wantStatus gofuse.Status
		initial    string
		wantBytes  string
	}{
		{"mode-denied", http.StatusOK, gofuse.EACCES, "", "close-sync content"},
		{"content-denied", http.StatusForbidden, gofuse.EACCES, "", ""},
		{"concurrent-create", http.StatusConflict, gofuse.Status(syscall.EEXIST), "other writer", "other writer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batches, puts, chmods atomic.Int32
			var remote atomic.Value
			remote.Store(tc.initial)
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/fs:batch-write":
					batches.Add(1)
					item := readCloseSyncBatchItem(t, r)
					if !item.HasMode || item.ExpectedRevision != 0 {
						t.Errorf("combined create = %+v", item)
					}
					replyCloseSyncBatch(w, item, http.StatusForbidden, 0)
				case r.Method == http.MethodPut:
					puts.Add(1)
					if r.Header.Get("X-Dat9-Expected-Revision") != "0" {
						t.Error("fallback lost create-only CAS")
					}
					data, err := io.ReadAll(r.Body)
					if err != nil || string(data) != "close-sync content" {
						t.Errorf("fallback bytes = %q, err=%v", data, err)
					}
					if tc.putStatus != http.StatusOK {
						w.WriteHeader(tc.putStatus)
						return
					}
					remote.Store(string(data))
					_, _ = io.WriteString(w, `{"revision":1}`)
				case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
					chmods.Add(1)
					if remote.Load().(string) != "close-sync content" {
						t.Error("chmod ran before content committed")
					}
					w.WriteHeader(http.StatusForbidden)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
			fh, input := createCloseSyncShadowTestFile(t, fs, "scoped.txt", 0o755)
			generation := fh.ShadowStageGen
			if st := fs.Flush(nil, input); st != tc.wantStatus {
				t.Fatalf("Flush = %v, want %v", st, tc.wantStatus)
			}
			wantChmod := int32(0)
			if tc.putStatus == http.StatusOK {
				wantChmod = 1
			}
			if batches.Load() != 1 || puts.Load() != 1 || chmods.Load() != wantChmod || remote.Load().(string) != tc.wantBytes {
				t.Fatalf("batch=%d put=%d chmod=%d remote=%q", batches.Load(), puts.Load(), chmods.Load(), remote.Load())
			}
			if !fh.HasPendingMode || !fh.Dirty.HasDirtyParts() || fs.shadowStore.ActiveGeneration(fh.Path) != generation {
				t.Fatal("failed mode/content commit discarded pending state")
			}
			if !fs.closeSyncBatchAvailable(time.Now()) {
				t.Fatal("per-item denial disabled batching for other creates")
			}
		})
	}
}

func TestCloseSyncCombinedModeErrorsKeepDirtyWithoutFallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(http.ResponseWriter, client.BatchWriteItem)
	}{
		{"server-error", func(w http.ResponseWriter, _ client.BatchWriteItem) { w.WriteHeader(http.StatusInternalServerError) }},
		{"request-conflict", func(w http.ResponseWriter, _ client.BatchWriteItem) { w.WriteHeader(http.StatusConflict) }},
		{"request-forbidden", func(w http.ResponseWriter, _ client.BatchWriteItem) { w.WriteHeader(http.StatusForbidden) }},
		{"item-forbidden-with-revision", func(w http.ResponseWriter, i client.BatchWriteItem) {
			replyCloseSyncBatch(w, i, http.StatusForbidden, 1)
		}},
		{"item-conflict", func(w http.ResponseWriter, i client.BatchWriteItem) {
			replyCloseSyncBatch(w, i, http.StatusConflict, 0)
		}},
		{"item-not-found", func(w http.ResponseWriter, i client.BatchWriteItem) {
			replyCloseSyncBatch(w, i, http.StatusNotFound, 0)
		}},
		{"post-commit-error", func(w http.ResponseWriter, i client.BatchWriteItem) {
			replyCloseSyncBatch(w, i, http.StatusInternalServerError, 1)
		}},
		{"missing-revision", func(w http.ResponseWriter, i client.BatchWriteItem) { replyCloseSyncBatch(w, i, http.StatusOK, 0) }},
		{"wrong-path", func(w http.ResponseWriter, i client.BatchWriteItem) {
			i.Path = "/wrong"
			replyCloseSyncBatch(w, i, http.StatusOK, 1)
		}},
		{"truncated-response", func(w http.ResponseWriter, _ client.BatchWriteItem) { _, _ = io.WriteString(w, `{"results":[`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batches, fallback atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/fs:batch-write" {
					fallback.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				item := readCloseSyncBatchItem(t, r)
				if batches.Add(1) == 1 {
					tc.fail(w, item)
					return
				}
				replyCloseSyncBatch(w, item, http.StatusOK, 1)
			})
			fh, input := createCloseSyncShadowTestFile(t, fs, "error.txt", 0o755)
			gen, modeGen := fh.ShadowStageGen, fh.PendingModeGen
			if st := fs.Flush(nil, input); st == gofuse.OK {
				t.Fatal("failed combined upload returned success")
			}
			if fallback.Load() != 0 || batches.Load() != 1 || !fh.Dirty.HasDirtyParts() || !fh.HasPendingMode || fh.PendingModeGen != modeGen || fs.shadowStore.ActiveGeneration(fh.Path) != gen || !fh.IsNew || fh.BaseRev != 0 {
				t.Fatal("failed request fell back or changed pending data/mode ownership")
			}
			if st := fs.Flush(nil, input); st != gofuse.OK || batches.Load() != 2 || fallback.Load() != 0 {
				t.Fatalf("explicit retry: status=%v batches=%d fallback=%d", st, batches.Load(), fallback.Load())
			}
		})
	}
}

// Lost-ACK conflicts deliberately remain errors until the application reconciles
// the remote file. Equal bytes do not establish ownership of a prior commit;
// automatic adoption requires a server-verifiable idempotency identity.
func TestCloseSyncCombinedModeLostAckDoesNotOverwriteCommittedFile(t *testing.T) {
	var batches, fallback atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs:batch-write" {
			fallback.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		item := readCloseSyncBatchItem(t, r)
		if item.ExpectedRevision != 0 {
			t.Error("unknown outcome must not advance the CAS base")
		}
		if batches.Add(1) == 1 {
			// Model a committed transaction whose HTTP response is lost.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		replyCloseSyncBatch(w, item, http.StatusConflict, 0)
	})
	fh, input := createCloseSyncShadowTestFile(t, fs, "lost-ack.txt", 0o755)
	for range 2 {
		if st := fs.Flush(nil, input); st == gofuse.OK {
			t.Fatal("lost acknowledgement/conflict returned success")
		}
	}
	if batches.Load() != 2 || fallback.Load() != 0 || fh.BaseRev != 0 || !fh.HasPendingMode || !fh.Dirty.HasDirtyParts() {
		t.Fatal("unknown outcome was retried as an overwrite or discarded")
	}
}

func TestCloseSyncCombinedModePreservesConcurrentChmod(t *testing.T) {
	for _, nextMode := range []uint32{0o600, 0o644} {
		t.Run(fmt.Sprintf("%o", nextMode), func(t *testing.T) {
			started, allow := make(chan struct{}), make(chan struct{})
			var release, startedOnce sync.Once
			var batches, chmods atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/fs:batch-write":
					batches.Add(1)
					item := readCloseSyncBatchItem(t, r)
					if item.Mode != 0o755 {
						t.Errorf("first mode = %o", item.Mode)
					}
					startedOnce.Do(func() { close(started) })
					<-allow
					replyCloseSyncBatch(w, item, http.StatusOK, 1)
				case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
					chmods.Add(1)
					var req struct{ Mode uint32 }
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mode != nextMode {
						t.Errorf("newer chmod = %o, err=%v", req.Mode, err)
					}
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
			// This only bounds SetAttr's commit wait while the channel holds upload;
			// channels, not the timeout, determine the acknowledgement ordering.
			fs.opts.RemoteCommitWaitTimeout = 10 * time.Millisecond
			t.Cleanup(func() { release.Do(func() { close(allow) }) })
			fh, input := createCloseSyncShadowTestFile(t, fs, "race.txt", 0o755)
			sibling := &FileHandle{Ino: fh.Ino, Path: fh.Path, Dirty: NewWriteBuffer(fh.Path, maxPreloadSize, 0)}
			fs.allocateFileHandle(sibling)
			done := make(chan gofuse.Status, 1)
			go func() { done <- fs.Flush(nil, input) }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("combined upload did not start")
			}
			var attr gofuse.AttrOut
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				InHeader: input.InHeader, Fh: input.Fh, Valid: gofuse.FATTR_MODE | gofuse.FATTR_FH, Mode: nextMode,
			}}, &attr); st != gofuse.OK {
				t.Fatalf("concurrent chmod: %v", st)
			}
			release.Do(func() { close(allow) })
			select {
			case st := <-done:
				if st != gofuse.OK {
					t.Fatalf("Flush: %v", st)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Flush did not finish")
			}
			entry, _ := fs.inodes.GetEntry(fh.Ino)
			if batches.Load() != 1 || chmods.Load() != 1 || fh.HasPendingMode || sibling.HasPendingMode || entry.Mode&0o777 != nextMode {
				t.Fatalf("newer mode lost: batches=%d chmods=%d pending=%t/%t mode=%o", batches.Load(), chmods.Load(), fh.HasPendingMode, sibling.HasPendingMode, entry.Mode)
			}
		})
	}
}

func TestCloseSyncCombinedModeConcurrentChmodFailureKeepsCommittedCAS(t *testing.T) {
	started, allow := make(chan struct{}), make(chan struct{})
	var release, startedOnce sync.Once
	var batches, puts, chmods atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/fs:batch-write":
			batches.Add(1)
			item := readCloseSyncBatchItem(t, r)
			startedOnce.Do(func() { close(started) })
			<-allow
			replyCloseSyncBatch(w, item, http.StatusOK, 1)
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			if chmods.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
			}
		case r.Method == http.MethodPut:
			puts.Add(1)
			if r.Header.Get("X-Dat9-Expected-Revision") != "1" {
				t.Error("retry must retain the confirmed content revision")
			}
			_, _ = io.WriteString(w, `{"revision":2}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	// This only bounds SetAttr's commit wait while the channel holds upload;
	// channels, not the timeout, determine the acknowledgement ordering.
	fs.opts.RemoteCommitWaitTimeout = 10 * time.Millisecond
	t.Cleanup(func() { release.Do(func() { close(allow) }) })
	fh, input := createCloseSyncShadowTestFile(t, fs, "chmod-retry.txt", 0o755)
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Flush(nil, input) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("combined upload did not start")
	}
	var attr gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: input.InHeader, Fh: input.Fh, Valid: gofuse.FATTR_MODE | gofuse.FATTR_FH, Mode: 0o644,
	}}, &attr); st != gofuse.OK {
		t.Fatalf("concurrent chmod: %v", st)
	}
	release.Do(func() { close(allow) })
	select {
	case st := <-done:
		if st == gofuse.OK {
			t.Fatal("failed newer chmod returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not finish")
	}
	entry, _ := fs.inodes.GetEntry(fh.Ino)
	if fh.IsNew || fh.BaseRev != 1 || !fh.HasPendingMode || fh.PendingMode != 0o644 || !fh.Dirty.HasDirtyParts() || entry.Mode != 0o755 {
		t.Fatal("newer chmod failure lost the committed content/mode baseline")
	}
	if st := fs.Flush(nil, input); st != gofuse.OK || batches.Load() != 1 || puts.Load() != 1 || chmods.Load() != 2 || fh.HasPendingMode {
		t.Fatalf("retry: status=%v batch=%d put=%d chmod=%d pending=%t", st, batches.Load(), puts.Load(), chmods.Load(), fh.HasPendingMode)
	}
}

func TestCloseSyncCombinedModeDoesNotFinalizeChangedHandle(t *testing.T) {
	for _, mutation := range []string{"rename", "unlink", "write"} {
		t.Run(mutation, func(t *testing.T) {
			started, allow := make(chan struct{}), make(chan struct{})
			var release, startedOnce sync.Once
			var otherCalls atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/fs:batch-write" {
					otherCalls.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				item := readCloseSyncBatchItem(t, r)
				startedOnce.Do(func() { close(started) })
				<-allow
				replyCloseSyncBatch(w, item, http.StatusOK, 1)
			})
			t.Cleanup(func() { release.Do(func() { close(allow) }) })
			fh, input := createCloseSyncShadowTestFile(t, fs, "changed.txt", 0o755)
			done := make(chan gofuse.Status, 1)
			go func() { done <- fs.Flush(nil, input) }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("combined upload did not start")
			}
			// Model the handle transitions under fh.mu while the network call
			// is in flight. Namespace-operation serialization has separate tests.
			fh.Lock()
			switch mutation {
			case "rename":
				fs.inodes.Rename(fh.Path, "/renamed.txt")
				fh.Path = "/renamed.txt"
			case "unlink":
				fh.Unlinked = true
			case "write":
				fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
			}
			fh.Unlock()
			release.Do(func() { close(allow) })
			select {
			case st := <-done:
				if st != gofuse.OK {
					t.Fatalf("Flush: %v", st)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Flush did not finish")
			}
			if otherCalls.Load() != 0 || !fh.Dirty.HasDirtyParts() {
				t.Fatal("stale acknowledgement finalized changed handle state")
			}
			if mutation != "unlink" && (!fh.HasPendingMode || fh.BaseRev != 0) {
				t.Fatal("stale acknowledgement cleared newer path/content ownership")
			}
		})
	}
}

func TestCloseSyncCombinedModeKeepsMultipartBoundary(t *testing.T) {
	for _, threshold := range []int64{0, int64(len("close-sync content"))} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			var initiates atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/uploads/initiate" {
					initiates.Add(1)
				} else {
					t.Errorf("non-inline file selected wrong endpoint: %s", r.URL)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			fs.client.SetSmallFileThresholdForTests(threshold)
			fh, input := createCloseSyncShadowTestFile(t, fs, "multipart.bin", 0o755)
			if st := fs.Flush(nil, input); st == gofuse.OK || initiates.Load() != 1 || !fh.HasPendingMode || !fh.Dirty.HasDirtyParts() {
				t.Fatalf("multipart failure: status=%v initiates=%d pending=%t", st, initiates.Load(), fh.HasPendingMode)
			}
		})
	}
}

func TestCloseSyncCombinedModeLeavesOtherUploadsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*FileHandle)
		mode   uint32
	}{
		{"default-mode", func(*FileHandle) {}, 0o644},
		{"write-sync", func(fh *FileHandle) { fh.WritePolicy = WritePolicyWriteSync }, 0o755},
		{"writeback", func(fh *FileHandle) { fh.WritePolicy = WritePolicyWriteBack }, 0o755},
		{"overwrite", func(fh *FileHandle) { fh.IsNew = false; fh.BaseRev = 3 }, 0o755},
		{"legacy-generation", func(fh *FileHandle) { fh.ShadowStageGen = 0 }, 0o755},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts, chmods atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut:
					puts.Add(1)
					want := "0"
					if tc.name == "overwrite" {
						want = "3"
					}
					if got := r.Header.Get("X-Dat9-Expected-Revision"); got != want {
						t.Errorf("expected revision = %s, want %s", got, want)
					}
					_, _ = io.WriteString(w, `{"revision":4}`)
				case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
					chmods.Add(1)
				default:
					t.Errorf("out-of-scope upload changed: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			})
			fh, _ := createCloseSyncShadowTestFile(t, fs, "scope.txt", tc.mode)
			fh.Lock()
			tc.modify(fh)
			st := fs.syncHandleToRemoteWithoutAppendLogLocked(t.Context(), fh, shadowUploadRemoteDurable)
			fh.Unlock()
			wantChmod := int32(1)
			if tc.mode == 0o644 {
				wantChmod = 0
			}
			if st != gofuse.OK || puts.Load() != 1 || chmods.Load() != wantChmod {
				t.Fatalf("legacy upload: status=%v put=%d chmod=%d", st, puts.Load(), chmods.Load())
			}
		})
	}
}

func TestCloseSyncCombinedModeReprobesAfterCooldown(t *testing.T) {
	var batches, puts, chmods atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/fs:batch-write":
			if batches.Add(1) == 1 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			replyCloseSyncBatch(w, readCloseSyncBatchItem(t, r), http.StatusOK, 1)
		case r.Method == http.MethodPut:
			puts.Add(1)
			_, _ = io.WriteString(w, `{"revision":1}`)
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmods.Add(1)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	for i := range 3 {
		if i == 2 {
			retryAt := time.Now().Add(-time.Second)
			fs.closeSyncBatchRetryAt.Store(&retryAt)
		}
		_, input := createCloseSyncShadowTestFile(t, fs, fmt.Sprintf("retry-batch-%d", i), 0o755)
		if st := fs.Flush(nil, input); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	if batches.Load() != 2 || puts.Load() != 2 || chmods.Load() != 2 {
		t.Fatalf("batch=%d put=%d chmod=%d", batches.Load(), puts.Load(), chmods.Load())
	}
}

func TestCloseSyncCombinedModeLogsFallbackOncePerCooldown(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusMethodNotAllowed) })
	core, logs := observer.New(zap.WarnLevel)
	ctx := logger.WithContext(context.Background(), zap.New(core))
	fh, _ := createCloseSyncShadowTestFile(t, fs, "fallback-log.txt", 0o755)
	errors := make(chan error, 8)
	for range cap(errors) {
		go func() {
			result, err := fs.uploadCloseSyncCreateWithMode(ctx, fh.Path, fh.Dirty.Size(), fh.ShadowStageGen, &closeSyncModeCommit{mode: 0o755})
			if err == nil && result.outcome != closeSyncUploadUnsupported {
				err = fmt.Errorf("unsupported endpoint outcome = %d", result.outcome)
			}
			errors <- err
		}()
	}
	for range cap(errors) {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}
	if logs.Len() != 1 {
		t.Fatalf("fallback log count=%d, want 1", logs.Len())
	}
}

func TestCloseSyncBatchCooldownDoesNotExtendForInflightFailures(t *testing.T) {
	fs := &Dat9FS{}
	now := time.Now()
	if !fs.closeSyncBatchAvailable(now) || !fs.deferCloseSyncBatchRetry(now) {
		t.Fatal("first failure did not start a cooldown")
	}
	retryAt := now.Add(closeSyncBatchRetryInterval)
	justBefore := retryAt.Add(-time.Nanosecond)
	if fs.closeSyncBatchAvailable(justBefore) || fs.deferCloseSyncBatchRetry(justBefore) {
		t.Fatal("in-flight failure restarted the active cooldown")
	}
	if !fs.closeSyncBatchAvailable(retryAt) || !fs.deferCloseSyncBatchRetry(retryAt) {
		t.Fatal("cooldown did not allow a new probe at the original deadline")
	}
}

func TestCloseSyncCombinedModeAcceptsLastInlineSize(t *testing.T) {
	var batches atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs:batch-write" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		batches.Add(1)
		replyCloseSyncBatch(w, readCloseSyncBatchItem(t, r), http.StatusOK, 1)
	})
	fs.client.SetSmallFileThresholdForTests(int64(len("close-sync content")) + 1)
	_, input := createCloseSyncShadowTestFile(t, fs, "last-inline.txt", 0o755)
	if st := fs.Flush(nil, input); st != gofuse.OK || batches.Load() != 1 {
		t.Fatalf("status=%v batch=%d", st, batches.Load())
	}
}

func TestCloseSyncCombinedModeCannotBypassLocalBarrier(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprint(staged), func(t *testing.T) {
			var calls atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
			fh, input := createCloseSyncShadowTestFile(t, fs, "local-barrier.txt", 0o755)
			if staged {
				if _, err := fs.pendingIndex.PutShadowSpill(fh.Path, fh.Dirty.Size(), PendingNew, 0); err != nil {
					t.Fatal(err)
				}
			} else {
				fs.syncMode = SyncInteractive
			}
			makeCloseSyncShadowUnsyncable(t, fs.shadowStore, fh.Path)
			if st := fs.Flush(nil, input); st == gofuse.OK || calls.Load() != 0 {
				t.Fatalf("combined commit bypassed local sync: status=%v calls=%d", st, calls.Load())
			}
		})
	}
}
