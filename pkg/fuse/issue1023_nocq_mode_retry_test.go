package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Data commits before the owned WB uploader's post-content chmod fails.
// A retry must complete that mode obligation and consume its fresh cache
// generation even when the data's exact live SID has already been published.
func TestR6NoCQOwnWriteBackDataProofDoesNotSkipFailedMode(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	fs.commitQueue.DrainAll()
	fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
	var modeCalls atomic.Int32
	var remoteMode atomic.Uint32
	remoteMode.Store(0o644)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.RawQuery == "chmod" {
			var body struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if modeCalls.Add(1) == 1 {
				http.Error(w, "one controlled post-content chmod failure", http.StatusForbidden)
				return
			}
			remoteMode.Store(body.Mode)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
		w.Header().Set("X-Dat9-Nlink", "2")
		w.Header().Set("X-Dat9-Mode", "420")
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + server.path
		clone.URL = &urlCopy
		server.serveHTTP(w, clone)
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, cache, 1)
	fs.SetWriteBack(cache, uploader)
	t.Cleanup(uploader.DrainAll)
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	var attr gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0], Valid: gofuse.FATTR_MODE | gofuse.FATTR_FH, Mode: 0o755,
	}}, &attr); st != gofuse.OK {
		t.Fatalf("deferred SetAttr=%v", st)
	}
	if modeCalls.Load() != 0 {
		t.Fatal("SetAttr did not defer mode behind dirty content")
	}
	fh, _ := fs.fileHandles.Get(ids[0])
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	staged, ok := cache.GetMeta(fh.Path)
	if !ok {
		t.Fatal("actual Flush WB metadata missing")
	}
	stagedGeneration := staged.Generation
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st == gofuse.OK {
		t.Fatal("first Fsync concealed post-content mode failure")
	}
	revision, bytes, puts := server.snapshot()
	if revision != 2 || string(bytes) != "baseA" || len(puts) != 1 || modeCalls.Load() != 1 || remoteMode.Load() != 0o644 {
		t.Fatalf("failure boundary lost: rev=%d bytes=%q puts=%d modecalls=%d mode=%o", revision, bytes, len(puts), modeCalls.Load(), remoteMode.Load())
	}
	meta, present := cache.GetMeta(fh.Path)
	if !present || meta.Kind != PendingChmod || !meta.HasMode || meta.Mode != 0o755 {
		t.Fatalf("post-content mode cache missing: %+v", meta)
	}
	fh.Lock()
	pending := fh.HasPendingMode && fh.DirtySeq != 0 && fh.WriteBackSeq == fh.DirtySeq && meta.Generation != stagedGeneration && meta.Generation == fh.WriteBackGen
	fh.Unlock()
	if !pending {
		t.Fatal("failed mode did not retain FH obligation/fresh PendingChmod generation")
	}
	proof := fs.landedAppendCommit(fh.Path)
	if proof.rev != revision || proof.snapshotID == "" || !landedIdentityMatches(proof, revision, int64(len(bytes)), bytes) {
		t.Fatal("actual successful data was not published with exact live proof")
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Errorf("retry Fsync=%v, want full content/mode success", st)
	}
	if modeCalls.Load() != 2 || remoteMode.Load() != 0o755 {
		t.Errorf("retry skipped failed mode: calls=%d mode=%o", modeCalls.Load(), remoteMode.Load())
	}
	fh.Lock()
	clean := !fh.HasPendingMode && fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts()
	fh.Unlock()
	if !clean {
		t.Error("successful retry left handle content/mode obligation")
	}
	if meta, present := cache.GetMeta(fh.Path); present {
		t.Errorf("successful retry left pending cache: %+v", meta)
	}
	if revision2, bytes2, puts2 := server.snapshot(); revision2 != revision || string(bytes2) != "baseA" || len(puts2) != 1 {
		t.Errorf("mode-only retry rewrote content: rev=%d bytes=%q puts=%d", revision2, bytes2, len(puts2))
	}
	for _, id := range ids {
		if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); st != gofuse.OK {
			t.Errorf("close Flush=%v", st)
		}
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
	}
	if err := uploader.WaitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	uploader.DrainAll()
	for _, path := range []string{"/hardlink-original", "/hardlink-alias"} {
		if fs.hasPendingLocalState(path) {
			t.Errorf("pending state after drain: %s", path)
		}
	}
}
