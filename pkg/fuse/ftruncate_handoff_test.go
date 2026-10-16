package fuse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

type truncateHandoffRemote struct {
	mu              sync.Mutex
	data            []byte
	revision        int64
	puts            int
	resource        string
	nlink           int
	inlineThreshold int64
	patchParts      []int
	omitPatchRead   bool
	shortPatchRead  bool
	rejectPatchDone bool
}

func (s *truncateHandoffRemote) snapshot() ([]byte, int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.data), s.revision, s.puts
}
func truncateHandoffFS(t *testing.T, initial []byte) (*Dat9FS, uint64, *truncateHandoffRemote) {
	t.Helper()
	remote := &truncateHandoffRemote{data: bytes.Clone(initial), revision: 1, resource: "handoff-resource", nlink: 1, inlineThreshold: 32 << 20}
	var uploaded []byte
	var uploadSize, uploadRevision int64
	var patchOriginal []byte
	var patchPart, patchPartSize int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			remote.mu.Lock()
			threshold := remote.inlineThreshold
			remote.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": threshold})
			return
		}
		if r.URL.Path == "/handoff-original-part" {
			remote.mu.Lock()
			defer remote.mu.Unlock()
			data := patchOriginal
			if remote.shortPatchRead && len(data) > 0 {
				data = data[:len(data)-1]
			}
			http.ServeContent(w, r, "original.bin", time.Unix(1000, 0), bytes.NewReader(data))
			return
		}
		if r.URL.Path == "/v1/uploads/handoff-patch/complete" {
			remote.mu.Lock()
			defer remote.mu.Unlock()
			partStart := (patchPart - 1) * patchPartSize
			partSize := min(patchPartSize, uploadSize-partStart)
			if remote.rejectPatchDone || remote.revision != uploadRevision || int64(len(uploaded)) != partSize {
				w.WriteHeader(http.StatusConflict)
				return
			}
			data := bytes.Clone(patchOriginal[:uploadSize])
			copy(data[partStart:], uploaded)
			remote.data, remote.revision, remote.puts = data, remote.revision+1, remote.puts+1
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": remote.revision})
			return
		}
		if r.URL.Path == "/v2/uploads/initiate" {
			var request struct {
				TotalSize int64 `json:"total_size"`
				Expected  int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if request.Expected != remote.revision {
				w.WriteHeader(http.StatusConflict)
				return
			}
			uploadSize, uploadRevision, uploaded = request.TotalSize, request.Expected, nil
			if uploadSize <= 0 || uploadSize > DefaultPartSize {
				t.Errorf("fixture only accepts one bounded part: %d", uploadSize)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id": "handoff", "key": "handoff", "part_size": DefaultPartSize, "total_parts": 1,
			})
			return
		}
		if r.URL.Path == "/v2/uploads/handoff/presign-batch" || r.URL.Path == "/v2/uploads/handoff/presign" {
			remote.mu.Lock()
			defer remote.mu.Unlock()
			part := map[string]any{"number": 1, "url": server.URL + "/handoff-part", "size": uploadSize}
			if r.URL.Path == "/v2/uploads/handoff/presign-batch" {
				_ = json.NewEncoder(w).Encode(map[string]any{"parts": []map[string]any{part}})
			} else {
				_ = json.NewEncoder(w).Encode(part)
			}
			return
		}
		if r.URL.Path == "/handoff-part" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote.mu.Lock()
			uploaded = data
			remote.mu.Unlock()
			w.Header().Set("ETag", "handoff-part")
			return
		}
		if r.URL.Path == "/v2/uploads/handoff/complete" {
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if remote.revision != uploadRevision || int64(len(uploaded)) != uploadSize {
				w.WriteHeader(http.StatusConflict)
				return
			}
			remote.data, remote.revision, remote.puts = bytes.Clone(uploaded), remote.revision+1, remote.puts+1
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": remote.revision})
			return
		}
		if r.URL.Path == "/v2/uploads/handoff/abort" {
			return
		}
		if r.URL.Path == "/v1/fs/" && r.Method == http.MethodHead {
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Resource-ID", "handoff-root")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
			return
		}
		if r.URL.Path != "/v1/fs/file.bin" && r.URL.Path != "/v1/fs/alias.bin" {
			http.NotFound(w, r)
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
		w.Header().Set("X-Dat9-Resource-ID", remote.resource)
		w.Header().Set("X-Dat9-Nlink", strconv.Itoa(remote.nlink))
		w.Header().Set("X-Dat9-IsDir", "false")
		if int64(len(remote.data)) >= remote.inlineThreshold {
			w.Header().Set("X-Dat9-Storage-Type", "s3")
		} else {
			w.Header().Set("X-Dat9-Storage-Type", "db9")
		}
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remote.data)))
		case http.MethodGet:
			http.ServeContent(w, r, "file.bin", time.Unix(1000, 0), bytes.NewReader(remote.data))
		case http.MethodPut:
			expected := r.Header.Get("X-Dat9-Expected-Revision")
			if expected != "" && expected != strconv.FormatInt(remote.revision, 10) {
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			remote.data, remote.revision, remote.puts = data, remote.revision+1, remote.puts+1
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			_, _ = fmt.Fprintf(w, `{"status":"ok","revision":%d}`, remote.revision)
		case http.MethodPatch:
			var request struct {
				NewSize  int64 `json:"new_size"`
				Expected int64 `json:"expected_revision"`
				PartSize int64 `json:"part_size"`
				Parts    []int `json:"dirty_parts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Parts) != 1 || request.PartSize <= 0 || request.NewSize <= 0 || request.NewSize > int64(len(remote.data)) {
				t.Errorf("fixture requires one retained PATCH part: %+v, err=%v", request, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if request.Expected != remote.revision {
				w.WriteHeader(http.StatusConflict)
				return
			}
			patchPart, patchPartSize = int64(request.Parts[0]), request.PartSize
			partStart := (patchPart - 1) * patchPartSize
			if partStart < 0 || partStart >= request.NewSize {
				t.Errorf("invalid retained PATCH part: %+v", request)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			uploadSize, uploadRevision, uploaded = request.NewSize, request.Expected, nil
			patchOriginal = bytes.Clone(remote.data)
			remote.patchParts = append(remote.patchParts, request.Parts[0])
			part := map[string]any{"number": patchPart, "url": server.URL + "/handoff-part", "size": min(patchPartSize, uploadSize-partStart)}
			if !remote.omitPatchRead {
				part["read_url"] = server.URL + "/handoff-original-part"
				part["read_headers"] = map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", partStart, min(partStart+patchPartSize, uploadSize)-1)}
			}
			var copied []int
			for pn := 1; int64(pn-1)*patchPartSize < uploadSize; pn++ {
				if int64(pn) != patchPart {
					copied = append(copied, pn)
				}
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "handoff-patch", "part_size": patchPartSize, "upload_parts": []map[string]any{part}, "copied_parts": copied})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.client.SetSmallFileThresholdForTests(32 << 20)
	var err error
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, cache, 1, "/")
	uploader.OnSuccess = fs.onWriteBackUploadSuccess
	uploader.SnapshotStagingGens = fs.snapshotStagingGens
	fs.SetWriteBack(cache, uploader)
	uploader.OnDataCommitted = fs.onWriteBackDataCommitted
	fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue.PathLock = fs.lockRemoteCommitPath
	fs.commitQueue.OnSuccess, fs.commitQueue.OnUploaded = fs.onCommitQueueSuccess, fs.onCommitQueueUploaded
	fs.commitQueue.OnCleanup, fs.commitQueue.OnDiscard = fs.onCommitQueueCleanup, fs.onCommitQueueDiscard
	fs.commitQueue.IsSuperseded, fs.commitQueue.DurableWatermark = fs.commitEntrySuperseded, fs.latestCommittedRevision
	t.Cleanup(func() {
		for _, fh := range fs.openHandles.SnapshotPath("/file.bin") {
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
		}
		fs.commitQueue.DrainAll()
		uploader.DrainAll()
	})
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(initial)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.inodes.SetIdentity(ino, remote.resource, 1)
	return fs, ino, remote
}
func truncateHandoffOpen(t *testing.T, fs *Dat9FS, ino uint64, flags int) uint64 {
	t.Helper()
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(flags)}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	return out.Fh
}
func truncateHandoffTruncate(t *testing.T, fs *Dat9FS, ino, id uint64, size int) {
	t.Helper()
	var out gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Valid: gofuse.FATTR_FH | gofuse.FATTR_SIZE, Size: uint64(size)}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
}
func truncateHandoffRead(fs *Dat9FS, ino, id uint64, offset int64, size int) ([]byte, gofuse.Status) {
	buf := make([]byte, size)
	result, st := fs.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id, Offset: uint64(offset), Size: uint32(size)}, buf)
	if st != gofuse.OK || result == nil {
		return nil, st
	}
	defer result.Done()
	data, status := result.Bytes(buf)
	if status != gofuse.OK {
		return nil, status
	}
	return bytes.Clone(data), st
}
func TestFtruncateSettledObserverExternalUpdate(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		truncated, warm, refreshAttr bool
		payload                      string
	}{
		{"no-truncate", false, true, true, "world"},
		{"same-size", true, true, true, "world"},
		{"grow", true, true, true, "world extended"},
		{"empty", true, true, true, ""},
		{"sse-only", true, true, false, "world extended"},
		{"cold-pinned", true, false, false, "world"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			writer := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			if !tc.warm {
				if err := fs.shadowStore.WriteFull("/file.bin", []byte("hello world"), 1); err != nil {
					t.Fatal(err)
				}
			}
			reader := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			before := []byte("hello world")
			if tc.truncated {
				truncateHandoffTruncate(t, fs, ino, writer, 5)
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer}); st != gofuse.OK {
					t.Fatalf("local Fsync=%v", st)
				}
				before = []byte("hello")
			}
			if tc.warm {
				if got, st := truncateHandoffRead(fs, ino, reader, 0, 64); st != gofuse.OK || !bytes.Equal(got, before) {
					t.Fatalf("PREMISE local settled bytes=%q/%v", got, st)
				}
			}
			_, rev, _ := remote.snapshot()
			external := newTestClient(fs.client.BaseURL())
			external.SetSmallFileThresholdForTests(32 << 20)
			newRev, err := external.WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte(tc.payload), rev)
			if err != nil {
				t.Fatal(err)
			}
			data, actualRev, _ := remote.snapshot()
			if actualRev != rev+1 || newRev != actualRev || string(data) != tc.payload {
				t.Fatal("PREMISE external SDK write did not commit")
			}
			watcher := &SSEWatcher{fs: fs, actor: "local-mount"}
			watcher.handleChange(&client.ChangeEvent{Seq: 9, Path: "/file.bin", Op: "write", Actor: "other-client"})
			if tc.refreshAttr {
				var attr gofuse.AttrOut
				if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &attr); st != gofuse.OK {
					t.Fatalf("PREMISE GetAttr after SSE=%v", st)
				}
			}
			for attempt := 0; attempt < 3; attempt++ {
				got, st := truncateHandoffRead(fs, ino, reader, 0, 64)
				if st != gofuse.OK || string(got) != tc.payload {
					t.Errorf("external update unreadable on attempt %d: %q/%v, want %q/OK", attempt, got, st, tc.payload)
				}
			}
			if tc.truncated && truncateHandoffEvent(fs, reader) != nil {
				t.Error("settled observation was not retired")
			}
		})
	}
}
func TestFtruncateIncompleteRootAfterRelease(t *testing.T) {
	for _, shape := range []string{"large", "lazy-large", "lazy-bounded"} {
		for _, readonly := range []bool{true, false} {
			t.Run(fmt.Sprintf("shape=%s/ro=%t", shape, readonly), func(t *testing.T) {
				originalSize, retained := 4<<20, 2<<20
				if shape == "lazy-large" {
					originalSize, retained = 16<<20, 9<<20
				}
				if shape == "lazy-bounded" {
					originalSize, retained = 11, 11
				}
				initial := make([]byte, originalSize)
				for i := range initial {
					initial[i] = byte(i*37 + i/251)
				}
				fs, ino, remote := truncateHandoffFS(t, initial)
				writer := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				flags := syscall.O_RDWR
				if readonly {
					flags = syscall.O_RDONLY
				}
				reader := truncateHandoffOpen(t, fs, ino, flags)
				truncateHandoffTruncate(t, fs, ino, writer, retained)
				source, _ := fs.fileHandles.Get(writer)
				source.Lock()
				isLazy := !source.Dirty.CanMaterializeFull()
				partSize := source.Dirty.PartSize()
				source.Unlock()
				event := truncateHandoffEvent(fs, reader)
				t.Logf("ROOT shape=%s actualLazy=%t partSize=%d event=%v", shape, isLazy, partSize, event)
				if shape != "large" && !isLazy {
					t.Fatal("PREMISE lazy root was already materialized")
				}
				if event == nil || event.complete {
					t.Fatal("PREMISE root event is complete")
				}
				want := bytes.Clone(initial[:retained])
				if shape != "large" {
					got, st := truncateHandoffRead(fs, ino, writer, 0, retained)
					if st != gofuse.OK || !bytes.Equal(got, want) {
						t.Fatalf("PREMISE real source Read did not materialize retained bytes: got=%d/%v want=%d", len(got), st, len(want))
					}
					source.Lock()
					complete := source.Dirty.CanMaterializeFull()
					source.Unlock()
					if !complete {
						t.Fatal("PREMISE source Read did not materialize the full retained image")
					}
					if truncateHandoffEvent(fs, reader).complete {
						t.Fatal("PREMISE immutable incomplete root changed during Read")
					}
					t.Logf("MATERIALIZED shape=%s through actual Read; original event remains incomplete", shape)
				}
				if shape == "lazy-bounded" {
					if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer}, []byte("X")); n != 1 || st != gofuse.OK {
						t.Fatalf("PREMISE descendant Write=%d/%v", n, st)
					}
					want[0] = 'X'
				}
				gate, entered := make(chan struct{}), make(chan struct{})
				var gateOnce, enteredOnce sync.Once
				release := func() { gateOnce.Do(func() { close(gate) }) }
				t.Cleanup(release)
				originalLock := fs.commitQueue.PathLock
				fs.commitQueue.PathLock = func(path string) func() {
					if path == "/file.bin" {
						enteredOnce.Do(func() { close(entered) })
						<-gate
					}
					return originalLock(path)
				}
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer}); st != gofuse.OK {
					t.Fatalf("PREMISE real Flush=%v", st)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer})
				if _, exists := fs.fileHandles.Get(writer); exists {
					t.Fatal("PREMISE source is still registered")
				}
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("PREMISE actual Release did not enqueue a gated upload")
				}
				meta, exists := fs.pendingIndex.GetMeta("/file.bin")
				if !exists || meta.Size != int64(retained) {
					t.Fatalf("PREMISE pending root absent: %+v", meta)
				}
				shadow, err := fs.shadowStore.ReadAll("/file.bin")
				if err != nil || !bytes.Equal(shadow, want) {
					t.Fatalf("PREMISE exact retained shadow missing: size=%d err=%v", len(shadow), err)
				}
				_, rev, puts := remote.snapshot()
				if rev != 1 || puts != 0 {
					t.Fatalf("PREMISE upload already landed: rev=%d puts=%d", rev, puts)
				}
				for _, offset := range []int64{0, max(0, int64(retained-17)), int64(retained)} {
					got, st := truncateHandoffRead(fs, ino, reader, offset, 64)
					end := min(offset+64, int64(retained))
					expected := want[offset:end]
					t.Logf("STAGED shape=%s ro=%t sourceGone=true staged=%d remoteRev=%d offset=%d got=%d status=%v", shape, readonly, len(shadow), rev, offset, len(got), st)
					if st != gofuse.OK || !bytes.Equal(got, expected) {
						t.Errorf("staged root unreadable after actual Release: offset=%d bytes=%d/%v want=%d/OK", offset, len(got), st, len(expected))
					}
				}
				release()
			})
		}
	}
}

func truncateHandoffEvent(fs *Dat9FS, id uint64) *ftruncateInheritance {
	fh, _ := fs.fileHandles.Get(id)
	return fh.pendingFtruncate.Load()
}

func TestFtruncateSettledObserverKeepsPendingChild(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	writer := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	reader := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
	truncateHandoffTruncate(t, fs, ino, writer, 5)
	hardlinkSync(t, fs, ino, writer)
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writer}, []byte("H")); n != 1 || st != gofuse.OK {
		t.Fatalf("write=%d/%v", n, st)
	}
	_, rev, _ := remote.snapshot()
	if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("world"), rev); err != nil {
		t.Fatal(err)
	}
	watcher := &SSEWatcher{fs: fs, actor: "local"}
	watcher.handleChange(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"})
	if got, st := truncateHandoffRead(fs, ino, reader, 0, 64); st != gofuse.OK || string(got) != "Hello" {
		t.Fatalf("pending child hidden by remote update: %q/%v", got, st)
	}
	if truncateHandoffEvent(fs, reader) == nil {
		t.Fatal("uncommitted child lost its observation")
	}
}

func TestFtruncateSettledObserverRejectsReplacement(t *testing.T) {
	fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
	writer := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	reader := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
	truncateHandoffTruncate(t, fs, ino, writer, 5)
	hardlinkSync(t, fs, ino, writer)
	remote.mu.Lock()
	remote.data, remote.revision, remote.resource = []byte("replacement"), 3, "replacement-resource"
	remote.mu.Unlock()
	watcher := &SSEWatcher{fs: fs, actor: "local"}
	watcher.handleChange(&client.ChangeEvent{Path: "/file.bin", Op: "write", Actor: "external"})
	if got, st := truncateHandoffRead(fs, ino, reader, 0, 64); st != gofuse.EAGAIN || len(got) != 0 {
		t.Fatalf("replacement supplied bytes: %q/%v", got, st)
	}
	if truncateHandoffEvent(fs, reader) == nil {
		t.Fatal("replacement retired the observation")
	}
}

// Fault tests use this exact, naturally incomplete same-sequence root. The
// ordinary regression above separately exercises real Flush and Release.
func newIncompleteStagedFtruncateFS(t *testing.T) (*Dat9FS, uint64, uint64) {
	t.Helper()
	fs, ino, writerID, _, _ := newFtruncateCommitFS(t)
	readerID := openFtruncateObserver(t, fs, ino)
	reviewFtruncate(t, fs, ino, writerID, 2<<20)
	event := truncateHandoffEvent(fs, readerID)
	if event == nil || event.complete || event.id == "" {
		t.Fatal("expected a trusted incomplete root")
	}
	writer, _ := fs.fileHandles.Get(writerID)
	writer.Lock()
	err := fs.stageShadowLocked(writer, true)
	fs.releaseHandleRemoteCommitPathLocked(writer)
	writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fs.deleteFileHandle(writerID, writer)
	meta, ok := fs.pendingIndex.GetMeta("/file.bin")
	if !ok || meta.MutationSeq != event.seq || meta.SnapshotID != event.id {
		t.Fatal("staged root does not match the original event")
	}
	return fs, ino, readerID
}

func TestFtruncateIncompleteRootMissingPayload(t *testing.T) {
	fs, ino, reader := newIncompleteStagedFtruncateFS(t)
	fs.pendingIndex.Remove("/file.bin")
	fs.shadowStore.Remove("/file.bin")
	for _, offset := range []int64{0, 2 << 20} {
		got, st := truncateHandoffRead(fs, ino, reader, offset, 64)
		if st != gofuse.EAGAIN || len(got) != 0 {
			t.Fatalf("missing payload at %d read=%q/%v", offset, got, st)
		}
	}
}
