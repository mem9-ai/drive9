package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newGVisorMutationHandle(t *testing.T, fs *Dat9FS, path string, ino uint64) (uint64, *FileHandle) {
	t.Helper()
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Dirty:    fs.newWriteBuffer(path, maxPreloadSize, 0),
		IsNew:    true,
		ZeroBase: true,
	}
	if err := fh.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = fs.markDirtySize(ino, 0)
	return fs.allocateFileHandle(fh), fh
}

func TestGVisorCompatReleaseDiscardsSupersededCrossHandleTruncate(t *testing.T) {
	want := []byte("newer content from the create handle")

	var (
		mu       sync.Mutex
		remote   []byte
		revision int64
		putCount int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			expected, err := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				t.Errorf("parse expected revision: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if expected != revision {
				t.Errorf("expected revision = %d, remote revision = %d", expected, revision)
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read PUT body: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			remote = append(remote[:0], body...)
			revision++
			putCount++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": revision})
		case http.MethodHead:
			if revision == 0 {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(remote)
		default:
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{
		GVisorCompat: true,
		SyncMode:     SyncInteractive,
		WritePolicy:  WritePolicyWriteBack,
	}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)

	var created gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL),
		Mode:     defaultRegularFileMode,
	}, "SKILL.md", &created); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Flags:    uint32(syscall.O_WRONLY),
	}, &opened); st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}

	var attrOut gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: created.NodeId},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       opened.Fh,
			Size:     0,
		},
	}, &attrOut); st != gofuse.OK {
		t.Fatalf("SetAttr(B) status = %v, want OK", st)
	}

	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
	}, want); st != gofuse.OK || int(n) != len(want) {
		t.Fatalf("Write(A) = %d, %v; want %d, OK", n, st, len(want))
	}

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	})

	var gotAttr gofuse.AttrOut
	if st := fs.GetAttr(nil, &gofuse.GetAttrIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
	}, &gotAttr); st != gofuse.OK {
		t.Fatalf("GetAttr after Release(A) status = %v, want OK", st)
	}
	if got := int64(gotAttr.Size); got != int64(len(want)) {
		t.Fatalf("GetAttr size after Release(A) = %d, want %d", got, len(want))
	}

	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       opened.Fh,
	})

	mu.Lock()
	gotRemote := append([]byte(nil), remote...)
	gotRevision := revision
	gotPutCount := putCount
	mu.Unlock()
	if gotPutCount != 1 {
		t.Fatalf("remote PUT count = %d, want 1", gotPutCount)
	}
	if gotRevision != 1 {
		t.Fatalf("remote revision = %d, want 1", gotRevision)
	}
	if string(gotRemote) != string(want) {
		t.Fatalf("remote content = %q, want %q", gotRemote, want)
	}
}

func TestGVisorCompatStageAfterCommitFenceDiscardsSupersededHandle(t *testing.T) {
	const filePath = "/commit-fence.txt"
	newer := []byte("newer committed content")
	putStarted := make(chan struct{})
	releasePut := make(chan struct{})
	var releaseOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs"+filePath {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !bytes.Equal(body, newer) {
			t.Errorf("PUT body = %q, want %q", body, newer)
		}
		close(putStarted)
		<-releasePut
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true, SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releasePut) })
		cq.DrainAll()
	})

	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	_, stale := newGVisorMutationHandle(t, fs, filePath, ino)
	newerSeq := fs.markDirtySize(ino, int64(len(newer)))
	if err := shadow.WriteFull(filePath, newer, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(filePath, int64(len(newer)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:        filePath,
		Inode:       ino,
		MutationSeq: newerSeq,
		Size:        int64(len(newer)),
		Kind:        PendingNew,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-putStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for newer upload")
	}

	staleLocked := make(chan struct{})
	stageDone := make(chan error, 1)
	go func() {
		stale.Lock()
		close(staleLocked)
		err := fs.stageShadowForQueuedCommitLocked(stale, true)
		stale.Unlock()
		stageDone <- err
	}()
	<-staleLocked
	releaseOnce.Do(func() { close(releasePut) })

	select {
	case err := <-stageDone:
		if err != nil {
			t.Fatalf("stage stale handle: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale handle did not resume after newer commit")
	}

	stale.Lock()
	defer stale.Unlock()
	if stale.DirtySeq != 0 || stale.Dirty.HasDirtyParts() {
		t.Fatalf("stale handle remains dirty after commit-fence wait: seq=%d dirty=%t", stale.DirtySeq, stale.Dirty.HasDirtyParts())
	}
	if stale.BaseRev != 1 || stale.Dirty.Size() != int64(len(newer)) {
		t.Fatalf("stale handle base/size = %d/%d, want 1/%d", stale.BaseRev, stale.Dirty.Size(), len(newer))
	}
	if shadow.Has(filePath) {
		staged, err := shadow.ReadAll(filePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(staged, newer) {
			t.Fatalf("superseded handle replaced newer shadow with %q", staged)
		}
	}
}

func TestGVisorCompatCommitQueueSkipsSupersededEntryBeforeUpload(t *testing.T) {
	const filePath = "/superseded-before-upload.txt"
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
			return
		}
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	staleSeq := fs.markDirtySize(ino, int64(len("stale")))
	newerSeq := fs.markDirtySize(ino, int64(len("newer")))
	fs.recordCommittedMutation(ino, newerSeq, 1, int64(len("newer")))
	if err := shadow.WriteFull(filePath, []byte("stale"), 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, int64(len("stale")), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.IsSuperseded = fs.commitEntrySuperseded
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{
		Path:            filePath,
		Inode:           ino,
		MutationSeq:     staleSeq,
		PendingIndexGen: pendingGen,
		ShadowGen:       shadow.ActiveGeneration(filePath),
		Size:            int64(len("stale")),
		Kind:            PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := cq.WaitIdle(waitCtx); err != nil {
		t.Fatal(err)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("PUT calls = %d, want 0", got)
	}
	if shadow.Has(filePath) || pending.HasPending(filePath) {
		t.Fatal("superseded entry staging was not removed")
	}
}

func TestGVisorCompatCommitQueueOrdersHardlinkAliasesByInode(t *testing.T) {
	const ino = uint64(42)
	older := &CommitEntry{Path: "/alias-b", Inode: ino, MutationSeq: 1}
	newer := &CommitEntry{Path: "/alias-a", Inode: ino, MutationSeq: 2}

	cq := &CommitQueue{
		queue:                   []*CommitEntry{newer, older},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{newer.Path: {newer: {}}, older.Path: {older: {}}},
		inFlight:                map[string]*CommitEntry{newer.Path: newer},
		serializeMutationInodes: true,
	}
	if !cq.hasNewerMutation(older.Path, ino, older.MutationSeq) {
		t.Fatal("newer queued hardlink alias was not visible to stale staging")
	}
	if cq.tryBeginInFlight(older) {
		t.Fatal("hardlink alias upload began while the same inode was already in flight")
	}

	cq = &CommitQueue{
		queue:                   []*CommitEntry{older, newer},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{newer.Path: {newer: {}}, older.Path: {older: {}}},
		inFlight:                make(map[string]*CommitEntry),
		serializeMutationInodes: true,
	}
	if cq.tryBeginInFlight(newer) {
		t.Fatal("newer hardlink alias bypassed the older queued inode mutation")
	}
	if !cq.tryBeginInFlight(older) {
		t.Fatal("oldest queued inode mutation did not begin")
	}
}

func TestGVisorCompatDisabledCommitQueueDoesNotOrderHardlinkAliases(t *testing.T) {
	const ino = uint64(42)
	older := &CommitEntry{Path: "/alias-b", Inode: ino, MutationSeq: 1}
	newer := &CommitEntry{Path: "/alias-a", Inode: ino, MutationSeq: 2}
	cq := &CommitQueue{
		queue:        []*CommitEntry{newer, older},
		queuedByPath: map[string]map[*CommitEntry]struct{}{newer.Path: {newer: {}}, older.Path: {older: {}}},
		inFlight:     map[string]*CommitEntry{newer.Path: newer},
	}

	if cq.hasNewerMutation(older.Path, ino, older.MutationSeq) {
		t.Fatal("disabled compatibility mode inspected another hardlink path")
	}
	if !cq.tryBeginInFlight(older) {
		t.Fatal("disabled compatibility mode serialized hardlink aliases")
	}
}

func TestGVisorCompatSupersededEntryDoesNotRemoveNewerStaging(t *testing.T) {
	const filePath = "/superseded-generation.txt"
	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	staleSeq := fs.markDirtySize(ino, int64(len("stale")))
	if err := shadow.WriteFull(filePath, []byte("stale"), 0); err != nil {
		t.Fatal(err)
	}
	stalePendingGen, err := pending.PutWithBaseRev(filePath, int64(len("stale")), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path:            filePath,
		Inode:           ino,
		MutationSeq:     staleSeq,
		PendingIndexGen: stalePendingGen,
		ShadowGen:       shadow.ActiveGeneration(filePath),
		Size:            int64(len("stale")),
		Kind:            PendingNew,
	}

	newerSeq := fs.markDirtySize(ino, int64(len("newer")))
	fs.recordCommittedMutation(ino, newerSeq, 1, int64(len("newer")))
	if err := shadow.WriteFull(filePath, []byte("newer"), 0); err != nil {
		t.Fatal(err)
	}
	newPendingGen, err := pending.PutWithBaseRev(filePath, int64(len("newer")), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient("http://127.0.0.1"), shadow, pending, nil, 1, 8)
	cq.IsSuperseded = fs.commitEntrySuperseded
	defer cq.DrainAll()
	if err := cq.CommitNow(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	got, err := shadow.ReadAll(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("newer")) {
		t.Fatalf("shadow content = %q, want newer", got)
	}
	if gotGen := pending.Generation(filePath); gotGen != newPendingGen {
		t.Fatalf("pending generation = %d, want newer generation %d", gotGen, newPendingGen)
	}
}

func TestGVisorCompatCommitQueueSkipsEntrySupersededBeforeLWW(t *testing.T) {
	const filePath = "/superseded-before-lww.txt"
	localData := []byte("stale local")
	remoteData := []byte("newer remote")
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var releaseOnce sync.Once
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			if putCalls.Add(1) == 1 {
				http.Error(w, "revision conflict", http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 3})
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			close(readStarted)
			<-releaseRead
			_, _ = w.Write(remoteData)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(filePath, false, int64(len(localData)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	seq := fs.markDirtySize(ino, int64(len(localData)))
	if err := shadow.WriteFull(filePath, localData, 1); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, int64(len(localData)), PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.IsSuperseded = fs.commitEntrySuperseded
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseRead) })
		cq.DrainAll()
	})
	if err := cq.Enqueue(&CommitEntry{
		Path:            filePath,
		Inode:           ino,
		MutationSeq:     seq,
		PendingIndexGen: pendingGen,
		ShadowGen:       shadow.ActiveGeneration(filePath),
		BaseRev:         1,
		Size:            int64(len(localData)),
		Kind:            PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-readStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for conflict read")
	}
	newerSeq := fs.markDirtySize(ino, int64(len("latest local")))
	fs.recordCommittedMutation(ino, newerSeq, 2, int64(len("latest local")))
	releaseOnce.Do(func() { close(releaseRead) })
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := cq.WaitIdle(waitCtx); err != nil {
		t.Fatal(err)
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want only initial conflicting PUT", got)
	}
	if shadow.Has(filePath) || pending.HasPending(filePath) {
		t.Fatal("superseded LWW entry staging was not removed")
	}
}

func TestGVisorCompatCommitQueueFiltersSupersededBatchEntry(t *testing.T) {
	const (
		stalePath = "/batch-stale.txt"
		validPath = "/batch-valid.txt"
	)
	var batchCalls atomic.Int32
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-write":
			batchCalls.Add(1)
			http.Error(w, "superseded entry must be filtered before batching", http.StatusInternalServerError)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+validPath:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	staleIno := fs.inodes.Lookup(stalePath, false, 5, time.Now())
	staleSeq := fs.markDirtySize(staleIno, 5)
	newerSeq := fs.markDirtySize(staleIno, 6)
	fs.recordCommittedMutation(staleIno, newerSeq, 1, 6)
	validIno := fs.inodes.Lookup(validPath, false, 5, time.Now())
	validSeq := fs.markDirtySize(validIno, 5)
	if err := shadow.WriteFull(stalePath, []byte("stale"), 0); err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(validPath, []byte("valid"), 0); err != nil {
		t.Fatal(err)
	}
	stalePendingGen, err := pending.PutWithBaseRev(stalePath, 5, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	validPendingGen, err := pending.PutWithBaseRev(validPath, 5, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.IsSuperseded = fs.commitEntrySuperseded
	cq.ConfigureBatchWrite(20*time.Millisecond, 8, 1<<20)
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{
		Path:            stalePath,
		Inode:           staleIno,
		MutationSeq:     staleSeq,
		PendingIndexGen: stalePendingGen,
		ShadowGen:       shadow.ActiveGeneration(stalePath),
		Size:            5,
		Kind:            PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:            validPath,
		Inode:           validIno,
		MutationSeq:     validSeq,
		PendingIndexGen: validPendingGen,
		ShadowGen:       shadow.ActiveGeneration(validPath),
		Size:            5,
		Kind:            PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := cq.WaitIdle(waitCtx); err != nil {
		t.Fatal(err)
	}
	if got := batchCalls.Load(); got != 0 {
		t.Fatalf("batch calls = %d, want 0 after filtering to one entry", got)
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("valid PUT calls = %d, want 1", got)
	}
	if shadow.Has(stalePath) || pending.HasPending(stalePath) {
		t.Fatal("superseded batch staging was not removed")
	}
}

func TestGVisorCompatDisabledCommitQueueDoesNotFilterMutationSequence(t *testing.T) {
	const filePath = "/disabled-sequence-filter.txt"
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		putCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(filePath, []byte("stale"), 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, 5, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup(filePath, false, 5, time.Now())
	staleSeq := fs.markDirtySize(ino, 5)
	fs.markDirtySize(ino, 6)

	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.IsSuperseded = fs.commitEntrySuperseded
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{
		Path:            filePath,
		Inode:           ino,
		MutationSeq:     staleSeq,
		PendingIndexGen: pendingGen,
		ShadowGen:       shadow.ActiveGeneration(filePath),
		Size:            5,
		Kind:            PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := cq.WaitIdle(waitCtx); err != nil {
		t.Fatal(err)
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1 with GVisorCompat disabled", got)
	}
}

func TestGVisorCompatUnstagedWriteDoesNotSupersedeDurableEntry(t *testing.T) {
	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	ino := fs.inodes.Lookup("/durable-before-private.txt", false, 0, time.Now())
	durableSeq := fs.markDirtySize(ino, 7)
	fs.markDirtySize(ino, 8)
	entry := &CommitEntry{Inode: ino, MutationSeq: durableSeq}
	if fs.commitEntrySuperseded(entry) {
		t.Fatal("unstaged private write superseded an older durable entry")
	}
}

func TestGVisorCompatDiscardSupersededMutationRemovesOwnedStaging(t *testing.T) {
	const filePath = "/owned-staging.txt"
	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	_, stale := newGVisorMutationHandle(t, fs, filePath, ino)
	if err := shadow.WriteFull(filePath, nil, 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, 0, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	stale.ShadowStageGen = shadow.ActiveGeneration(filePath)
	stale.PendingIndexGen = pendingGen
	newerSeq := fs.markDirtySize(ino, 9)
	fs.recordCommittedMutation(ino, newerSeq, 1, 9)
	stale.Lock()
	discarded := fs.discardSupersededMutationLocked(stale)
	stale.Unlock()
	if !discarded {
		t.Fatal("stale mutation was not discarded")
	}
	if shadow.Has(filePath) || pending.HasPending(filePath) {
		t.Fatal("discarded handle left generation-owned staging behind")
	}
}

func TestGVisorCompatLayerDiscardRebindsCommittedShadow(t *testing.T) {
	const filePath = "/layer-file.txt"
	want := []byte("layer committed content")
	opts := &MountOptions{GVisorCompat: true, LayerRef: "layer-test"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	_, stale := newGVisorMutationHandle(t, fs, filePath, ino)
	if err := shadow.WriteFull(filePath, want, 0); err != nil {
		t.Fatal(err)
	}
	newerSeq := fs.markDirtySize(ino, int64(len(want)))
	fs.recordCommittedMutation(ino, newerSeq, 0, int64(len(want)))
	stale.Lock()
	discarded := fs.discardSupersededMutationLocked(stale)
	got := stale.Dirty.Bytes()
	stale.Unlock()
	if !discarded {
		t.Fatal("layer sibling was not discarded")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("layer rebound content = %q, want %q", got, want)
	}
}

func TestGVisorCompatLayerQueueUploadRebindsExistingFileFromShadow(t *testing.T) {
	const filePath = "/layer-existing.txt"
	committed := []byte("layer committed content")
	opts := &MountOptions{GVisorCompat: true, LayerRef: "layer-test"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	ino := fs.inodes.Lookup(filePath, false, int64(len(committed)), time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	_, stale := newGVisorMutationHandle(t, fs, filePath, ino)
	if err := shadow.WriteFull(filePath, committed, 7); err != nil {
		t.Fatal(err)
	}
	newerSeq := fs.markDirtySize(ino, int64(len(committed)))
	fs.onCommitQueueUploaded(&CommitEntry{
		Path:        filePath,
		Inode:       ino,
		MutationSeq: newerSeq,
		BaseRev:     7,
		Size:        int64(len(committed)),
	}, 0)

	stale.Lock()
	discarded := fs.discardSupersededMutationLocked(stale)
	got := stale.Dirty.Bytes()
	stale.Unlock()
	if !discarded {
		t.Fatal("layer sibling was not discarded")
	}
	if !bytes.Equal(got, committed) {
		t.Fatalf("layer rebound content = %q, want %q", got, committed)
	}
}

func TestGVisorCompatMarkOpenHandlesUnlinkedSnapshotFailureIsGated(t *testing.T) {
	const filePath = "/snapshot-failure.txt"
	for _, tc := range []struct {
		name         string
		gvisorCompat bool
		wantErr      bool
		wantMarked   bool
	}{
		{name: "disabled", wantMarked: true},
		{name: "enabled", gvisorCompat: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "snapshot read failed", http.StatusInternalServerError)
			}))
			defer ts.Close()

			opts := &MountOptions{GVisorCompat: tc.gvisorCompat}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			ino := fs.inodes.Lookup(filePath, false, 4, time.Now())
			fh := &FileHandle{Ino: ino, Path: filePath}
			fs.allocateFileHandle(fh)

			marked, anyOpen, err := fs.markOpenHandlesUnlinked(context.Background(), filePath, true)
			if (err != nil) != tc.wantErr {
				t.Fatalf("markOpenHandlesUnlinked error = %v, want error=%t", err, tc.wantErr)
			}
			if anyOpen != tc.wantMarked || len(marked) != map[bool]int{false: 0, true: 1}[tc.wantMarked] {
				t.Fatalf("marked/anyOpen = %d/%t, want %t", len(marked), anyOpen, tc.wantMarked)
			}
			fh.Lock()
			gotUnlinked := fh.Unlinked
			fh.Unlock()
			if gotUnlinked != tc.wantMarked {
				t.Fatalf("handle unlinked = %t, want %t", gotUnlinked, tc.wantMarked)
			}
		})
	}
}

func TestGVisorCompatCommitNowOrdersHardlinkAliasesByInode(t *testing.T) {
	const ino = uint64(42)
	older := &CommitEntry{Path: "/alias-b", Inode: ino, MutationSeq: 1}
	newer := &CommitEntry{Path: "/alias-a", Inode: ino, MutationSeq: 2}
	cq := &CommitQueue{
		queue:                   []*CommitEntry{older},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{older.Path: {older: {}}},
		inFlight:                map[string]*CommitEntry{older.Path: older},
		serializeMutationInodes: true,
	}

	type result struct {
		release func()
		discard bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		release, discard, err := cq.beginImmediateMutationCommit(context.Background(), newer, false)
		done <- result{release: release, discard: discard, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("newer immediate commit started before older alias drained: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}

	cq.mu.Lock()
	delete(cq.inFlight, older.Path)
	cq.queue = nil
	cq.queuedByPath = map[string]map[*CommitEntry]struct{}{}
	cq.mu.Unlock()

	select {
	case got := <-done:
		if got.err != nil || got.discard || got.release == nil {
			t.Fatalf("newer immediate result = %+v, want acquired release", got)
		}
		got.release()
	case <-time.After(time.Second):
		t.Fatal("newer immediate commit did not resume after older alias drained")
	}

	cq = &CommitQueue{
		queue:                   []*CommitEntry{newer},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{newer.Path: {newer: {}}},
		inFlight:                make(map[string]*CommitEntry),
		serializeMutationInodes: true,
	}
	release, discard, err := cq.beginImmediateMutationCommit(context.Background(), older, false)
	if err != nil || !discard || release != nil {
		t.Fatalf("older immediate result = release:%v discard:%t err:%v, want discarded", release != nil, discard, err)
	}
}

func TestGVisorCompatPathLockedImmediateCommitDoesNotWaitForSamePathWorker(t *testing.T) {
	const ino = uint64(42)
	older := &CommitEntry{Path: "/same-path", Inode: ino, MutationSeq: 1}
	newer := &CommitEntry{Path: "/same-path", Inode: ino, MutationSeq: 2}
	queuedAlias := &CommitEntry{Path: "/alias-path", Inode: ino, MutationSeq: 1}
	cq := &CommitQueue{
		inFlight:                map[string]*CommitEntry{older.Path: older},
		queue:                   []*CommitEntry{queuedAlias},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{queuedAlias.Path: {queuedAlias: {}}},
		immediate:               make(map[*CommitEntry]struct{}),
		serializeMutationInodes: true,
	}

	done := make(chan struct {
		release func()
		discard bool
		err     error
	}, 1)
	go func() {
		release, discard, err := cq.beginImmediateMutationCommit(context.Background(), newer, true)
		done <- struct {
			release func()
			discard bool
			err     error
		}{release: release, discard: discard, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil || got.discard || got.release == nil {
			t.Fatalf("path-locked immediate result = %+v, want acquired release", got)
		}
		got.release()
	case <-time.After(time.Second):
		t.Fatal("path-locked immediate commit waited for same-path in-flight worker")
	}
}

func TestGVisorCompatPathLockedImmediateCommitDiscardsForNewerQueuedAlias(t *testing.T) {
	const ino = uint64(42)
	worker := &CommitEntry{Path: "/same-path", Inode: ino, MutationSeq: 1}
	entry := &CommitEntry{Path: "/same-path", Inode: ino, MutationSeq: 2}
	newerAlias := &CommitEntry{Path: "/alias-path", Inode: ino, MutationSeq: 3}
	cq := &CommitQueue{
		inFlight:                map[string]*CommitEntry{worker.Path: worker},
		queue:                   []*CommitEntry{newerAlias},
		queuedByPath:            map[string]map[*CommitEntry]struct{}{newerAlias.Path: {newerAlias: {}}},
		immediate:               make(map[*CommitEntry]struct{}),
		serializeMutationInodes: true,
	}

	release, discard, err := cq.beginImmediateMutationCommit(context.Background(), entry, true)
	if err != nil || !discard || release != nil {
		t.Fatalf("path-locked immediate result = release:%t discard:%t err:%v, want discarded by newer alias", release != nil, discard, err)
	}
}

func TestGVisorCompatImmediateCommitOccupiesPathAndCanBeCanceled(t *testing.T) {
	const filePath = "/immediate.txt"
	entry := &CommitEntry{Path: filePath, Inode: 42, MutationSeq: 1}
	cq := &CommitQueue{
		inFlight:                make(map[string]*CommitEntry),
		queuedByPath:            make(map[string]map[*CommitEntry]struct{}),
		immediate:               make(map[*CommitEntry]struct{}),
		serializeMutationInodes: true,
	}
	release, discard, err := cq.beginImmediateMutationCommit(context.Background(), entry, false)
	if err != nil || discard || release == nil {
		t.Fatalf("begin immediate = release:%t discard:%t err:%v", release != nil, discard, err)
	}
	if !cq.HasPath(filePath) {
		t.Fatal("immediate commit was invisible to HasPath")
	}
	if cq.WaitPathTimeout(filePath, 0) {
		t.Fatal("immediate commit was invisible to WaitPathTimeout")
	}
	if cq.WaitPrefixTimeout("/", 0) {
		t.Fatal("immediate commit was invisible to WaitPrefixTimeout")
	}
	if snap := cq.Snapshot(); snap.InFlight != 1 || snap.Pending != 1 {
		t.Fatalf("immediate commit snapshot = %+v, want one in-flight pending entry", snap)
	}
	cq.CancelPath(filePath)
	if !entry.canceled {
		t.Fatal("CancelPath did not mark immediate commit canceled")
	}
	release()
	if cq.HasPath(filePath) {
		t.Fatal("released immediate commit remained visible to HasPath")
	}
}

func TestGVisorCompatImmediateCancelPreservesLocalStaging(t *testing.T) {
	const filePath = "/immediate-preserve.txt"
	data := []byte("preserve this staged content")
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(filePath, data, 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, int64(len(data)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient("http://127.0.0.1"), shadow, pending, nil, 1, 8)
	cq.serializeMutationInodes = true
	defer cq.DrainAll()
	entry := &CommitEntry{
		Path:            filePath,
		Inode:           42,
		MutationSeq:     1,
		PendingIndexGen: pendingGen,
		ShadowGen:       shadow.ActiveGeneration(filePath),
		Size:            int64(len(data)),
		Kind:            PendingNew,
	}
	release, discard, err := cq.beginImmediateMutationCommit(context.Background(), entry, false)
	if err != nil || discard || release == nil {
		t.Fatalf("begin immediate = release:%t discard:%t err:%v", release != nil, discard, err)
	}
	defer release()

	cq.CancelPathPreserveLocal(filePath)
	if err := cq.commitNowClaimedPathLocked(context.Background(), entry); err != nil {
		t.Fatalf("commit canceled immediate entry: %v", err)
	}
	if !shadow.Has(filePath) {
		t.Fatal("CancelPathPreserveLocal removed the immediate entry shadow")
	}
	if !pending.HasPending(filePath) {
		t.Fatal("CancelPathPreserveLocal removed the immediate entry pending state")
	}
}

func TestGVisorCompatLayerShadowCommitDoesNotRelockPathFence(t *testing.T) {
	const filePath = "/layer-shadow.txt"
	data := []byte("layer shadow data")
	var pathLockCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-test/entries" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "layer-test", "path": filePath, "op": "upsert", "kind": "file"})
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true, LayerRef: "layer-test"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(filePath, data, 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, int64(len(data)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.SetLayerRef("layer-test")
	cq.serializeMutationInodes = true
	cq.PathLock = func(string) func() {
		pathLockCalls.Add(1)
		return func() {}
	}
	defer cq.DrainAll()
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	ino := fs.inodes.Lookup(filePath, false, int64(len(data)), time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:             ino,
		Path:            filePath,
		Dirty:           dirty,
		DirtySeq:        fs.markDirtySize(ino, int64(len(data))),
		ShadowSpill:     true,
		ShadowStageGen:  shadow.ActiveGeneration(filePath),
		PendingIndexGen: pendingGen,
		IsNew:           true,
	}
	fh.Lock()
	err = fs.commitLayerShadowLocked(context.Background(), fh, false, true)
	fh.Unlock()
	if err != nil {
		t.Fatalf("commitLayerShadowLocked: %v", err)
	}
	if got := pathLockCalls.Load(); got != 0 {
		t.Fatalf("path fence locks = %d, want 0 while already held", got)
	}
}

func TestLayerShadowCommitDoesNotRelockInheritedFence(t *testing.T) {
	const filePath = "/layer-shadow-inherited-fence.txt"
	data := []byte("layer shadow data")
	var pathLockCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-test/entries" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "layer-test", "path": filePath, "op": "upsert", "kind": "file"})
	}))
	defer ts.Close()

	opts := &MountOptions{LayerRef: "layer-test"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(filePath, data, 0); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, nil, nil, 1, 8)
	cq.SetLayerRef("layer-test")
	cq.PathLock = func(string) func() {
		pathLockCalls.Add(1)
		return func() {}
	}
	defer cq.DrainAll()
	fs.shadowStore = shadow
	fs.commitQueue = cq
	ino := fs.inodes.Lookup(filePath, false, int64(len(data)), time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:                ino,
		Path:               filePath,
		Dirty:              dirty,
		ShadowSpill:        true,
		ShadowStageGen:     shadow.ActiveGeneration(filePath),
		IsNew:              true,
		RemoteCommitUnlock: func() {},
	}
	fh.Lock()
	err = fs.commitLayerShadowLocked(context.Background(), fh, false, false)
	fh.Unlock()
	if err != nil {
		t.Fatalf("commitLayerShadowLocked: %v", err)
	}
	if got := pathLockCalls.Load(); got != 0 {
		t.Fatalf("path fence locks = %d, want 0 while inherited fence is held", got)
	}
}

func TestGVisorCompatLayerShadowCommitPreservesConcurrentWrite(t *testing.T) {
	const filePath = "/layer-shadow-concurrent.txt"
	original := []byte("original layer shadow data")
	updated := []byte("updated after layer commit started")
	uploadStarted := make(chan struct{})
	allowUpload := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-test/entries" {
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
			return
		}
		close(uploadStarted)
		<-allowUpload
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "layer-test", "path": filePath, "op": "upsert", "kind": "file"})
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true, LayerRef: "layer-test"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(filePath, original, 0); err != nil {
		t.Fatal(err)
	}
	pendingGen, err := pending.PutWithBaseRev(filePath, int64(len(original)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.SetLayerRef("layer-test")
	cq.serializeMutationInodes = true
	defer cq.DrainAll()
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	ino := fs.inodes.Lookup(filePath, false, int64(len(original)), time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, original); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:                ino,
		Path:               filePath,
		Dirty:              dirty,
		DirtySeq:           fs.markDirtySize(ino, int64(len(original))),
		ShadowSpill:        true,
		ShadowStageGen:     shadow.ActiveGeneration(filePath),
		PendingIndexGen:    pendingGen,
		IsNew:              true,
		RemoteCommitUnlock: func() {},
	}
	commitDone := make(chan error, 1)
	go func() {
		fh.Lock()
		err := fs.commitLayerShadowLocked(context.Background(), fh, false, false)
		fh.Unlock()
		commitDone <- err
	}()
	select {
	case <-uploadStarted:
	case err := <-commitDone:
		t.Fatalf("layer commit ended before upload started: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("layer commit did not start")
	}
	writeDone := make(chan error, 1)
	go func() {
		fh.Lock()
		unlockRemoteCommit := fs.lockHandleRemoteCommitPathLocked(fh)
		_, err := fh.Dirty.Write(0, updated)
		if err == nil {
			fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
		}
		unlockRemoteCommit()
		fh.Unlock()
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("concurrent Write failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Write did not complete")
	}
	close(allowUpload)
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatalf("commitLayerShadowLocked: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("layer commit did not complete")
	}
	fh.Lock()
	dirtySeq := fh.DirtySeq
	hasDirty := fh.Dirty.HasDirtyParts()
	fh.Unlock()
	if dirtySeq == 0 || !hasDirty {
		t.Fatalf("concurrent layer write was cleared: seq=%d dirty=%t", dirtySeq, hasDirty)
	}
}

func TestGVisorCompatReleaseFallbackUnlocksCommitPath(t *testing.T) {
	const filePath = "/release-fallback.txt"
	opts := &MountOptions{GVisorCompat: true, WritePolicy: WritePolicyWriteBack}
	opts.setDefaults()
	c := newTestClient("http://127.0.0.1")
	fs := NewDat9FS(c, opts)
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(c, cache, 1)
	t.Cleanup(uploader.DrainAll)
	fs.SetWriteBack(cache, uploader)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(filePath, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)
	cq.mu.Lock()
	cq.maxPending = 0
	cq.mu.Unlock()
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq

	ino := fs.inodes.Lookup(filePath, false, 4, time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	var unlocks atomic.Int32
	fh := &FileHandle{
		Ino:                ino,
		Path:               filePath,
		Dirty:              dirty,
		DirtySeq:           1,
		WriteBackSeq:       1,
		RemoteCommitUnlock: func() { unlocks.Add(1) },
	}
	fhID := fs.allocateFileHandle(fh)

	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID})
	if got := unlocks.Load(); got != 1 {
		t.Fatalf("commit-path unlock calls = %d, want 1 after fallback failure", got)
	}
}

func TestGVisorCompatQueueSuccessSynthesizesUnknownRevision(t *testing.T) {
	const filePath = "/unknown-revision.txt"
	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	ino := fs.inodes.Lookup(filePath, false, 11, time.Now())
	seq := fs.markDirtySize(ino, 11)
	fs.onCommitQueueSuccess(&CommitEntry{Path: filePath, Inode: ino, MutationSeq: seq, Size: 11, Kind: PendingNew}, 0)
	state, ok := fs.committedMutation(ino)
	if !ok || state.committedRevision != 1 {
		t.Fatalf("committed mutation state = %+v, %t; want synthesized revision 1", state, ok)
	}
}

func TestGVisorCompatDisabledQueueUploadDoesNotSynthesizeRevision(t *testing.T) {
	const filePath = "/disabled-unknown-revision.txt"
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	ino := fs.inodes.Lookup(filePath, false, 11, time.Now())
	fh := &FileHandle{
		Ino:     ino,
		Path:    filePath,
		Dirty:   fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		BaseRev: 0,
	}
	fs.allocateFileHandle(fh)
	fs.onCommitQueueUploaded(&CommitEntry{Path: filePath, Inode: ino, MutationSeq: 1, Size: 11, Kind: PendingNew}, 0)

	fh.Lock()
	gotRevision := fh.BaseRev
	fh.Unlock()
	if gotRevision != 0 {
		t.Fatalf("non-gVisor queue upload synthesized handle revision %d, want 0", gotRevision)
	}
}

func TestGVisorCompatSameHandleTruncateThenWriteRemainsCorrect(t *testing.T) {
	want := []byte("same handle content")
	var (
		mu       sync.Mutex
		putCount int
		putBody  []byte
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mu.Lock()
		putCount++
		putBody = append(putBody[:0], body...)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
	}))
	defer ts.Close()

	opts := &MountOptions{
		GVisorCompat: true,
		SyncMode:     SyncInteractive,
		WritePolicy:  WritePolicyWriteBack,
	}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1024)
	fs := NewDat9FS(c, opts)

	var created gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL),
		Mode:     defaultRegularFileMode,
	}, "same-handle.txt", &created); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	var out gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: created.NodeId},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       created.Fh,
			Size:     0,
		},
	}, &out); st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}, want); st != gofuse.OK || int(n) != len(want) {
		t.Fatalf("Write = %d, %v; want %d, OK", n, st, len(want))
	}
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	})

	mu.Lock()
	defer mu.Unlock()
	if putCount != 1 || !bytes.Equal(putBody, want) {
		t.Fatalf("remote PUTs/body = %d/%q, want 1/%q", putCount, putBody, want)
	}
}

func TestGVisorCompatCommittedMutationSupersedesSiblingAndAllowsNewWrite(t *testing.T) {
	const filePath = "/file.bin"
	committed := []byte("committed")
	fs, ino, closeServer := newTestDat9FS(t, int64(len(committed)), func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(committed)
	})
	defer closeServer()
	fs.opts.GVisorCompat = true
	fhID, sibling := newGVisorMutationHandle(t, fs, filePath, ino)

	newerSeq := fs.markDirtySize(ino, int64(len(committed)))
	fs.onCommitQueueSuccess(&CommitEntry{
		Path:        filePath,
		Inode:       ino,
		MutationSeq: newerSeq,
		Size:        int64(len(committed)),
		Kind:        PendingNew,
	}, 1)

	sibling.Lock()
	if sibling.DirtySeq != 0 || sibling.Dirty.HasDirtyParts() {
		t.Fatalf("superseded sibling remains dirty: seq=%d dirty=%t", sibling.DirtySeq, sibling.Dirty.HasDirtyParts())
	}
	if sibling.BaseRev != 1 || sibling.Dirty.Size() != int64(len(committed)) || sibling.Dirty.maxSize < int64(len(committed)) {
		t.Fatalf("superseded sibling base/size/max = %d/%d/%d, want 1/%d/>=%d", sibling.BaseRev, sibling.Dirty.Size(), sibling.Dirty.maxSize, len(committed), len(committed))
	}
	sibling.Unlock()

	later := []byte("later")
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
		Offset:   0,
	}, later); st != gofuse.OK || int(n) != len(later) {
		t.Fatalf("later Write = %d, %v; want %d, OK", n, st, len(later))
	}
	sibling.Lock()
	if sibling.DirtySeq <= newerSeq {
		t.Fatalf("later DirtySeq = %d, want > committed %d", sibling.DirtySeq, newerSeq)
	}
	if got := sibling.Dirty.Bytes()[:len(later)]; !bytes.Equal(got, later) {
		t.Fatalf("later dirty prefix = %q, want %q", got, later)
	}
	sibling.Unlock()
}

func TestGVisorCompatCommittedMutationAllowsLaterExplicitTruncate(t *testing.T) {
	const filePath = "/later-truncate.txt"
	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	fhID, sibling := newGVisorMutationHandle(t, fs, filePath, ino)

	newerSeq := fs.markDirtySize(ino, int64(len("committed")))
	fs.onCommitQueueSuccess(&CommitEntry{
		Path:        filePath,
		Inode:       ino,
		MutationSeq: newerSeq,
		Size:        int64(len("committed")),
		Kind:        PendingNew,
	}, 1)

	var out gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE | gofuse.FATTR_FH,
			Fh:       fhID,
			Size:     0,
		},
	}, &out); st != gofuse.OK {
		t.Fatalf("later SetAttr status = %v, want OK", st)
	}
	sibling.Lock()
	defer sibling.Unlock()
	if sibling.DirtySeq <= newerSeq {
		t.Fatalf("truncate DirtySeq = %d, want > committed %d", sibling.DirtySeq, newerSeq)
	}
	if sibling.Dirty.Size() != 0 || !sibling.Dirty.HasDirtyParts() {
		t.Fatalf("later truncate state = size:%d dirty:%t, want size:0 dirty:true", sibling.Dirty.Size(), sibling.Dirty.HasDirtyParts())
	}
	if fs.discardSupersededMutationLocked(sibling) {
		t.Fatal("genuinely later truncate was incorrectly superseded")
	}
}

func TestGVisorCompatDisabledDoesNotSupersedeSiblingMutation(t *testing.T) {
	const filePath = "/disabled.txt"
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
	_, sibling := newGVisorMutationHandle(t, fs, filePath, ino)
	staleSeq := sibling.DirtySeq
	newerSeq := fs.markDirtySize(ino, 9)

	fs.onCommitQueueSuccess(&CommitEntry{
		Path:        filePath,
		Inode:       ino,
		MutationSeq: newerSeq,
		Size:        9,
		Kind:        PendingNew,
	}, 1)

	sibling.Lock()
	defer sibling.Unlock()
	if sibling.DirtySeq != staleSeq || !sibling.Dirty.HasDirtyParts() {
		t.Fatalf("disabled sibling changed: seq=%d dirty=%t, want seq=%d dirty=true", sibling.DirtySeq, sibling.Dirty.HasDirtyParts(), staleSeq)
	}
}

func TestGVisorCompatRenamePreflightRetriesInterruptedTargetStat(t *testing.T) {
	for _, tc := range []struct {
		name         string
		targetExists bool
	}{
		{name: "missing target"},
		{name: "existing target", targetExists: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			firstStarted := make(chan struct{})
			var headCalls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/dst" {
					http.Error(w, "unexpected request", http.StatusInternalServerError)
					return
				}
				if headCalls.Add(1) == 1 {
					close(firstStarted)
					<-r.Context().Done()
					return
				}
				if !tc.targetExists {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Length", "3")
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "4")
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			opts := &MountOptions{GVisorCompat: true, LookupRetryCount: 1, LookupRetryTimeout: time.Second}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			fs.inodes.Lookup("/src", false, 3, time.Now())

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct {
				info renamePathInfo
				st   gofuse.Status
			}, 1)
			go func() {
				_, target, st := fs.renamePreflight(ctx, &gofuse.RenameIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Newdir:   1,
				}, "/src", "/dst")
				done <- struct {
					info renamePathInfo
					st   gofuse.Status
				}{info: target, st: st}
			}()

			select {
			case <-firstStarted:
				cancel()
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for initial target stat")
			}
			select {
			case result := <-done:
				if result.st != gofuse.OK {
					t.Fatalf("renamePreflight status = %v, want OK", result.st)
				}
				if result.info.exists != tc.targetExists {
					t.Fatalf("target exists = %t, want %t", result.info.exists, tc.targetExists)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("renamePreflight timed out")
			}
			if got := headCalls.Load(); got != 2 {
				t.Fatalf("target HEAD calls = %d, want 2", got)
			}
		})
	}
}

func TestGVisorCompatRenamePreflightRevalidatesNegativeTargetCache(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.NotFound(w, r)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/src", false, 3, time.Now())
	fs.cacheNegativePath("/dst")

	_, target, st := fs.renamePreflight(context.Background(), &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "/src", "/dst")
	if st != gofuse.OK {
		t.Fatalf("renamePreflight status = %v, want OK", st)
	}
	if target.exists {
		t.Fatal("negative cached target unexpectedly exists")
	}
	if got := remoteCalls.Load(); got != 1 {
		t.Fatalf("remote calls = %d, want 1", got)
	}
}

func TestGVisorCompatRenamePreflightRevalidatesNegativeTargetInStickyDirectory(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/dst" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.uid = 0
	fs.gid = 0
	fs.inodes.UpdateMode(1, uint32(syscall.S_IFDIR)|0o1777)
	fs.inodes.UpdateOwner(1, 0, 0, true, true)
	srcIno := fs.inodes.Lookup("/src", false, 3, time.Now())
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFREG)|0o644)
	fs.inodes.UpdateOwner(srcIno, 65534, 65534, true, true)
	fs.cacheNegativePath("/dst")

	_, target, st := fs.renamePreflight(context.Background(), &gofuse.RenameIn{
		InHeader: gofuse.InHeader{
			NodeId: 1,
			Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 65534, Gid: 65534}},
		},
		Newdir: 1,
	}, "/src", "/dst")
	if st != gofuse.EPERM {
		t.Fatalf("renamePreflight status = %v, want EPERM", st)
	}
	if !target.exists {
		t.Fatal("revalidated target unexpectedly missing")
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("target HEAD calls = %d, want 1", got)
	}
}

func TestGVisorCompatRenamePreflightRevalidatesNegativeTargetForDirectorySource(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/dst" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.Lookup("/src", true, 0, time.Now())
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFDIR)|0o755)
	fs.cacheNegativePath("/dst")

	_, target, st := fs.renamePreflight(context.Background(), &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "/src", "/dst")
	if st != gofuse.Status(syscall.ENOTDIR) {
		t.Fatalf("renamePreflight status = %v, want ENOTDIR", st)
	}
	if !target.exists {
		t.Fatal("revalidated target unexpectedly missing")
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("target HEAD calls = %d, want 1", got)
	}
}

func TestGVisorCompatRenamePreflightRevalidatesNegativeTargetForSpecialSource(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/dst" {
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.Lookup("/src", false, 0, time.Now())
	fs.inodes.UpdateMode(srcIno, uint32(syscall.S_IFIFO)|0o644)
	fs.cacheNegativePath("/dst")

	_, target, st := fs.renamePreflight(context.Background(), &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "/src", "/dst")
	if st != gofuse.OK {
		t.Fatalf("renamePreflight status = %v, want OK", st)
	}
	if !target.exists {
		t.Fatal("revalidated target unexpectedly missing")
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("target HEAD calls = %d, want 1", got)
	}
}

func TestGVisorCompatDisabledRenamePreflightDoesNotRetryInterrupt(t *testing.T) {
	firstStarted := make(chan struct{})
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		headCalls.Add(1)
		close(firstStarted)
		<-r.Context().Done()
	}))
	defer ts.Close()

	opts := &MountOptions{LookupRetryCount: 1, LookupRetryTimeout: time.Second}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/src", false, 3, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan gofuse.Status, 1)
	go func() {
		_, _, st := fs.renamePreflight(ctx, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: 1},
			Newdir:   1,
		}, "/src", "/dst")
		done <- st
	}()
	select {
	case <-firstStarted:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for target stat")
	}
	select {
	case st := <-done:
		if st != gofuse.EAGAIN {
			t.Fatalf("renamePreflight status = %v, want EAGAIN", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("renamePreflight timed out")
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("target HEAD calls = %d, want 1", got)
	}
}

func TestGVisorCompatRenamePreflightRejectsRetryAfterMountViewReset(t *testing.T) {
	firstStarted := make(chan struct{})
	retryStarted := make(chan struct{})
	allowRetry := make(chan struct{})
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch headCalls.Add(1) {
		case 1:
			close(firstStarted)
			<-r.Context().Done()
		case 2:
			close(retryStarted)
			<-allowRetry
			http.NotFound(w, r)
		default:
			http.Error(w, "unexpected retry", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true, LookupRetryCount: 1, LookupRetryTimeout: time.Second}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/src", false, 3, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan gofuse.Status, 1)
	go func() {
		_, _, st := fs.renamePreflight(ctx, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: 1},
			Newdir:   1,
		}, "/src", "/dst")
		done <- st
	}()

	select {
	case <-firstStarted:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial target stat")
	}
	select {
	case <-retryStarted:
		fs.resetMountView()
		close(allowRetry)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for detached retry")
	}
	select {
	case st := <-done:
		if st != gofuse.EAGAIN {
			t.Fatalf("renamePreflight status = %v, want EAGAIN", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("renamePreflight timed out after mount reset")
	}
	if got := headCalls.Load(); got != 2 {
		t.Fatalf("target HEAD calls = %d, want 2", got)
	}
}

func TestGVisorCompatUnlinkSnapshotSurvivesInterrupt(t *testing.T) {
	const filePath = "/dir/snapshot.txt"
	data := []byte("open handle snapshot")
	for _, tc := range []struct {
		name         string
		gvisorCompat bool
		wantStatus   gofuse.Status
		wantDeletes  int32
	}{
		{name: "gvisor", gvisorCompat: true, wantStatus: gofuse.OK, wantDeletes: 1},
		{name: "disabled", wantStatus: gofuse.EAGAIN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshotStarted := make(chan struct{})
			snapshotCanceled := make(chan struct{})
			allowSnapshot := make(chan struct{})
			var (
				snapshotCalls atomic.Int32
				deleteCalls   atomic.Int32
				allowOnce     sync.Once
			)
			t.Cleanup(func() { allowOnce.Do(func() { close(allowSnapshot) }) })

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					if snapshotCalls.Add(1) != 1 {
						http.Error(w, "unexpected snapshot retry", http.StatusInternalServerError)
						return
					}
					close(snapshotStarted)
					select {
					case <-r.Context().Done():
						close(snapshotCanceled)
						return
					case <-allowSnapshot:
						_, _ = w.Write(data)
					}
				case http.MethodDelete:
					deleteCalls.Add(1)
					w.WriteHeader(http.StatusOK)
				default:
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
				}
			}))
			defer ts.Close()

			opts := &MountOptions{GVisorCompat: tc.gvisorCompat}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			parentIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
			ino := fs.inodes.Lookup(filePath, false, int64(len(data)), time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			fs.allocateFileHandle(&FileHandle{
				Ino:      ino,
				Path:     filePath,
				Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
				OrigSize: int64(len(data)),
				BaseRev:  1,
			})

			cancel := make(chan struct{})
			done := make(chan gofuse.Status, 1)
			go func() {
				done <- fs.Unlink(cancel, &gofuse.InHeader{NodeId: parentIno}, "snapshot.txt")
			}()

			select {
			case <-snapshotStarted:
				close(cancel)
			case <-time.After(time.Second):
				t.Fatal("snapshot GET did not start")
			}

			if tc.gvisorCompat {
				select {
				case <-snapshotCanceled:
					t.Fatal("gVisor snapshot GET was canceled by the FUSE request")
				case <-time.After(100 * time.Millisecond):
					allowOnce.Do(func() { close(allowSnapshot) })
				}
			} else {
				select {
				case <-snapshotCanceled:
				case <-time.After(time.Second):
					t.Fatal("disabled snapshot GET was not canceled")
				}
			}

			select {
			case st := <-done:
				if st != tc.wantStatus {
					t.Fatalf("Unlink status = %v, want %v", st, tc.wantStatus)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Unlink timed out")
			}
			if got := deleteCalls.Load(); got != tc.wantDeletes {
				t.Fatalf("DELETE calls = %d, want %d", got, tc.wantDeletes)
			}
		})
	}
}

func TestGVisorCompatUnlinkKeepsRemoteMutationAliveAfterInterrupt(t *testing.T) {
	const filePath = "/dir/file.txt"
	deleteStarted := make(chan struct{})
	deleteCanceled := make(chan struct{})
	allowDelete := make(chan struct{})
	var (
		deleteCalls atomic.Int32
		closeAllow  sync.Once
	)
	t.Cleanup(func() { closeAllow.Do(func() { close(allowDelete) }) })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			if deleteCalls.Add(1) != 1 {
				http.Error(w, "unexpected retry", http.StatusInternalServerError)
				return
			}
			close(deleteStarted)
			select {
			case <-r.Context().Done():
				close(deleteCanceled)
				return
			case <-allowDelete:
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			}
		case http.MethodHead:
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	parentIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
	fs.inodes.Lookup(filePath, false, 4, time.Now())
	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Unlink(cancel, &gofuse.InHeader{NodeId: parentIno}, "file.txt")
	}()

	select {
	case <-deleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote DELETE")
	}
	select {
	case <-deleteCanceled:
		// Current behavior: the assertion below will expose EAGAIN.
	case <-time.After(100 * time.Millisecond):
		closeAllow.Do(func() { close(allowDelete) })
	}
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Unlink status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Unlink timed out")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("DELETE calls = %d, want 1", got)
	}
	if _, ok := fs.inodes.GetInode(filePath); ok {
		t.Fatal("successful unlink kept the local path")
	}
}

func TestGVisorCompatRenameKeepsRemoteMutationAliveAfterInterrupt(t *testing.T) {
	renameStarted := make(chan struct{})
	renameCanceled := make(chan struct{})
	allowRename := make(chan struct{})
	var (
		renameCalls atomic.Int32
		closeAllow  sync.Once
	)
	t.Cleanup(func() { closeAllow.Do(func() { close(allowRename) }) })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs/dst" && r.URL.RawQuery == "rename":
			if renameCalls.Add(1) == 1 {
				close(renameStarted)
				select {
				case <-r.Context().Done():
					close(renameCanceled)
					return
				case <-allowRename:
					w.WriteHeader(http.StatusOK)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/dst":
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/src":
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/src", false, 3, time.Now())
	fs.cacheNegativePath("/dst")
	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: 1},
			Newdir:   1,
		}, "src", "dst")
	}()

	select {
	case <-renameStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote rename")
	}
	select {
	case <-renameCanceled:
		// The request must remain live in compatibility mode; count catches retry.
	case <-time.After(100 * time.Millisecond):
		closeAllow.Do(func() { close(allowRename) })
	}
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Rename timed out")
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("remote rename calls = %d, want 1", got)
	}
	if _, ok := fs.inodes.GetInode("/dst"); !ok {
		t.Fatal("successful rename did not install target inode")
	}
}

func TestGVisorCompatOpenHandleSnapshotUsesRevisionMatchingReadCache(t *testing.T) {
	const (
		filePath = "/cached-snapshot.txt"
		revision = int64(7)
	)
	data := []byte("cached snapshot bytes")
	for _, tc := range []struct {
		name         string
		gvisorCompat bool
		cacheRev     int64
		wantGets     int32
	}{
		{name: "enabled matching revision", gvisorCompat: true, cacheRev: revision, wantGets: 0},
		{name: "enabled stale revision", gvisorCompat: true, cacheRev: revision - 1, wantGets: 1},
		{name: "disabled matching revision", cacheRev: revision, wantGets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var getCalls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
					return
				}
				getCalls.Add(1)
				_, _ = w.Write(data)
			}))
			defer ts.Close()

			opts := &MountOptions{GVisorCompat: tc.gvisorCompat}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			ino := fs.inodes.Lookup(filePath, false, int64(len(data)), time.Now())
			fs.inodes.UpdateRevision(ino, revision)
			fs.readCache.Put(filePath, data, tc.cacheRev)
			fh := &FileHandle{
				Ino:      ino,
				Path:     filePath,
				Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
				OrigSize: int64(len(data)),
				BaseRev:  revision,
			}
			fs.allocateFileHandle(fh)

			if err := fs.snapshotOpenHandlesBeforeUnlink(context.Background(), filePath); err != nil {
				t.Fatalf("snapshotOpenHandlesBeforeUnlink: %v", err)
			}
			fh.Lock()
			gotSnapshot := append([]byte(nil), fh.UnlinkedData...)
			fh.Unlock()
			if !bytes.Equal(gotSnapshot, data) {
				t.Fatalf("snapshot = %q, want %q", gotSnapshot, data)
			}
			if got := getCalls.Load(); got != tc.wantGets {
				t.Fatalf("remote GET calls = %d, want %d", got, tc.wantGets)
			}
		})
	}
}

func TestGVisorCompatOpenHandleSnapshotRejectsRevisionBehindCommittedFrontier(t *testing.T) {
	const (
		filePath          = "/committed-frontier-snapshot.txt"
		inodeRevision     = int64(1)
		committedRevision = int64(2)
	)
	oldData := []byte("old bytes")
	newData := []byte("new bytes")
	var getCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		getCalls.Add(1)
		_, _ = w.Write(newData)
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup(filePath, false, int64(len(oldData)), time.Now())
	fs.inodes.UpdateRevision(ino, inodeRevision)
	fs.readCache.Put(filePath, oldData, inodeRevision)
	seq := fs.markDirtySize(ino, int64(len(newData)))
	fs.recordCommittedRevision(filePath, committedRevision)
	fs.recordCommittedMutation(ino, seq, committedRevision, int64(len(newData)))
	fh := &FileHandle{
		Ino:      ino,
		Path:     filePath,
		Dirty:    fs.newWriteBuffer(filePath, maxPreloadSize, 0),
		OrigSize: int64(len(oldData)),
		BaseRev:  inodeRevision,
	}
	fs.allocateFileHandle(fh)

	if err := fs.snapshotOpenHandlesBeforeUnlink(context.Background(), filePath); err != nil {
		t.Fatalf("snapshotOpenHandlesBeforeUnlink: %v", err)
	}
	fh.Lock()
	got := append([]byte(nil), fh.UnlinkedData...)
	fh.Unlock()
	if !bytes.Equal(got, newData) {
		t.Fatalf("snapshot = %q, want latest committed %q", got, newData)
	}
	if got := getCalls.Load(); got != 1 {
		t.Fatalf("remote GET calls = %d, want 1 after rejecting stale cache", got)
	}
}

func TestGVisorCompatShadowSpillFlushPreservesConcurrentWrite(t *testing.T) {
	testGVisorCompatShadowSpillSyncCommitPreservesConcurrentWrite(t, func(fs *Dat9FS, ino, fhID uint64) gofuse.Status {
		return fs.Flush(nil, &gofuse.FlushIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
		})
	})
}

func TestGVisorCompatShadowSpillFsyncPreservesConcurrentWrite(t *testing.T) {
	testGVisorCompatShadowSpillSyncCommitPreservesConcurrentWrite(t, func(fs *Dat9FS, ino, fhID uint64) gofuse.Status {
		return fs.Fsync(nil, &gofuse.FsyncIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
		})
	})
}

func testGVisorCompatShadowSpillSyncCommitPreservesConcurrentWrite(t *testing.T, commit func(*Dat9FS, uint64, uint64) gofuse.Status) {
	t.Helper()
	const filePath = "/shadowspill-sync-concurrent-write.bin"
	original := bytes.Repeat([]byte("a"), writeBackThreshold)
	updated := []byte("newer write after old shadow upload")
	uploadStarted := make(chan struct{})
	allowUpload := make(chan struct{})
	var (
		uploadMu sync.Mutex
		uploaded []byte
		putCalls atomic.Int32
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write(original)
			return
		case http.MethodPut:
		default:
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		uploadMu.Lock()
		uploaded = append(uploaded[:0], body...)
		putCall := putCalls.Add(1)
		uploadMu.Unlock()
		if putCall == 1 {
			close(uploadStarted)
			<-allowUpload
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": int64(putCall)})
	}))
	defer ts.Close()

	opts := &MountOptions{
		GVisorCompat: true,
		SyncMode:     SyncStrict,
		WritePolicy:  WritePolicyWriteBack,
	}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(int64(writeBackThreshold + 1))
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	ino := fs.inodes.Lookup(filePath, false, int64(len(original)), time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, original); err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(filePath, original, 0); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:            ino,
		Path:           filePath,
		Dirty:          dirty,
		DirtySeq:       fs.markDirtySize(ino, int64(len(original))),
		ShadowReady:    true,
		ShadowSpill:    true,
		ShadowStageGen: shadow.ActiveGeneration(filePath),
		IsNew:          true,
		WritePolicy:    WritePolicyWriteBack,
	}
	fhID := fs.allocateFileHandle(fh)

	commitDone := make(chan gofuse.Status, 1)
	go func() {
		commitDone <- commit(fs, ino, fhID)
	}()
	select {
	case <-uploadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("old shadow upload did not start")
	}

	writeDone := make(chan gofuse.Status, 1)
	go func() {
		_, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
			Offset:   0,
			Size:     uint32(len(updated)),
		}, updated)
		writeDone <- st
	}()

	// Flush/Fsync has released fh.mu for the upload. Wait until Write holds
	// it while blocked on the still-held path fence, then release the upload.
	deadline := time.Now().Add(5 * time.Second)
	for fh.TryLock() {
		fh.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("concurrent Write did not acquire fh.mu")
		}
		runtime.Gosched()
	}
	close(allowUpload)

	select {
	case st := <-writeDone:
		if st != gofuse.OK {
			t.Fatalf("concurrent Write status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Write did not finish")
	}
	select {
	case st := <-commitDone:
		if st != gofuse.OK {
			t.Fatalf("sync commit status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sync commit did not finish")
	}

	uploadMu.Lock()
	firstUpload := append([]byte(nil), uploaded...)
	uploadMu.Unlock()
	if !bytes.Equal(firstUpload, original) {
		t.Fatal("old sync commit did not upload its captured shadow snapshot")
	}
	fh.Lock()
	dirtySeq := fh.DirtySeq
	hasDirty := fh.Dirty != nil && fh.Dirty.HasDirtyParts()
	fh.Unlock()
	if dirtySeq == 0 || !hasDirty {
		t.Fatalf("concurrent Write was cleared: dirty_seq=%d dirty=%t", dirtySeq, hasDirty)
	}

	if st := commit(fs, ino, fhID); st != gofuse.OK {
		t.Fatalf("next sync commit status = %v, want OK", st)
	}
	wantRemote := append([]byte(nil), original...)
	copy(wantRemote, updated)
	uploadMu.Lock()
	gotRemote := append([]byte(nil), uploaded...)
	uploadMu.Unlock()
	if !bytes.Equal(gotRemote, wantRemote) {
		t.Fatalf("next sync commit body = %q, want updated dirty buffer", gotRemote[:len(updated)])
	}
}

func TestGVisorCompatShadowSpillFlushRechecksSupersessionAfterFence(t *testing.T) {
	const filePath = "/shadowspill-stale-after-fence.bin"
	stale := bytes.Repeat([]byte("s"), writeBackThreshold)
	newer := []byte("newer committed sibling data")
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		putCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
	}))
	defer ts.Close()

	opts := &MountOptions{
		GVisorCompat: true,
		SyncMode:     SyncStrict,
		WritePolicy:  WritePolicyWriteBack,
	}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(int64(writeBackThreshold + 1))
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow

	ino := fs.inodes.Lookup(filePath, false, int64(len(stale)), time.Now())
	dirty := fs.newWriteBuffer(filePath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, stale); err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(filePath, stale, 0); err != nil {
		t.Fatal(err)
	}
	staleSeq := fs.markDirtySize(ino, int64(len(stale)))
	fh := &FileHandle{
		Ino:            ino,
		Path:           filePath,
		Dirty:          dirty,
		DirtySeq:       staleSeq,
		ShadowReady:    true,
		ShadowSpill:    true,
		ShadowStageGen: shadow.ActiveGeneration(filePath),
		IsNew:          true,
		WritePolicy:    WritePolicyWriteBack,
	}
	fhID := fs.allocateFileHandle(fh)

	reachedFence := make(chan struct{})
	allowFence := make(chan struct{})
	previousHook := testHookBeforeGVisorShadowSpillFlushFence
	testHookBeforeGVisorShadowSpillFlushFence = func(path string) {
		if path != filePath {
			return
		}
		close(reachedFence)
		<-allowFence
	}
	t.Cleanup(func() {
		testHookBeforeGVisorShadowSpillFlushFence = previousHook
	})

	flushDone := make(chan gofuse.Status, 1)
	go func() {
		flushDone <- fs.Flush(nil, &gofuse.FlushIn{
			InHeader: gofuse.InHeader{NodeId: ino},
			Fh:       fhID,
		})
	}()
	select {
	case <-reachedFence:
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not reach the post-check, pre-fence boundary")
	}

	newerSeq := fs.markDirtySize(ino, int64(len(newer)))
	fs.recordCommittedMutation(ino, newerSeq, 1, int64(len(newer)))
	close(allowFence)

	select {
	case st := <-flushDone:
		if st != gofuse.OK {
			t.Fatalf("stale Flush status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale Flush did not finish")
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("stale Flush remote PUTs = %d, want 0", got)
	}
}

func TestGVisorCompatWriteBackRequiresCommitQueue(t *testing.T) {
	for _, tc := range []struct {
		name        string
		gvisor      bool
		writePolicy WritePolicy
		writeBack   bool
		commitQueue bool
		want        bool
	}{
		{
			name:        "gvisor writeback legacy fallback rejected",
			gvisor:      true,
			writePolicy: WritePolicyWriteBack,
			writeBack:   true,
			want:        true,
		},
		{
			name:      "gvisor default writeback legacy fallback rejected",
			gvisor:    true,
			writeBack: true,
			want:      true,
		},
		{
			name:        "gvisor writeback queue available",
			gvisor:      true,
			writePolicy: WritePolicyWriteBack,
			writeBack:   true,
			commitQueue: true,
		},
		{
			name:        "non gvisor legacy fallback preserved",
			writePolicy: WritePolicyWriteBack,
			writeBack:   true,
		},
		{
			name:        "gvisor close sync has no legacy uploader path",
			gvisor:      true,
			writePolicy: WritePolicyCloseSync,
			writeBack:   true,
		},
		{
			name:        "gvisor without writeback cache",
			gvisor:      true,
			writePolicy: WritePolicyWriteBack,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := &MountOptions{GVisorCompat: tc.gvisor, WritePolicy: tc.writePolicy}
			opts.setDefaults()
			var writeBack *WriteBackCache
			if tc.writeBack {
				writeBack = &WriteBackCache{}
			}
			var commitQueue *CommitQueue
			if tc.commitQueue {
				commitQueue = &CommitQueue{}
			}
			if got := gvisorWriteBackRequiresCommitQueue(opts, writeBack, commitQueue); got != tc.want {
				t.Fatalf("gvisorWriteBackRequiresCommitQueue = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestGVisorCompatRenameMetadataOnlySpecialTargetDeleteSurvivesInterrupt(t *testing.T) {
	deleteStarted := make(chan struct{})
	deleteCanceled := make(chan struct{})
	allowDelete := make(chan struct{})
	var (
		deleteCalls atomic.Int32
		closeAllow  sync.Once
	)
	t.Cleanup(func() { closeAllow.Do(func() { close(allowDelete) }) })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/pipe":
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/target":
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/target":
			if deleteCalls.Add(1) != 1 {
				http.Error(w, "unexpected retry", http.StatusInternalServerError)
				return
			}
			close(deleteStarted)
			select {
			case <-r.Context().Done():
				close(deleteCanceled)
				return
			case <-allowDelete:
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	var out gofuse.EntryOut
	if st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "pipe", &out); st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "pipe", "target")
	}()
	select {
	case <-deleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for target DELETE")
	}
	select {
	case <-deleteCanceled:
		// The final status assertion exposes an interruptible mutation context.
	case <-time.After(100 * time.Millisecond):
		closeAllow.Do(func() { close(allowDelete) })
	}
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Rename status = %v, want OK", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rename did not finish")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("target DELETE calls = %d, want 1", got)
	}
	if _, ok := fs.specialNodeEntry("/target"); !ok {
		t.Fatal("special source was not renamed to target")
	}
}

func TestGVisorCompatDisabledRenameMetadataOnlySpecialTargetDeleteRemainsInterruptible(t *testing.T) {
	deleteStarted := make(chan struct{})
	var deleteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/pipe":
			http.NotFound(w, r)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/target":
			w.Header().Set("Content-Length", "1")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/target":
			deleteCalls.Add(1)
			close(deleteStarted)
			<-r.Context().Done()
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	var out gofuse.EntryOut
	if st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFIFO) | 0o644,
	}, "pipe", &out); st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "pipe", "target")
	}()
	select {
	case <-deleteStarted:
		close(cancel)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for target DELETE")
	}
	select {
	case st := <-done:
		if st != gofuse.EAGAIN {
			t.Fatalf("Rename status = %v, want EAGAIN", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rename did not return after interrupt")
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("target DELETE calls = %d, want 1", got)
	}
}

func TestGVisorCompatPendingNewGitRenameSyncCommitSurvivesInterrupt(t *testing.T) {
	testGVisorCompatPendingNewRenameSyncCommitInterrupt(t, true, true, false, gofuse.OK)
}

func TestGVisorCompatPendingNewRenameQueueFallbackSyncCommitSurvivesInterrupt(t *testing.T) {
	testGVisorCompatPendingNewRenameSyncCommitInterrupt(t, true, false, true, gofuse.OK)
}

func TestGVisorCompatDisabledPendingNewGitRenameSyncCommitRemainsInterruptible(t *testing.T) {
	testGVisorCompatPendingNewRenameSyncCommitInterrupt(t, false, true, false, gofuse.EAGAIN)
}

func testGVisorCompatPendingNewRenameSyncCommitInterrupt(t *testing.T, gvisorCompat, gitLooseObject, queueStopped bool, wantStatus gofuse.Status) {
	t.Helper()
	oldP := "/repo/.git/objects/70/tmp_obj_interrupt"
	newP := "/repo/.git/objects/70/24234d93f61104585962ac664bc5a7ed1d241d"
	oldName := "tmp_obj_interrupt"
	newName := "24234d93f61104585962ac664bc5a7ed1d241d"
	parentPath := "/repo/.git/objects/70"
	if !gitLooseObject {
		oldP = "/repo/tmp_obj_interrupt"
		newP = "/repo/final-object"
		oldName = "tmp_obj_interrupt"
		newName = "final-object"
		parentPath = "/repo"
	}
	data := []byte("loose object final commit")
	putStarted := make(chan struct{})
	putCanceled := make(chan struct{})
	allowPut := make(chan struct{})
	var (
		putCalls   atomic.Int32
		closeAllow sync.Once
	)
	t.Cleanup(func() { closeAllow.Do(func() { close(allowPut) }) })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && (r.URL.Path == "/v1/fs"+oldP || r.URL.Path == "/v1/fs"+newP):
			http.NotFound(w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+newP:
			if putCalls.Add(1) != 1 {
				http.Error(w, "unexpected retry", http.StatusInternalServerError)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if string(body) != string(data) {
				http.Error(w, "unexpected final upload body", http.StatusBadRequest)
				return
			}
			close(putStarted)
			select {
			case <-r.Context().Done():
				close(putCanceled)
				return
			case <-allowPut:
				_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 1})
			}
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{GVisorCompat: gvisorCompat}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 16)
	defer cq.DrainAll()
	if queueStopped {
		cq.DrainAll()
	}
	fs.commitQueue = cq

	if err := shadow.WriteFull(oldP, data, 0); err != nil {
		t.Fatalf("WriteFull old shadow: %v", err)
	}
	if _, err := pending.PutWithBaseRev(oldP, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatalf("PutWithBaseRev old pending: %v", err)
	}
	fs.inodes.Lookup(oldP, false, int64(len(data)), time.Now())
	dirIno := fs.inodes.Lookup(parentPath, true, 0, time.Now())

	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.Rename(cancel, &gofuse.RenameIn{
			InHeader: gofuse.InHeader{NodeId: dirIno},
			Newdir:   dirIno,
		}, oldName, newName)
	}()
	select {
	case <-putStarted:
		close(cancel)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for final-path PUT")
	}
	if gvisorCompat {
		select {
		case <-putCanceled:
			// The final status assertion exposes an interruptible commit context.
		case <-time.After(100 * time.Millisecond):
			closeAllow.Do(func() { close(allowPut) })
		}
	} else {
		select {
		case <-putCanceled:
		case <-time.After(5 * time.Second):
			t.Fatal("non-gVisor final-path PUT was not canceled")
		}
	}
	select {
	case st := <-done:
		if st != wantStatus {
			t.Fatalf("Rename status = %v, want %v", st, wantStatus)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rename did not finish")
	}
	if got := putCalls.Load(); got != 1 {
		t.Fatalf("final-path PUTs = %d, want 1", got)
	}
	if gvisorCompat {
		if pending.HasPending(newP) {
			t.Fatal("final path remained pending after detached commit")
		}
		if shadow.Has(newP) {
			t.Fatal("final shadow remained after detached commit")
		}
	}
}
