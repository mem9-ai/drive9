package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
			deleteCalls.Add(1)
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
