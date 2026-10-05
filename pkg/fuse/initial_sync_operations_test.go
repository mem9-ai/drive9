package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func newInitialSyncOperationFS(t *testing.T, opts *MountOptions, handler http.HandlerFunc) *Dat9FS {
	t.Helper()
	server := httptest.NewServer(handler)
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	t.Cleanup(func() {
		fs.directoryPrefetch.shutdown()
		server.Close()
	})
	return fs
}

func TestInitialSyncAccessUsesUnifiedAttrStage(t *testing.T) {
	fixture := newInitialSyncStatFixture(t)
	ino := fixture.seedFile()
	fixture.beforeHead = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
		if call == 1 {
			fixture.initialReset()
		}
		return false
	}
	if st := fixture.fs.Access(nil, &gofuse.AccessIn{InHeader: gofuse.InHeader{NodeId: ino}, Mask: gofuse.F_OK}); st != gofuse.OK {
		t.Fatal(st)
	}
	if fixture.headCalls.Load() != 2 || mustGetInodeEntry(t, fixture.fs, ino).Nlookup != 1 {
		t.Fatal("Access repeated references or did not recover metadata")
	}
}

func TestInitialSyncFreshDetachedReadUsesOwnerDeadline(t *testing.T) {
	var fs *Dat9FS
	var calls atomic.Int32
	fs = newInitialSyncOperationFS(t, &MountOptions{}, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "transient", http.StatusServiceUnavailable)
			return
		}
		<-r.Context().Done()
	})
	owner, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	deadline, _ := owner.Deadline()
	start := time.Now()
	_, err := fs.readSmallFileWithRetry(owner, "/file", deadline)
	if err == nil || time.Since(start) > time.Second || calls.Load() != 2 {
		t.Fatalf("err=%v duration=%s calls=%d; detached retry renewed owner window", err, time.Since(start), calls.Load())
	}
}

func TestInitialSyncLookupPreservesLayerBudget(t *testing.T) {
	fixture := newInitialSyncStatFixture(t)
	fixture.fs.opts.LayerRef = "layer"
	fixture.beforeHead = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
		switch call {
		case 1, 3, 4:
			fixture.fs.resetMountView()
		case 2:
			fixture.initialReset()
		}
		return false
	}
	st, ino, _ := fixture.run("Lookup", nil)
	if st != gofuse.OK || fixture.headCalls.Load() != 5 || mustGetInodeEntry(t, fixture.fs, ino).Nlookup != 1 {
		t.Fatalf("status=%v calls=%d inode=%d; want 3 layer retries plus 1 first retry", st, fixture.headCalls.Load(), ino)
	}
}

func TestInitialSyncReadRecomputesMetadataAndEOF(t *testing.T) {
	for _, route := range []string{"whole", "range", "new EOF", "unlinked"} {
		t.Run(route, func(t *testing.T) {
			var fs *Dat9FS
			var handle *FileHandle
			var heads, reads atomic.Int32
			old, current := []byte("old-data"), []byte("new-data-is-longer")
			newSize := len(current)
			if route == "new EOF" {
				newSize = 0
			}
			opts := &MountOptions{TrustLocalEvents: true, LookupRetryCount: -1}
			if route == "range" || route == "new EOF" {
				opts.ReadCacheMaxFileBytes = 4
			}
			fs = newInitialSyncOperationFS(t, opts, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads.Add(1)
					w.Header().Set("Content-Length", strconv.Itoa(newSize))
					w.Header().Set("X-Dat9-Revision", "2")
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method != http.MethodGet {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				if r.URL.Path != "/object" && (route == "range" || route == "new EOF") {
					w.Header().Set("Location", fs.client.BaseURL()+"/object")
					w.WriteHeader(http.StatusFound)
					return
				}
				data := current
				if reads.Add(1) == 1 {
					data = old
					if route == "unlinked" {
						handle.Lock()
						handle.Unlinked = true
						handle.UnlinkedData = []byte("private")
						handle.UnlinkedSize = 7
						handle.Unlock()
					}
					fs.resetMountViewWithInitialSync(true)
				}
				if r.URL.Path == "/object" {
					var start, end int
					if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
						t.Error(err)
					}
					data = data[start : end+1]
					w.WriteHeader(http.StatusPartialContent)
				}
				_, _ = w.Write(data)
			})
			ino := fs.inodes.Lookup("/file.bin", false, int64(len(old)), time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			handle = &FileHandle{Ino: ino, Path: "/file.bin", BaseRev: 1}
			fh := fs.allocateFileHandle(handle)
			defer fs.fileHandles.Delete(fh)
			if route == "range" || route == "new EOF" {
				handle.Prefetch = NewPrefetcher(fs.client, handle.Path, int64(len(old)))
				defer handle.Prefetch.Close()
			}
			data, st, err := readDat9FSTestRange(fs, ino, fh, 0, len(old))
			want := current[:len(old)]
			wantHeads, wantReads := int32(1), int32(2)
			switch route {
			case "new EOF":
				want, wantReads = nil, 1
			case "unlinked":
				want, wantHeads, wantReads = []byte("private"), 0, 1
			}
			if err != nil || st != gofuse.OK || !bytes.Equal(data, want) || heads.Load() != wantHeads || reads.Load() != wantReads {
				t.Fatalf("data=%q status=%v err=%v HEAD=%d GET=%d; want %q/%d/%d", data, st, err, heads.Load(), reads.Load(), want, wantHeads, wantReads)
			}
			if handle.BaseRev != 1 || handle.DirtySeq != 0 || mustGetInodeEntry(t, fs, ino).Nlookup != 1 {
				t.Fatal("recovery changed owned state or references")
			}
			if handle.Prefetch != nil && handle.Prefetch.fileSize != int64(newSize) {
				t.Fatalf("prefetch size=%d want=%d", handle.Prefetch.fileSize, newSize)
			}
		})
	}
}

func TestInitialSyncRmdirRetriesOnlyEmptyCheck(t *testing.T) {
	for _, layer := range []bool{false, true} {
		t.Run(strconv.FormatBool(layer), func(t *testing.T) {
			var fs *Dat9FS
			var lists, mutations atomic.Int32
			opts := &MountOptions{}
			if layer {
				opts.LayerRef = "layer"
			}
			fs = newInitialSyncOperationFS(t, opts, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					if lists.Add(1) == 1 {
						fs.resetMountViewWithInitialSync(true)
					}
					_, _ = w.Write([]byte(`{"entries":[]}`))
				case http.MethodDelete:
					if layer {
						t.Error("layer Rmdir used ordinary DELETE")
					}
					mutations.Add(1)
					w.WriteHeader(http.StatusNoContent)
				case http.MethodPost:
					var entry client.FSLayerEntryRequest
					if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if !layer || r.URL.Path != "/v1/layers/layer/entries" || entry.Path != "/dir" || entry.Op != "whiteout" || entry.Kind != "dir" {
						t.Errorf("unexpected layer mutation %s %+v", r.URL, entry)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mutations.Add(1)
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer", Path: entry.Path, Op: entry.Op, Kind: entry.Kind, EntrySeq: 1})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			})
			fs.inodes.Lookup("/dir", true, 0, time.Now())
			if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir"); st != gofuse.OK {
				t.Fatal(st)
			}
			if lists.Load() != 2 || mutations.Load() != 1 {
				t.Fatalf("LIST=%d mutations=%d want 2/1", lists.Load(), mutations.Load())
			}
		})
	}
}

func TestInitialSyncWritableRemoteFallbackKeepsFDSize(t *testing.T) {
	var fs *Dat9FS
	var calls atomic.Int32
	fs = newInitialSyncOperationFS(t, &MountOptions{TrustLocalEvents: true, ReadCacheMaxFileBytes: 4}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("owned fallback issued %s instead of retaining its base", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		data := []byte("NEW-BASE-IS-LONGER")
		if calls.Add(1) == 1 {
			fs.resetMountViewWithInitialSync(true)
			data = []byte("old-data")
		} else if got := r.Header.Get("Range"); got != "bytes=0-7" {
			t.Errorf("fresh Range=%q want bytes=0-7", got)
		}
		_, _ = w.Write(data)
	})
	ino := fs.inodes.Lookup("/file.bin", false, 8, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	dirty := NewWriteBuffer("/file.bin", 0, 4)
	// A lazy clean writable base leaves both parts unloaded; the original
	// buffer reader must fall through to remote I/O without loading/rebinding.
	dirty.totalSize, dirty.remoteSize = 8, 8
	if dirty.HasDirtyParts() || dirty.IsPartLoaded(0) || dirty.IsPartLoaded(1) {
		t.Fatal("fixture is not a lazy clean writable base")
	}
	handle := &FileHandle{Ino: ino, Path: "/file.bin", Dirty: dirty, BaseRev: 1, OrigSize: 8}
	fh := fs.allocateFileHandle(handle)
	defer fs.fileHandles.Delete(fh)
	data, st, err := readDat9FSTestRange(fs, ino, fh, 0, 16)
	if err != nil || st != gofuse.OK || string(data) != "NEW-BASE" || calls.Load() != 2 || handle.BaseRev != 1 || handle.Dirty.Size() != 8 {
		t.Fatalf("data=%q status=%v err=%v calls=%d base=%d size=%d", data, st, err, calls.Load(), handle.BaseRev, handle.Dirty.Size())
	}
}

func TestInitialSyncRenameProbeRecoversBeforeInodePublication(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(strconv.FormatBool(absent), func(t *testing.T) {
			var fs *Dat9FS
			var heads atomic.Int32
			fs = newInitialSyncOperationFS(t, &MountOptions{GVisorCompat: true, LookupRetryCount: -1}, func(w http.ResponseWriter, r *http.Request) {
				call := heads.Add(1)
				if _, ok := fs.inodes.GetInode("/probe"); ok {
					t.Error("inode published before accepted probe")
				}
				if call == 1 {
					fs.resetMountViewWithInitialSync(true)
				}
				if absent {
					http.NotFound(w, r)
					return
				}
				writeInitialSyncStatResponse(w, call)
			})
			info, err := fs.renamePathInfo(context.Background(), "/probe", &initialSyncRequest{})
			if err != nil || heads.Load() != 2 || info.exists == absent {
				t.Fatalf("info=%+v err=%v heads=%d", info, err, heads.Load())
			}
			if !absent && (info.entry.Revision != 22 || info.entry.Nlookup != 0) {
				t.Fatalf("published=%+v", info.entry)
			}
		})
	}
}

func TestInitialSyncParallelReadKeepsConcreteFailure(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(strconv.FormatBool(mixed), func(t *testing.T) {
			var fs *Dat9FS
			var heads, reads atomic.Int32
			arrived := make(chan struct{}, 2)
			release := make(chan struct{})
			fs = newInitialSyncOperationFS(t, &MountOptions{TrustLocalEvents: true, ReadCacheMaxFileBytes: 4, ParallelReadBlockSize: 4, ParallelReadConcurrency: 2}, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					heads.Add(1)
					w.Header().Set("Content-Length", "8")
					w.Header().Set("X-Dat9-Revision", "2")
					return
				}
				if r.Header.Get("Range") == "" {
					// Inline first hop has no reusable target. Its range 403
					// below is a terminal API error, not an expired S3 presign.
					w.WriteHeader(http.StatusOK)
					return
				}
				var start, end int
				_, _ = fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
				call := reads.Add(1)
				data := []byte("ABCDEFGH")
				if call <= 2 {
					arrived <- struct{}{}
					<-release
					if mixed && start == 4 {
						http.Error(w, "forbidden", http.StatusForbidden)
						return
					}
					data = []byte("old-data")
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/8", start, end))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[start : end+1])
			})
			fs.diskReadCache = newTestDiskReadCache(t, 1<<20)
			ino := fs.inodes.Lookup("/file.bin", false, 8, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			fh := openDat9FSTestHandle(t, fs, ino, "/file.bin")
			defer fs.fileHandles.Delete(fh)
			done := make(chan gofuse.Status, 1)
			go func() {
				data, st, err := readDat9FSTestRange(fs, ino, fh, 0, 8)
				if !mixed && (err != nil || !bytes.Equal(data, []byte("ABCDEFGH"))) {
					t.Errorf("data=%q err=%v", data, err)
				}
				done <- st
			}()
			for range 2 {
				select {
				case <-arrived:
				case <-time.After(2 * time.Second):
					close(release)
					t.Fatal("parallel reads did not start")
				}
			}
			fs.resetMountViewWithInitialSync(true)
			close(release)
			select {
			case st := <-done:
				want, wantHeads := gofuse.OK, int32(1)
				if mixed {
					want, wantHeads = gofuse.Status(syscall.EACCES), 0
				}
				if st != want || heads.Load() != wantHeads {
					t.Fatalf("status=%v heads=%d want=%v/%d", st, heads.Load(), want, wantHeads)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("parallel read did not finish")
			}
		})
	}
}
