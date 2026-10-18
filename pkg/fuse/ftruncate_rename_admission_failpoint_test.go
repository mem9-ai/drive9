//go:build failpoint

package fuse

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type renameFtruncateAdmissionRemote struct {
	mu                          sync.Mutex
	path, data                  string
	revision                    int64
	renamed                     bool
	oldPuts, conflicts, renames int
}

func newRenameFtruncateAdmissionFS(t *testing.T, directory bool, parentPID uint32) (*Dat9FS, uint64, uint64, *renameFtruncateAdmissionRemote, string, string) {
	t.Helper()
	oldPath, newPath := "/old", "/new"
	if directory {
		oldPath, newPath = "/old/file.bin", "/new/file.bin"
	}
	remote := &renameFtruncateAdmissionRemote{path: oldPath, data: "hello world", revision: 1}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/fs"), "/")
		if p == "" {
			p = "/"
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if p == "/" || directory && (p == "/old" && !remote.renamed || p == "/new" && remote.renamed) {
				w.Header().Set("X-Dat9-IsDir", "true")
				w.Header().Set("Content-Length", "0")
				return
			}
			if p != remote.path {
				http.Error(w, "not found", 404)
				return
			}
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("Content-Length", strconv.Itoa(len(remote.data)))
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			w.Header().Set("X-Dat9-Resource-ID", "rename-admission-same-file")
		case http.MethodGet:
			if r.URL.Query().Has("list") {
				_, _ = io.WriteString(w, `{"entries":[]}`)
				return
			}
			if p != remote.path {
				http.Error(w, "not found", 404)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			_, _ = io.WriteString(w, remote.data)
		case http.MethodPost:
			if p != "/new" || r.Header.Get("X-Dat9-Rename-Source") != "/old" {
				t.Errorf("unexpected rename %s source=%q", r.URL.String(), r.Header.Get("X-Dat9-Rename-Source"))
				w.WriteHeader(400)
				return
			}
			remote.renames++
			remote.renamed, remote.path = true, newPath
		case http.MethodPut:
			if remote.renamed && p == oldPath {
				remote.oldPuts++
			}
			if p != remote.path || r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(remote.revision, 10) {
				remote.conflicts++
				http.Error(w, "revision conflict", 409)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			remote.data, remote.revision = string(body), remote.revision+1
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(remote.revision, 10))
			_, _ = fmt.Fprintf(w, `{"revision":%d}`, remote.revision)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
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
	fs.writeBack, err = NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.uploader = &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", uploadCh: make(chan string, 8)}
	fs.uploader.OnDataCommitted, fs.uploader.OnSuccess = fs.onWriteBackDataCommitted, fs.onWriteBackUploadSuccess
	fs.uploader.SnapshotStagingGens = fs.snapshotStagingGens
	fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
	fs.commitQueue.PathLock = fs.lockRemoteCommitPath
	fs.commitQueue.OnUploaded, fs.commitQueue.OnSuccess = fs.onCommitQueueUploaded, fs.onCommitQueueSuccess
	fs.commitQueue.OnCleanup, fs.commitQueue.DurableWatermark = fs.onCommitQueueCleanup, fs.latestCommittedRevision
	fs.commitQueue.IsSuperseded = fs.commitEntrySuperseded
	if directory {
		fs.inodes.Lookup("/old", true, 0, time.Now())
	}
	ino := fs.inodes.Lookup(oldPath, false, 11, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	t.Cleanup(func() {
		for _, fh := range fs.openHandles.SnapshotInode(ino) {
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
		}
		fs.commitQueue.DrainAll()
	})
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: parentPID}}, Flags: uint32(syscall.O_RDWR)}, &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, opened.Fh, 5)
	return fs, ino, opened.Fh, remote, oldPath, newPath
}

func TestFtruncateRenameAdmissionBeforeHandleRetarget(t *testing.T) {
	orders := []string{"child_fsync_parent_release", "parent_release_child_fsync", "child_fsync_parent_fsync_release", "dirty_successor_refusal"}
	for _, caller := range []struct {
		name          string
		parent, child uint32
	}{{"zero_pid", 0, 0}, {"same_caller", 123, 123}, {"foreign_caller", 123, 456}} {
		t.Run(caller.name, func(t *testing.T) {
			t.Run("direct_open_trunc", func(t *testing.T) {
				runRenameFtruncateAdmission(t, caller.parent, caller.child, false, orders)
			})
			t.Run("split_open_trunc", func(t *testing.T) {
				runRenameFtruncateAdmission(t, caller.parent, caller.child, true, orders)
			})
		})
	}
}

func runRenameFtruncateAdmission(t *testing.T, parentPID, childPID uint32, split bool, orders []string) {
	t.Helper()
	for _, directory := range []bool{false, true} {
		for _, order := range orders {
			name := fmt.Sprintf("directory=%t/split=%t/%s", directory, split, order)
			t.Run(name, func(t *testing.T) {
				fs, ino, parentID, remote, oldPath, newPath := newRenameFtruncateAdmissionFS(t, directory, parentPID)
				parent, _ := fs.fileHandles.Get(parentID)
				originalEvent := parent.pendingFtruncate.Load()
				if originalEvent == nil || originalEvent.id == "" {
					t.Fatal("missing initial truncate lineage")
				}
				dirtySuccessor := order == "dirty_successor_refusal"
				if dirtySuccessor {
					if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: parentID}, []byte("H")); st != gofuse.OK {
						t.Fatal(st)
					}
				}
				beforeData, beforeSeq, beforeID := string(parent.Dirty.Bytes()), parent.DirtySeq, ftruncateSnapshotID(parent)
				entered, resume := make(chan struct{}), make(chan struct{})
				var resumeOnce sync.Once
				unblock := func() { resumeOnce.Do(func() { close(resume) }) }
				result := make(chan gofuse.Status, 1)
				enableQueueRenameHook(t, fs, func(observed *Dat9FS, phase string) {
					if phase == "after_fence_release" {
						close(entered)
						<-resume
					}
				})
				go func() {
					result <- fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
				}()
				t.Cleanup(func() {
					unblock()
					select {
					case st := <-result:
						if st != gofuse.OK {
							t.Errorf("cleanup rename=%v", st)
						}
					case <-time.After(5 * time.Second):
						t.Error("rename failed to resume")
					}
				})
				select {
				case <-entered:
				case st := <-result:
					result <- st
					t.Fatalf("rename exited before boundary: %v", st)
				case <-time.After(5 * time.Second):
					t.Fatal("rename never reached fence-release boundary")
				}
				entry, _ := fs.inodes.GetEntry(ino)
				if entry.Path != newPath || parent.Path != oldPath || parent.pendingFtruncate.Load().path != oldPath {
					t.Fatalf("wrong boundary: inode=%s handle=%s event=%s", entry.Path, parent.Path, parent.pendingFtruncate.Load().path)
				}
				var opened gofuse.OpenOut
				flags := uint32(syscall.O_RDWR | syscall.O_TRUNC)
				if split {
					flags = uint32(syscall.O_RDWR)
				}
				st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Flags: flags}, &opened)
				if split && st == gofuse.OK {
					st = fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{})
				}
				t.Logf("ADMISSION split=%t dirty_successor=%t status=%v", split, dirtySuccessor, st)
				if dirtySuccessor {
					if st != gofuse.EAGAIN {
						t.Errorf("dirty successor admission=%v want EAGAIN", st)
					}
					if string(parent.Dirty.Bytes()) != beforeData || parent.DirtySeq != beforeSeq || ftruncateSnapshotID(parent) != beforeID {
						t.Errorf("refused truncate changed accepted writer: data=%q seq=%d id=%s", parent.Dirty.Bytes(), parent.DirtySeq, ftruncateSnapshotID(parent))
					}
				} else {
					if st != gofuse.OK {
						t.Fatalf("clean truncate admission=%v", st)
					}
					child, _ := fs.fileHandles.Get(opened.Fh)
					event := child.pendingFtruncate.Load()
					if event == nil || event.path != newPath || event.ino != ino || event.view != originalEvent.view || !child.LineageTrusted || (!split && !ftruncateDescends(child, originalEvent.id)) {
						t.Fatalf("new-path truncate lacks ordered lineage: event=%+v trusted=%t inherited=%q snapshot=%q", event, child.LineageTrusted, child.ftruncateInherited, ftruncateSnapshotID(child))
					}
					if originalEvent.path != oldPath {
						t.Fatal("lookup mutated immutable source event")
					}
				}
				unblock()
				select {
				case rst := <-result:
					result <- rst
					if rst != gofuse.OK {
						t.Fatal(rst)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("rename did not complete after admission")
				}
				if dirtySuccessor {
					// Check both refusal and eventual persistence of the previously accepted H.
					if t.Failed() {
						return
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: parentID}); st != gofuse.OK {
						t.Fatalf("accepted writer fsync=%v", st)
					}
					if opened.Fh != 0 {
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: opened.Fh})
					}
				} else {
					if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: opened.Fh}, []byte("X")); st != gofuse.OK {
						t.Fatalf("child write=%v", st)
					}
					if order == "parent_release_child_fsync" {
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: parentID})
						fs.commitQueue.WaitPath(newPath)
					}
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: opened.Fh}); st != gofuse.OK {
						t.Fatalf("child fsync=%v", st)
					}
					if order == "child_fsync_parent_fsync_release" {
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: parentID}); st != gofuse.OK {
							t.Fatalf("old fd fsync=%v", st)
						}
					}
					if order != "parent_release_child_fsync" {
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: parentID})
					}
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: childPID}}, Fh: opened.Fh})
				}
				fs.commitQueue.WaitPath(newPath)
				remote.mu.Lock()
				defer remote.mu.Unlock()
				want := "X"
				if dirtySuccessor {
					want = "Hello"
				}
				if remote.data != want || remote.oldPuts != 0 || remote.conflicts != 0 || remote.renames != 1 {
					t.Errorf("RESULT data=%q want=%q old_puts=%d conflicts=%d renames=%d", remote.data, want, remote.oldPuts, remote.conflicts, remote.renames)
				}
				if meta, ok := fs.pendingIndex.GetMeta(newPath); ok && meta.Kind == PendingConflict {
					t.Errorf("retained conflict: %+v", meta)
				}
				t.Logf("RESULT data=%q old_puts=%d conflicts=%d old_fd_progress=true", remote.data, remote.oldPuts, remote.conflicts)
			})
		}
	}
}
