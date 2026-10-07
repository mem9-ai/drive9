package fuse

import (
	"bytes"
	"net/http"
	"strconv"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestRebasePassiveTruncateObservation(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-passive", true: "active-owner"}[active], func(t *testing.T) {
			fs, ino, a, c, remote := newFtruncateCommitFS(t)
			fs.opts.WritePolicy = WritePolicyCloseSync
			interceptFtruncateHTTP(t, fs, func(_ http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodPut && r.Header.Get("X-Dat9-Expected-Revision") == "" {
					r.Header.Set("X-Dat9-Expected-Revision", strconv.FormatInt(fs.inodes.GetRevision(ino), 10))
				}
				return false
			})
			for _, id := range []uint64{a, c} {
				h, _ := fs.fileHandles.Get(id)
				h.WritePolicy = WritePolicyCloseSync
				h.OpenPID = 123
			}
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, c, 5)
			if !active {
				hardlinkSync(t, fs, ino, c)
			}
			header := gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: 123}}
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: header, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			passive, _ := fs.fileHandles.Get(a)
			if passive.DirtySeq != 0 || passive.Dirty.HasDirtyParts() {
				t.Fatal("passive became publisher")
			}
			for _, id := range []uint64{a, ro} {
				readHardlinkWant(t, fs, ino, id, "")
			}
			if active {
				hardlinkWrite(t, fs, ino, c, "new")
				hardlinkSync(t, fs, ino, c)
			}
			want := ""
			if active {
				want = "new"
			}
			for _, id := range []uint64{a, ro} {
				readHardlinkWant(t, fs, ino, id, want)
			}
			hardlinkWrite(t, fs, ino, a, "N")
			hardlinkSync(t, fs, ino, a)
			if active {
				want = "New"
			} else {
				want = "N"
			}
			if remote() != want {
				t.Fatalf("remote=%q want=%q", remote(), want)
			}
		})
	}
}

func TestRebaseDataCallbackRetiresRotatedClaim(t *testing.T) {
	test, header := newCloseSyncRotatedShadowTest(t)
	fs := test.fs
	fh, _ := fs.fileHandles.Get(test.old)
	fh.ShadowStageSeq = 17
	beforeGen := fs.shadowStore.ActiveGeneration(fh.Path)
	before, _ := fs.shadowStore.ReadAll(fh.Path)
	want := append(append([]byte(nil), header...), []byte("new tail")...)
	test.mu.Lock()
	test.content, test.revision = want, 4
	test.mu.Unlock()
	fs.onWriteBackDataCommitted(WriteBackMeta{Path: fh.Path, Inode: fh.Ino, Size: int64(len(want))}, 4, StagingGens{})
	if fh.ShadowReady || fh.ShadowSpill || fh.ShadowStageGen != 0 || fh.ShadowStageSeq != 0 {
		t.Error("callback retained rotated shadow claim")
	}
	after, _ := fs.shadowStore.ReadAll(fh.Path)
	if fs.shadowStore.ActiveGeneration(fh.Path) != beforeGen || !bytes.Equal(after, before) {
		t.Fatal("callback mutated another shadow generation")
	}
	got, st, err := readDat9FSTestRange(fs, fh.Ino, test.old, 0, len(want))
	if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("read=%q/%v/%v want=%q", got, st, err, want)
	}
}
