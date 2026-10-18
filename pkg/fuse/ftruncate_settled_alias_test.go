package fuse

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestFtruncateSettledHardlinkExternalUpdate(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		truncated, warm, refreshAttr bool
		payload                      string
	}{
		{"no-truncate-cold", false, false, true, "world"},
		{"no-truncate-warm", false, true, true, "world"},
		{"cold", true, false, true, "world"},
		{"warm", true, true, true, "world"},
		{"grow", true, true, true, "world extended"},
		{"empty", true, true, true, ""},
		{"sse-only", true, true, false, "world extended"},
		{"cold-sse-only", true, false, false, "world"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			parent, _ := fs.fileHandles.Get(a)
			reader, _ := fs.fileHandles.Get(b)
			if parent.Path == reader.Path {
				t.Fatal("expected two actual hardlink paths")
			}
			before := "hello world"
			if tc.truncated {
				truncateHandoffTruncate(t, fs, ino, a, 5)
				hardlinkSync(t, fs, ino, a)
				before = "hello"
			}
			if tc.warm {
				if got, st := truncateHandoffRead(fs, ino, b, 0, 64); st != gofuse.OK || string(got) != before {
					t.Fatalf("warm read=%q/%v", got, st)
				}
			}
			_, rev, _ := remote.snapshot()
			newRev, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte(tc.payload), rev)
			if err != nil || newRev != rev+1 {
				t.Fatalf("external commit=rev%d/%v", newRev, err)
			}
			(&SSEWatcher{fs: fs, actor: "local"}).handleChange(&client.ChangeEvent{Seq: 9, Path: "/file.bin", Op: "write", Actor: "external"})
			if tc.refreshAttr {
				if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gofuse.AttrOut{}); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			for attempt := 0; attempt < 3; attempt++ {
				if got, st := truncateHandoffRead(fs, ino, b, 0, 64); st != gofuse.OK || string(got) != tc.payload {
					t.Errorf("external read%d=%q/%v, want%q/OK", attempt, got, st, tc.payload)
				}
			}
			if tc.truncated && (reader.pendingFtruncate.Load() != nil || reader.LineageTrusted || reader.ContentSnapshotID != "" || reader.StagedSnapshotID != "") {
				t.Error("settled reader retained truncate lineage after external commit")
			}
		})
	}
}

func TestFtruncatePathZeroCommitSettlesObservers(t *testing.T) {
	for _, alias := range []bool{false, true} {
		for _, reset := range []bool{false, true} {
			t.Run(fmt.Sprintf("alias=%t/reset=%t", alias, reset), func(t *testing.T) {
				fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
				a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				if alias {
					addTruncateHandoffAlias(t, fs, ino, remote)
				}
				b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
				ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
				truncateHandoffTruncate(t, fs, ino, a, 5)
				hardlinkSync(t, fs, ino, a)
				if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 0,
				}}, &gofuse.AttrOut{}); st != gofuse.OK {
					t.Fatal(st)
				}
				data, rev, _ := remote.snapshot()
				if len(data) != 0 || rev != 3 {
					t.Fatalf("acknowledged pathname zero=%q/rev%d", data, rev)
				}
				committed := stateOf(fs, ino)
				for _, id := range []uint64{a, b, ro} {
					if event := truncateHandoffEvent(fs, id); event != nil && event.seq > committed.committedSeq {
						t.Errorf("remote-acknowledged zero has unresolved event%d above committed%d", event.seq, committed.committedSeq)
					}
					if got, st := truncateHandoffRead(fs, ino, id, 0, 64); st != gofuse.OK || len(got) != 0 {
						t.Fatalf("acknowledged zero read=%q/%v", got, st)
					}
				}
				if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("world"), rev); err != nil {
					t.Fatal(err)
				}
				(&SSEWatcher{fs: fs, actor: "local"}).handleChange(&client.ChangeEvent{Seq: 9, Path: "/file.bin", Op: "write", Actor: "external"})
				if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gofuse.AttrOut{}); st != gofuse.OK {
					t.Fatal(st)
				}
				if reset {
					fs.resetMountView()
				}
				for _, id := range []uint64{a, b, ro} {
					if got, st := truncateHandoffRead(fs, ino, id, 0, 64); st != gofuse.OK || !bytes.Equal(got, []byte("world")) {
						t.Errorf("post-zero external read=%q/%v, want world/OK", got, st)
					}
				}
			})
		}
	}
}

func TestFtruncateSettledHardlinkProtectsIdentityAndChild(t *testing.T) {
	for _, mode := range []string{"replacement-same-revision", "replacement-new-revision", "pending-child"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			truncateHandoffTruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			if got, st := truncateHandoffRead(fs, ino, b, 0, 64); st != gofuse.OK || string(got) != "hello" {
				t.Fatalf("warm read=%q/%v", got, st)
			}
			if mode == "pending-child" {
				hardlinkWrite(t, fs, ino, b, "H")
				_, rev, _ := remote.snapshot()
				if _, err := newTestClient(fs.client.BaseURL()).WriteCtxConditionalWithRevision(t.Context(), "/file.bin", []byte("world"), rev); err != nil {
					t.Fatal(err)
				}
			} else {
				remote.mu.Lock()
				remote.data, remote.resource = []byte("world"), "replacement-resource"
				if mode == "replacement-new-revision" {
					remote.revision++
				}
				remote.mu.Unlock()
			}
			(&SSEWatcher{fs: fs, actor: "local"}).handleChange(&client.ChangeEvent{Seq: 9, Path: "/file.bin", Op: "write", Actor: "external"})
			got, st := truncateHandoffRead(fs, ino, b, 0, 64)
			if mode == "pending-child" {
				if st != gofuse.OK || string(got) != "Hello" {
					t.Fatalf("accepted child hidden by external update: %q/%v", got, st)
				}
			} else if st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("replacement supplied bytes: %q/%v", got, st)
			}
			if truncateHandoffEvent(fs, b) == nil {
				t.Fatal("unsettled child or replacement retired the observation")
			}
		})
	}
}

func TestFtruncatePathZeroCommitPolicy(t *testing.T) {
	for _, policy := range []WritePolicy{WritePolicyCloseSync, WritePolicyWriteSync} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			fs.opts.WritePolicy = policy
			a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			addTruncateHandoffAlias(t, fs, ino, remote)
			b := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
			ro := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
			truncateHandoffTruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 0}}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatal(st)
			}
			data, rev, _ := remote.snapshot()
			if len(data) != 0 || rev != 3 {
				t.Fatalf("PREMISE zero not acknowledged: %q/rev%d", data, rev)
			}
			for _, id := range []uint64{a, b, ro} {
				if got, st := truncateHandoffRead(fs, ino, id, 0, 64); st != gofuse.OK || len(got) != 0 {
					t.Errorf("ack zero fd%d=%q/%v", id, got, st)
				}
			}
			hardlinkWrite(t, fs, ino, b, "X")
			if got, st := truncateHandoffRead(fs, ino, ro, 0, 64); st != gofuse.OK || string(got) != "X" {
				t.Errorf("accepted child=%q/%v", got, st)
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Error(st)
			}
			hardlinkSync(t, fs, ino, b)
			data, _, _ = remote.snapshot()
			if string(data) != "X" {
				t.Fatalf("accepted child lost: %q", data)
			}
		})
	}
}
