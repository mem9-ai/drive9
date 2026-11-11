package fuse

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type viewCacheProbe struct {
	calls []viewCacheEntry
	inode func(uint64) gofuse.Status
	entry func(uint64, string) gofuse.Status
}

func (p *viewCacheProbe) InodeNotify(node uint64, off, length int64) gofuse.Status {
	if off != 0 || length != 0 {
		return gofuse.EINVAL
	}
	p.calls = append(p.calls, viewCacheEntry{parent: node})
	if p.inode != nil {
		return p.inode(node)
	}
	return gofuse.OK
}
func (p *viewCacheProbe) EntryNotify(parent uint64, name string) gofuse.Status {
	p.calls = append(p.calls, viewCacheEntry{parent: parent, name: name})
	if p.entry != nil {
		return p.entry(parent, name)
	}
	return gofuse.OK
}

func TestViewCacheInvalidationIncludesRootDirectoriesAndHardlinkAliases(t *testing.T) {
	inodes := NewInodeToPath()
	dir := inodes.Lookup("/dir", true, 0, time.Time{})
	file := inodes.LookupWithIdentity("/a", "same-file", 2, false, 8, time.Time{})
	if alias := inodes.LookupWithIdentity("/dir/b", "same-file", 2, false, 8, time.Time{}); alias != file {
		t.Fatal("fixture did not share inode")
	}
	snapshot := inodes.Snapshot()
	plan, err := prepareViewCacheInvalidation(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	// The plan must retain the original names across target installation.
	for i := range snapshot {
		clear(snapshot[i].Paths)
	}
	probe := &viewCacheProbe{inode: func(uint64) gofuse.Status {
		// Notifications are executed without holding the source inode mutex.
		_ = inodes.Snapshot()
		return gofuse.OK
	}}
	if err := plan.invalidate(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	want := []viewCacheEntry{{parent: 1}, {parent: dir}, {parent: file}, {parent: 1, name: "a"}, {parent: 1, name: "dir"}, {parent: dir, name: "b"}}
	if !reflect.DeepEqual(probe.calls, want) {
		t.Fatalf("calls=%v want=%v", probe.calls, want)
	}
}

func TestViewCacheInvalidationWaitsAndReportsPartialFailure(t *testing.T) {
	plan, err := prepareViewCacheInvalidation(NewInodeToPath().Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	probe := &viewCacheProbe{inode: func(uint64) gofuse.Status {
		close(entered)
		<-finish
		return gofuse.EIO
	}}
	done := make(chan error, 1)
	go func() { done <- plan.invalidate(context.Background(), probe) }()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("returned before notification completed: %v", err)
	default:
	}
	close(finish)
	if err := <-done; !errors.Is(err, syscall.EIO) {
		t.Fatalf("lost failure: %v", err)
	}
	// A later attempt repeats the original plan. Missing targets are already
	// invalid, but unsupported notification is not success.
	probe.inode = func(uint64) gofuse.Status { return gofuse.ENOENT }
	if err := plan.invalidate(context.Background(), probe); err != nil {
		t.Fatal(err)
	}
	probe.inode = func(uint64) gofuse.Status { return gofuse.ENOSYS }
	if err := plan.invalidate(context.Background(), probe); !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("unsupported notification accepted: %v", err)
	}
}

func TestViewCacheInvalidationEntryFailureAndCancellation(t *testing.T) {
	inodes := NewInodeToPath()
	inodes.Lookup("/a", false, 0, time.Time{})
	inodes.Lookup("/b", false, 0, time.Time{})
	plan, err := prepareViewCacheInvalidation(inodes.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	probe := &viewCacheProbe{entry: func(uint64, string) gofuse.Status { return gofuse.EIO }}
	if err := plan.invalidate(context.Background(), probe); !errors.Is(err, syscall.EIO) || len(probe.calls) != 4 {
		t.Fatalf("partial error=%v calls=%v", err, probe.calls)
	}
	probe.calls, probe.entry = nil, nil
	if err := plan.invalidate(context.Background(), probe); err != nil || len(probe.calls) != 5 {
		t.Fatalf("retry=%v calls=%v", err, probe.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	probe.calls = nil
	probe.inode = func(uint64) gofuse.Status { cancel(); return gofuse.OK }
	if err := plan.invalidate(ctx, probe); !errors.Is(err, context.Canceled) || len(probe.calls) != 1 {
		t.Fatalf("cancel=%v calls=%v", err, probe.calls)
	}
	probe.calls = nil
	if err := plan.invalidate(ctx, probe); !errors.Is(err, context.Canceled) || len(probe.calls) != 0 {
		t.Fatalf("already canceled=%v calls=%v", err, probe.calls)
	}
	if err := plan.invalidate(context.Background(), nil); err == nil {
		t.Fatal("missing notifier accepted")
	}
}

func TestViewCacheInvalidationRejectsIncompleteSnapshot(t *testing.T) {
	root := NewInodeToPath().Snapshot()[0]
	for _, snapshot := range [][]InodeEntry{
		nil,
		{root, root},
		{root, {Ino: 0}},
		{root, {Ino: 2, Paths: map[string]struct{}{"/missing/a": {}}}},
		{root, {Ino: 2, IsDir: true, Paths: map[string]struct{}{"/": {}}}},
	} {
		if _, err := prepareViewCacheInvalidation(snapshot); err == nil {
			t.Fatalf("accepted incomplete snapshot: %+v", snapshot)
		}
	}
}

type viewCacheReplyFS struct {
	gofuse.RawFileSystem
}

func (v *viewCacheReplyFS) GetAttr(_ <-chan struct{}, in *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	out.SetTimeout(time.Minute)
	out.Mode = 0700
	return gofuse.OK
}
func (v *viewCacheReplyFS) SetAttr(c <-chan struct{}, in *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	return v.GetAttr(c, &gofuse.GetAttrIn{InHeader: in.InHeader}, out)
}
func (v *viewCacheReplyFS) Statx(_ <-chan struct{}, in *gofuse.StatxIn, out *gofuse.StatxOut) gofuse.Status {
	out.SetTimeout(time.Minute)
	return gofuse.OK
}
func (v *viewCacheReplyFS) Lookup(_ <-chan struct{}, h *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	out.SetEntryTimeout(time.Minute)
	out.SetAttrTimeout(time.Minute)
	if name == "exists" {
		out.NodeId = 9
	} else if name == "root" {
		out.NodeId = 1
	}
	return gofuse.OK
}

func TestViewCacheRepliesCannotExtendRootOrNegativeCacheTTL(t *testing.T) {
	v := &viewRequestFS{inner: &viewCacheReplyFS{}}
	for _, node := range []uint64{1, 9} {
		want := time.Minute
		if node == 1 {
			want = 0
		}
		var attr gofuse.AttrOut
		if st := v.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: node}}, &attr); st != gofuse.OK || attr.Timeout() != want || attr.Mode != 0700 {
			t.Fatalf("getattr=%+v %v", attr, st)
		}
		if st := v.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: node}}}, &attr); st != gofuse.OK || attr.Timeout() != want || attr.Mode != 0700 {
			t.Fatalf("setattr=%+v %v", attr, st)
		}
		var stat gofuse.StatxOut
		if st := v.Statx(nil, &gofuse.StatxIn{InHeader: gofuse.InHeader{NodeId: node}}, &stat); st != gofuse.OK || stat.Timeout() != want {
			t.Fatalf("statx=%+v %v", stat, st)
		}
	}
	for _, name := range []string{"missing", "exists"} {
		var out gofuse.EntryOut
		want := time.Minute
		if name == "missing" {
			want = 0
		}
		if st := v.Lookup(nil, &gofuse.InHeader{NodeId: 1}, name, &out); st != gofuse.OK || out.EntryTimeout() != want || out.AttrTimeout() != want {
			t.Fatalf("lookup=%+v %v", out, st)
		}
	}
}

func TestViewCacheRootEntryAndAbsentMount(t *testing.T) {
	v := &viewRequestFS{inner: &viewCacheReplyFS{}}
	var out gofuse.EntryOut
	if st := v.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "root", &out); st != gofuse.OK || out.AttrTimeout() != 0 || out.EntryTimeout() != time.Minute {
		t.Fatalf("root entry=%+v status=%v", out, st)
	}
	if err := (&FS{}).invalidateViewCache(context.Background(), viewCacheInvalidation{}); err == nil {
		t.Fatal("absent mounted server accepted")
	}
}
