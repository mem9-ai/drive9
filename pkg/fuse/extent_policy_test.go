package fuse

import (
	"context"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestExtentLayoutForCreateIgnoresWriteSync(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{
		WritePolicy: WritePolicyWriteSync,
		ExtentPaths: []string{"*-wal", "*.sqlite"},
		Profile:     "coding-agent",
	}}
	if got := fs.extentLayoutForCreate("/data/app.db-wal"); got != client.ContentLayoutExtent {
		t.Fatalf("db-wal layout = %q, want extent under write-sync", got)
	}
	if got := fs.extentLayoutForCreate("/data/app.sqlite"); got != client.ContentLayoutExtent {
		t.Fatalf("sqlite layout = %q, want extent under write-sync", got)
	}
	if got := fs.extentLayoutForCreate("/notes/readme.txt"); got != client.ContentLayoutSingle {
		t.Fatalf("txt layout = %q, want single", got)
	}
}

func TestExtentLayoutForCreateStarMatchesAllFiles(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{
		ExtentPaths: []string{"*"},
		Profile:     "coding-agent-extent",
	}}
	for _, p := range []string{"/notes/readme.txt", "/data/app.db", "/nested/dir/foo.c"} {
		if got := fs.extentLayoutForCreate(p); got != client.ContentLayoutExtent {
			t.Fatalf("layout for %s = %q, want extent", p, got)
		}
	}
	if got := fs.extentLayoutForCreate("/data/app.db-shm"); got != client.ContentLayoutExtent {
		t.Fatalf("profile * must select WAL index as extent, got %q", got)
	}
	if got := fs.extentLayoutForCreate("/data/app.db-journal"); got != client.ContentLayoutExtent {
		t.Fatalf("profile * must select rollback journal as extent, got %q", got)
	}
}

func TestExtentLayoutForCreateNoneProfile(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{
		WritePolicy: WritePolicyWriteSync,
		Profile:     "none",
	}}
	if got := fs.extentLayoutForCreate("/data/app.db-wal"); got != client.ContentLayoutSingle {
		t.Fatalf("none profile must not auto-extent, got %q", got)
	}
}

func TestExtentLayoutForCreateRequiresProfilePatterns(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{Profile: "coding-agent"}}
	if got := fs.extentLayoutForCreate("/data/app.db-wal"); got != client.ContentLayoutSingle {
		t.Fatalf("without [extent] globs, layout = %q, want single", got)
	}
	fs.opts.ExtentPaths = []string{"*.db", "*.sqlite", "*-wal", "*-journal"}
	if got := fs.extentLayoutForCreate("/data/app.db-wal"); got != client.ContentLayoutExtent {
		t.Fatalf("with [extent] globs, layout = %q, want extent", got)
	}
	if got := fs.extentLayoutForCreate("/notes/readme.txt"); got != client.ContentLayoutSingle {
		t.Fatalf("unmatched path layout = %q, want single", got)
	}
	if got := fs.extentLayoutForCreate("/logs/events.jsonl"); got != client.ContentLayoutSingle {
		t.Fatalf("jsonl layout = %q, want single (sqlite-only [extent])", got)
	}
}

func TestExtentFlushDoesNotConvertExistingSingleMatchingGlob(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*.db", "*-wal"}}}
	wb := NewWriteBuffer("/data/app.db", 1<<20, 0)
	if _, err := wb.Write(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Path:          "/data/app.db",
		Dirty:         wb,
		OrigSize:      5,
		BaseRev:       0,
		IsNew:         false,
		ContentLayout: client.ContentLayoutSingle,
		extentDirty:   &extentDirtySet{},
	}
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	if st != gofuse.OK {
		t.Fatalf("status=%v", st)
	}
	if handled {
		t.Fatal("existing single file must keep standard flush even when [extent] now matches")
	}
	if fh.ContentLayout != client.ContentLayoutSingle {
		t.Fatalf("ContentLayout=%q, must stay single", fh.ContentLayout)
	}
}

func TestExtentAttachWriterIgnoresExistingSingleMatchingGlob(t *testing.T) {
	fs := &Dat9FS{
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*.db"}},
	}
	fh := &FileHandle{
		Ino:           9,
		Path:          "/data/app.db",
		IsNew:         false,
		ContentLayout: client.ContentLayoutSingle,
	}
	fs.attachExtentWriter(fh)
	if fh.extentWriter != nil || fh.extentOpen != nil {
		t.Fatal("existing single file must not get an extent writer just because the glob matches")
	}
}

func TestExtentDirtySetMergeAndClip(t *testing.T) {
	d := &extentDirtySet{}
	d.add(8, 16)
	d.add(0, 8)
	d.add(12, 24)
	if len(d.ranges) != 1 || d.ranges[0].start != 0 || d.ranges[0].end != 24 {
		t.Fatalf("merged ranges=%+v", d.ranges)
	}
	d.clip(10)
	if len(d.ranges) != 1 || d.ranges[0].end != 10 {
		t.Fatalf("clipped ranges=%+v", d.ranges)
	}
}

func TestTruncateExtentPreservesOrigSize(t *testing.T) {
	fs := &Dat9FS{dirtyInodes: map[uint64]dirtyInodeState{}}
	wb := NewWriteBuffer("/app.db-wal", 64, 0)
	if _, err := wb.Write(0, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Dirty:         wb,
		OrigSize:      32,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
	}
	fh.extentDirty.add(0, 32)
	if _, err := fs.truncateWritableHandleLocked(fh, 8); err != nil {
		t.Fatal(err)
	}
	if fh.OrigSize != 32 {
		t.Fatalf("OrigSize=%d want 32 (remote committed size)", fh.OrigSize)
	}
	if fh.Dirty.Size() != 8 {
		t.Fatalf("dirty size=%d want 8", fh.Dirty.Size())
	}
}

func TestExtentWriteInvalidatesOpenFileChunkCache(t *testing.T) {
	// JuiceFS meta.Write InvalidateChunk. Merging the new slice into the
	// snap served a stale SQLite header (walthread2 SQLITE_NOTADB).
	c := newExtentReadCache(1 << 20)
	c.putBlock("blocks/old", 0, 8, []byte("abcdefgh"))
	c.putSlices("/f", 1, 1, 8, []client.SliceRow{{
		FileOff: 0, Len: 8, BlockKey: "blocks/old", BlockOff: 0, Kind: "data",
	}})
	c.invalidatePath("/f")
	fs := &Dat9FS{extentCache: c}
	if _, ok := fs.extentCachedWindow("/f", 0, 0, 8); ok {
		t.Fatal("JuiceFS meta.Write must InvalidateChunk")
	}
}

func TestExtentReadCacheServesWindow(t *testing.T) {
	c := newExtentReadCache(1 << 20)
	c.putBlock("blocks/a/1", 0, 8, []byte("abcdefgh"))
	c.putSlices("/f", 2, 1, 8, []client.SliceRow{{
		FileOff: 0, Len: 8, BlockKey: "blocks/a/1", BlockOff: 0,
	}})
	fs := &Dat9FS{extentCache: c}
	got, ok := fs.extentCachedWindow("/f", 2, 2, 3)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(got) != "cde" {
		t.Fatalf("got %q want cde", got)
	}
	if _, ok := fs.extentCachedWindow("/f", 3, 2, 3); ok {
		t.Fatal("stale revision must miss")
	}
}

func TestExtentReadCacheDropsCompleteSnapOnGeneration(t *testing.T) {
	c := newExtentReadCache(1 << 20)
	c.putBlock("old", 0, 8, []byte("AAAAAAAA"))
	c.putBlock("new", 0, 8, []byte("BBBBBBBB"))
	c.putSlices("/f", 2, 1, 8, []client.SliceRow{{
		FileOff: 0, Len: 8, BlockKey: "old", BlockOff: 0,
	}})
	c.mergePlanSlices("/f", 2, 2, 8, []client.SliceRow{{
		FileOff: 0, Len: 8, BlockKey: "new", BlockOff: 0,
	}})
	fs := &Dat9FS{extentCache: c}
	if _, ok := c.slicesForGen("/f", 2, 1); ok {
		t.Fatal("compact generation must miss the old complete snap")
	}
	got, ok := fs.extentCachedWindow("/f", 2, 0, 8)
	if !ok {
		t.Fatal("new generation snap must hit")
	}
	if string(got) != "BBBBBBBB" {
		t.Fatalf("got %q want compacted bytes", got)
	}
}

func TestExtentWindowFromPartsRejectsUnlabeledGap(t *testing.T) {
	// JuiceFS buildSlice labels gaps as Id=0. A plan that skips a middle
	// data slice must not KEEP_CACHE zeros for that page.
	c := newExtentReadCache(1 << 20)
	c.putBlock("a", 0, 4, []byte("AAAA"))
	c.putBlock("c", 0, 4, []byte("CCCC"))
	fs := &Dat9FS{extentCache: c}
	if _, ok := fs.extentWindowFromParts([]client.ExtentReadPart{
		{FileOff: 0, Len: 4, BlockKey: "a", BlockOff: 0},
		{FileOff: 8, Len: 4, BlockKey: "c", BlockOff: 0},
	}, 0, 12); ok {
		t.Fatal("unlabeled 4-byte gap must miss, not serve zeros")
	}
	got, ok := fs.extentWindowFromParts([]client.ExtentReadPart{
		{FileOff: 0, Len: 4, BlockKey: "a", BlockOff: 0},
		{FileOff: 4, Len: 4},
		{FileOff: 8, Len: 4, BlockKey: "c", BlockOff: 0},
	}, 0, 12)
	if !ok {
		t.Fatal("explicit hole part must cover the gap")
	}
	if string(got) != "AAAA\x00\x00\x00\x00CCCC" {
		t.Fatalf("got %q", got)
	}
}

func TestExtentWindowFromPartsUsesThisPlan(t *testing.T) {
	c := newExtentReadCache(1 << 20)
	c.putBlock("stale", 0, 8, []byte("AAAAAAAA"))
	c.putBlock("fresh", 0, 4, []byte("BBBB"))
	c.putSlices("/f", 5, 1, 8, []client.SliceRow{{
		FileOff: 0, Len: 8, BlockKey: "stale", BlockOff: 0,
	}})
	fs := &Dat9FS{extentCache: c}
	got, ok := fs.extentWindowFromParts([]client.ExtentReadPart{{
		FileOff: 0, Len: 4, BlockKey: "fresh", BlockOff: 0,
	}}, 0, 4)
	if !ok {
		t.Fatal("plan parts in cache must hit")
	}
	if string(got) != "BBBB" {
		t.Fatalf("got %q want BBBB (this plan, not stale complete snap)", got)
	}
}

func TestExtentCachedWindowFlattensOverlappingSeq(t *testing.T) {
	c := newExtentReadCache(1 << 20)
	c.putBlock("old", 0, 8, []byte("AAAAAAAA"))
	c.putBlock("new", 0, 4, []byte("BBBB"))
	c.putSlices("/f", 3, 1, 8, []client.SliceRow{
		{Chunk: 0, Seq: 1, FileOff: 0, Len: 8, BlockKey: "old", BlockOff: 0},
		{Chunk: 0, Seq: 2, FileOff: 2, Len: 4, BlockKey: "new", BlockOff: 0},
	})
	fs := &Dat9FS{extentCache: c}
	got, ok := fs.extentCachedWindow("/f", 3, 0, 8)
	if !ok {
		t.Fatal("flattened overlapping snapshot must hit")
	}
	if string(got) != "AABBBBAA" {
		t.Fatalf("got %q want AABBBBAA", got)
	}
}

func TestExtentReadCacheIncompleteWindowMisses(t *testing.T) {
	c := newExtentReadCache(1 << 20)
	c.putBlock("blocks/a/1", 0, 4, []byte("abcd"))
	c.mergePlanSlices("/f", 2, 1, 16, []client.SliceRow{{
		FileOff: 0, Len: 4, BlockKey: "blocks/a/1", BlockOff: 0,
	}})
	fs := &Dat9FS{extentCache: c}
	if _, ok := fs.extentCachedWindow("/f", 2, 4, 4); ok {
		t.Fatal("partial slice index must not serve uncovered bytes as zeros")
	}
	got, ok := fs.extentCachedWindow("/f", 2, 0, 4)
	if !ok {
		t.Fatal("covered window must hit")
	}
	if string(got) != "abcd" {
		t.Fatalf("got %q", got)
	}
	c.putSlices("/f", 2, 1, 16, []client.SliceRow{{
		FileOff: 0, Len: 4, BlockKey: "blocks/a/1", BlockOff: 0,
	}})
	zeros, ok := fs.extentCachedWindow("/f", 2, 4, 4)
	if !ok {
		t.Fatal("complete snapshot may serve holes as zeros")
	}
	if string(zeros) != "\x00\x00\x00\x00" {
		t.Fatalf("hole=%q", zeros)
	}
}

func TestTruncateExtentGrowLeavesHole(t *testing.T) {
	fs := &Dat9FS{dirtyInodes: map[uint64]dirtyInodeState{}}
	wb := NewWriteBuffer("/app.db-wal", 64, 0)
	if _, err := wb.Write(0, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Dirty:         wb,
		OrigSize:      8,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 24); err != nil {
		t.Fatalf("grow truncate: %v", err)
	}
	if fh.OrigSize != 8 {
		t.Fatalf("OrigSize=%d want 8", fh.OrigSize)
	}
	if fh.Dirty.Size() != 24 {
		t.Fatalf("dirty size=%d want 24", fh.Dirty.Size())
	}
	if !fh.extentDirty.empty() {
		t.Fatalf("grow must not dirty zeros, dirty=%+v", fh.extentDirty.ranges)
	}
	got := make([]byte, 16)
	if n := fh.Dirty.ReadAt(8, got); n != 16 {
		t.Fatalf("ReadAt n=%d", n)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("gap byte %d = %d, want 0", i, b)
		}
	}
}
