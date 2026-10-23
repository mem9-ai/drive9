package appendlog

// Ported from v1 pkg/fuse/append_log_publication_test.go: publication
// visibility — a commit retires stale readers (they serve the new image and
// the new EOF), read-your-writes crosses handles through the live-handle
// set (new behavior), O_APPEND baselines refresh from a sibling's unflushed
// tail (new behavior), a late landing preserves the newer dirty generation,
// and fresh opens observe the landed revision.

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestAppendLogTailInvalidatesPrefetchedEOF: after a tail commit, a
// handle opened before the write serves the new bytes and the new EOF — no
// stale short-read survives the publication.
func TestPortAppendLogTailCommitInvalidatesStaleReaderEOF(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	ch := newStack(t, b, "/log.txt")
	ctx := context.Background()

	reader := openLog(t, ch, "/log.txt")
	writer := openLog(t, ch, "/log.txt").(*Handle)
	if err := writer.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 7)
	if n, err := reader.Read(ctx, 0, buf); err != nil || n != 7 || string(buf) != "pretail" {
		t.Fatalf("post-commit read = %q n=%d err=%v, want pretail", buf[:n], n, err)
	}
	if n, err := reader.Read(ctx, 7, buf[:1]); err != nil || n != 0 {
		t.Fatalf("read at new EOF = n=%d err=%v, want 0", n, err)
	}
}

// v1 TestAppendLogOrdinaryCommitDetachesRetiredReader: after an append or
// rewrite commit, every concurrent read through a previously-open handle
// serves the published image (append and rewrite routes).
func TestPortAppendLogOrdinaryCommitDetachesRetiredReader(t *testing.T) {
	for _, route := range []string{"append", "rewrite"} {
		t.Run(route, func(t *testing.T) {
			payload := []byte("tail") // lands after the "pre" base
			want := []byte("pretail")
			off := int64(3)
			if route == "rewrite" {
				payload, off = []byte("rewrite"), 0
				want = []byte("rewrite")
			}
			b := testbackend.NewBackend()
			b.Seed("/log.txt", []byte("pre"))
			ch := newStack(t, b, "/log.txt")
			ctx := context.Background()

			reader := openLog(t, ch, "/log.txt")
			writer := openLog(t, ch, "/log.txt").(*Handle)
			if err := writer.Write(ctx, off, payload); err != nil {
				t.Fatal(err)
			}
			if err := writer.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			var reads sync.WaitGroup
			var mu sync.Mutex
			failed := false
			for range 8 {
				reads.Add(1)
				go func() {
					defer reads.Done()
					buf := make([]byte, len(want))
					n, err := reader.Read(ctx, 0, buf)
					if err != nil || n != len(want) || !bytes.Equal(buf[:n], want) {
						mu.Lock()
						failed = true
						mu.Unlock()
					}
				}()
			}
			reads.Wait()
			if failed {
				t.Fatalf("reads after %s commit served stale bytes", route)
			}
			if node, _ := b.Node("/log.txt"); !bytes.Equal(node.Data, want) {
				t.Fatalf("backend = %q, want %q", node.Data, want)
			}
		})
	}
}

// New-behavior row (liveHandleSet): read-your-writes crosses handles — a
// handle with no buffer of its own serves another live handle's unflushed
// dirty bytes of the same path instead of the stale remote.
func TestPortAppendLogCrossHandleReadYourWrites(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	ch := newStack(t, b, "/log.txt")
	ctx := context.Background()

	writer := openLog(t, ch, "/log.txt").(*Handle)
	reader := openLog(t, ch, "/log.txt")
	if err := writer.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 7)
	if n, err := reader.Read(ctx, 0, buf); err != nil || n != 7 || string(buf) != "pretail" {
		t.Fatalf("cross-handle read = %q n=%d err=%v, want the writer's unflushed pretail", buf[:n], n, err)
	}
	// Reads clamp at the dirty buffer's EOF.
	if n, err := reader.Read(ctx, 7, buf[:4]); err != nil || n != 0 {
		t.Fatalf("beyond dirty EOF = n=%d err=%v, want 0", n, err)
	}
	if _, ok := b.Node("/log.txt"); !ok || string(n(b, "/log.txt").Data) != "pre" {
		t.Fatal("cross-handle reads must not publish anything")
	}
}

// New-behavior row (refreshAppendBaselineLocked, issue #935): an O_APPEND
// write at kernel offset zero first extends its baseline with another live
// handle's unflushed tail, so the append lands after — not over — the
// concurrent appender's bytes.
func TestPortAppendLogOAppendBaselineRefreshPicksUpSiblingTail(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	ch := newStack(t, b, "/log.txt")
	ctx := context.Background()

	openAppend := func() *Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/log.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND),
			Mode:  0o644,
		})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*Handle)
	}
	first := openAppend()
	second := openAppend()

	// The first appender's tail is still unflushed.
	if err := first.Write(ctx, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	// The kernel hands O_APPEND writes offset 0: the second appender must
	// re-derive the tail from the first appender's dirty buffer.
	if err := second.Write(ctx, 0, []byte("y")); err != nil {
		t.Fatal(err)
	}
	if err := second.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/log.txt"); string(node.Data) != "prexy" {
		t.Fatalf("backend = %q, want prexy (sibling tail preserved)", node.Data)
	}
}

// v1 TestAppendLogLateFinalizerPreservesNewerCommit, retargeted to the new
// three-transport flush: when a write lands while the tail append is in
// flight, the flush keeps going — the tail is recorded, and because the
// handle still carries the newer dirty generation, the full-rewrite
// transport publishes the complete image within the same flush. The newer
// generation is never lost or clobbered by the stale tail snapshot.
func TestPortAppendLogLateLandingPublishesNewerGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, withheldCommit(b, func(r *http.Request) bool {
		return r.Method == http.MethodPost && r.URL.Query().Has("append-log")
	}, started, rel.ch)), "/log.txt")
	ctx := context.Background()

	h := openLog(t, ch, "/log.txt").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	if err := h.Write(ctx, 7, []byte("next")); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; err != nil {
		t.Fatalf("late landing flush: %v", err)
	}

	// The frozen tail landed first; the newer generation followed through
	// the whole-image rewrite in the same flush.
	if node, _ := b.Node("/log.txt"); string(node.Data) != "pretailnext" {
		t.Fatalf("backend after flush = %q, want pretailnext (newer generation preserved)", node.Data)
	}
	if h.dirty {
		t.Fatal("flush must finish with a clean handle")
	}
	// A fresh reader sees the complete image.
	reader := openLog(t, ch, "/log.txt")
	buf := make([]byte, 11)
	if n, err := reader.Read(ctx, 0, buf); err != nil || n != 11 || string(buf) != "pretailnext" {
		t.Fatalf("fresh read = %q n=%d err=%v", buf[:n], n, err)
	}
}

// v1 visibility rows: after publication, a fresh open and a fresh stat both
// observe the landed revision and size.
func TestPortAppendLogPublicationVisibleToFreshOpen(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/log.txt", []byte("pre"))
	ch, events := newStackWithEvents(t, b, "/log.txt")
	collector := &landedCollector{}
	detach := collector.attach(events)
	defer detach()
	ctx := context.Background()

	h := openLog(t, ch, "/log.txt").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/log.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 7 || res.Attr.Rev == 0 {
		t.Fatalf("fresh stat = %+v, want size 7 at the landed revision", res.Attr)
	}
	// The landed event carries exactly what a fresh observer sees.
	evts := collector.snapshot()
	if len(evts) != 1 || evts[0].Size != res.Attr.Size || evts[0].Rev != res.Attr.Rev {
		t.Fatalf("landed event = %+v, stat = %+v (must agree)", evts, res.Attr)
	}
}
