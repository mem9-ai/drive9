package writeback

// qiffang twelfth round (post-#984): an alias rebase advances wgen so a
// stale in-flight upload cannot retire the rebase's dirty state; the
// admission sweep's lock ordering uses lock-held path snapshots.

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// An upload that captured the sibling's PRE-rebase generation (a
// close-sync Flush or a Drain snapshot in flight when the truncate
// landed) must not retire the truncate's dirty state on completion: the
// rebase advances wgen, so generation equality fails and the mutation
// keeps its own publisher.
func TestPortAliasTruncateRebaseFencesInFlightUpload(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/orig", []byte("hello world"))
	b.Seed("/alias", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	wb.SetInodeTable(&fakeInodeTable{
		byPath:  map[string]uint64{"/orig": 7, "/alias": 7},
		aliases: map[uint64][]string{7: {"/orig", "/alias"}},
	})
	ctx := context.Background()

	resO, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/orig", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	resA, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/alias", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	sib := writebackHandleOf(resA.Handle)

	// Materialize the sibling's buffer and let an upload snapshot its
	// generation (what Flush/DrainFlush capture before the PUT).
	sib.mu.Lock()
	if err := sib.ensureBufferLocked(ctx); err != nil {
		sib.mu.Unlock()
		t.Fatal(err)
	}
	stagedGen := sib.wgen
	sib.mu.Unlock()

	// The owner truncates; the admission rebases the alias sibling (a
	// publisher now) — the rebase must advance wgen past stagedGen.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/orig", Size: 5, Handle: resO.Handle, Ino: 7}); err != nil {
		t.Fatalf("ftruncate: %v", err)
	}
	sib.mu.Lock()
	if sib.wgen == stagedGen {
		sib.mu.Unlock()
		t.Fatal("alias rebase did not advance wgen past the in-flight upload's generation")
	}
	// The stale upload completes and tries to retire its generation.
	sib.retireDirtyLocked(stagedGen)
	dirty := sib.dirty
	sib.mu.Unlock()
	if !dirty {
		t.Fatal("stale in-flight upload retired the truncate's dirty state")
	}
	// The surviving publisher lands the truncated image at close.
	if err := resA.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resO.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/orig"); !ok || string(n.Data) != "hello" {
		t.Fatalf("remote /orig = %q ok=%v, want hello", n.Data, ok)
	}
	if n, ok := b.Node("/alias"); !ok || string(n.Data) != "hello" {
		t.Fatalf("remote /alias = %q ok=%v, want hello", n.Data, ok)
	}
}

// Renames racing fd-truncate admissions must stay data-race free (the
// sweep's ordering keys are snapshotted under each handle's lock) and the
// truncates must cleanly succeed or refuse — never interleave a partial
// sibling set. Run under -race.
func TestPortRenameDuringFtruncateStaysRaceFree(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("hello world"))
	b.Seed("/b", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	wb.SetInodeTable(&fakeInodeTable{
		byPath:  map[string]uint64{"/a": 7, "/b": 7},
		aliases: map[uint64][]string{7: {"/a", "/b"}},
	})
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := writebackHandleOf(res.Handle)
	// A second descriptor on the alias keeps the sibling set non-trivial
	// while renames flip path ownership underneath the admissions.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: uint32(syscall.O_RDWR)}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			if flip {
				wb.retarget("/a", "/b")
			} else {
				wb.retarget("/b", "/a")
			}
			flip = !flip
		}
	}()
	var refused int
	for i := 0; i < 100; i++ {
		size := int64(i % 7)
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/a", Size: size, Handle: res.Handle, Ino: 7})
		if err != nil {
			if !errors.Is(err, syscall.EAGAIN) {
				t.Fatalf("truncate: %v", err)
			}
			refused++
		}
	}
	close(stop)
	wg.Wait()

	// Post-quiescence the sweep converges: with renames stopped the
	// admission completes on the NAMING descriptor. (A same-path sibling
	// is deliberately untouched by the admission, so only the owner's
	// size is asserted; the race detector carries the interleaving
	// guarantee.)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/a", Size: 5, Handle: res.Handle, Ino: 7}); err != nil {
		t.Fatalf("final truncate after renames stopped: %v", err)
	}
	h.mu.Lock()
	size := int64(0)
	if h.buf != nil {
		size = h.buf.Size()
	}
	h.mu.Unlock()
	if size != 5 {
		t.Fatalf("owner buffer size = %d, want 5", size)
	}
	if refused == 100 {
		t.Fatal("every admission was refused — the sweep never completed against quiesced renames")
	}
}

// A sibling whose mutex sits over a stalled operation (buffer hydration
// blocked on a remote read) must not hang an fd-truncate admission: the
// ordering-key snapshot is TryLock-bounded too, so the admission exhausts
// its budget and fails closed with EAGAIN instead of blocking on the
// snapshot before any deadline check (review P1).
func TestPortTruncateAdmissionBoundedByStalledSibling(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("hello world"))
	b.Seed("/c", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	wb.SetInodeTable(&fakeInodeTable{
		byPath:  map[string]uint64{"/a": 7, "/c": 7},
		aliases: map[uint64][]string{7: {"/a", "/c"}},
	})
	ctx := context.Background()

	resA, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	resC, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/c", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	owner := writebackHandleOf(resA.Handle)
	stalled := writebackHandleOf(resC.Handle)

	// Hold the sibling's mutex as a stalled hydration would.
	stalled.mu.Lock()
	defer stalled.mu.Unlock()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/a", Size: 5, Handle: resA.Handle, Ino: 7})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("admission under stalled sibling = %v, want EAGAIN", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("admission took %s past its budget before failing closed", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("admission hung on the stalled sibling's mutex")
	}
	_ = owner
}
