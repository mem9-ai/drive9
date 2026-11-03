package writeback

// Ports of v1 pkg/fuse/close_sync_truncate_release_test.go: the path-truncate
// ownership contract through the new public path (OpSetAttr interception —
// a live writer adopts the truncation locally through truncateLocal, a clean
// path waits for the path's own pending landing before the terminal
// truncate, and hardlink-alias handles rebase through the inode table).
//
// Not ported (v1-internal machinery with no public-path equivalent here):
//   - every append-log / SQLite-WAL row (RetiresRotatedShadow,
//     RebasesAppendLogState, AdoptsConfirmedAppendLogBaseline,
//     RevisionOnlyRefreshAppendsAtCommittedSize,
//     SiblingWriteBeforeTruncateOwnerCommit, CallerOwnedTruncateCommits-
//     BeforeSiblingAppend, WriteSyncPathTruncateAdoptsConfirmedAppendLog-
//     Baseline, GrowthRebaseCannotResurrectOlderSQLiteWALGeneration): the
//     append-log matcher, WAL header state and committed-baseline tracking
//     are not part of this package;
//   - committed-revision refresh rows (RefreshCommittedRevisionRetires-
//     RotatedShadow, CommittedRefreshPreservesPendingWriterShadow,
//     OwnerTruncateCommitReadOnlyFreshness, PinnedReadersFollowOwner-
//     TruncateCommit): pinned shadow readers and the shadow-claim retirement
//     they exercise were facade state in v1;
//   - TestCloseSyncPathTruncateDoesNotMutateNewerShadow and RechecksOwner-
//     AfterConcurrentFlush: white-box shadow-generation and testHook state;
//     the queue-side generation fence is covered by shadow_durability and
//     the stale-payload suites;
//   - TestCloseSyncPathTruncatePreservesIndependentWriterConflict: the new
//     plane keeps one writer per path (live[path] → newest handle), so a
//     path SetAttr always routes to that handle — the v1 multi-handle
//     ownership race has no public-path construction;
//   - TestPassiveCloseSyncHandleExcludesUncommittedCreate: predicate over
//     v1 FileHandle flags.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// fakeInodeTable is a minimal identity table exposing hardlink aliases to
// syncAliasHandlesAfterTruncate.
type fakeInodeTable struct {
	byPath  map[string]uint64
	aliases map[uint64][]string
}

func (f *fakeInodeTable) Ino(path string) (uint64, bool) {
	ino, ok := f.byPath[path]
	return ino, ok
}

func (f *fakeInodeTable) Path(ino uint64) (string, bool) {
	for p, i := range f.byPath {
		if i == ino {
			return p, true
		}
	}
	return "", false
}

func (f *fakeInodeTable) Epoch(string) uint64     { return 0 }
func (f *fakeInodeTable) NextEpoch(string) uint64 { return 1 }
func (f *fakeInodeTable) Aliases(ino uint64) []string {
	return f.aliases[ino]
}

// v1 TestCloseSyncPathTruncateBeforeDelayedRelease (portable rows): the
// truncate adopts the live handle's buffer locally; the remote keeps the
// pre-truncation image until the owning write's commit boundary, and the
// final published image is the truncated base plus the trailing write. The
// v1 release-ordering rows collapse here: Release commits synchronously, so
// only the reuse-vs-second-handle distinction remains.
func TestPortPathTruncateBeforeDelayedRelease(t *testing.T) {
	for _, size := range []int64{0, 2, 8} {
		for _, order := range []string{"reuse-live-handle", "open-second-handle"} {
			t.Run(fmt.Sprintf("size-%d/%s", size, order), func(t *testing.T) {
				b := testbackend.NewBackend()
				ch, _ := newStack(t, PolicyCloseSync, b)
				ctx := context.Background()

				old := open(t, ch, "/race.txt")
				if err := old.Write(ctx, 0, []byte("seed")); err != nil {
					t.Fatal(err)
				}
				if err := old.Flush(ctx); err != nil {
					t.Fatal(err)
				}

				if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/race.txt", Size: size}); err != nil {
					t.Fatalf("truncate: %v", err)
				}
				// The live handle owns the truncation locally; the remote
				// keeps the pre-truncation image until the commit boundary.
				if n, _ := b.Node("/race.txt"); string(n.Data) != "seed" {
					t.Fatalf("remote after truncate = %q, want seed (local adoption)", n.Data)
				}

				writer := old
				if order == "open-second-handle" {
					// The truncation belongs to the handle that was live at
					// SetAttr time; a second handle preloads whatever the
					// remote holds when it first materializes. Releasing the
					// truncation owner first publishes the truncated base the
					// second handle then writes over.
					if err := old.Release(ctx); err != nil {
						t.Fatal(err)
					}
					truncated := make([]byte, size)
					copy(truncated, "seed")
					if n, _ := b.Node("/race.txt"); string(n.Data) != string(truncated) {
						t.Fatalf("remote after owner release = %q, want %q", n.Data, truncated)
					}
					writer = open(t, ch, "/race.txt")
				}
				if err := writer.Write(ctx, 0, []byte("final")); err != nil {
					t.Fatal(err)
				}
				if err := writer.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if err := writer.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if writer != old {
					// The owner already released above.
					want := make([]byte, max(size, 5))
					copy(want, "final")
					if n, _ := b.Node("/race.txt"); string(n.Data) != string(want) {
						t.Fatalf("remote = %q, want %q", n.Data, want)
					}
					return
				}
				if err := old.Release(ctx); err != nil {
					t.Fatal(err)
				}
				want := make([]byte, max(size, 5))
				copy(want, "final")
				if n, _ := b.Node("/race.txt"); string(n.Data) != string(want) {
					t.Fatalf("remote = %q, want %q", n.Data, want)
				}
			})
		}
	}
}

// v1 TestCloseSyncPathTruncateKeepsDirtyOwnerAndCleanSibling: a live dirty
// writer owns the truncate — sizing its clean sibling (the still-open seeded
// handle) publishes nothing, and the remote waits for the owner's commit.
func TestPortPathTruncateKeepsDirtyOwnerAndCleanSibling(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	old := open(t, ch, "/race.txt")
	if err := old.Write(ctx, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := old.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	writer := open(t, ch, "/race.txt")
	if err := writer.Write(ctx, 0, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/race.txt", Size: 0}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if n, _ := b.Node("/race.txt"); string(n.Data) != "seed" {
		t.Fatalf("remote after owner-held truncate = %q, want seed", n.Data)
	}
	if err := old.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/race.txt"); string(n.Data) != "seed" {
		t.Fatalf("clean sibling release published the uncommitted truncate: %q", n.Data)
	}
	if err := writer.Write(ctx, 0, []byte("final")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/race.txt"); string(n.Data) != "final" {
		t.Fatalf("remote = %q, want final", n.Data)
	}
}

// v1 TestCloseSyncPathTruncateRefreshesCleanHardlinkHandle: a truncate
// through one hardlink alias rebases every other open handle of the same
// inode (syncAliasHandlesAfterTruncate). A new-plane alias handle only holds
// a buffer once it has written, so the rebase is pinned on a flushed-then-
// clean alias writer: its retained buffer serves the truncated size and —
// the new behavior, where the rebase marks the alias dirty — its own close
// publishes the truncated image instead of republishing the stale tail.
func TestPortPathTruncateRefreshesCleanHardlinkHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/orig", []byte("seed"))
	b.SeedHardlink("/orig", "/alias")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	wb.SetInodeTable(&fakeInodeTable{
		byPath:  map[string]uint64{"/orig": 7, "/alias": 7},
		aliases: map[uint64][]string{7: {"/orig", "/alias"}},
	})
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	orig := open(t, ch, "/orig")
	alias := open(t, ch, "/alias")
	// Materialize (and clean) the alias buffer over the current image.
	if err := alias.Write(ctx, 0, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	if err := alias.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/orig", Ino: 7, Size: 0}); err != nil {
		t.Fatalf("truncate alias: %v", err)
	}
	// The alias handle was rebased: reads serve the truncated size.
	buf := make([]byte, 4)
	if n, err := alias.Read(ctx, 0, buf); err != nil || n != 0 {
		t.Fatalf("alias read after truncate = %d %v, want 0 bytes", n, err)
	}
	// The rebased alias is a publisher of the truncated image (the rebase
	// marks it dirty): its close publishes the truncation, never the stale
	// pre-truncation tail.
	if err := alias.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/alias"); len(n.Data) != 0 {
		t.Fatalf("alias release republished a stale image: %q", n.Data)
	}
	if err := orig.Write(ctx, 0, []byte("final")); err != nil {
		t.Fatal(err)
	}
	if err := orig.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := orig.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/alias"); string(n.Data) != "final" {
		t.Fatalf("alias remote = %q, want final", n.Data)
	}
}

// v1 TestCloseSyncPathTruncateWaitsForRemoteWithCleanHandle: with no live
// handle, the path truncate commits through the terminal synchronously — it
// must not answer before the remote commit, and a failed commit leaves the
// pre-truncation image untouched.
func TestPortPathTruncateWaitsForRemoteWithCleanHandle(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail-%t", fail), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var publications atomic.Int32
			b := testbackend.NewBackend()
			b.Seed("/clean.txt", []byte("seed"))
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/clean.txt") {
					publications.Add(1)
					once.Do(func() { close(entered) })
					<-release
					if fail {
						http.Error(w, `{"error":"denied"}`, http.StatusForbidden)
						return
					}
				}
				b.ServeHTTP(w, r)
			}))
			t.Cleanup(ts.Close)
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			cl := client.New(ts.URL, "")
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ch, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan error, 1)
			go func() {
				_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/clean.txt", Size: 0})
				done <- err
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("truncate returned before remote commit: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("truncate did not start remote commit")
			}
			select {
			case err := <-done:
				t.Fatalf("truncate returned while commit blocked: %v", err)
			default:
			}
			close(release)
			select {
			case err := <-done:
				if fail && err == nil {
					t.Fatal("failed remote truncate returned success")
				}
				if !fail && err != nil {
					t.Fatalf("truncate: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("truncate did not finish")
			}
			if got := publications.Load(); got != 1 {
				t.Fatalf("truncate publications=%d, want 1", got)
			}
			n, _ := b.Node("/clean.txt")
			if fail && string(n.Data) != "seed" {
				t.Fatalf("failed truncate disturbed the remote: %q", n.Data)
			}
			if !fail && len(n.Data) != 0 {
				t.Fatalf("remote after truncate = %q, want empty", n.Data)
			}
		})
	}
}

// Shadow retirement (v1's queued-image ordering rows): a clean-path truncate
// must not race the path's own pending landing — the queued commit lands
// first, then the terminal truncates the settled base, and the queue's
// staging (shadow + pending) is retired by the landing.
func TestPortQueuedImageLandsBeforeCleanTruncate(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b := testbackend.NewBackend()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deferred.txt") {
			once.Do(func() { close(entered) })
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/deferred.txt")
	if err := h.Write(ctx, 0, []byte("pending-image")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The queued upload is held; the release has handed the image to the
	// queue, so the path has no live writer.
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("queued upload did not start")
	}

	truncated := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/deferred.txt", Size: 2})
		truncated <- err
	}()
	select {
	case err := <-truncated:
		t.Fatalf("truncate returned while its own pending commit was blocked: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-truncated:
		if err != nil {
			t.Fatalf("truncate: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("truncate did not finish after the landing")
	}
	if n, _ := b.Node("/deferred.txt"); string(n.Data) != "pe" {
		t.Fatalf("remote = %q, want the landed image truncated to pe", n.Data)
	}
	// The landing retired the queue's staging state.
	if ledger.Pending.HasPending("/deferred.txt") {
		t.Fatal("pending entry survived the landing")
	}
	if ledger.Shadow.Has("/deferred.txt") {
		t.Fatal("shadow bytes survived the landing")
	}
}

// v1 TestCloseSyncPassivePathTruncateResetsBufferedStreamer: a path truncate
// of a handle with an armed part streamer must not publish stale pre-truncate
// parts — the post-truncate write lands on the rebased buffer only. The new
// v1: a path truncate invalidates the streamed-part cycle — the truncate
// resets WriteBuffer.ResetStreamingState and abandons the active streamer,
// so the finish publishes the truncated image (not the pre-truncation
// bytes of an already-submitted part).
func TestPortPathTruncateResetsStreamedParts(t *testing.T) {
	b := testbackend.NewBackend()
	seed := make([]byte, 4<<20)
	for i := range seed {
		seed[i] = byte('A')
	}
	b.Seed("/stream.bin", seed)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, PreloadLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/stream.bin")
	if bh, ok := h.(*bufferingHandle); ok {
		bh.w.preload = 1 << 20 // force the streaming arm on the 4MiB base
	}
	payload := make([]byte, 9<<20) // completes the first 8MiB part mid-stream
	for i := range payload {
		payload[i] = byte('B')
	}
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/stream.bin", Size: 8}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := h.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/stream.bin")
	// POSIX: ftruncate(8) keeps the first 8 bytes of the current image —
	// the buffered prefix is 'B's from the pre-truncation write, then
	// "new" overwrites [0,3). v1 expected zeros here because its evicted
	// (streamed) part re-materialized zero-filled after the truncate reset
	// — a data-losing quirk; the new plane serves the actual bytes
	// (ledger D-16).
	want := append([]byte("new"), []byte("BBBBB")...)
	if len(n.Data) != 8 || string(n.Data) != string(want) {
		t.Fatalf("path truncate mishandled streamed parts: remote = %d bytes %q, want 8 bytes %q", len(n.Data), n.Data, want)
	}
}
