package extent

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// The server resolves a hardlink's SOURCE by name, so that name must exist
// remotely before the link is issued. An extent create lives only in the
// bridge until its first commit, and the write-back plane cannot flush it
// (engine handles are registered here), so an immediate link after create(2)
// — pjdfstest link/06, link/07 and unlink/00 — answered ENOENT for a file
// create(2) had already reported as existing.
func TestLinkAfterExtentCreatePublishesTheSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write int
	}{
		{"empty", 0},
		{"written", 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeV2(t)
			f.setDir("/d", true)
			h := newTestHandler(t, f, 0) // full mode: the engine claims creates
			res, err, passed := dispatchCreate(context.Background(), h, "/d/a.bin", syscall.O_RDWR|syscall.O_CREAT)
			if err != nil || passed {
				t.Fatalf("create err=%v passedToClassic=%v", err, passed)
			}
			if tc.write > 0 {
				if err := res.Handle.Write(context.Background(), 0, make([]byte, tc.write)); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			f.mu.Lock()
			published := f.hasClassic("/d/a.bin") || f.extent["/d/a.bin"] != ""
			f.mu.Unlock()
			if published {
				t.Fatal("precondition: the name was already remote, so this proves nothing")
			}

			var visibleDuringLink bool
			if _, lerr := h.Do(context.Background(), &fs.Op{
				Kind: fs.OpLink, Path: "/d/a.bin", Path2: "/d/b.bin", Caller: fs.Caller{Pid: 1},
			}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
				f.mu.Lock()
				visibleDuringLink = f.hasClassic(op.Path) || f.extent[op.Path] != ""
				f.mu.Unlock()
				return fs.Result{}, nil
			})); lerr != nil {
				t.Fatalf("link: %v", lerr)
			}
			if !visibleDuringLink {
				t.Fatal("the source name is still unpublished when the hardlink runs: the server answers ENOENT")
			}
			// Publishing the NAME is not enough: the linked object must carry
			// the bytes too. A flush that stops at the bridge's pending list
			// leaves the JuiceFS writer's buffer (and the new length) behind,
			// so the source materializes as an EMPTY file and the alias reads
			// EOF despite the successful write.
			if tc.write > 0 {
				f.mu.Lock()
				got := f.extent["/d/a.bin"]
				if got == "" {
					got = f.classic["/d/a.bin"]
				}
				f.mu.Unlock()
				if len(got) != tc.write {
					t.Fatalf("source published %d bytes at link time, want %d: the writer buffer was not flushed",
						len(got), tc.write)
				}
			}
		})
	}
}
