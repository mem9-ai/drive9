// v1 dat9fs_test.go bypass-arming rows (TestSyncCommitNotifyFailure/
// SuccessLeavePerInodeBypass, the O_TRUNC and setattr-size arming sites):
// the writeback extension arms the DIRECT_IO window at its commit
// boundaries through fs.CacheBypassArmer — kernel-notify success or
// failure is the facade's business and no longer conditions the arming.
package writeback

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

type recordingArmer struct {
	calls []struct {
		path   string
		rev    int64
		size   int64
		reason string
	}
}

func (a *recordingArmer) ArmBypass(path string, rev, size int64, reason string) {
	a.calls = append(a.calls, struct {
		path   string
		rev    int64
		size   int64
		reason string
	}{path, rev, size, reason})
}

func (a *recordingArmer) has(path, reason string) bool {
	for _, c := range a.calls {
		if c.path == path && c.reason == reason {
			return true
		}
	}
	return false
}

// v1 TestSyncCommitNotifyFailureLeavesPerInodeBypass /
// TestSyncCommitNotifySuccessLeavesPerInodeBypassUntilTTL (arming half):
// every close-sync/write-sync commit arms the bypass window.
func TestPortSyncCommitArmsBypass(t *testing.T) {
	for _, policy := range []Policy{PolicyCloseSync, PolicyWriteSync} {
		b := testbackend.NewBackend()
		s, w := newStack(t, policy, b)
		rec := &recordingArmer{}
		w.SetBypassArmer(rec)

		h := open(t, s, "/f")
		if err := h.Write(context.Background(), 0, []byte("hello")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := h.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !rec.has("/f", "sync-commit") {
			t.Fatalf("policy %d: arm calls = %+v, want a sync-commit arm", policy, rec.calls)
		}
		if n, ok := b.Node("/f"); !ok || string(n.Data) != "hello" {
			t.Fatalf("policy %d: backend = %+v", policy, n)
		}
	}
}

// v1 O_TRUNC arming site: opening an existing file with O_TRUNC arms the
// bypass window (the truncate-first commit invalidates page cache).
func TestPortOpenTruncateArmsBypass(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("stale page cache"))
	s, w := newStack(t, PolicyCloseSync, b)
	rec := &recordingArmer{}
	w.SetBypassArmer(rec)

	res, err := s.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(context.Background(), 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !rec.has("/f", "open-truncate") {
		t.Fatalf("arm calls = %+v, want an open-truncate arm", rec.calls)
	}
}

// v1 setattr-size arming site (TestSetAttr_*PathTruncate* family): a
// path-addressed truncate on a staged writer adopts the handle and arms
// the bypass window for the truncate-first commit.
func TestPortSetattrSizeArmsBypass(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("truncate-me"))
	s, w := newStack(t, PolicyWriteBack, b)
	rec := &recordingArmer{}
	w.SetBypassArmer(rec)

	h := open(t, s, "/f")
	if err := h.Write(context.Background(), 0, []byte("truncate-me")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 5}); err != nil {
		t.Fatal(err)
	}
	if !rec.has("/f", "setattr-size") {
		t.Fatalf("arm calls = %+v, want a setattr-size arm", rec.calls)
	}
}
