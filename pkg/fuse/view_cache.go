package fuse

import (
	"context"
	"fmt"
	"sort"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type viewCacheNotifier interface {
	InodeNotify(node uint64, off, length int64) gofuse.Status
	EntryNotify(parent uint64, name string) gofuse.Status
}

type viewCacheEntry struct {
	parent uint64
	name   string
}

// viewCacheInvalidation is a detached list of kernel cache targets. Prepare it
// before the durable switch decision, while ordinary admission is closed and
// old reference safety is checked separately. Its successful execution is only
// the notification part of switching, not a kernel reply barrier.
type viewCacheInvalidation struct {
	inodes  []uint64
	entries []viewCacheEntry
}

func prepareViewCacheInvalidation(snapshot []InodeEntry) (viewCacheInvalidation, error) {
	plan := viewCacheInvalidation{}
	parents := make(map[string]uint64)
	seen := make(map[uint64]bool)
	for _, entry := range snapshot {
		if entry.Ino == 0 || seen[entry.Ino] {
			return plan, fmt.Errorf("fuse: invalid cache inode snapshot")
		}
		seen[entry.Ino] = true
		plan.inodes = append(plan.inodes, entry.Ino)
		if !entry.IsDir {
			continue
		}
		for p := range entry.Paths {
			if old, ok := parents[p]; ok && old != entry.Ino {
				return plan, fmt.Errorf("fuse: ambiguous cache parent %q", p)
			}
			parents[p] = entry.Ino
		}
	}
	if parents["/"] != gofuse.FUSE_ROOT_ID {
		return plan, fmt.Errorf("fuse: cache snapshot missing mount root")
	}
	aliases := make(map[viewCacheEntry]bool)
	for _, entry := range snapshot {
		for p := range entry.Paths {
			if p == "/" {
				continue
			}
			parent, ok := parents[parentDir(p)]
			if !ok {
				return plan, fmt.Errorf("fuse: cache snapshot missing parent of %q", p)
			}
			aliases[viewCacheEntry{parent: parent, name: pathutil.BaseName(p)}] = true
		}
	}
	for alias := range aliases {
		plan.entries = append(plan.entries, alias)
	}
	sort.Slice(plan.inodes, func(i, j int) bool { return plan.inodes[i] < plan.inodes[j] })
	sort.Slice(plan.entries, func(i, j int) bool {
		a, b := plan.entries[i], plan.entries[j]
		return a.parent < b.parent || a.parent == b.parent && a.name < b.name
	})
	return plan, nil
}

// invalidateViewCache uses the mounted server; absence is a failure, never an
// already-completed cache invalidation.
func (f *FS) invalidateViewCache(ctx context.Context, plan viewCacheInvalidation) error {
	server := f.currentServer()
	if server == nil {
		return fmt.Errorf("fuse: no mounted kernel cache notifier")
	}
	return plan.invalidate(ctx, server)
}

// invalidate waits for each notification and stops on any unconfirmed failure.
// Call only after the binding decision, with foreground I/O still closed, and
// without admission/inode locks: kernel notifications may require callbacks.
// An error leaves the switch RECOVERING. Retry the same full plan; notifications
// are idempotent. ENOENT is allowed only for non-root targets: the root must
// still belong to this live mount connection, not have disappeared on unmount.
// Cancellation is checked between calls; go-fuse cannot cancel a notify syscall.
func (p viewCacheInvalidation) invalidate(ctx context.Context, notifier viewCacheNotifier) error {
	if notifier == nil {
		return fmt.Errorf("fuse: no kernel cache notifier")
	}
	for _, ino := range p.inodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		// off=0, len=0 invalidates attributes and all cached data, including
		// root directory state. No goroutine hides a partial failure.
		if status := notifier.InodeNotify(ino, 0, 0); status != gofuse.OK && (status != gofuse.ENOENT || ino == gofuse.FUSE_ROOT_ID) {
			return fmt.Errorf("fuse: invalidate inode %d: %w", ino, syscall.Errno(status))
		}
	}
	for _, entry := range p.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if status := notifier.EntryNotify(entry.parent, entry.name); status != gofuse.OK && status != gofuse.ENOENT {
			return fmt.Errorf("fuse: invalidate entry %d/%s: %w", entry.parent, entry.name, syscall.Errno(status))
		}
	}
	return ctx.Err()
}
