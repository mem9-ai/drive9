// v1 SQLite read-cache routing: -wal/-journal sidecars never enter the
// whole-file or disk tiers (a rolling journal must observe every landing),
// and a main database becomes ineligible while any of its sidecars is open
// (the sidecar's landing invalidates what the cached main image claims).
package cache

import (
	"path"
	"strings"
	"sync"
)

// sqliteJournalPath reports a -wal/-journal sidecar name (the bare names
// "-wal"/"-journal" are not sidecars — v1 isSQLitePersistentJournalPath).
func sqliteJournalPath(p string) bool {
	name := path.Base(p)
	if name == "-wal" || name == "-journal" {
		return false
	}
	return strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-journal")
}

// sqliteMainDatabasePath reports a .db/.sqlite/.sqlite3 database name.
func sqliteMainDatabasePath(p string) bool {
	name := path.Base(p)
	if name == "" || name == "." || name == "/" {
		return false
	}
	return strings.HasSuffix(name, ".db") || strings.HasSuffix(name, ".sqlite") || strings.HasSuffix(name, ".sqlite3")
}

// sqliteMainDatabaseSidecarPaths lists the sidecars of a main database.
func sqliteMainDatabaseSidecarPaths(p string) []string {
	if !sqliteMainDatabasePath(p) {
		return nil
	}
	return []string{p + "-wal", p + "-journal", p + "-shm"}
}

// sidecarTracker holds the open -wal/-journal handles (v1 openHandles.Has).
type sidecarTracker struct {
	mu   sync.RWMutex
	open map[string]int
}

func (t *sidecarTracker) add(p string) {
	t.mu.Lock()
	if t.open == nil {
		t.open = map[string]int{}
	}
	t.open[p]++
	t.mu.Unlock()
}

func (t *sidecarTracker) remove(p string) {
	t.mu.Lock()
	if t.open[p] > 1 {
		t.open[p]--
	} else {
		delete(t.open, p)
	}
	t.mu.Unlock()
}

func (t *sidecarTracker) has(p string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.open[p] > 0
}

// sqliteBypassStableCaches ports v1 bypassStableRemoteReadCaches: sidecars
// always bypass; a main database bypasses while a sidecar handle is open.
func (c *Cache) sqliteBypassStableCaches(p string) bool {
	if sqliteJournalPath(p) {
		return true
	}
	if !sqliteMainDatabasePath(p) {
		return false
	}
	for _, sidecar := range sqliteMainDatabaseSidecarPaths(p) {
		if c.sidecars.has(sidecar) {
			return true
		}
	}
	return false
}

// trackSidecarOpen registers an open sidecar and drops the main database's
// cached image: its next read must observe the sidecar's landings.
func (c *Cache) trackSidecarOpen(p string) {
	c.sidecars.add(p)
	main := strings.TrimSuffix(strings.TrimSuffix(p, "-wal"), "-journal")
	if main == p || !sqliteMainDatabasePath(main) {
		return
	}
	c.Invalidate(main)
	if c.dsk != nil {
		c.dsk.InvalidateFile(pathDiskReadCacheFileID(main))
	}
}
