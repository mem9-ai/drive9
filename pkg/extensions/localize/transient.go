package localize

import (
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// noteDBClose retires one open reference to a path (and, for WAL sidecars,
// to the main database they belong to).
func (f *Localize) noteDBClose(viewPath string) {
	if pathutil.BaseName(viewPath) == "-shm" {
		return
	}
	f.openDBMu.Lock()
	defer f.openDBMu.Unlock()
	if n := f.openDBs[viewPath]; n <= 1 {
		delete(f.openDBs, viewPath)
		delete(f.unlinked, viewPath)
	} else {
		f.openDBs[viewPath] = n - 1
	}
	if key, ok := sqliteSidecarParentKey(viewPath); ok {
		if n := f.openDBs[key]; n <= 1 {
			delete(f.openDBs, key)
			delete(f.unlinked, key)
		} else {
			f.openDBs[key] = n - 1
		}
	}
}

// noteDBOpen records one open reference to a path (and, for WAL sidecars,
// to the main database they belong to).
func (f *Localize) noteDBOpen(viewPath string) {
	if pathutil.BaseName(viewPath) == "-shm" {
		return
	}
	f.openDBMu.Lock()
	defer f.openDBMu.Unlock()
	f.openDBs[viewPath]++
	if key, ok := sqliteSidecarParentKey(viewPath); ok {
		f.openDBs[key]++
	}
}

// noteUnlinked marks a path as removed while references to it are still
// open. The kernel can still flush state (inode timestamps) through such a
// handle after the name is gone.
func (f *Localize) noteUnlinked(viewPath string) {
	f.openDBMu.Lock()
	defer f.openDBMu.Unlock()
	if f.openDBs[viewPath] > 0 {
		f.unlinked[viewPath] = true
	}
}

// transientFor routes SQLite WAL-index scratch files (-shm) to the
// transient plane while the database (or one of its WAL sidecars) has an
// open handle: the shared-memory index is per-mount scratch that must
// never reach the remote namespace, and it only exists while the database
// is open. A bare "-shm" name never routes (nothing precedes the suffix).
func (f *Localize) transientFor(viewPath string, dirHint bool) *Overlay {
	if f.transient == nil || dirHint || reservedViewPath(viewPath) {
		return nil
	}
	name := pathutil.BaseName(viewPath)
	if name == "-shm" || !strings.HasSuffix(name, "-shm") {
		return nil
	}
	key := strings.TrimSuffix(viewPath, "-shm")
	f.openDBMu.Lock()
	defer f.openDBMu.Unlock()
	if f.openDBs[key] > 0 {
		return f.transient
	}
	return nil
}

// wasUnlinked reports whether the path was removed while references to it
// are still open.
func (f *Localize) wasUnlinked(viewPath string) bool {
	f.openDBMu.Lock()
	defer f.openDBMu.Unlock()
	return f.unlinked[viewPath]
}

// transientEntryCount counts entries in the transient backing root; -1
// when the root cannot be read.
func (f *Localize) transientEntryCount() int {
	if f.transient == nil {
		return 0
	}
	entries, err := os.ReadDir(f.transient.Root())
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		return -1
	}
	return len(entries)
}

// sqliteSidecarParentKey maps a WAL/journal sidecar path to its main
// database path.
func sqliteSidecarParentKey(viewPath string) (string, bool) {
	name := pathutil.BaseName(viewPath)
	if name == "-wal" || name == "-journal" {
		return "", false
	}
	for _, suffix := range []string{"-wal", "-journal"} {
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(viewPath, suffix), true
		}
	}
	return "", false
}
