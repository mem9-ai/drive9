package fuse

import (
	"slices"

	"github.com/pingcap/failpoint"
)

func (fs *Dat9FS) ftruncateAliasLinked(fh *FileHandle) bool {
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || entry.Unlinked {
		return false
	}
	_, linked := entry.Paths[fh.Path]
	ino, exists := fs.inodes.GetInode(fh.Path)
	return linked && exists && ino == fh.Ino
}

// Observations retain commit bookkeeping across a reset; their old view never
// grants read or write authority. A late successful upload must remain visible
// to lazy clean-handle recovery.
func (fs *Dat9FS) hasFtruncateInheritance(ino uint64) bool {
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Unlinked {
		return false
	}
	for _, fh := range fs.openHandles.SnapshotInode(ino) {
		if e := fh.pendingFtruncate.Load(); e != nil && e.ino == ino {
			if _, linked := entry.Paths[e.path]; linked {
				return true
			}
		}
	}
	return false
}

func (fs *Dat9FS) ftruncateAliasPaths(ino uint64) []string {
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Unlinked {
		return nil
	}
	paths := make([]string, 0, len(entry.Paths))
	for p := range entry.Paths {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

func (fs *Dat9FS) ftruncateAliasesValid(ino uint64, paths []string, view uint64) bool {
	if view != fs.mountViewGeneration.Load() || !slices.Equal(paths, fs.ftruncateAliasPaths(ino)) {
		return false
	}
	for _, p := range paths {
		if current, ok := fs.inodes.GetInode(p); !ok || current != ino {
			return false
		}
	}
	return len(paths) != 0
}

// The existing handle token owns the complete lock set. No sibling handle is
// waited on while holding it; callers use TryLock and unwind on contention.
func (fs *Dat9FS) tryLockFtruncateAliases(fh *FileHandle) (func(), bool) {
	if fh.Unlinked || fh.UnlinkedSnapshot || fh.UnlinkedData != nil {
		return fs.tryLockRemoteCommitPath(fh.Path)
	}
	if !fs.ftruncateAliasLinked(fh) {
		return nil, false
	}
	paths, view := fs.ftruncateAliasPaths(fh.Ino), fs.mountViewGeneration.Load()
	releases := make([]func(), 0, len(paths))
	unlock := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for _, p := range paths {
		release, ok := fs.tryLockRemoteCommitPath(p)
		if !ok {
			unlock()
			return nil, false
		}
		releases = append(releases, release)
	}
	failpoint.InjectCall("ftruncateAliasFenceAcquired", fs, fh)
	if !fs.ftruncateAliasesValid(fh.Ino, paths, view) {
		unlock()
		return nil, false
	}
	return unlock, true
}

func (fs *Dat9FS) ftruncateAliasRevision(fh *FileHandle) int64 {
	var revision int64
	for _, p := range fs.ftruncateAliasPaths(fh.Ino) {
		revision = max(revision, fs.latestCommittedRevision(p))
	}
	return revision
}

func (fs *Dat9FS) ftruncateAliasProof(fh *FileHandle) pathCommitLandmark {
	proof := pathCommitLandmark{}
	if fs.commitQueue != nil {
		for _, p := range fs.ftruncateAliasPaths(fh.Ino) {
			if other := fs.commitQueue.landedCommit(p); other.rev > proof.rev {
				proof = other
			}
		}
	}
	return proof
}
