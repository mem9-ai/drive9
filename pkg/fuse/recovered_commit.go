package fuse

// publishRecoveredCommit publishes a recovery entry without changing its
// immutable queue inode binding. The queue owns the remote-path lock; the
// pending path lock additionally fences generation replacement through cache
// publication. No handle locks or network I/O are taken in that section.
func (fs *Dat9FS) publishRecoveredCommit(entry *CommitEntry, committedRev int64) {
	if fs.pendingIndex == nil || entry.PendingIndexGen == 0 {
		return
	}
	published := func() bool {
		pl := fs.pendingIndex.acquirePathLock(entry.Path)
		defer fs.pendingIndex.releasePathLock(entry.Path, pl)
		meta, ok := fs.pendingIndex.GetMeta(entry.Path)
		if !ok || meta.Generation != entry.PendingIndexGen {
			return false
		}
		_, name := cacheParentName(entry.Path)
		// A cache-only identity may belong to a removed incarnation. Use
		// pending metadata for cold paths and let a fresh listing fill identity.
		info := cachedInfoFromWriteBackMeta(name, meta)
		fs.dirtyMu.Lock()
		// Serialize dirty-size selection with publication, using the existing
		// dirtyMu -> inode mutex order. Shadow size, not possibly stale pending
		// metadata size, is authoritative unless a newer dirty write exists.
		inode := func() *InodeEntry {
			m := fs.inodes
			m.mu.Lock()
			defer m.mu.Unlock()
			ino, found := m.byPath[entry.Path]
			if found {
				live := m.byInode[ino]
				if live.IsDir || live.Unlinked {
					return nil
				}
				info = cachedInfoFromEntry(name, live)
			}
			size := entry.Size
			if dirty, ok := fs.dirtyInodes[ino]; found && ok {
				size = dirty.size
			}
			if info.Mtime.IsZero() {
				info.Mtime = meta.Mtime
			}
			ino = m.ensureInodeWithIdentityLocked(entry.Path, info.ResourceID, info.Nlink, false, size, info.Mtime)
			live := m.byInode[ino]
			if committedRev <= 0 {
				// Do not label committed bytes with the pre-upload base revision.
				live.Revision = 0
			} else if committedRev > live.Revision {
				live.Revision = committedRev
			}
			if meta.HasMode {
				live.Mode, live.HasMode = meta.Mode, true
			} else if info.HasMode {
				live.Mode, live.HasMode = info.Mode, true
			}
			if info.HasUID {
				live.Uid, live.HasUID = info.Uid, true
			}
			if info.HasGID {
				live.Gid, live.HasGID = info.Gid, true
			}
			return copyInodeEntryLocked(live)
		}()
		if inode != nil {
			fs.cacheEntryForPath(entry.Path, inode)
		}
		fs.dirtyMu.Unlock()
		if inode == nil {
			return false
		}
		if committedRev > 0 {
			if entry.Kind == PendingNew {
				fs.replaceCommittedRevision(entry.Path, committedRev)
			} else {
				fs.recordCommittedRevision(entry.Path, committedRev)
			}
		} else {
			fs.forgetCommittedRevision(entry.Path)
		}
		return true
	}()
	if published {
		fs.invalidateReadCacheAndTargets(entry.Path)
	}
}
