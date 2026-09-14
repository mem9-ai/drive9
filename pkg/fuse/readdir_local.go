package fuse

// dirLocalMetadata is a read-only snapshot of published local metadata. Each
// listing conversion and each READDIRPLUS page takes its own fresh snapshot.
type dirLocalMetadata struct {
	version uint64
	pending map[string]*WriteBackMeta
}

func (fs *Dat9FS) snapshotDirLocal(dirPath string) dirLocalMetadata {
	local := dirLocalMetadata{
		version: fs.inodes.AttrVersion(),
		pending: make(map[string]*WriteBackMeta),
	}
	prefix := dirPath
	if prefix != "/" {
		prefix += "/"
	}
	add := func(metas []*WriteBackMeta) {
		for _, meta := range metas {
			if parentDir(meta.Path) == dirPath {
				local.pending[meta.Path] = meta
			}
		}
	}
	if fs.writeBack != nil {
		add(fs.writeBack.snapshotPublishedMeta(prefix))
	}
	if fs.pendingIndex != nil {
		add(fs.pendingIndex.ListByPrefix(prefix))
	}
	return local
}

// localDirEntryInfo projects local attributes without consuming generations or
// changing an inode. A mutation after the snapshot makes the live inode newer.
func (fs *Dat9FS) localDirEntryInfo(childP string, item CachedFileInfo, live *InodeEntry, local dirLocalMetadata) (CachedFileInfo, bool) {
	if live != nil && !live.IsDir {
		// Dirty sizes can advance after the directory snapshot was captured.
		if size, ok := fs.dirtyHandleSize(live.Ino); ok {
			item = cachedInfoFromEntry(item.Name, live)
			item.Size = size
			return item, true
		}
		if live.attrVersion > local.version {
			return cachedInfoFromEntry(item.Name, live), true
		}
	}
	if meta := local.pending[childP]; meta != nil {
		item.Size = meta.Size
		item.IsDir = false
		if !meta.Mtime.IsZero() {
			item.Mtime = meta.Mtime
		}
		if meta.HasMode {
			item.Mode, item.HasMode = meta.Mode, true
		}
		return item, true
	}
	if sameKnownDirEntryResource(live, item) && live.Revision > item.Revision {
		return cachedInfoFromEntry(item.Name, live), true
	}
	return item, false
}

// Revision ordering is meaningful only for the same known resource and type.
// Without identity information, the local observation version still fences
// concurrent mutations, but a fresh remote observation must remain visible.
func sameKnownDirEntryResource(live *InodeEntry, item CachedFileInfo) bool {
	return live != nil && live.IsDir == item.IsDir && live.ResourceID != "" && live.ResourceID == item.ResourceID
}
