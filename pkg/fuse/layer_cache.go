package fuse

import (
	"bytes"
	"context"

	"github.com/mem9-ai/drive9/pkg/client"
)

// LayerCacheIdentity identifies the remote content retained in a clean cache.
// Sequences are scoped to a Layer; ancestor and child sequences can overlap.
type LayerCacheIdentity struct {
	LayerID  string
	EntrySeq int64
}

func layerCacheIdentity(entry *client.FSLayerEntry, fallbackLayer string) LayerCacheIdentity {
	if entry == nil {
		return LayerCacheIdentity{}
	}
	id := entry.LayerID
	if id == "" {
		id = fallbackLayer
	}
	return LayerCacheIdentity{LayerID: id, EntrySeq: entry.EntrySeq}
}

func (fs *Dat9FS) upsertLayerFile(ctx context.Context, localPath string, data []byte, expectedRevision int64, mode uint32, hasMode bool) (LayerCacheIdentity, error) {
	if fs.isLayerAbandoned() {
		return LayerCacheIdentity{}, errLayerRolledBack
	}
	start := fs.perfStart()
	var entry *client.FSLayerEntry
	var err error
	if int64(len(data)) > maxInlineLayerEntryBytes {
		entry, err = fs.client.UploadFSLayerFile(ctx, fs.layerRef(), fs.remotePath(localPath), bytes.NewReader(data), int64(len(data)), expectedRevision, mode, hasMode)
	} else {
		req := client.FSLayerEntryRequest{
			Path: fs.remotePath(localPath), Op: "upsert", Kind: "file",
			BaseRevision: expectedRevision, Content: data, SizeBytes: int64(len(data)),
		}
		if hasMode {
			req.Mode = mode & 0o777
		}
		entry, err = fs.client.UpsertFSLayerEntry(ctx, fs.layerRef(), req)
	}
	fs.perfRecordRemote(perfRemoteMutation, start, err, uint64(len(data)))
	if err != nil {
		return LayerCacheIdentity{}, err
	}
	if hasMode {
		fs.markLayerFileMode(localPath, mode)
	} else {
		fs.markLayerFile(localPath)
	}
	return layerCacheIdentity(entry, fs.layerRef()), nil
}
