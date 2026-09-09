package fuse

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// uploadShadowSpill binds only small, generation-checked images. The private
// bytes are both the upload source and the landed checksum source, so replacement
// of the active shadow during upload cannot erase or substitute the parent proof.
// Larger spills retain the streaming path and do not authorize growth rebases.
func (cq *CommitQueue) uploadShadowSpill(ctx context.Context, entry *CommitEntry, expectedRevision int64) (int64, error) {
	apiPath := cq.remotePath(entry.Path)
	if entry.Size < 0 || entry.Size > maxLandedPayloadBytes {
		return uploadFromShadowRemoteWithRevisionAndGeneration(ctx, cq.client, cq.shadows, entry.Path, apiPath, expectedRevision, entry.ShadowGen)
	}
	if err := cq.shadows.SyncIfGeneration(entry.Path, entry.ShadowGen); err != nil {
		return 0, err
	}
	if !entry.payloadBound {
		data, err := cq.readSmallSpillPayload(entry)
		if err != nil {
			return 0, err
		}
		entry.bindPayload(data)
	}
	threshold := cq.directPutThreshold()
	if entry.Size == 0 || (threshold > 0 && entry.Size < threshold) {
		return cq.client.WriteCtxConditionalWithRevision(ctx, apiPath, entry.payload, expectedRevision)
	}
	err := cq.client.WriteMultipartStreamConditional(ctx, apiPath, bytes.NewReader(entry.payload), entry.Size, nil, expectedRevision)
	return 0, err
}

func (cq *CommitQueue) readSmallSpillPayload(entry *CommitEntry) ([]byte, error) {
	fd, size, release, err := cq.shadows.OpenIfGeneration(entry.Path, entry.ShadowGen)
	if err != nil {
		return nil, err
	}
	defer release()
	defer func() { _ = fd.Close() }()
	// Check actual size before allocating, not just queued metadata.
	if size < 0 || size != entry.Size || size > maxLandedPayloadBytes {
		return nil, fmt.Errorf("spill %s size mismatch: metadata=%d actual=%d", entry.Path, entry.Size, size)
	}
	data := make([]byte, size)
	_, err = io.ReadFull(fd, data)
	return data, err
}
