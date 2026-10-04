package fuse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

func layerMountCheckpointFunc(c *client.Client, opts *MountOptions) mountCheckpointFunc {
	if c == nil || !mountSupportsCheckpointControl(opts) {
		return nil
	}
	layerID := opts.LayerRef
	return func(ctx context.Context, checkpointID string) (mountcontrol.Checkpoint, error) {
		created, createErr := c.CheckpointFSLayer(ctx, layerID, client.FSLayerCheckpointRequest{
			CheckpointID: checkpointID,
		})
		if createErr != nil {
			var getErr error
			created, getErr = c.GetFSLayerCheckpoint(ctx, checkpointID)
			if getErr != nil {
				return mountcontrol.Checkpoint{}, fmt.Errorf("create LayerFS checkpoint: %w", errors.Join(createErr, getErr))
			}
		}
		if created.CheckpointID != checkpointID || created.LayerID != layerID || created.DurableSeq < 0 {
			return mountcontrol.Checkpoint{}, errors.New("created LayerFS checkpoint does not match the mounted layer")
		}
		verified, err := c.GetFSLayerCheckpoint(ctx, checkpointID)
		if err != nil {
			return mountcontrol.Checkpoint{}, fmt.Errorf("verify LayerFS checkpoint: %w", err)
		}
		if verified.CheckpointID != created.CheckpointID ||
			verified.LayerID != created.LayerID ||
			verified.DurableSeq != created.DurableSeq {
			return mountcontrol.Checkpoint{}, errors.New("LayerFS checkpoint create and independent read disagree")
		}
		return mountcontrol.Checkpoint{
			CheckpointID: verified.CheckpointID,
			LayerID:      verified.LayerID,
			DurableSeq:   verified.DurableSeq,
		}, nil
	}
}

func mountSupportsCheckpointControl(opts *MountOptions) bool {
	return opts != nil &&
		strings.TrimSpace(opts.LayerRef) != "" &&
		!opts.ReadOnly &&
		strings.TrimSpace(opts.CheckpointRef) == ""
}
