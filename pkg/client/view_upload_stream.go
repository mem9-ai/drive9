package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

// Phase IDs stay distinct from the caller's file-publication ID. The server
// rejects reuse of any one ID for a different operation or request body.
func fixedUploadPhaseID(phase, seed string) string {
	sum := sha256.Sum256([]byte("drive9-fixed-upload:" + phase + ":" + seed))
	return hex.EncodeToString(sum[:])
}

func (c *Client) writeFixedExtentStream(ctx context.Context, path string, ra io.ReaderAt, size int64, progress ProgressFunc, revision int64, summary *UploadSummary, tags map[string]string, description, checksum string, precheck func() error) error {
	if len(tags) != 0 || description != "" || checksum != "" {
		return fmt.Errorf("fixed extent uploads do not support tags, description or whole-file checksum")
	}
	operationID, _ := ctx.Value(viewOperationKey{}).(string)
	if operationID == "" {
		operationID = "fs-" + rand.Text()
	}
	ctx, err := WithViewOperationID(ctx, operationID)
	if err != nil {
		return err
	}
	started := time.Now()
	plan, err := c.InitiateFixedExtentUpload(ctx, fixedUploadPhaseID("initiate", operationID), path, size, revision, "")
	if err != nil {
		return err
	}
	return c.writeExtentUpload(ctx, &plan, ra, size, progress, summary, nil, "", precheck, time.Since(started))
}
