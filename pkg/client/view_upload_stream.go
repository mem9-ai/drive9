package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// FixedExtentCompletionError retains the exact completion request after a stream
// has uploaded its parts. Retry only resends completion on the original client
// binding; it never starts an upload or reads the source again. It is an in-memory
// recovery value, not a persistent upload journal. Use errors.As to obtain it.
type FixedExtentCompletionError struct {
	client      *Client
	operationID string
	uploadID    string
	parts       []completePart
	size        int64
	cause       error
}

func (e *FixedExtentCompletionError) Error() string { return e.cause.Error() }
func (e *FixedExtentCompletionError) Unwrap() error { return e.cause }

// Retry resolves the original publication and any pending reservation release.
// Failure preserves the request for another attempt; success returns its receipt.
func (e *FixedExtentCompletionError) Retry(ctx context.Context) (FixedUploadCompletion, error) {
	if e == nil || e.client == nil {
		return FixedUploadCompletion{}, errors.New("missing fixed completion request")
	}
	return e.client.completeFixedExtentStream(ctx, e.operationID, e.uploadID, e.parts, e.size)
}

func (c *Client) completeFixedExtentStream(ctx context.Context, id, uploadID string, parts []completePart, size int64) (FixedUploadCompletion, error) {
	receipt, err := c.CompleteFixedExtentUpload(ctx, id, uploadID, parts)
	if err == nil && receipt.Size != size {
		err = markCommitAttempt(&ViewOperationError{LayerID: c.view.LayerID, OperationID: id, Err: errors.New("fixed upload completion size mismatch")})
	}
	if err != nil {
		return FixedUploadCompletion{}, &FixedExtentCompletionError{client: c, operationID: id, uploadID: uploadID, parts: append([]completePart(nil), parts...), size: size, cause: err}
	}
	return receipt, nil
}
