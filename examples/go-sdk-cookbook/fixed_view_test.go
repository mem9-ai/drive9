package go_sdk_cookbook_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	drive9 "github.com/mem9-ai/drive9/pkg/client"
)

// This example is compile-only. Fixed data publication requires a backend with
// the matching fixed-view write, quota and final-authorization implementation.
func ExampleClient_prepareFixedView() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	// Persist these IDs before making requests. An unknown result must not
	// regenerate IDs or run the work callback a second time.
	req := drive9.FixedViewPrepareRequest{
		Create:          drive9.FixedViewCreate{LayerID: "attempt-1", OperationID: "create-attempt-1"},
		WorkOperationID: "work-attempt-1", SealOperationID: "seal-attempt-1", ValidationOperationID: "validate-attempt-1",
		WorkTTL: 30 * time.Minute, ValidationTTL: 10 * time.Minute,
	}
	prepared, err := c.PrepareFixedView(ctx, req, func(ctx context.Context, bound *drive9.Client, _ drive9.ViewWork) error {
		ctx, err := drive9.WithViewOperationID(ctx, "write-result-1")
		if err != nil {
			return err
		}
		return bound.WriteCtx(ctx, "/result.txt", []byte("result"))
	}, func(ctx context.Context, bound *drive9.Client, _ drive9.ViewWork) error {
		data, err := bound.ReadCtx(ctx, "/result.txt")
		if err != nil {
			return err
		}
		if string(data) != "result" {
			return errors.New("validation failed")
		}
		return nil
	})
	if err != nil {
		// Retain prepared.Stage and prepared.OperationID. An error may refer
		// to a tool operation inside the callback, whose ID takes precedence.
		var operation *drive9.ViewOperationError
		if errors.As(err, &operation) {
			_, _ = c.GetFixedViewOperation(ctx, operation.LayerID, operation.OperationID)
		}
		return
	}
	_ = prepared.Validation // Input to a separate, authorized conditional merge.
	// No filesystem merge or automatic deletion has happened.
}

func ExampleClient_fixedViewControls() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	_, err := c.CreateFixedView(ctx, drive9.FixedViewCreate{LayerID: "candidate", OperationID: "create-candidate"})
	if err != nil {
		return
	}
	_, _ = c.ListFixedViews(ctx, "", 100)
	_, _ = c.GetFixedView(ctx, "candidate")
	claim, err := c.BeginViewWork(ctx, "candidate", "begin-candidate", time.Minute, false)
	if err != nil || !claim.Launch {
		return // Receipt replay cannot authorize work execution.
	}
	bound, err := c.WithView(claim.Work.Binding())
	if err != nil {
		return
	}
	_ = bound.ViewBinding()
	work, err := c.RenewViewWork(ctx, claim.Work, time.Minute)
	if err != nil {
		return
	}
	sealed, err := c.SealFixedView(ctx, work, "seal-candidate")
	if err != nil || sealed.State != "DONE" || sealed.Layer.Phase != "READY" {
		return // RUNNING needs recovery using the original seal ID.
	}
	// Explicitly discard this demonstration candidate. This is not rollback
	// of externally published data or automatic error handling.
	_, _ = c.EndFixedView(ctx, "candidate", "delete-candidate", true)
}

func ExampleClient_fixedUploadControls() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	// Use the actual work and upload identities retained by the caller.
	bound, err := c.WithView(drive9.ViewBinding{LayerID: "candidate", WorkID: "issued-work", Fence: 1})
	if err != nil {
		return
	}
	digest := sha256.Sum256([]byte("part"))
	_, err = bound.PresignFixedExtentPart(ctx, "issued-upload", 1, 4, hex.EncodeToString(digest[:]))
	if err != nil {
		return
	}
	// This example explicitly abandons the upload instead of publishing it.
	_, _ = bound.AbortFixedUpload(ctx, "abort-upload-1", "issued-upload")
}
