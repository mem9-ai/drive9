package go_sdk_cookbook_test

import (
	"bytes"
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

// Empty files use a conditional PUT; chmod uses the returned file identity.
func ExampleClient_fixedEmptyFileAndMode() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	bound, err := c.WithView(drive9.ViewBinding{LayerID: "candidate", WorkID: "issued-work", Fence: 1})
	if err != nil {
		return
	}
	// Persist both operation IDs and their request inputs before sending.
	file, err := bound.WriteFixedEmptyFile(ctx, "create-empty-1", "/empty.txt", 0)
	if err != nil {
		return // Recover the original PUT; do not adopt a newer path revision.
	}
	_, err = bound.ChmodFixedFile(ctx, "chmod-empty-1", "/empty.txt", file.ResourceID, file.Revision, 0o600)
	if err != nil {
		return // Retry only the same chmod inputs and operation ID.
	}
}

func ExampleClient_fixedExtentUpload() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	bound, err := c.WithView(drive9.ViewBinding{LayerID: "candidate", WorkID: "issued-work", Fence: 1})
	if err != nil {
		return
	}
	data := []byte("immutable upload source")
	size := int64(len(data))
	// Save the binding, path, size, expected revision and distinct phase IDs.
	plan, err := bound.InitiateFixedExtentUpload(ctx, "initiate-file-1", "/file.bin", size, 0, "")
	if err != nil {
		return // An uncertain initiation is retried with identical inputs.
	}
	parts, err := bound.UploadFixedExtentParts(ctx, plan, bytes.NewReader(data), size, nil)
	if err != nil {
		return // Transfer failure attempts cancellation; do not publish parts.
	}
	// Durably save plan.UploadID, parts, size, binding and "complete-file-1"
	// BEFORE this call. Persist them in the application's recovery journal.
	receipt, err := bound.CompleteFixedExtentUploadWithSize(ctx, "complete-file-1", plan.UploadID, parts, size)
	if err != nil {
		// Keep that saved request. Do not cancel or initiate another upload:
		// publication may have succeeded. FixedExtentCompletionError also
		// supports explicit Retry while this process remains alive.
		return
	}
	_ = receipt.ResourceID
	_ = receipt.Revision
}

func ExampleClient_fixedExtentCompletionReplay() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	bound, err := c.WithView(drive9.ViewBinding{LayerID: "candidate", WorkID: "issued-work", Fence: 1})
	if err != nil {
		return
	}
	// In a recovery program, load the exact binding, upload ID, operation ID
	// and part list saved BEFORE the original completion. These illustrative
	// values represent one uploaded part, whose ETag is its SHA-256 address.
	digest := sha256.Sum256([]byte("part"))
	parts := []drive9.FixedExtentPart{{Number: 1, ETag: hex.EncodeToString(digest[:])}}
	receipt, err := bound.CompleteFixedExtentUpload(ctx, "complete-part-1", "issued-upload", parts)
	if err != nil {
		return // Retain the same saved request for later explicit recovery.
	}
	// The lower-level method validates a positive size. Compare the original
	// size here, or prefer CompleteFixedExtentUploadWithSize above.
	if receipt.Size != 4 {
		return // A mismatched receipt does not prove rollback; retain identity.
	}
}

func ExampleClient_mergeFixedView() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	// Use the actual validation claim AFTER the application has validated its
	// candidate. Persist it together with the merge operation ID beforehand.
	validation := drive9.ViewWork{LayerID: "candidate", ID: "issued-validation", Fence: 2, Validation: true}
	out, err := c.MergeFixedView(ctx, validation, "merge-candidate-1")
	if err != nil || out.State == "RUNNING" {
		return // Retry this POST with the same inputs, never rerun candidate work.
	}
	if out.State == "FAILED" {
		_ = out.Code // Terminal rejection; main was not published by this request.
		return
	}
	// DONE from this POST includes quota bookkeeping. A GET receipt alone
	// does not resolve ACCOUNTING_PENDING after an earlier lost response.
}

func ExampleClient_writeMainExtent() {
	ctx := context.Background()
	c := drive9.New("https://drive9.example.com", "api-key")
	data := []byte("replacement extent body")
	// Explicit extent layout, even for a short body. Use an observed positive
	// revision for replacement, or zero to require an absent main path.
	if err := c.WriteExtentStreamConditional(ctx, "/main.bin", bytes.NewReader(data), int64(len(data)), nil, 0); err != nil {
		return
	}
}
