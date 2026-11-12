package writeback

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// fixedExtentPublication survives replacement of the path's staged bytes. It
// identifies only the image whose parts have already been uploaded. Generations
// are the persisted pending-index/cache tokens, never process-local shadow IDs.
// The mount's journal scope supplies the authenticated filesystem identity.
type fixedExtentPublication struct {
	Version           int                      `json:"version"`
	Binding           client.ViewBinding       `json:"binding"`
	Path              string                   `json:"path"`
	OperationID       string                   `json:"operation_id"`
	UploadID          string                   `json:"upload_id"`
	Size              int64                    `json:"size"`
	PendingGeneration uint64                   `json:"pending_generation,omitempty"`
	CacheGeneration   uint64                   `json:"cache_generation,omitempty"`
	Parts             []client.FixedExtentPart `json:"parts"`
}

func (p fixedExtentPublication) validate(c *client.Client) error {
	normalized, pathErr := pathutil.Canonicalize(p.Path)
	if p.Version != 1 || p.Binding.LayerID == "" || p.Binding.Validation || p.Binding.Validate() != nil || c == nil || c.ViewBinding() != p.Binding ||
		p.Path == "/" || pathErr != nil || normalized != p.Path || p.Size <= 0 || p.Size > 256*(64<<20) ||
		p.PendingGeneration == 0 && p.CacheGeneration == 0 || viewprotocol.ValidateOperationID(p.OperationID) != nil || viewprotocol.ValidateOperationID(p.UploadID) != nil ||
		len(p.Parts) < 1 || len(p.Parts) > 256 {
		return fmt.Errorf("invalid fixed extent publication record")
	}
	for i, part := range p.Parts {
		digest, err := hex.DecodeString(part.ETag)
		if err != nil || len(digest) != 32 || part.Number != i+1 {
			return fmt.Errorf("invalid fixed extent publication parts")
		}
	}
	return nil
}

// publishFixedExtent records the exact completion before sending it. Validation
// and local-identity protection failures do not attempt a new WAL append or HTTP
// request. Once PrepareFixedUpload is called, a save error can leave a request
// with unknown durability. The caller must retain the original publication and
// inspect/replay its WAL identity; an error alone never justifies aborting the
// upload or allocating a replacement operation. Settle the WAL only AFTER
// durable, generation-checked cleanup.
func publishFixedExtent(ctx context.Context, c *client.Client, journal *staging.Journal, pending *staging.PendingIndex, cache *staging.WriteBackCache, publication fixedExtentPublication) (client.FixedUploadCompletion, error) {
	if err := publication.validate(c); err != nil {
		return client.FixedUploadCompletion{}, err
	}
	if journal == nil {
		return client.FixedUploadCompletion{}, fmt.Errorf("fixed extent publication requires a journal")
	}
	// Reserve both local identities before writing a recoverable request. A
	// newer image or unlink may remove their metadata before completion, so
	// reserving only during post-commit cleanup would be too late.
	if publication.PendingGeneration != 0 {
		if pending == nil {
			return client.FixedUploadCompletion{}, fmt.Errorf("fixed extent publication requires its pending index")
		}
		if err := pending.ProtectFixedUploadGeneration(publication.Path, publication.PendingGeneration); err != nil {
			return client.FixedUploadCompletion{}, err
		}
	}
	if publication.CacheGeneration != 0 {
		if cache == nil {
			return client.FixedUploadCompletion{}, fmt.Errorf("fixed extent publication requires its cache")
		}
		if err := cache.ProtectFixedUploadGeneration(publication.Path, publication.CacheGeneration); err != nil {
			return client.FixedUploadCompletion{}, err
		}
	}
	raw, err := json.Marshal(publication)
	if err != nil {
		return client.FixedUploadCompletion{}, err
	}
	if err := journal.PrepareFixedUpload(publication.OperationID, publication.Path, raw); err != nil {
		return client.FixedUploadCompletion{}, fmt.Errorf("save fixed extent publication: %w", err)
	}
	// No source reads or initiation occur beyond the durable boundary.
	return publication.complete(ctx, c)
}

func (p fixedExtentPublication) complete(ctx context.Context, c *client.Client) (client.FixedUploadCompletion, error) {
	if err := p.validate(c); err != nil {
		return client.FixedUploadCompletion{}, err
	}
	return c.CompleteFixedExtentUploadWithSize(ctx, p.OperationID, p.UploadID, p.Parts, p.Size)
}

// loadFixedExtentPublications validates every unresolved record before returning
// any work. A mismatched work binding must not be rebound to the current client.
func loadFixedExtentPublications(c *client.Client, journal *staging.Journal) ([]fixedExtentPublication, error) {
	if journal == nil {
		return nil, fmt.Errorf("fixed extent recovery requires a journal")
	}
	entries, err := journal.PendingFixedUploads()
	if err != nil {
		return nil, err
	}
	publications := make([]fixedExtentPublication, 0, len(entries))
	for _, entry := range entries {
		var p fixedExtentPublication
		if err := json.Unmarshal(entry.Meta, &p); err != nil {
			return nil, fmt.Errorf("decode fixed extent publication: %w", err)
		}
		if err := p.validate(c); err != nil {
			return nil, err
		}
		if entry.Path != p.Path || entry.OperationID != p.OperationID {
			return nil, fmt.Errorf("fixed extent publication journal identity mismatch")
		}
		publications = append(publications, p)
	}
	return publications, nil
}
