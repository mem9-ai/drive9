package writeback

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"strings"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// fixedExtentPublication survives replacement of the path's staged bytes. It
// identifies only the image whose parts have already been uploaded. Generations
// are the persisted pending-index/cache tokens, never process-local shadow IDs.
// The mount's journal scope supplies the authenticated filesystem identity.
type fixedQueueMetadata struct {
	BaseRevision int64               `json:"base_revision"`
	Kind         staging.PendingKind `json:"kind"`
	Mode         uint32              `json:"mode"`
	HasMode      bool                `json:"has_mode"`
}

type fixedExtentPublication struct {
	QueueMetadata     *fixedQueueMetadata      `json:"queue_metadata,omitempty"`
	ShadowToken       string                   `json:"shadow_token,omitempty"`
	Version           int                      `json:"version"`
	Binding           client.ViewBinding       `json:"binding"`
	Path              string                   `json:"path"`
	OperationID       string                   `json:"operation_id"`
	UploadID          string                   `json:"upload_id"`
	Size              int64                    `json:"size"`
	PendingGeneration uint64                   `json:"pending_generation,omitempty"`
	CacheGeneration   uint64                   `json:"cache_generation,omitempty"`
	RemotePath        string                   `json:"remote_path,omitempty"`
	CacheMetadata     *staging.WriteBackMeta   `json:"cache_metadata,omitempty"`
	Parts             []client.FixedExtentPart `json:"parts"`
	// EmptyExpectedRevision selects an empty PUT; no upload session or parts exist.
	EmptyExpectedRevision *int64 `json:"empty_expected_revision,omitempty"`
}

func (p fixedExtentPublication) validate(c *client.Client) error {
	normalized, pathErr := pathutil.Canonicalize(p.Path)
	if p.Version != 1 || p.Binding.LayerID == "" || p.Binding.Validation || p.Binding.Validate() != nil || c == nil || c.ViewBinding() != p.Binding ||
		p.Path == "/" || pathErr != nil || normalized != p.Path || p.Size < 0 || p.Size > 256*(64<<20) ||
		p.PendingGeneration == 0 && p.CacheGeneration == 0 || viewprotocol.ValidateOperationID(p.OperationID) != nil {
		return fmt.Errorf("invalid fixed extent publication record")
	}
	if p.EmptyExpectedRevision != nil {
		if p.Size != 0 || *p.EmptyExpectedRevision < 0 || p.UploadID != "" || len(p.Parts) != 0 || p.CacheMetadata == nil && p.QueueMetadata == nil {
			return fmt.Errorf("invalid fixed empty publication")
		}
		expected, err := int64(0), error(nil)
		if p.CacheMetadata != nil {
			expected, err = expectedRevisionForWriteBack(p.CacheMetadata)
		} else {
			expected = p.QueueMetadata.BaseRevision
		}
		if err != nil || expected != *p.EmptyExpectedRevision {
			return fmt.Errorf("fixed empty publication changed condition")
		}
	} else if p.Size == 0 || viewprotocol.ValidateOperationID(p.UploadID) != nil || len(p.Parts) < 1 || len(p.Parts) > 256 {
		return fmt.Errorf("invalid fixed extent publication")
	}
	if p.CacheMetadata != nil {
		m := p.CacheMetadata
		remote, err := pathutil.Canonicalize(p.RemotePath)
		if p.PendingGeneration != 0 || p.CacheGeneration == 0 || m.Generation != p.CacheGeneration || m.Path != p.Path || m.Size != p.Size ||
			err != nil || remote != p.RemotePath || remote == "/" || (m.Kind != staging.PendingNew && m.Kind != staging.PendingOverwrite) {
			return fmt.Errorf("invalid fixed cache publication owner")
		}
	}
	if p.QueueMetadata != nil {
		q := p.QueueMetadata
		remote, err := pathutil.Canonicalize(p.RemotePath)
		if p.CacheMetadata != nil || p.CacheGeneration != 0 || p.PendingGeneration == 0 || viewprotocol.ValidateOperationID(p.ShadowToken) != nil || err != nil || remote != p.RemotePath || remote == "/" || q.BaseRevision < 0 || q.Kind != staging.PendingNew && q.Kind != staging.PendingOverwrite || q.Kind == staging.PendingOverwrite && q.BaseRevision == 0 {
			return fmt.Errorf("invalid fixed queue publication owner")
		}
	} else if p.ShadowToken != "" {
		return fmt.Errorf("fixed shadow without queue owner")
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
	if p.EmptyExpectedRevision != nil {
		return c.WriteFixedEmptyFile(ctx, p.OperationID, p.RemotePath, *p.EmptyExpectedRevision)
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

// Every runtime owner is validated before any replay. Re-fsync even a readable
// record: a previous save may have renamed/appended but failed synchronization.
func durableFixedPublications(c *client.Client, journal *staging.Journal, root string) ([]fixedExtentPublication, error) {
	records, err := loadFixedExtentPublications(c, journal)
	if err != nil {
		return nil, err
	}
	for _, p := range records {
		if p.CacheMetadata == nil && p.QueueMetadata == nil || p.RemotePath != mountpath.ToRemote(root, p.Path) {
			return nil, fmt.Errorf("fixed recovery owner or root mismatch")
		}
	}
	for _, p := range records {
		raw, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		if err := journal.PrepareFixedUpload(p.OperationID, p.Path, raw); err != nil {
			return nil, err
		}
	}
	return records, nil
}
func lockFixedPublicationNamespace(c *client.Client, journal *staging.Journal, root string, mu *sync.RWMutex, paths ...string) (func(), error) {
	mu.Lock()
	records, err := durableFixedPublications(c, journal, root)
	if err == nil {
		for _, p := range records {
			for _, path := range paths {
				if p.Path == path || strings.HasPrefix(p.Path, strings.TrimSuffix(path, "/")+"/") {
					err = syscall.EAGAIN
				}
			}
		}
	}
	if err != nil {
		mu.Unlock()
		return nil, err
	}
	return mu.Unlock, nil
}
