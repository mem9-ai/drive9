package backend

import (
	"context"
	"database/sql"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

// CreateMetadataResult describes the authoritative metadata created for a new
// empty file before any content is uploaded.
type CreateMetadataResult struct {
	Path      string
	Revision  int64
	SizeBytes int64
	Status    datastore.FileStatus
	CreatedAt time.Time
}

// CreateMetadataOnlyCtx creates a confirmed empty file in authoritative server
// metadata without writing object storage content.
func (b *Dat9Backend) CreateMetadataOnlyCtx(ctx context.Context, path string) (_ *CreateMetadataResult, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "create_metadata_only", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}

	createdAt := time.Now().UTC()
	fileID := b.genID()
	nodeID := b.genID()
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		return b.store.CreateConfirmedEmptyFileTx(tx, datastore.CreateConfirmedEmptyFileParams{
			Path:      path,
			FileID:    fileID,
			NodeID:    nodeID,
			CreatedAt: createdAt,
		}, b.genID)
	}); err != nil {
		return nil, err
	}

	// Metadata-only create intentionally stops at namespace creation. Any
	// content-driven side effects remain tied to later confirmed writes.
	b.syncCentralFileCreate(ctx, fileID, 0, "")

	return &CreateMetadataResult{
		Path:      path,
		Revision:  1,
		SizeBytes: 0,
		Status:    datastore.StatusConfirmed,
		CreatedAt: createdAt,
	}, nil
}
