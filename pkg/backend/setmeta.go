package backend

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// setmetaAfterResolveFailpoint gates the window between path→inode
// resolution and the metadata transaction so tests can deterministically
// delete+recreate the dentry in between.
const setmetaAfterResolveFailpoint = "setmetaAfterResolve"

// ErrSetMetadataOnDirectory is returned when file metadata (tags/description)
// is set on a directory. Only regular files carry tags and descriptions.
var ErrSetMetadataOnDirectory = errors.New("cannot set file metadata on a directory")

// FileMetadataUpdate describes a metadata-only update of an existing file.
// A nil Tags map leaves the tag set unchanged; a non-nil map (including an
// empty one) replaces all tags (this store has no tag provenance column, so
// the replace covers every tag row, matching upload-time semantics). A nil
// Description leaves the description unchanged; a non-nil pointer sets it
// (empty string clears).
type FileMetadataUpdate struct {
	Tags        map[string]string
	Description *string
}

// SetFileMetadataCtx updates tags and/or description of an existing file
// without rewriting its content. The file revision is left unchanged; a
// description change re-triggers description embedding (app-managed mode
// force-requeues a semantic task pinned to the current revision;
// auto-embedding mode lets the database derive the new vector). It returns
// the current revision.
func (b *Dat9Backend) SetFileMetadataCtx(ctx context.Context, path string, upd FileMetadataUpdate) (revision int64, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, b.tidbCloudOrgID, "set_metadata", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return 0, err
	}
	if path == "/" {
		return 0, datastore.ErrInvalidRootDentry
	}
	resolvedPath, node, err := b.resolveNodePath(ctx, path)
	if err != nil {
		return 0, err
	}
	if node.IsDirectory {
		return 0, ErrSetMetadataOnDirectory
	}
	fileID := node.InodeID
	if fileID == "" {
		fileID = node.FileID
	}
	if fileID == "" {
		return 0, datastore.ErrNotFound
	}
	failpoint.InjectCall(setmetaAfterResolveFailpoint, resolvedPath)

	enqueued := false
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		// Fence the path→inode resolution (which happened outside this
		// transaction): if the dentry was deleted or recreated meanwhile it
		// now points at a different (or no) identity, and the metadata must
		// not land on the replacement inode.
		liveID, err := b.store.LockDentryFileIDTx(tx, resolvedPath)
		if err != nil {
			return err
		}
		if liveID != fileID {
			return datastore.ErrRevisionConflict
		}
		rev, err := b.store.LockConfirmedFileRevisionTx(tx, fileID)
		if err != nil {
			return err
		}
		revision = rev
		if upd.Tags != nil {
			if err := b.store.ReplaceFileTagsTx(tx, fileID, upd.Tags); err != nil {
				return err
			}
		}
		if upd.Description != nil {
			if b.UsesDatabaseAutoEmbedding() {
				// The database owns vector state; updating the description
				// re-derives the generated column. No embed task.
				if err := b.store.UpdateFileDescriptionAutoEmbeddingTx(tx, fileID, *upd.Description); err != nil {
					return err
				}
				return nil
			}
			// Lock-order contract with the embed writeback path: the task row
			// is taken before the semantic row. The revision is intentionally
			// unchanged, so a previously completed or in-flight embed task
			// for this revision may already exist; force-requeue re-queues
			// such rows in place and invalidates any in-flight owner's lease
			// (plain enqueue would dedupe terminal rows away, ensure would
			// leave active leases alone).
			if b.shouldEnqueueEmbedForRevision(resolvedPath, "", "", *upd.Description) {
				created, err := b.forceRequeueEmbedTaskTx(tx, fileID, rev)
				if err != nil {
					return err
				}
				enqueued = created
			}
			if err := b.store.UpdateFileDescriptionTx(tx, fileID, *upd.Description); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if enqueued {
		b.notifyWorkEnqueued(BackendWorkSemantic)
	}
	return revision, nil
}
