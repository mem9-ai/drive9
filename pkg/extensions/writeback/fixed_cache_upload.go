package writeback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func fixedCacheModeID(id string) string {
	sum := sha256.Sum256([]byte("drive9-fixed-cache-mode:" + id))
	return hex.EncodeToString(sum[:])
}

func (u *WriteBackUploader) fixedCachePublications() ([]fixedExtentPublication, error) {
	records, err := loadFixedExtentPublications(u.client, u.fixedJournal)
	if err != nil {
		return nil, err
	}
	// Validate all owners before any request, including records whose cache was
	// already removed before a crash. Recovery never adopts a different root.
	for _, p := range records {
		if p.CacheMetadata == nil || p.RemotePath != u.remotePath(p.Path) {
			return nil, fmt.Errorf("fixed cache recovery has an unsupported publication owner")
		}
	}
	// A prior Prepare may have appended successfully but failed its fsync. A
	// readable record is not enough evidence to send completion: retry the
	// same immutable save before any recovered request is sent.
	for _, p := range records {
		raw, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		if err := u.fixedJournal.PrepareFixedUpload(p.OperationID, p.Path, raw); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// Called under the uploader path gate. First resolve old durable requests even
// if their local payload has disappeared or a newer generation now owns it.
// Empty files save a conditional PUT in the same journal before sending it.
func (u *WriteBackUploader) uploadFixedCache(ctx context.Context, path string) (int64, *staging.WriteBackMeta, bool, error) {
	records, err := u.fixedCachePublications()
	if err != nil {
		return 0, nil, true, err
	}
	var revision int64
	var settled *staging.WriteBackMeta
	for _, p := range records {
		if p.Path != path {
			continue
		}
		receipt, err := p.complete(ctx, u.client)
		if err != nil {
			return 0, nil, true, err
		}
		revision, err = u.finishFixedCache(ctx, p, receipt, nil)
		if err != nil {
			return 0, nil, true, err
		}
		settled = p.CacheMetadata
	}
	meta, ok := u.cache.GetMeta(path)
	if !ok {
		return revision, settled, true, nil
	}
	if meta.Kind == staging.PendingChmod {
		return 0, nil, true, fmt.Errorf("fixed cache mode lacks an original file receipt")
	}
	if u.ledgerOwnedRefused(path) {
		return 0, nil, true, errLedgerOwned
	}
	meta, data, ok := u.cache.GetMetaAndView(path)
	if !ok || meta.Size != int64(len(data)) {
		return 0, nil, true, fmt.Errorf("fixed cache image unavailable")
	}
	expected, err := expectedRevisionForWriteBack(meta)
	if err != nil {
		return 0, nil, true, err
	}
	seed, err := u.cache.FixedUploadSeed(path, meta.Generation)
	if err != nil {
		return 0, nil, true, err
	}
	request, _ := json.Marshal(struct {
		Seed, Path     string
		Binding        client.ViewBinding
		Size, Revision int64
	}{seed, u.remotePath(path), u.client.ViewBinding(), meta.Size, expected})
	sum := sha256.Sum256(request)
	initiationID := hex.EncodeToString(sum[:])
	id := fixedCacheModeID("complete:" + initiationID)
	p := fixedExtentPublication{Version: 1, Binding: u.client.ViewBinding(), Path: path, RemotePath: u.remotePath(path), OperationID: id, Size: meta.Size, CacheGeneration: meta.Generation, CacheMetadata: meta}
	if meta.Size == 0 {
		p.EmptyExpectedRevision = &expected
	} else {
		plan, err := u.client.InitiateFixedExtentUpload(ctx, initiationID, u.remotePath(path), meta.Size, expected, "")
		if err != nil {
			return 0, nil, true, err
		}
		parts, err := u.client.UploadFixedExtentParts(ctx, plan, bytes.NewReader(data), meta.Size, nil)
		if err != nil {
			// The transfer helper may already have canceled the session. Only
			// a confirmed terminal outcome permits a fresh attempt on these
			// same bytes; a lost cancellation reply retains the original seed.
			cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			outcome, abortErr := u.client.AbortFixedUpload(cancelCtx, fixedCacheModeID("abort:"+initiationID), plan.UploadID)
			cancel()
			if abortErr != nil {
				return 0, nil, true, errors.Join(err, abortErr)
			}
			if outcome.Status != "ABORTED" && outcome.Status != "EXPIRED" {
				return 0, nil, true, fmt.Errorf("fixed cache transfer unexpectedly published: %w", err)
			}
			if renewErr := u.cache.RenewFixedUploadSeedAfterAbort(path, meta.Generation, seed); renewErr != nil {
				return 0, nil, true, errors.Join(err, renewErr)
			}
			return 0, nil, true, err
		}
		p.UploadID, p.Parts = plan.UploadID, parts
	}
	receipt, err := publishFixedExtent(ctx, u.client, u.fixedJournal, nil, u.cache, p)
	if err != nil {
		return 0, nil, true, err
	}
	revision, err = u.finishFixedCache(ctx, p, receipt, data)
	if err != nil {
		return 0, nil, true, err
	}
	return revision, meta, true, nil
}

func (u *WriteBackUploader) finishFixedCache(ctx context.Context, p fixedExtentPublication, receipt client.FixedUploadCompletion, data []byte) (int64, error) {
	revision := receipt.Revision
	if p.CacheMetadata.HasMode {
		var err error
		revision, err = u.client.ChmodFixedFile(ctx, fixedCacheModeID(p.OperationID), p.RemotePath, receipt.ResourceID, receipt.Revision, p.CacheMetadata.Mode)
		if err != nil {
			return 0, err
		}
	}
	if data != nil {
		u.recordLanding(p.Path, data, revision, p.CacheMetadata)
	}
	// Both methods are retryable after partial local cleanup. Their persisted
	// generation floor prevents a restart from assigning the old token to new data.
	if _, err := u.cache.RetireFixedUploadGeneration(p.Path, p.CacheGeneration); err != nil {
		return 0, err
	}
	if err := u.fixedJournal.SettleFixedUpload(p.OperationID); err != nil {
		return 0, err
	}
	if u.OnSuccess != nil {
		u.OnSuccess(*p.CacheMetadata, revision, StagingGens{WriteBackGen: p.CacheGeneration})
	}
	return revision, nil
}

// Start calls this before either ordinary recovery track can send fresh writes.
func (u *WriteBackUploader) recoverFixedCache(ctx context.Context) error {
	records, err := u.fixedCachePublications()
	if err != nil {
		return err
	}
	for _, p := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		receipt, err := p.complete(ctx, u.client)
		if err != nil {
			return err
		}
		if _, err := u.finishFixedCache(ctx, p, receipt, nil); err != nil {
			return err
		}
	}
	return nil
}

// Held through namespace mutation AND local removal/retargeting. An unknown
// completion cannot be discarded by unlink or replayed into a renamed subtree.
func (u *WriteBackUploader) lockFixedNamespace(paths ...string) (func(), error) {
	if u == nil || u.client.ViewBinding().LayerID == "" {
		return func() {}, nil
	}
	u.fixedNamespaceMu.Lock()
	records, err := u.fixedCachePublications()
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
		u.fixedNamespaceMu.Unlock()
		return nil, err
	}
	return u.fixedNamespaceMu.Unlock, nil
}
