// Store landing helpers: the mutations of the dirty mirror expressed as
// plain fs operations dispatched into the chain below the extension, so
// overlay writes ride the same commit pipeline (conditional whole-image
// writes, extent layout, batching at the transport) as any other file.
package git

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

// errStoreMissing fires when a mutation arrives before assembly attached
// the store view (a wiring bug, not a runtime condition).
var errStoreMissing = fmt.Errorf("git: store view not attached")

// isStoreExistErr reports an already-exists reply from the store (a lost
// create race must not fail a mirror landing).
func isStoreExistErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrExist) {
		return true
	}
	var errno syscall.Errno
	return errors.As(err, &errno) && errno == syscall.EEXIST
}

// landOverlayFile writes one whole mirror image (creating parents,
// clearing any blackout) and refreshes the routing index.
func (g *Git) landOverlayFile(ctx context.Context, w *workspace, rel string, image []byte, mode uint32) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	p := gitws.OverlayPath(w.id, rel)
	if err := store.EnsureParent(ctx, storeDirOf(p)); err != nil {
		return fmt.Errorf("git: overlay parent %s: %w", rel, err)
	}
	if err := g.clearBlackout(ctx, w, rel); err != nil {
		return err
	}
	if err := store.Write(ctx, p, image, fs.WriteOptions{Mode: mode}); err != nil {
		return fmt.Errorf("git: overlay write %s: %w", rel, err)
	}
	w.set(rel, overlayNode{kind: fs.KindFile, mode: mode, size: int64(len(image))})
	return nil
}

// landOverlaySymlink mirrors one symlink (target stored as the link).
func (g *Git) landOverlaySymlink(ctx context.Context, w *workspace, rel, target string) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	p := gitws.OverlayPath(w.id, rel)
	if err := store.EnsureParent(ctx, storeDirOf(p)); err != nil {
		return fmt.Errorf("git: overlay parent %s: %w", rel, err)
	}
	if err := g.clearBlackout(ctx, w, rel); err != nil {
		return err
	}
	if err := store.Symlink(ctx, target, p); err != nil && !isStoreExistErr(err) {
		return fmt.Errorf("git: overlay symlink %s: %w", rel, err)
	}
	w.set(rel, overlayNode{kind: fs.KindSymlink, mode: 0o777, size: int64(len(target)), target: target})
	return nil
}

// landOverlayDir mirrors one directory node.
func (g *Git) landOverlayDir(ctx context.Context, w *workspace, rel string, mode uint32) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	p := gitws.OverlayPath(w.id, rel)
	if err := store.EnsureParent(ctx, storeDirOf(p)); err != nil {
		return fmt.Errorf("git: overlay parent %s: %w", rel, err)
	}
	if err := g.clearBlackout(ctx, w, rel); err != nil {
		return err
	}
	if err := store.Mkdir(ctx, p, mode); err != nil && !isStoreExistErr(err) {
		return fmt.Errorf("git: overlay mkdir %s: %w", rel, err)
	}
	w.set(rel, overlayNode{kind: fs.KindDir, mode: mode})
	return nil
}

// removeOverlayNode deletes the mirrored node from the store.
func (g *Git) removeOverlayNode(ctx context.Context, w *workspace, rel string, node overlayNode) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	p := gitws.OverlayPath(w.id, rel)
	var err error
	if node.kind == fs.KindDir {
		err = store.Rmdir(ctx, p)
	} else {
		err = store.Unlink(ctx, p)
	}
	if err != nil && !fs.IsNotFound(err) {
		return fmt.Errorf("git: overlay remove %s: %w", rel, err)
	}
	return nil
}

// setBlackout writes the deletion marker that hides a tracked path. The
// marker carries a body (a zero-byte upload is not a portable "exists").
func (g *Git) setBlackout(ctx context.Context, w *workspace, rel string) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	p := gitws.BlackoutPath(w.id, rel)
	if err := store.EnsureParent(ctx, storeDirOf(p)); err != nil {
		return fmt.Errorf("git: blackout parent %s: %w", rel, err)
	}
	if err := store.Write(ctx, p, []byte(gitws.StateDeleted+"\n"), fs.WriteOptions{Mode: 0o644}); err != nil {
		return fmt.Errorf("git: blackout write %s: %w", rel, err)
	}
	w.set(rel, overlayNode{blackout: true})
	return nil
}

// clearBlackout removes the deletion marker ahead of a re-mirror.
func (g *Git) clearBlackout(ctx context.Context, w *workspace, rel string) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	if err := store.Unlink(ctx, gitws.BlackoutPath(w.id, rel)); err != nil && !fs.IsNotFound(err) {
		return fmt.Errorf("git: blackout clear %s: %w", rel, err)
	}
	return nil
}

// putObjectPack stores one git object pack (content addressed, so
// retried checkpoints dedupe) plus its sidecar metadata.
func (g *Git) putObjectPack(ctx context.Context, id string, content []byte) (string, error) {
	store := g.storeView()
	if store == nil {
		return "", errStoreMissing
	}
	packID := gitws.PackIDFor(content)
	if err := store.EnsureParent(ctx, gitws.PacksDir(id)); err != nil {
		return "", err
	}
	if err := store.Write(ctx, gitws.PackPath(id, packID), content, fs.WriteOptions{Mode: 0o644}); err != nil {
		return "", fmt.Errorf("git: pack write %s: %w", packID, err)
	}
	meta, err := json.Marshal(gitws.PackMeta{
		ChecksumSHA256: gitws.ChecksumHex(content),
		SizeBytes:      int64(len(content)),
		CreatedAt:      gitws.NowUTC(),
	})
	if err != nil {
		return "", err
	}
	if err := store.Write(ctx, gitws.PackMetaPath(id, packID), meta, fs.WriteOptions{Mode: 0o644}); err != nil {
		return "", fmt.Errorf("git: pack meta write %s: %w", packID, err)
	}
	return packID, nil
}

// putGitState stores the .git state archive plus its sidecar metadata.
func (g *Git) putGitState(ctx context.Context, id, checkpointCommit string, content []byte) error {
	store := g.storeView()
	if store == nil {
		return errStoreMissing
	}
	meta, err := json.Marshal(gitws.GitStateMeta{
		CheckpointCommit: checkpointCommit,
		StorageType:      gitStateStorageTarGzNoObjects,
		ChecksumSHA256:   gitws.ChecksumHex(content),
		SizeBytes:        int64(len(content)),
		CreatedAt:        gitws.NowUTC(),
	})
	if err != nil {
		return err
	}
	if err := store.Write(ctx, gitws.GitStatePath(id), content, fs.WriteOptions{Mode: 0o644}); err != nil {
		return fmt.Errorf("git: state write: %w", err)
	}
	if err := store.Write(ctx, gitws.GitStateMetaPath(id), meta, fs.WriteOptions{Mode: 0o644}); err != nil {
		return fmt.Errorf("git: state meta write: %w", err)
	}
	return nil
}

// storedGitState is the restore-side view of the .git state record.
type storedGitState struct {
	checkpointCommit string
	storageType      string
	content          []byte
}

// getGitState loads the .git state record; ok=false when none exists.
func (g *Git) getGitState(ctx context.Context, id string) (storedGitState, bool, error) {
	store := g.storeView()
	if store == nil {
		return storedGitState{}, false, errStoreMissing
	}
	metaBody, err := store.Read(ctx, gitws.GitStateMetaPath(id), 0, 0)
	if err != nil {
		if fs.IsNotFound(err) {
			return storedGitState{}, false, nil
		}
		return storedGitState{}, false, err
	}
	var meta gitws.GitStateMeta
	if err := json.Unmarshal(metaBody, &meta); err != nil {
		return storedGitState{}, false, fmt.Errorf("git: state meta parse: %w", err)
	}
	content, err := store.Read(ctx, gitws.GitStatePath(id), 0, 0)
	if err != nil {
		if fs.IsNotFound(err) {
			return storedGitState{}, false, nil
		}
		return storedGitState{}, false, err
	}
	if meta.SizeBytes != 0 && meta.SizeBytes != int64(len(content)) {
		return storedGitState{}, false, fmt.Errorf("git: state archive size mismatch: meta %d, got %d", meta.SizeBytes, len(content))
	}
	if meta.ChecksumSHA256 != "" && !strings.EqualFold(meta.ChecksumSHA256, gitws.ChecksumHex(content)) {
		return storedGitState{}, false, fmt.Errorf("git: state archive checksum mismatch")
	}
	return storedGitState{
		checkpointCommit: meta.CheckpointCommit,
		storageType:      meta.StorageType,
		content:          content,
	}, true, nil
}

// storedObjectPack is the restore-side view of one stored pack.
type storedObjectPack struct {
	packID         string
	checksumSHA256 string
	sizeBytes      int64
	content        []byte
}

// listObjectPacks enumerates the stored packs (metadata + bodies).
func (g *Git) listObjectPacks(ctx context.Context, id string) ([]storedObjectPack, error) {
	store := g.storeView()
	if store == nil {
		return nil, errStoreMissing
	}
	entries, err := store.List(ctx, gitws.PacksDir(id))
	if err != nil {
		if fs.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var packs []storedObjectPack
	for _, e := range entries {
		if e.Attr.IsDir() || !strings.HasSuffix(e.Name, ".json") {
			continue
		}
		packID := strings.TrimSuffix(e.Name, ".json")
		metaBody, err := store.Read(ctx, gitws.PackMetaPath(id, packID), 0, 0)
		if err != nil {
			if fs.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		var meta gitws.PackMeta
		if err := json.Unmarshal(metaBody, &meta); err != nil {
			return nil, fmt.Errorf("git: pack meta parse %s: %w", packID, err)
		}
		content, err := store.Read(ctx, gitws.PackPath(id, packID), 0, 0)
		if err != nil {
			if fs.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		packs = append(packs, storedObjectPack{
			packID:         packID,
			checksumSHA256: meta.ChecksumSHA256,
			sizeBytes:      meta.SizeBytes,
			content:        content,
		})
	}
	return packs, nil
}
