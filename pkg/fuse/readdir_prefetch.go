package fuse

import (
	"context"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
)

const (
	defaultReadDirPrefetchMaxFiles = 32
	defaultReadDirPrefetchMaxBytes = 1 << 20
	defaultReadDirPrefetchTimeout  = time.Second
)

type readDirPrefetchCandidate struct {
	localPath  string
	remotePath string
	revision   int64
}

func (fs *Dat9FS) prefetchReadCacheForDir(ctx context.Context, dirPath string, items []CachedFileInfo) {
	if fs == nil || fs.opts == nil || !fs.opts.ReadDirPrefetch || len(items) == 0 {
		return
	}
	candidates := fs.readDirPrefetchCandidates(dirPath, items)
	if len(candidates) == 0 {
		return
	}

	for start := 0; start < len(candidates); start += client.MaxBatchReadSmallPaths {
		end := start + client.MaxBatchReadSmallPaths
		if end > len(candidates) {
			end = len(candidates)
		}
		chunk := candidates[start:end]
		paths := make([]string, len(chunk))
		for i, candidate := range chunk {
			paths[i] = candidate.remotePath
		}

		prefetchCtx, cancel := context.WithTimeout(ctx, fs.opts.PrefetchTimeout)
		results, err := fs.client.BatchReadSmallCtx(prefetchCtx, paths, fs.opts.PrefetchMaxFileBytes)
		cancel()
		if err != nil {
			logger.Warn(ctx, "fuse_batch_read_small_prefetch_failed",
				zap.String("dir", dirPath),
				zap.Int("start", start),
				zap.Int("end", end),
				zap.Error(err))
			return
		}
		for i, result := range results {
			if !result.OK() {
				continue
			}
			candidate := chunk[i]
			if int64(len(result.Data)) > fs.opts.PrefetchMaxFileBytes {
				continue
			}
			if fs.hasLocalWriteState(candidate.localPath) {
				continue
			}
			revision := candidate.revision
			if result.Revision > 0 {
				revision = result.Revision
			}
			fs.readCache.Put(candidate.localPath, result.Data, revision)
			if revision > 0 {
				if ino, ok := fs.inodes.GetInode(candidate.localPath); ok {
					fs.inodes.UpdateRevision(ino, revision)
				}
			}
		}
	}
}

func (fs *Dat9FS) readDirPrefetchCandidates(dirPath string, items []CachedFileInfo) []readDirPrefetchCandidate {
	maxFiles := fs.opts.PrefetchMaxFiles
	maxFileBytes := fs.opts.PrefetchMaxFileBytes
	maxBytes := fs.opts.PrefetchMaxBytes
	if maxFiles <= 0 || maxFileBytes <= 0 || maxBytes <= 0 {
		return nil
	}

	candidates := make([]readDirPrefetchCandidate, 0, maxFiles)
	var totalBytes int64
	for _, item := range items {
		if len(candidates) >= maxFiles {
			break
		}
		if item.IsDir || item.Size <= 0 || item.Size > maxFileBytes {
			continue
		}
		localPath := dirEntryChildPath(dirPath, item.Name)
		if isLockFilePath(localPath) || fs.hasLocalWriteState(localPath) {
			continue
		}
		if _, hit := fs.readCache.Get(localPath, item.Revision); hit {
			continue
		}
		if totalBytes+item.Size > maxBytes {
			// Keep scanning so later smaller files can still fit the aggregate budget.
			continue
		}
		candidates = append(candidates, readDirPrefetchCandidate{
			localPath:  localPath,
			remotePath: fs.remotePath(localPath),
			revision:   item.Revision,
		})
		totalBytes += item.Size
	}
	return candidates
}

func (fs *Dat9FS) hasLocalWriteState(p string) bool {
	if fs.hasPendingLocalState(p) || fs.hasQueuedCommit(p) {
		return true
	}
	if ino, ok := fs.inodes.GetInode(p); ok {
		if fs.hasOpenHandle(ino, p) {
			return true
		}
		if _, dirty := fs.dirtyHandleSize(ino); dirty {
			return true
		}
	}
	for _, fh := range fs.fileHandles.Snapshot() {
		if fh != nil && fh.Path == p && fh.Dirty != nil {
			return true
		}
	}
	return false
}
