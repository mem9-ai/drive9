package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	directoryPrefetchConcurrency      = 4
	directoryPrefetchCandidateLimit   = 64
	directoryPrefetchMaxResponseBytes = 1 << 20
)

var (
	directoryPrefetchTimeout = 5 * time.Second
	errDirectoryLoadRejected = errors.New("directory load rejected by cache fence")
)

type directoryLoadKey struct {
	path                string
	resourceID          string
	mountGeneration     uint64
	mutationGeneration  uint64
	namespaceGeneration uint64
}

type directoryLoadCall struct {
	done       chan struct{}
	entries    []CachedFileInfo
	err        error
	background bool
	joined     bool
}

type siblingDirectoryPrefetch struct {
	mu     sync.Mutex
	calls  map[directoryLoadKey]*directoryLoadCall
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

func newSiblingDirectoryPrefetch() *siblingDirectoryPrefetch {
	ctx, cancel := context.WithCancel(context.Background())
	return &siblingDirectoryPrefetch{
		calls:  make(map[directoryLoadKey]*directoryLoadCall),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (p *siblingDirectoryPrefetch) shutdown() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func directoryLoadKeyFor(snapshot directoryPrefetchSnapshot, mountGeneration uint64) directoryLoadKey {
	return directoryLoadKey{
		path:                snapshot.targetPath,
		resourceID:          snapshot.resourceID,
		mountGeneration:     mountGeneration,
		mutationGeneration:  snapshot.mutationGeneration,
		namespaceGeneration: snapshot.namespaceGeneration,
	}
}

func (p *siblingDirectoryPrefetch) schedule(fs *Dat9FS, snapshot directoryPrefetchSnapshot, mountGeneration uint64) (scheduled, full bool) {
	if p == nil || fs == nil {
		return false, false
	}
	key := directoryLoadKeyFor(snapshot, mountGeneration)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false, false
	}
	if _, exists := p.calls[key]; exists {
		p.mu.Unlock()
		return false, false
	}
	background := 0
	for _, call := range p.calls {
		if call.background {
			background++
		}
	}
	if background >= directoryPrefetchConcurrency {
		p.mu.Unlock()
		return false, true
	}
	call := &directoryLoadCall{done: make(chan struct{}), background: true}
	p.calls[key] = call
	p.wg.Add(1)
	active := background + 1
	p.mu.Unlock()

	if fs.perf != nil {
		fs.perf.directoryPrefetchIssued.add(1)
		fs.perf.directoryPrefetchMaxConcurrent.max(uint64(active))
	}
	go p.run(fs, key, snapshot, call)
	return true, false
}

func (p *siblingDirectoryPrefetch) loadForeground(ctx context.Context, fs *Dat9FS, snapshot directoryPrefetchSnapshot, mountGeneration uint64) ([]CachedFileInfo, bool, error) {
	if p == nil || fs == nil {
		return nil, false, nil
	}
	key := directoryLoadKeyFor(snapshot, mountGeneration)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, false, nil
	}
	call, exists := p.calls[key]
	if exists {
		call.joined = true
	} else {
		call = &directoryLoadCall{done: make(chan struct{})}
		p.calls[key] = call
		p.wg.Add(1)
	}
	p.mu.Unlock()

	if exists {
		if fs.perf != nil {
			fs.perf.directoryPrefetchJoined.add(1)
		}
	} else {
		go p.run(fs, key, snapshot, call)
	}

	started := time.Now()
	select {
	case <-ctx.Done():
		if fs.perf != nil {
			fs.perf.directoryPrefetchForegroundWaitNS.add(uint64(time.Since(started)))
		}
		return nil, true, ctx.Err()
	case <-call.done:
		if fs.perf != nil {
			fs.perf.directoryPrefetchForegroundWaitNS.add(uint64(time.Since(started)))
		}
	}
	if call.err == nil {
		if exists && fs.perf != nil {
			fs.perf.directoryPrefetchJoinSuccess.add(1)
		}
		return call.entries, true, nil
	}
	if call.background || errors.Is(call.err, errDirectoryLoadRejected) || errors.Is(call.err, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return nil, true, ctx.Err()
		}
		return nil, false, nil
	}
	return nil, true, call.err
}

func (p *siblingDirectoryPrefetch) run(fs *Dat9FS, key directoryLoadKey, snapshot directoryPrefetchSnapshot, call *directoryLoadCall) {
	defer p.wg.Done()
	started := time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, directoryPrefetchTimeout)
	entries, responseBytes, err := fs.loadRemoteDirectory(ctx, snapshot.targetPath, key.mountGeneration, &snapshot, call.background)
	cancel()

	p.mu.Lock()
	call.entries = entries
	call.err = err
	delete(p.calls, key)
	joined := call.joined
	close(call.done)
	p.mu.Unlock()

	if fs.perf == nil {
		return
	}
	if call.background {
		fs.perf.directoryPrefetchDurationNS.add(uint64(time.Since(started)))
		if err == nil {
			fs.perf.directoryPrefetchEntries.add(uint64(len(entries)))
		}
	}
	if responseBytes > 0 {
		fs.perf.directoryPrefetchBytes.add(uint64(responseBytes))
	}
	if errors.Is(err, errDirectoryLoadRejected) {
		fs.perf.directoryPrefetchRejected.add(1)
	}
	if call.background && !joined {
		fs.perf.directoryPrefetchCompletedUnjoined.add(1)
	}
}

func (fs *Dat9FS) maybePrefetchSiblingDirectories(currentPath string) {
	if !fs.directoryPrefetchEligiblePath(currentPath) {
		return
	}
	mountGeneration := fs.mountViewGeneration.Load()
	candidates := fs.dirCache.directoryPrefetchCandidates(currentPath, directoryPrefetchCandidateLimit)
	for _, snapshot := range candidates {
		if !fs.directoryPrefetchEligiblePath(snapshot.targetPath) || fs.hasPendingLocalState(snapshot.targetPath) || fs.hasQueuedCommit(snapshot.targetPath) {
			continue
		}
		_, full := fs.directoryPrefetch.schedule(fs, snapshot, mountGeneration)
		if full {
			return
		}
	}
}

func (fs *Dat9FS) directoryPrefetchEligiblePath(localPath string) bool {
	if fs == nil || fs.client == nil || fs.dirCache == nil || fs.directoryPrefetch == nil || fs.layerEnabled() || fs.extentDiscoveryEnabled() || fs.opts == nil || fs.opts.LegacyDirStatFallback || !fs.statCacheVerified() || localPath == "" || localPath == "/" {
		return false
	}
	if fs.localPolicy != nil && fs.localPolicy.Classify(localPath) != PathLayerRemotePersistent {
		return false
	}
	if _, _, ok := fs.loadedGitWorkspaceForPath(localPath); ok {
		return false
	}
	return true
}

func (fs *Dat9FS) loadRemoteDirectory(ctx context.Context, dirPath string, mountGeneration uint64, snapshot *directoryPrefetchSnapshot, bounded bool) ([]CachedFileInfo, int64, error) {
	request := fs.dirCache.BeginRequest(dirPath)
	defer fs.dirCache.EndRequest(request)

	listStart := fs.perfStart()
	var (
		items         []client.FileInfo
		responseBytes int64
		err           error
	)
	if bounded {
		result, listErr := fs.client.ListWithOptionsCtx(ctx, fs.remotePath(dirPath), client.ListOptions{MaxResponseBytes: directoryPrefetchMaxResponseBytes})
		items = result.Entries
		responseBytes = result.ResponseBytes
		err = listErr
	} else {
		result, listErr := fs.client.ListWithOptionsCtx(ctx, fs.remotePath(dirPath), client.ListOptions{})
		items = result.Entries
		responseBytes = result.ResponseBytes
		err = listErr
	}
	fs.perfRecordRemote(perfRemoteList, listStart, err, uint64(max(responseBytes, 0)))
	if err != nil {
		return nil, responseBytes, fs.resetMountViewOnAuthorizationError(err)
	}

	cached := cachedFileInfos(items)
	if err := fs.applyBatchStats(ctx, dirPath, cached); err != nil {
		return nil, responseBytes, err
	}
	if !fs.lockMountViewRead(mountGeneration) {
		return nil, responseBytes, syscall.EAGAIN
	}
	defer fs.mountViewMu.RUnlock()

	if snapshot == nil {
		return fs.putDirectoryListing(dirPath, cached, request), responseBytes, nil
	}
	if !fs.statCacheVerified() {
		return nil, responseBytes, errDirectoryLoadRejected
	}
	view, receipt, installed := fs.dirCache.putListingIfSnapshot(cached, request, *snapshot)
	if !installed {
		return nil, responseBytes, errDirectoryLoadRejected
	}
	if fs.metadataPrefetch != nil && receipt.complete {
		fs.metadataPrefetch.rememberDirectorySize(dirPath, receipt.childCount, receipt.mutationGeneration, receipt.namespaceGeneration)
	}
	return view, responseBytes, nil
}

func (fs *Dat9FS) loadForegroundRemoteDirectory(ctx context.Context, dirPath string, mountGeneration uint64) ([]CachedFileInfo, error) {
	if fs.directoryPrefetchEligiblePath(dirPath) {
		if snapshot, ok := fs.dirCache.directoryPrefetchSnapshot(dirPath); ok {
			entries, handled, err := fs.directoryPrefetch.loadForeground(ctx, fs, snapshot, mountGeneration)
			if handled {
				return entries, err
			}
		}
	}
	entries, _, err := fs.loadRemoteDirectory(ctx, dirPath, mountGeneration, nil, false)
	return entries, err
}
