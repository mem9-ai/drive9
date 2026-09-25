package fuse

import (
	"context"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	metadataPrefetchMissWindow       = time.Second
	metadataPrefetchCooldown         = time.Second
	metadataPrefetchListThreshold    = client.MaxBatchStatPaths
	metadataPrefetchMaxHotPaths      = client.MaxBatchStatPaths
	metadataPrefetchMaxTrackedDirs   = 1024
	metadataPrefetchMissesToActivate = 2
)

type metadataPrefetchMarker struct {
	mountGeneration     uint64
	mutationGeneration  uint64
	namespaceGeneration uint64
	epoch               uint64
	expiresAt           time.Time
	paths               map[string]struct{}
}

type metadataPrefetchDirState struct {
	misses         map[string]struct{}
	missWindowFrom time.Time
	nextAllowed    time.Time
	knownChildren  int
	knownMutation  uint64
	knownNamespace uint64
	childrenKnown  bool
	hot            map[string]uint64
	lastUsed       uint64
	marker         metadataPrefetchMarker
}

type metadataPrefetchRequest struct {
	key                 string
	parentPath          string
	mountGeneration     uint64
	mutationGeneration  uint64
	epoch               uint64
	namespaceGeneration uint64
	list                bool
	hotPaths            []string
}

type siblingMetadataPrefetch struct {
	mu       sync.Mutex
	dirs     map[string]*metadataPrefetchDirState
	active   map[string]metadataPrefetchRequest
	flight   *SingleFlight
	ttl      time.Duration
	now      func() time.Time
	epoch    uint64
	sequence uint64
}

func newSiblingMetadataPrefetch(ttl time.Duration) *siblingMetadataPrefetch {
	if ttl <= 0 {
		ttl = defaultDirCacheTTL
	}
	return &siblingMetadataPrefetch{
		dirs:   make(map[string]*metadataPrefetchDirState),
		active: make(map[string]metadataPrefetchRequest),
		flight: NewSingleFlight(),
		ttl:    ttl,
		now:    time.Now,
	}
}

func (p *siblingMetadataPrefetch) clear() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.epoch++
	p.active = make(map[string]metadataPrefetchRequest)
	for _, state := range p.dirs {
		state.misses = nil
		state.missWindowFrom = time.Time{}
		state.childrenKnown = false
		state.marker = metadataPrefetchMarker{}
	}
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) noteAccess(filePath string) {
	if p == nil || filePath == "" || filePath == "/" {
		return
	}
	parentPath := parentDir(filePath)
	p.mu.Lock()
	state := p.dirStateLocked(parentPath)
	p.noteHotPathLocked(state, filePath)
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) noteHotPathLocked(state *metadataPrefetchDirState, filePath string) {
	p.sequence++
	state.lastUsed = p.sequence
	if state.hot == nil {
		state.hot = make(map[string]uint64)
	}
	state.hot[filePath] = p.sequence
	if len(state.hot) > metadataPrefetchMaxHotPaths {
		var oldestPath string
		oldestSequence := ^uint64(0)
		for candidate, sequence := range state.hot {
			if sequence < oldestSequence {
				oldestPath = candidate
				oldestSequence = sequence
			}
		}
		delete(state.hot, oldestPath)
	}
}

func (p *siblingMetadataPrefetch) beginMiss(filePath string, mountGeneration, mutationGeneration, namespaceGeneration uint64) (metadataPrefetchRequest, bool) {
	if p == nil || filePath == "" || filePath == "/" {
		return metadataPrefetchRequest{}, false
	}
	parentPath := parentDir(filePath)
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.dirStateLocked(parentPath)
	p.noteHotPathLocked(state, filePath)
	now := p.now()
	key := fmt.Sprintf("%d:%d:%d:%d:%s", p.epoch, mountGeneration, mutationGeneration, namespaceGeneration, parentPath)
	if state.childrenKnown && (state.knownMutation != mutationGeneration || state.knownNamespace != namespaceGeneration) {
		state.childrenKnown = false
	}
	request := metadataPrefetchRequest{
		key:                 key,
		parentPath:          parentPath,
		mountGeneration:     mountGeneration,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: namespaceGeneration,
		epoch:               p.epoch,
		list:                state.childrenKnown && state.knownChildren <= metadataPrefetchListThreshold,
	}
	if active, ok := p.active[key]; ok {
		return active, true
	}
	if now.Before(state.nextAllowed) {
		return metadataPrefetchRequest{}, false
	}
	if state.missWindowFrom.IsZero() || now.Sub(state.missWindowFrom) > metadataPrefetchMissWindow {
		state.misses = make(map[string]struct{})
		state.missWindowFrom = now
	}
	if state.misses == nil {
		state.misses = make(map[string]struct{})
	}
	state.misses[filePath] = struct{}{}
	if len(state.misses) < metadataPrefetchMissesToActivate {
		return metadataPrefetchRequest{}, false
	}
	state.misses = nil
	state.missWindowFrom = time.Time{}
	state.nextAllowed = now.Add(metadataPrefetchCooldown)
	request.hotPaths = hottestPaths(state.hot, metadataPrefetchMaxHotPaths)
	p.active[key] = request
	return request, true
}

func (p *siblingMetadataPrefetch) finish(request metadataPrefetchRequest) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.active, request.key)
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) discardMiss(filePath string) {
	if p == nil || filePath == "" || filePath == "/" {
		return
	}
	p.mu.Lock()
	state, ok := p.dirs[parentDir(filePath)]
	if ok && state.misses != nil {
		delete(state.misses, filePath)
		if len(state.misses) == 0 {
			state.misses = nil
			state.missWindowFrom = time.Time{}
		}
	}
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) rememberDirectorySize(parentPath string, childCount int, mutationGeneration, namespaceGeneration uint64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	state := p.dirStateLocked(parentPath)
	state.knownChildren = childCount
	state.knownMutation = mutationGeneration
	state.knownNamespace = namespaceGeneration
	state.childrenKnown = true
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) rememberListing(request metadataPrefetchRequest, receipt listingInstallReceipt) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if request.epoch != p.epoch || !receipt.installed || receipt.mutationGeneration != request.mutationGeneration || receipt.namespaceGeneration != request.namespaceGeneration {
		return
	}
	state := p.dirStateLocked(request.parentPath)
	if receipt.complete {
		state.knownChildren = receipt.childCount
		state.knownMutation = receipt.mutationGeneration
		state.knownNamespace = receipt.namespaceGeneration
		state.childrenKnown = true
	}
	paths := make(map[string]struct{})
	if receipt.childCount <= metadataPrefetchListThreshold {
		for _, item := range receipt.accepted {
			if item.Name != "" {
				paths[dirEntryChildPath(request.parentPath, item.Name)] = struct{}{}
			}
		}
	} else {
		listed := make(map[string]struct{}, len(receipt.accepted))
		for _, item := range receipt.accepted {
			if item.Name != "" {
				listed[dirEntryChildPath(request.parentPath, item.Name)] = struct{}{}
			}
		}
		for _, filePath := range request.hotPaths {
			if _, ok := listed[filePath]; ok {
				paths[filePath] = struct{}{}
			}
		}
	}
	state.marker = metadataPrefetchMarker{
		mountGeneration:     request.mountGeneration,
		mutationGeneration:  receipt.mutationGeneration,
		namespaceGeneration: request.namespaceGeneration,
		epoch:               request.epoch,
		expiresAt:           p.now().Add(p.ttl),
		paths:               paths,
	}
}

func (p *siblingMetadataPrefetch) rememberBatch(request metadataPrefetchRequest, items []CachedFileInfo, mutationGeneration, namespaceGeneration uint64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if request.epoch != p.epoch || mutationGeneration != request.mutationGeneration || namespaceGeneration != request.namespaceGeneration {
		return
	}
	state := p.dirStateLocked(request.parentPath)
	paths := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.Name != "" {
			paths[dirEntryChildPath(request.parentPath, item.Name)] = struct{}{}
		}
	}
	state.marker = metadataPrefetchMarker{
		mountGeneration:     request.mountGeneration,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: request.namespaceGeneration,
		epoch:               request.epoch,
		expiresAt:           p.now().Add(p.ttl),
		paths:               paths,
	}
}

func (p *siblingMetadataPrefetch) valid(parentPath, filePath string, mountGeneration, mutationGeneration, namespaceGeneration uint64) bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.dirs[parentPath]
	if !ok {
		return false
	}
	marker := state.marker
	if marker.epoch != p.epoch || marker.mountGeneration != mountGeneration || marker.mutationGeneration != mutationGeneration || marker.namespaceGeneration != namespaceGeneration || p.now().After(marker.expiresAt) {
		state.marker = metadataPrefetchMarker{}
		return false
	}
	_, ok = marker.paths[filePath]
	return ok
}

func (p *siblingMetadataPrefetch) dirStateLocked(parentPath string) *metadataPrefetchDirState {
	if state, ok := p.dirs[parentPath]; ok {
		return state
	}
	if len(p.dirs) >= metadataPrefetchMaxTrackedDirs {
		var oldestPath string
		oldestSequence := ^uint64(0)
		for candidate, state := range p.dirs {
			if state.lastUsed < oldestSequence {
				oldestPath = candidate
				oldestSequence = state.lastUsed
			}
		}
		delete(p.dirs, oldestPath)
	}
	state := &metadataPrefetchDirState{}
	p.dirs[parentPath] = state
	return state
}

func hottestPaths(hot map[string]uint64, limit int) []string {
	type hotPath struct {
		path     string
		sequence uint64
	}
	ordered := make([]hotPath, 0, len(hot))
	for filePath, sequence := range hot {
		ordered = append(ordered, hotPath{path: filePath, sequence: sequence})
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].sequence > ordered[j].sequence
	})
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	paths := make([]string, len(ordered))
	for i, item := range ordered {
		paths[i] = item.path
	}
	return paths
}

func (fs *Dat9FS) maybePrefetchSiblingMetadata(ctx context.Context, filePath string) {
	if fs == nil || fs.metadataPrefetch == nil || fs.dirCache == nil || fs.extentDiscoveryEnabled() || fs.opts.LegacyDirStatFallback || !fs.statCacheVerified() || filePath == "" || filePath == "/" || isLockFilePath(filePath) || fs.hasPendingLocalState(filePath) || fs.hasQueuedCommit(filePath) {
		return
	}
	mountGeneration := fs.mountViewGeneration.Load()
	parentPath := parentDir(filePath)
	mutationGeneration := fs.dirCache.mutationGeneration(parentPath)
	namespaceGeneration := fs.dirCache.namespaceGeneration()
	request, ok := fs.metadataPrefetch.beginMiss(filePath, mountGeneration, mutationGeneration, namespaceGeneration)
	if !ok {
		return
	}
	_, _, _ = fs.metadataPrefetch.flight.Do(ctx, request.key, func() ([]byte, error) {
		defer fs.metadataPrefetch.finish(request)
		return nil, fs.refreshSiblingMetadata(ctx, request)
	})
}

func (fs *Dat9FS) refreshSiblingMetadata(ctx context.Context, request metadataPrefetchRequest) error {
	if !fs.statCacheVerified() || fs.mountViewGeneration.Load() != request.mountGeneration || fs.dirCache.mutationGeneration(request.parentPath) != request.mutationGeneration || fs.dirCache.namespaceGeneration() != request.namespaceGeneration {
		return nil
	}
	if request.list {
		return fs.refreshSiblingMetadataList(ctx, request)
	}
	if len(request.hotPaths) == 0 {
		return nil
	}
	return fs.refreshSiblingMetadataBatch(ctx, request)
}

func (fs *Dat9FS) refreshSiblingMetadataList(ctx context.Context, request metadataPrefetchRequest) error {
	observation := fs.dirCache.BeginRequest(request.parentPath)
	defer fs.dirCache.EndRequest(observation)
	listStart := fs.perfStart()
	items, err := fs.client.ListCtx(ctx, fs.remotePath(request.parentPath))
	fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
	if err != nil {
		return fs.resetMountViewOnAuthorizationError(err)
	}
	cached := cachedFileInfos(items)
	if err := fs.applyBatchStats(ctx, request.parentPath, cached); err != nil {
		return err
	}
	if !fs.lockMountViewRead(request.mountGeneration) {
		return nil
	}
	_, receipt := fs.dirCache.putListing(request.parentPath, cached, observation)
	fs.mountViewMu.RUnlock()
	if fs.statCacheVerified() {
		fs.metadataPrefetch.rememberListing(request, receipt)
	}
	return nil
}

func (fs *Dat9FS) refreshSiblingMetadataBatch(ctx context.Context, request metadataPrefetchRequest) error {
	observation := fs.dirCache.BeginRequest(request.parentPath)
	defer fs.dirCache.EndRequest(observation)
	remotePaths := make([]string, 0, len(request.hotPaths))
	requested := make(map[string]string, len(request.hotPaths))
	for _, filePath := range request.hotPaths {
		remotePath := fs.remotePath(filePath)
		if _, duplicate := requested[remotePath]; duplicate {
			continue
		}
		requested[remotePath] = filePath
		remotePaths = append(remotePaths, remotePath)
	}
	statStart := fs.perfStart()
	results, err := fs.client.BatchStatCtx(ctx, remotePaths)
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err != nil {
		return fs.resetMountViewOnAuthorizationError(err)
	}
	itemsByPath := make(map[string]CachedFileInfo, len(results))
	seenResults := make(map[string]struct{}, len(results))
	invalidPaths := make(map[string]struct{})
	for _, result := range results {
		filePath, requestedPath := requested[result.Path]
		if !requestedPath || parentDir(filePath) != request.parentPath {
			continue
		}
		if _, duplicate := seenResults[result.Path]; duplicate {
			delete(itemsByPath, filePath)
			invalidPaths[filePath] = struct{}{}
			continue
		}
		seenResults[result.Path] = struct{}{}
		if _, invalid := invalidPaths[filePath]; invalid || !result.OK() {
			continue
		}
		mtime := time.Now()
		if result.Mtime > 0 {
			mtime = time.Unix(result.Mtime, 0)
		}
		itemsByPath[filePath] = CachedFileInfo{
			Name:       path.Base(filePath),
			Size:       result.Size,
			IsDir:      result.IsDir,
			Mtime:      mtime,
			Revision:   result.Revision,
			Mode:       result.Mode,
			HasMode:    result.HasMode,
			ResourceID: result.ResourceID,
			Nlink:      result.Nlink,
		}
	}
	items := make([]CachedFileInfo, 0, len(itemsByPath))
	for _, filePath := range request.hotPaths {
		if item, ok := itemsByPath[filePath]; ok {
			items = append(items, item)
		}
	}
	if len(items) == 0 || !fs.lockMountViewRead(request.mountGeneration) {
		return nil
	}
	_, accepted := fs.dirCache.observeBatch(request.parentPath, items, observation)
	mutationGeneration := fs.dirCache.mutationGeneration(request.parentPath)
	namespaceGeneration := fs.dirCache.namespaceGeneration()
	fs.mountViewMu.RUnlock()
	if accepted && fs.statCacheVerified() {
		fs.metadataPrefetch.rememberBatch(request, items, mutationGeneration, namespaceGeneration)
	}
	return nil
}

func (fs *Dat9FS) putDirectoryListing(dirPath string, items []CachedFileInfo, request RequestToken) []CachedFileInfo {
	view, receipt := fs.dirCache.putListing(dirPath, items, request)
	if fs.metadataPrefetch != nil && receipt.installed && receipt.complete {
		fs.metadataPrefetch.rememberDirectorySize(dirPath, receipt.childCount, receipt.mutationGeneration, receipt.namespaceGeneration)
	}
	return view
}
