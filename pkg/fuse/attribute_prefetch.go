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
	mountGeneration uint64
	dirGeneration   uint64
	epoch           uint64
	expiresAt       time.Time
	paths           map[string]struct{}
}

type metadataPrefetchDirState struct {
	misses         map[string]struct{}
	missWindowFrom time.Time
	nextAllowed    time.Time
	knownChildren  int
	hot            map[string]uint64
	lastUsed       uint64
	marker         metadataPrefetchMarker
}

type metadataPrefetchRequest struct {
	key             string
	parentPath      string
	mountGeneration uint64
	dirGeneration   uint64
	epoch           uint64
	large           bool
	hotPaths        []string
}

type siblingMetadataPrefetch struct {
	mu       sync.Mutex
	dirs     map[string]*metadataPrefetchDirState
	active   map[string]struct{}
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
		active: make(map[string]struct{}),
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
	p.active = make(map[string]struct{})
	for _, state := range p.dirs {
		state.misses = nil
		state.missWindowFrom = time.Time{}
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
	p.mu.Unlock()
}

func (p *siblingMetadataPrefetch) beginMiss(filePath string, mountGeneration, dirGeneration uint64) (metadataPrefetchRequest, bool) {
	if p == nil || filePath == "" || filePath == "/" {
		return metadataPrefetchRequest{}, false
	}
	parentPath := parentDir(filePath)
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.dirStateLocked(parentPath)
	p.sequence++
	state.lastUsed = p.sequence
	if state.hot == nil {
		state.hot = make(map[string]uint64)
	}
	state.hot[filePath] = p.sequence
	now := p.now()
	key := fmt.Sprintf("%d:%d:%d:%s", p.epoch, mountGeneration, dirGeneration, parentPath)
	request := metadataPrefetchRequest{
		key:             key,
		parentPath:      parentPath,
		mountGeneration: mountGeneration,
		dirGeneration:   dirGeneration,
		epoch:           p.epoch,
		large:           state.knownChildren > metadataPrefetchListThreshold,
	}
	request.hotPaths = hottestPaths(state.hot, metadataPrefetchMaxHotPaths)
	if _, ok := p.active[key]; ok {
		return request, true
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
	p.active[key] = struct{}{}
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

func (p *siblingMetadataPrefetch) rememberListing(request metadataPrefetchRequest, items []CachedFileInfo, dirGeneration uint64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if request.epoch != p.epoch {
		return
	}
	state := p.dirStateLocked(request.parentPath)
	state.knownChildren = len(items)
	paths := make(map[string]struct{})
	if len(items) <= metadataPrefetchListThreshold {
		for _, item := range items {
			if item.Name != "" {
				paths[dirEntryChildPath(request.parentPath, item.Name)] = struct{}{}
			}
		}
	} else {
		listed := make(map[string]struct{}, len(items))
		for _, item := range items {
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
		mountGeneration: request.mountGeneration,
		dirGeneration:   dirGeneration,
		epoch:           request.epoch,
		expiresAt:       p.now().Add(p.ttl),
		paths:           paths,
	}
}

func (p *siblingMetadataPrefetch) rememberBatch(request metadataPrefetchRequest, items []CachedFileInfo, dirGeneration uint64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if request.epoch != p.epoch {
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
		mountGeneration: request.mountGeneration,
		dirGeneration:   dirGeneration,
		epoch:           request.epoch,
		expiresAt:       p.now().Add(p.ttl),
		paths:           paths,
	}
}

func (p *siblingMetadataPrefetch) valid(parentPath, filePath string, mountGeneration, dirGeneration uint64) bool {
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
	if marker.epoch != p.epoch || marker.mountGeneration != mountGeneration || marker.dirGeneration != dirGeneration || p.now().After(marker.expiresAt) {
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
	if fs == nil || fs.metadataPrefetch == nil || fs.dirCache == nil || !fs.statCacheVerified() || filePath == "" || filePath == "/" || isLockFilePath(filePath) || fs.hasPendingLocalState(filePath) || fs.hasQueuedCommit(filePath) {
		return
	}
	mountGeneration := fs.mountViewGeneration.Load()
	dirGeneration := fs.dirCache.generation(parentDir(filePath))
	request, ok := fs.metadataPrefetch.beginMiss(filePath, mountGeneration, dirGeneration)
	if !ok {
		return
	}
	_, _, _ = fs.metadataPrefetch.flight.Do(ctx, request.key, func() ([]byte, error) {
		defer fs.metadataPrefetch.finish(request)
		return nil, fs.refreshSiblingMetadata(ctx, request)
	})
}

func (fs *Dat9FS) refreshSiblingMetadata(ctx context.Context, request metadataPrefetchRequest) error {
	if !fs.statCacheVerified() || fs.mountViewGeneration.Load() != request.mountGeneration || fs.dirCache.generation(request.parentPath) != request.dirGeneration {
		return nil
	}
	if request.large && len(request.hotPaths) > 0 {
		return fs.refreshSiblingMetadataBatch(ctx, request)
	}
	return fs.refreshSiblingMetadataList(ctx, request)
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
	fs.dirCache.PutListing(request.parentPath, cached, observation)
	dirGeneration := fs.dirCache.generation(request.parentPath)
	fs.mountViewMu.RUnlock()
	if fs.statCacheVerified() {
		fs.metadataPrefetch.rememberListing(request, cached, dirGeneration)
	}
	return nil
}

func (fs *Dat9FS) refreshSiblingMetadataBatch(ctx context.Context, request metadataPrefetchRequest) error {
	observation := fs.dirCache.BeginRequest(request.parentPath)
	defer fs.dirCache.EndRequest(observation)
	remotePaths := make([]string, len(request.hotPaths))
	for i, filePath := range request.hotPaths {
		remotePaths[i] = fs.remotePath(filePath)
	}
	statStart := fs.perfStart()
	results, err := fs.client.BatchStatCtx(ctx, remotePaths)
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err != nil {
		return fs.resetMountViewOnAuthorizationError(err)
	}
	items := make([]CachedFileInfo, 0, len(results))
	for i, result := range results {
		if !result.OK() || parentDir(request.hotPaths[i]) != request.parentPath {
			continue
		}
		mtime := time.Now()
		if result.Mtime > 0 {
			mtime = time.Unix(result.Mtime, 0)
		}
		items = append(items, CachedFileInfo{
			Name:       path.Base(request.hotPaths[i]),
			Size:       result.Size,
			IsDir:      result.IsDir,
			Mtime:      mtime,
			Revision:   result.Revision,
			Mode:       result.Mode,
			HasMode:    result.HasMode,
			ResourceID: result.ResourceID,
			Nlink:      result.Nlink,
		})
	}
	if len(items) == 0 || !fs.lockMountViewRead(request.mountGeneration) {
		return nil
	}
	dirGeneration, accepted := fs.dirCache.observeBatch(request.parentPath, items, observation)
	fs.mountViewMu.RUnlock()
	if accepted && fs.statCacheVerified() {
		fs.metadataPrefetch.rememberBatch(request, items, dirGeneration)
	}
	return nil
}
