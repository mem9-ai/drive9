package cache

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

const (
	dirPrefetchConcurrency    = 4
	dirPrefetchCandidateLimit = 64
	dirPrefetchTimeout        = 5 * time.Second

	metaPrefetchMissWindow       = time.Second
	metaPrefetchCooldown         = time.Second
	metaPrefetchMissesToActivate = 2
	metaPrefetchMaxHotPaths      = client.MaxBatchStatPaths
	metaPrefetchMaxTrackedDirs   = 1024

	contentPrefetchMaxFilesDefault = 32
	contentPrefetchMaxBytesDefault = 1 << 20
	contentPrefetchTimeoutDefault  = time.Second
)

type metaDirState struct {
	misses      map[string]struct{}
	windowFrom  time.Time
	nextAllowed time.Time
}

type dirLoadCall struct {
	done chan struct{}
}

// dirPrefetcher is the directory-neighborhood prefetcher: three
// cooperating tracks ride the mount's listings and lookup misses.
//
//   - sibling metadata: repeated lookup misses inside one directory (2+
//     within a 1s window) batch-stat the directory's hot children and
//     upsert the answers into the namespace cache, so following lookups
//     resolve without a remote round trip;
//   - sibling directories: after a directory lists, sibling directories
//     from the parent's cached listing without a cached listing of their
//     own load in the background (4 concurrent, 64 candidates), seeding
//     the namespace cache;
//   - readdir content: after a directory lists, small files (bounded by
//     count/bytes/timeout) batch-read into the read cache.
//
// Local-write state is derived from bus events (Write/Staged mark, Landed
// clears), and cache fencing rides the listing loads going through the
// chain below this handler (no re-entry).
type dirPrefetcher struct {
	mu       sync.Mutex
	dirLoads map[string]*dirLoadCall
	metaDirs map[string]*metaDirState
	busy     int
	writing  map[string]time.Time
	client   *client.Client
	root     string

	// namespace answers lookups from cached listings (facade attaches it).
	namespace fs.DirListingCache
	// content knobs (ReadDirPrefetch).
	contentEnabled  bool
	contentMaxFiles int
	contentMaxBytes int64
	contentMaxFile  int64
	contentTimeout  time.Duration
}

// SetNamespaceCache wires the facade-owned listing cache.
func (c *Cache) SetNamespaceCache(nc fs.DirListingCache) {
	if c.dirs != nil {
		c.dirs.namespace = nc
	}
}

func newDirPrefetcher(cl *client.Client, root string) *dirPrefetcher {
	return &dirPrefetcher{
		dirLoads: map[string]*dirLoadCall{},
		metaDirs: map[string]*metaDirState{},
		writing:  map[string]time.Time{},
		client:   cl,
		root:     root,
	}
}

func parentDirOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		return p[:i]
	}
	return "/"
}

func dirJoin(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// prefetchDirContents ports prefetchReadCacheForDir: batch-read small
// files of a freshly listed directory into the read cache.
func (c *Cache) prefetchDirContents(ctx context.Context, dir string, entries []fs.Entry) {
	p := c.dirs
	if p == nil || !p.contentEnabled || p.client == nil || len(entries) == 0 {
		return
	}
	type candidate struct {
		local  string
		remote string
		rev    int64
	}
	var candidates []candidate
	totalBytes := int64(0)
	for _, e := range entries {
		if len(candidates) >= p.contentMaxFiles {
			break
		}
		if e.Attr.IsDir() || e.Attr.Size <= 0 || e.Attr.Size > p.contentMaxFile {
			continue
		}
		local := dirJoin(dir, e.Name)
		if strings.HasSuffix(e.Name, ".lock") || p.hasLocalWriteState(local) {
			continue
		}
		if _, hit := c.mem.Get(local, e.Attr.Rev); hit {
			continue
		}
		if totalBytes+e.Attr.Size > p.contentMaxBytes {
			continue
		}
		candidates = append(candidates, candidate{local: local, remote: p.remotePath(local), rev: e.Attr.Rev})
		totalBytes += e.Attr.Size
	}
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
		for i, cand := range chunk {
			paths[i] = cand.remote
		}
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.contentTimeout)
		results, err := p.client.BatchReadSmallCtx(pctx, paths, p.contentMaxFile)
		cancel()
		if err != nil {
			return
		}
		for i, result := range results {
			if !result.OK() || i >= len(chunk) {
				continue
			}
			if int64(len(result.Data)) > p.contentMaxFile {
				continue
			}
			if p.hasLocalWriteState(chunk[i].local) {
				continue
			}
			rev := chunk[i].rev
			if result.Revision > 0 {
				rev = result.Revision
			}
			c.mem.PutOwned(chunk[i].local, result.Data, rev)
		}
	}
}

func (p *dirPrefetcher) remotePath(local string) string {
	if p.root == "" || p.root == "/" {
		return local
	}
	if local == "/" {
		return p.root
	}
	return p.root + local
}

// noteWrite approximates the hasLocalWriteState: a path with un-landed
// local writes must not be content-prefetched (the cached bytes would be
// stale until the next event).
func (p *dirPrefetcher) noteWrite(path string) {
	p.mu.Lock()
	p.writing[path] = time.Now().Add(time.Minute)
	p.mu.Unlock()
}

func (p *dirPrefetcher) noteLanded(path string) {
	p.mu.Lock()
	delete(p.writing, path)
	p.mu.Unlock()
}

func (p *dirPrefetcher) hasLocalWriteState(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	deadline, ok := p.writing[path]
	if !ok {
		return false
	}
	if time.Now().After(deadline) {
		delete(p.writing, path)
		return false
	}
	return true
}

// answeredFromNamespace reports whether the namespace cache already knows
// the name absent (a fresh negative row or a complete listing without it).
func (p *dirPrefetcher) answeredFromNamespace(path string) bool {
	looker, ok := p.namespace.(interface {
		LookupNamespace(parent, name string) (fs.DirEntryInfo, fs.LookupKind)
	})
	if !ok {
		return false
	}
	dir := parentDirOf(path)
	name := path[strings.LastIndexByte(path, '/')+1:]
	if name == "" {
		return false
	}
	_, kind := looker.LookupNamespace(dir, name)
	return kind == fs.LookupNegative || kind == fs.LookupCompleteMiss || kind == fs.LookupSessionMiss
}

// noteMiss tracks lookup misses per directory and activates sibling
// metadata prefetch when the window trips (2 misses within 1s). Misses the
// namespace cache already answered (a negative or complete-miss row) do
// not count — v1 counted only cache-unanswered misses, and re-counting the
// answered ones would re-fire a redundant batch-stat on every cooldown.
func (p *dirPrefetcher) noteMiss(ctx context.Context, path string) {
	if p == nil || p.namespace == nil || p.client == nil {
		return
	}
	if p.answeredFromNamespace(path) {
		return
	}
	dir := parentDirOf(path)
	p.mu.Lock()
	st := p.metaDirs[dir]
	if st == nil {
		// Bounded tracked-directory set (v1 metadataPrefetchMaxTrackedDirs).
		if len(p.metaDirs) >= metaPrefetchMaxTrackedDirs {
			var oldest string
			var oldestAt time.Time
			for d, s := range p.metaDirs {
				if oldest == "" || s.windowFrom.Before(oldestAt) {
					oldest, oldestAt = d, s.windowFrom
				}
			}
			delete(p.metaDirs, oldest)
		}
		st = &metaDirState{misses: map[string]struct{}{}, windowFrom: time.Now()}
		p.metaDirs[dir] = st
	}
	now := time.Now()
	if now.Sub(st.windowFrom) > metaPrefetchMissWindow {
		st.misses = map[string]struct{}{}
		st.windowFrom = now
	}
	st.misses[path] = struct{}{}
	activate := len(st.misses) >= metaPrefetchMissesToActivate && now.After(st.nextAllowed)
	if activate {
		st.nextAllowed = now.Add(metaPrefetchCooldown)
	}
	p.mu.Unlock()
	if activate {
		go p.refreshSiblingMetadata(ctx, dir)
	}
}

// refreshSiblingMetadata ports the refreshSiblingMetadata: batch-stat the
// directory's children (up to MaxBatchStatPaths) and record every hit into
// the namespace cache as a REMOTE OBSERVATION — a TTL-bound row, not
// mount-authoritative state (a plain Upsert would live until an explicit
// invalidation and mask remote deletes the SSE stream missed).
func (p *dirPrefetcher) refreshSiblingMetadata(ctx context.Context, dir string) {
	siblings, ok := p.namespace.Get(dir)
	if !ok || len(siblings) == 0 {
		return
	}
	names := make([]string, 0, len(siblings))
	for _, s := range siblings {
		if len(names) >= metaPrefetchMaxHotPaths {
			break
		}
		names = append(names, s.Name)
	}
	remote := make([]string, len(names))
	for i, n := range names {
		remote[i] = p.remotePath(dirJoin(dir, n))
	}
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dirPrefetchTimeout)
	defer cancel()
	results, err := p.client.BatchStatCtx(bctx, remote)
	if err != nil {
		return
	}
	observer, canObserve := p.namespace.(interface {
		ObserveRemote(parent string, entry fs.DirEntryInfo)
	})
	for i, r := range results {
		if !r.OK() || i >= len(names) {
			continue
		}
		item := siblings[i]
		item.Size = r.Size
		item.Rev = r.Revision
		item.IsDir = r.IsDir
		if r.HasMode {
			item.Mode = r.Mode
			item.HasMode = true
		}
		if canObserve {
			observer.ObserveRemote(dir, item)
			continue
		}
		p.namespace.Upsert(dir, item)
	}
}

// maybePrefetchSiblingDirectories ports maybePrefetchSiblingDirectories:
// after `dir` listed, background-load listings of its sibling directories
// (from the parent's cached listing) that are not cached yet. load lists
// one path through the chain below this handler (no re-entry).
func (p *dirPrefetcher) maybePrefetchSiblingDirectories(load func(ctx context.Context, op *fs.Op) (fs.Result, error), dir string) {
	if p == nil || p.namespace == nil || dir == "/" {
		return
	}
	parent := parentDirOf(dir)
	siblings, ok := p.namespace.Get(parent)
	if !ok {
		return
	}
	candidates := 0
	for _, s := range siblings {
		if !s.IsDir {
			continue
		}
		sib := dirJoin(parent, s.Name)
		if sib == dir {
			continue
		}
		if _, cached := p.namespace.Get(sib); cached {
			continue
		}
		if !p.scheduleDirLoad(sib) {
			continue
		}
		go p.loadSiblingDirectory(load, sib)
		candidates++
		if candidates >= dirPrefetchCandidateLimit {
			return
		}
	}
}

func (p *dirPrefetcher) scheduleDirLoad(dir string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.busy >= dirPrefetchConcurrency {
		return false
	}
	if _, exists := p.dirLoads[dir]; exists {
		return false
	}
	p.dirLoads[dir] = &dirLoadCall{done: make(chan struct{})}
	p.busy++
	return true
}

func (p *dirPrefetcher) finishDirLoad(dir string) {
	p.mu.Lock()
	if call, ok := p.dirLoads[dir]; ok {
		close(call.done)
		delete(p.dirLoads, dir)
	}
	if p.busy > 0 {
		p.busy--
	}
	p.mu.Unlock()
}

// loadSiblingDirectory lists one sibling through the chain below this
// handler (no re-entry) and seeds the namespace cache.
func (p *dirPrefetcher) loadSiblingDirectory(load func(ctx context.Context, op *fs.Op) (fs.Result, error), dir string) {
	defer p.finishDirLoad(dir)
	ctx, cancel := context.WithTimeout(context.Background(), dirPrefetchTimeout)
	defer cancel()
	res, err := load(ctx, &fs.Op{Kind: fs.OpList, Path: dir})
	if err != nil {
		return
	}
	items := make([]fs.DirEntryInfo, 0, len(res.Entries))
	for _, e := range res.Entries {
		items = append(items, fs.DirEntryInfo{Name: e.Name, IsDir: e.Attr.IsDir(), Size: e.Attr.Size, Mode: e.Attr.Mode, HasMode: true, Rev: e.Attr.Rev, Mtime: e.Attr.Mtime, Nlink: e.Attr.Nlink, ResourceID: e.Attr.ResourceID})
	}
	p.namespace.Put(dir, items)
}
