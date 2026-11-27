// The fast-clone view: the workspace's HEAD tree serves clean files from
// git objects, and the remote overlay log is fetched at load so other
// clients' modifications and whiteouts are visible (v1 gitEntry/tree
// machinery).
package git

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitcache"
)

// gitWorkspaceRescanInterval bounds the on-demand discovery loop (v1
// gitWorkspaceIndexRescanInterval): workspaces registered after the mount
// arm without a remount.
const gitWorkspaceRescanInterval = 60 * time.Second

// gitBlobCacheSize bounds the materialized-blob cache.
const gitBlobCacheSize = 64

// loadRemoteOverlay fetches the server-side overlay log into the workspace
// mirror (v1 rt.overlay): other clients' upserts and deletes become
// visible. Entries whose payload lives server-side (storage-backed) keep
// their metadata; their reads fall through to the base plane.
func (g *Git) loadRemoteOverlay(ctx context.Context, w *workspace) {
	// Snapshot the mirror's VALUES before the fetch: a mutation landing
	// while the list is in flight (the workspace is already published and
	// serving) must survive the replacement — the fetched log predates it.
	// Key membership alone fences only additions; an OVERWRITE of an
	// existing key would let the older response replace the acknowledged
	// newer bytes, and a local deletion would resurrect the deleted row.
	w.mu.RLock()
	before := make(map[string]overlayEntry, len(w.overlay))
	for rel, e := range w.overlay {
		before[rel] = e
	}
	w.mu.RUnlock()
	entries, err := g.cl.ListGitOverlayEntries(ctx, w.id)
	if err != nil {
		// 404-style absences are fine (a workspace with no overlay log).
		return
	}
	overlay := make(map[string]overlayEntry, len(entries))
	for _, ge := range entries {
		e := overlayEntry{Op: ge.Op, Kind: ge.Kind, Mode: ge.Mode, Content: ge.Content, mtime: ge.UpdatedAt}
		if e.Kind == "" {
			e.Kind = "file"
		}
		if e.Op == "" {
			e.Op = "upsert"
		}
		if len(e.Content) == 0 && ge.SizeBytes > 0 {
			// Storage-backed payload: the local mirror cannot serve the
			// bytes; record a marker so attrs list the size while reads
			// fall to the base view.
			e.remote = true
			e.size = ge.SizeBytes
		}
		// The fetched inline bytes ARE the authoritative image — including
		// a zero-length one (a synced ftruncate(0)). Without the marker
		// ensureBufferLocked skipped the overlay and reloaded the stale
		// HEAD/base under the next write.
		if !e.remote && (e.Kind == "file" || e.Kind == "symlink") {
			e.Materialized = true
		}
		overlay[strings.TrimPrefix(ge.Path, "/")] = e
	}
	w.mu.Lock()
	// Local mirrors of our own writes already match the log; the fetched
	// log is the server truth (our writes round-tripped through
	// PutGitOverlayEntry).
	for rel, e := range w.overlay {
		old, existed := before[rel]
		if !existed || !sameOverlayEntry(old, e) {
			// New, or locally mutated while the fetch was in flight: the
			// response's row predates the acknowledged change.
			overlay[rel] = e
		}
	}
	for rel := range before {
		if _, still := w.overlay[rel]; !still {
			// Deleted locally during the fetch: the response's row is
			// pre-deletion truth and must not resurrect the name.
			delete(overlay, rel)
		}
	}
	w.overlay = overlay
	w.mu.Unlock()
}

// sameOverlayEntry reports whether two mirror entries carry identical
// state — the fetch fence replays a local entry only when it differs from
// the fetch-start snapshot.
func sameOverlayEntry(a, b overlayEntry) bool {
	return a.Op == b.Op && a.Kind == b.Kind && a.Mode == b.Mode &&
		a.remote == b.remote && a.size == b.size &&
		a.mtime.Equal(b.mtime) && bytes.Equal(a.Content, b.Content)
}

// loadTree fetches the workspace's HEAD tree (v1 ListGitTree → rt.nodes):
// clean files resolve here before the base view.
func (g *Git) loadTree(ctx context.Context, w *workspace, head string) {
	nodes, err := g.cl.ListGitTree(ctx, w.id, head)
	if err != nil {
		return
	}
	tree := make(map[string]client.GitTreeNode, len(nodes))
	for _, n := range nodes {
		if n.Path == "" {
			continue
		}
		tree[strings.TrimPrefix(n.Path, "/")] = n
	}
	w.mu.Lock()
	if w.headCommit == head {
		w.tree = tree
		w.treeLoaded = head
	}
	w.mu.Unlock()
}

// treeLookup resolves rel against the HEAD tree.
func (w *workspace) treeLookup(rel string) (client.GitTreeNode, string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.tree == nil || rel == "" {
		return client.GitTreeNode{}, "", false
	}
	n, ok := w.tree[rel]
	return n, w.treeLoaded, ok
}

// treeAttr projects a tree node onto the framework attr shape. mtime is
// the workspace's stable clean-tree time (cleanTreeMtime).
func treeAttr(n client.GitTreeNode, mtime time.Time) fs.Attr {
	kind := fs.KindFile
	switch n.Kind {
	case "dir":
		kind = fs.KindDir
	case "symlink":
		kind = fs.KindSymlink
	}
	mode := parseMode(n.Mode)
	if mode == 0 {
		mode = 0o644
		switch kind {
		case fs.KindDir:
			mode = 0o755
		case fs.KindSymlink:
			mode = 0o777
		}
	}
	size := n.SizeBytes
	if size < 0 {
		// Blobless trees report unknown sizes; resolve lazily on read.
		size = 0
	}
	return fs.Attr{Kind: kind, Mode: mode, Size: size, Mtime: mtime}
}

type singleFlight struct {
	mu sync.Mutex
	m  map[string]*blobCall
}

type blobCall struct {
	canceled bool
	done     chan struct{}
	data     []byte
	err      error
}

func (s *singleFlight) do(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.m == nil {
			s.m = make(map[string]*blobCall)
		}
		if c := s.m[key]; c != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.done:
				// A cancelled producer released its pipe. A live waiter may
				// become the next producer with its own request context.
				if c.canceled {
					continue
				}
				return c.data, c.err
			}
		}
		c := &blobCall{done: make(chan struct{})}
		s.m[key] = c
		s.mu.Unlock()
		c.data, c.err = fn()
		if err := ctx.Err(); err != nil {
			c.data, c.err, c.canceled = nil, err, true
		}
		s.mu.Lock()
		delete(s.m, key)
		close(c.done)
		s.mu.Unlock()
		return c.data, c.err
	}
}

// blobCache memoizes materialized blobs (bounded).
var (
	blobCacheMu sync.Mutex
	blobCache   = map[string][]byte{}
)

func cacheBlob(key string, data []byte) {
	blobCacheMu.Lock()
	defer blobCacheMu.Unlock()
	if len(blobCache) >= gitBlobCacheSize {
		for k := range blobCache {
			delete(blobCache, k)
			break
		}
	}
	blobCache[key] = data
}

func cachedBlob(key string) ([]byte, bool) {
	blobCacheMu.Lock()
	defer blobCacheMu.Unlock()
	d, ok := blobCache[key]
	return d, ok
}

type catFileBatch struct {
	ctx            context.Context
	cancel         context.CancelFunc
	gitDir         string
	gate           chan struct{}
	content, sizes *catFileProcess
}

type catFileProcess struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdin  io.WriteCloser
	stdout *bufio.Reader
}

func startCatFileProc(ctx context.Context, gitDir, option string) (*catFileProcess, error) {
	ctx, cancel := context.WithCancel(ctx)
	cmd := newGitCommandContext(ctx, "--git-dir", gitDir, "cat-file", option)
	allowProtocols := gitcache.GitAllowedProtocols
	if gitcache.GitAllowLocalRepos() {
		allowProtocols = gitcache.GitLocalAllowedProtocols
	}
	cmd.Env = append(gitSafeEnv(os.Environ()), "GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL="+allowProtocols)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		_ = stdin.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	return &catFileProcess{cmd, cancel, stdin, bufio.NewReaderSize(stdout, 64*1024)}, nil
}

func (p *catFileProcess) close() {
	if p == nil {
		return
	}
	p.cancel()
	_ = p.stdin.Close()
	_ = p.cmd.Wait()
}

func (b *catFileBatch) close() {
	b.cancel()
	b.gate <- struct{}{}
	defer func() { <-b.gate }()
	b.content.close()
	b.sizes.close()
	b.content, b.sizes = nil, nil
}

func (p *catFileProcess) blob(sha string, content bool) ([]byte, int64, error) {
	if _, err := io.WriteString(p.stdin, sha+"\n"); err != nil {
		return nil, -1, err
	}
	hdr, err := p.stdout.ReadString('\n')
	if err != nil {
		return nil, -1, err
	}
	fields := strings.Fields(hdr)
	if len(fields) == 2 && strings.EqualFold(fields[0], sha) && fields[1] == "missing" {
		return nil, -1, nil
	}
	if len(fields) != 3 || !strings.EqualFold(fields[0], sha) || fields[1] != "blob" {
		return nil, -1, fmt.Errorf("unexpected cat-file header %q", strings.TrimSpace(hdr))
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 {
		return nil, -1, fmt.Errorf("invalid cat-file size %q", fields[2])
	}
	if !content {
		return nil, size, nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(p.stdout, buf); err != nil {
		return nil, -1, err
	}
	if end, err := p.stdout.ReadByte(); err != nil || end != '\n' {
		return nil, -1, fmt.Errorf("invalid cat-file record terminator")
	}
	return buf, size, nil
}

// Cancellation before admission leaves the current query alone. Once admitted,
// the request owns that process until its entire response has been read. A
// cancelled or malformed response discards the stream before the next request.
func (b *catFileBatch) query(ctx context.Context, sha string, content bool) ([]byte, int64, error) {
	if !isGitObjectID(sha) {
		return nil, -1, fmt.Errorf("invalid blob object ID")
	}
	select {
	case b.gate <- struct{}{}:
		defer func() { <-b.gate }()
	case <-ctx.Done():
		return nil, -1, ctx.Err()
	case <-b.ctx.Done():
		return nil, -1, errGitRuntimeStopped
	}
	if err := ctx.Err(); err != nil {
		return nil, -1, err
	}
	if b.ctx.Err() != nil {
		return nil, -1, errGitRuntimeStopped
	}
	slot, option := &b.sizes, "--batch-check"
	if content {
		slot, option = &b.content, "--batch"
	}
	if *slot == nil {
		p, err := startCatFileProc(b.ctx, b.gitDir, option)
		if err != nil {
			return nil, -1, err
		}
		*slot = p
	}
	p := *slot
	stopCancel := context.AfterFunc(ctx, p.cancel)
	data, size, err := p.blob(sha, content)
	if !stopCancel() || err != nil || ctx.Err() != nil {
		p.close()
		*slot = nil
	}
	if ctx.Err() != nil {
		return nil, -1, ctx.Err()
	}
	return data, size, err
}

func (g *Git) batchReaderFor(gitDir string) *catFileBatch {
	if g == nil || gitDir == "" {
		return nil
	}
	g.backgroundMu.Lock()
	defer g.backgroundMu.Unlock()
	if g.stopping {
		return nil
	}
	g.batchMu.Lock()
	defer g.batchMu.Unlock()
	if g.batchReaders == nil {
		g.batchReaders = make(map[string]*catFileBatch)
	}
	if b, ok := g.batchReaders[gitDir]; ok {
		return b
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &catFileBatch{gitDir: gitDir, ctx: ctx, cancel: cancel, gate: make(chan struct{}, 1)}
	g.batchReaders[gitDir] = b
	return b
}

func (g *Git) evictBatchReader(gitDir string) {
	if g == nil || gitDir == "" {
		return
	}
	g.batchMu.Lock()
	var b *catFileBatch
	if g.batchReaders != nil {
		b = g.batchReaders[gitDir]
		delete(g.batchReaders, gitDir)
	}
	g.batchMu.Unlock()
	if b != nil {
		b.close()
	}
}

func (g *Git) closeBatchReaders() {
	if g == nil {
		return
	}
	g.batchMu.Lock()
	readers := g.batchReaders
	g.batchReaders = nil
	g.batchMu.Unlock()
	for _, b := range readers {
		b.close()
	}
}

func (g *Git) resolveWorkspaceGitDir(w *workspace) string {
	if dir, ok := g.checkpointGitDir(w); ok {
		return dir
	}
	return g.gitDirFor(w.rootPath())
}

// materializeTreeEntry reads a clean tree file or symlink using the local
// hydrate tree/blob cache first (`gitcache`), then the workspace's persistent
// `git cat-file --batch` reader, falling back to one-shot `git cat-file blob`
// when a partial clone needs an on-demand lazy fetch (`--hydrate=off`).
func (g *Git) materializeTreeEntry(ctx context.Context, w *workspace, head, rel, sha string) ([]byte, error) {
	if !isGitObjectID(sha) {
		return nil, fmt.Errorf("git: invalid tree node object ID")
	}
	if d, ok := cachedBlob(w.id + ":" + sha); ok {
		return d, nil
	}
	if g.localRoot != "" {
		if ctx == nil {
			ctx = context.Background()
		}
		if head != "" {
			if rel != "" {
				if data, ok, err := gitcache.ReadTreeFile(ctx, g.localRoot, w.id, head, rel, 0, -1); err == nil && ok {
					cacheBlob(w.id+":"+sha, data)
					return data, nil
				}
			}
			if data, ok, err := gitcache.ReadBlob(ctx, g.localRoot, w.id, head, sha, 0, -1); err == nil && ok {
				cacheBlob(w.id+":"+sha, data)
				return data, nil
			}
		}
	}
	return g.materializeBlob(ctx, w, sha)
}

// materializeBlob reads one blob out of the workspace's restored local
// .git (v1 gitCatFileBlob): persistent `git cat-file --batch` first, then
// one-shot `git cat-file blob <sha>` fallback.
func (g *Git) materializeBlob(ctx context.Context, w *workspace, sha string) ([]byte, error) {
	if !isGitObjectID(sha) {
		return nil, fmt.Errorf("git: invalid tree node object ID")
	}
	if d, ok := cachedBlob(w.id + ":" + sha); ok {
		return d, nil
	}
	gitDir := g.resolveWorkspaceGitDir(w)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	data, err := g.blobReads.do(ctx, w.id+":"+sha, func() ([]byte, error) {
		if info, statErr := os.Stat(gitDir); statErr == nil && info.IsDir() {
			if b := g.batchReaderFor(gitDir); b != nil {
				if data, size, berr := b.query(ctx, sha, true); berr == nil && size >= 0 {
					return data, nil
				}
			}
		}
		return g.readGitBlobObject(ctx, gitDir, sha, "blob")
	})
	if err == nil {
		cacheBlob(w.id+":"+sha, data)
	}
	return data, err
}

// resolveTreeNodeSize resolves an unknown tree-node size (blobless -1) from
// the local hydrate cache first, then the local object store.
func (g *Git) resolveTreeNodeSize(ctx context.Context, w *workspace, head, rel, sha string) int64 {
	if g.localRoot != "" {
		if ctx == nil {
			ctx = context.Background()
		}
		if head != "" {
			if rel != "" {
				if sz, ok, err := gitcache.StatTreeFile(ctx, g.localRoot, w.id, head, rel); err == nil && ok {
					return sz
				}
			}
			if sz, ok, err := gitcache.StatBlob(ctx, g.localRoot, w.id, head, sha); err == nil && ok {
				return sz
			}
		}
	}
	return g.blobSize(ctx, w, sha)
}

// blobSize resolves an unknown tree-node size (blobless -1) from the
// local object store.
func (g *Git) blobSize(ctx context.Context, w *workspace, sha string) int64 {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if !isGitObjectID(sha) {
		return -1
	}
	gitDir := g.resolveWorkspaceGitDir(w)
	if info, statErr := os.Stat(gitDir); statErr == nil && info.IsDir() {
		if b := g.batchReaderFor(gitDir); b != nil {
			if _, sz, err := b.query(ctx, sha, false); err == nil && sz >= 0 {
				return sz
			}
		}
	}
	out, err := g.readGitBlobObject(ctx, gitDir, sha, "-s")
	if err != nil {
		return -1
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil || size < 0 {
		return -1
	}
	return size
}

func (g *Git) readGitBlobObject(ctx context.Context, gitDir, sha, mode string) ([]byte, error) {
	var out []byte
	err := g.runForeground(ctx, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		allowProtocols := gitcache.GitAllowedProtocols
		if gitcache.GitAllowLocalRepos() {
			allowProtocols = gitcache.GitLocalAllowedProtocols
		}
		cmd := newGitCommandContext(ctx, "--git-dir", gitDir, "cat-file", mode, sha)
		cmd.Env = append(gitSafeEnv(os.Environ()), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL="+allowProtocols)
		var err error
		out, err = cmd.Output()
		return err
	})
	return out, err
}
