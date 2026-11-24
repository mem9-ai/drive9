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
func (g *Git) loadTree(ctx context.Context, w *workspace) bool {
	head := w.head()
	if head == "" {
		return false
	}
	nodes, err := g.cl.ListGitTree(ctx, w.id, head)
	if err != nil {
		return false
	}
	tree := make(map[string]client.GitTreeNode, len(nodes))
	for _, n := range nodes {
		if n.Path == "" {
			continue
		}
		tree[strings.TrimPrefix(n.Path, "/")] = n
	}
	w.mu.Lock()
	w.tree = tree
	w.mu.Unlock()
	return true
}

// treeLookup resolves rel against the HEAD tree.
func (w *workspace) treeLookup(rel string) (client.GitTreeNode, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.tree == nil || rel == "" {
		return client.GitTreeNode{}, false
	}
	n, ok := w.tree[rel]
	return n, ok
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

// blobOnce deduplicates concurrent blob materializations (v1
// materializeGitBlob's singleflight).
var blobOnce singleFlight

type singleFlight struct {
	mu sync.Mutex
	m  map[string]*blobCall
}

type blobCall struct {
	done chan struct{}
	data []byte
	err  error
}

func (s *singleFlight) do(key string, fn func() ([]byte, error)) ([]byte, error) {
	s.mu.Lock()
	if s.m == nil {
		s.m = map[string]*blobCall{}
	}
	if c, ok := s.m[key]; ok {
		s.mu.Unlock()
		<-c.done
		return c.data, c.err
	}
	c := &blobCall{done: make(chan struct{})}
	s.m[key] = c
	s.mu.Unlock()
	c.data, c.err = fn()
	close(c.done)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
	return c.data, c.err
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
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	gitDir     string
	closed     bool
	contentCmd *exec.Cmd
	contentIn  io.WriteCloser
	contentOut *bufio.Reader
	checkCmd   *exec.Cmd
	checkIn    io.WriteCloser
	checkOut   *bufio.Reader
}

func startCatFileProc(ctx context.Context, gitDir string, args ...string) (*exec.Cmd, io.WriteCloser, *bufio.Reader, error) {
	full := append([]string{"--git-dir", gitDir, "cat-file"}, args...)
	cmd := newGitCommandContext(ctx, full...)
	allowProtocols := gitcache.GitAllowedProtocols
	if gitcache.GitAllowLocalRepos() {
		allowProtocols = gitcache.GitLocalAllowedProtocols
	}
	cmd.Env = append(gitSafeEnv(os.Environ()),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_NO_LAZY_FETCH=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ALLOW_PROTOCOL="+allowProtocols,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, nil, nil, err
	}
	return cmd, stdin, bufio.NewReaderSize(stdout, 64*1024), nil
}

func stopCatFileProc(cmd *exec.Cmd, stdin io.WriteCloser) {
	if stdin != nil {
		_ = stdin.Close()
	}
	if cmd != nil && cmd.Process != nil {
		_ = killGitCommandGroup(cmd)
		_ = cmd.Wait()
	}
}

func (b *catFileBatch) closeContentLocked() {
	stopCatFileProc(b.contentCmd, b.contentIn)
	b.contentCmd, b.contentIn, b.contentOut = nil, nil, nil
}

func (b *catFileBatch) closeCheckLocked() {
	stopCatFileProc(b.checkCmd, b.checkIn)
	b.checkCmd, b.checkIn, b.checkOut = nil, nil, nil
}

func (b *catFileBatch) close() {
	b.cancel() // Interrupt pipe I/O before waiting for its owner.
	b.mu.Lock()
	b.closed = true
	b.closeContentLocked()
	b.closeCheckLocked()
	b.mu.Unlock()
}

func (b *catFileBatch) readBlob(sha string) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !isGitObjectID(sha) {
		return nil, false, fmt.Errorf("invalid blob object ID")
	}
	if b.closed || b.ctx.Err() != nil {
		return nil, false, errGitRuntimeStopped
	}
	if b.contentCmd == nil {
		cmd, in, out, err := startCatFileProc(b.ctx, b.gitDir, "--batch")
		if err != nil {
			return nil, false, err
		}
		b.contentCmd, b.contentIn, b.contentOut = cmd, in, out
	}
	if _, err := io.WriteString(b.contentIn, sha+"\n"); err != nil {
		b.closeContentLocked()
		return nil, false, err
	}
	hdr, err := b.contentOut.ReadString('\n')
	if err != nil {
		b.closeContentLocked()
		return nil, false, err
	}
	fields := strings.Fields(hdr)
	if len(fields) == 2 && strings.EqualFold(fields[0], sha) && fields[1] == "missing" {
		return nil, false, nil
	}
	if len(fields) != 3 || !strings.EqualFold(fields[0], sha) || fields[1] != "blob" {
		b.closeContentLocked()
		return nil, false, fmt.Errorf("unexpected cat-file header %q", strings.TrimSpace(hdr))
	}
	sz, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || sz < 0 {
		b.closeContentLocked()
		return nil, false, fmt.Errorf("invalid cat-file size in %q", strings.TrimSpace(hdr))
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(b.contentOut, buf); err != nil {
		b.closeContentLocked()
		return nil, false, err
	}
	if end, err := b.contentOut.ReadByte(); err != nil || end != '\n' {
		b.closeContentLocked()
		return nil, false, fmt.Errorf("invalid cat-file record terminator")
	}
	return buf, true, nil
}

func (b *catFileBatch) sizeBlob(sha string) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !isGitObjectID(sha) || b.closed || b.ctx.Err() != nil {
		return 0, false
	}
	if b.checkCmd == nil {
		cmd, in, out, err := startCatFileProc(b.ctx, b.gitDir, "--batch-check=%(objectname) %(objecttype) %(objectsize)")
		if err != nil {
			return 0, false
		}
		b.checkCmd, b.checkIn, b.checkOut = cmd, in, out
	}
	if _, err := io.WriteString(b.checkIn, sha+"\n"); err != nil {
		b.closeCheckLocked()
		return 0, false
	}
	hdr, err := b.checkOut.ReadString('\n')
	if err != nil {
		b.closeCheckLocked()
		return 0, false
	}
	fields := strings.Fields(hdr)
	if len(fields) == 2 && strings.EqualFold(fields[0], sha) && fields[1] == "missing" {
		return 0, false
	}
	if len(fields) != 3 || !strings.EqualFold(fields[0], sha) || fields[1] != "blob" {
		b.closeCheckLocked()
		return 0, false
	}
	sz, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || sz < 0 {
		b.closeCheckLocked()
		return 0, false
	}
	return sz, true
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
	b := &catFileBatch{gitDir: gitDir, ctx: ctx, cancel: cancel}
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
func (g *Git) materializeTreeEntry(ctx context.Context, w *workspace, rel, sha string) ([]byte, error) {
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
		if head := w.head(); head != "" {
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
	data, err := blobOnce.do(w.id+":"+sha, func() ([]byte, error) {
		if info, statErr := os.Stat(gitDir); statErr == nil && info.IsDir() {
			if b := g.batchReaderFor(gitDir); b != nil {
				if data, found, berr := b.readBlob(sha); berr == nil && found {
					return data, nil
				}
			}
		}
		return g.readGitBlobObject(ctx, gitDir, sha, "blob", 30*time.Second)
	})
	if err == nil {
		cacheBlob(w.id+":"+sha, data)
	}
	return data, err
}

// resolveTreeNodeSize resolves an unknown tree-node size (blobless -1) from
// the local hydrate cache first, then the local object store.
func (g *Git) resolveTreeNodeSize(ctx context.Context, w *workspace, rel, sha string) int64 {
	if g.localRoot != "" {
		if ctx == nil {
			ctx = context.Background()
		}
		if head := w.head(); head != "" {
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
	if !isGitObjectID(sha) {
		return 0
	}
	gitDir := g.resolveWorkspaceGitDir(w)
	if info, statErr := os.Stat(gitDir); statErr == nil && info.IsDir() {
		if b := g.batchReaderFor(gitDir); b != nil {
			if sz, ok := b.sizeBlob(sha); ok {
				return sz
			}
		}
	}
	out, err := g.readGitBlobObject(ctx, gitDir, sha, "-s", 10*time.Second)
	if err != nil {
		return 0
	}
	var size int64
	_, _ = fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &size)
	return size
}

func (g *Git) readGitBlobObject(ctx context.Context, gitDir, sha, mode string, timeout time.Duration) ([]byte, error) {
	var out []byte
	err := g.runForeground(ctx, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
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
