package fuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

const gitCheckpointTimeout = 2 * time.Minute
const gitWorkspaceRefreshInterval = time.Second

type gitWorkspaceLayer struct {
	mu         sync.Mutex
	checkpoint sync.Mutex
	loaded     bool
	loadedAt   time.Time
	workspaces []*gitWorkspaceRuntime
}

type gitWorkspaceRuntime struct {
	workspace client.GitWorkspace
	localRoot string
	nodes     map[string]client.GitTreeNode
	children  map[string][]client.GitTreeNode
	overlay   map[string]client.GitOverlayEntry
	restored  bool
}

func newGitWorkspaceLayer() *gitWorkspaceLayer {
	return &gitWorkspaceLayer{}
}

func (fs *Dat9FS) ensureGitWorkspaces(ctx context.Context) error {
	if fs == nil || fs.git == nil || fs.client == nil {
		return nil
	}
	fs.git.mu.Lock()
	if fs.git.loaded && time.Since(fs.git.loadedAt) < gitWorkspaceRefreshInterval {
		fs.git.mu.Unlock()
		return nil
	}
	fs.git.mu.Unlock()

	workspaces, err := fs.client.ListGitWorkspaces(ctx)
	if err != nil {
		if client.IsNotFound(err) {
			err = nil
		}
		return err
	}
	loaded := make([]*gitWorkspaceRuntime, 0, len(workspaces))
	for i := range workspaces {
		ws := workspaces[i]
		localRoot, ok := fs.localPath(strings.TrimSuffix(ws.RootPath, "/"))
		if !ok {
			continue
		}
		if localRoot != "/" {
			localRoot = strings.TrimSuffix(localRoot, "/")
		}
		nodes, err := fs.client.ListGitTree(ctx, ws.WorkspaceID, ws.HeadCommit)
		if err != nil {
			continue
		}
		overlays, err := fs.client.ListGitOverlayEntries(ctx, ws.WorkspaceID)
		if err != nil && !client.IsNotFound(err) {
			continue
		}
		rt := &gitWorkspaceRuntime{
			workspace: ws,
			localRoot: localRoot,
			nodes:     make(map[string]client.GitTreeNode, len(nodes)),
			children:  make(map[string][]client.GitTreeNode),
			overlay:   make(map[string]client.GitOverlayEntry, len(overlays)),
		}
		for _, n := range nodes {
			rt.nodes[n.Path] = n
			rt.children[n.ParentPath] = append(rt.children[n.ParentPath], n)
		}
		for _, e := range overlays {
			rt.overlay[e.Path] = e
		}
		loaded = append(loaded, rt)
	}
	sort.Slice(loaded, func(i, j int) bool {
		return len(loaded[i].localRoot) > len(loaded[j].localRoot)
	})

	fs.git.mu.Lock()
	fs.git.workspaces = loaded
	fs.git.loaded = true
	fs.git.loadedAt = time.Now()
	fs.git.mu.Unlock()
	return nil
}

func (fs *Dat9FS) gitWorkspaceForPath(localPath string) (*gitWorkspaceRuntime, string, bool) {
	if fs == nil || fs.git == nil || fs.opts == nil || !fs.opts.EnableGitWorkspaces {
		return nil, "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), fuseTimeout)
	_ = fs.ensureGitWorkspaces(ctx)
	cancel()

	clean := path.Clean(localPath)
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	for _, rt := range fs.git.workspaces {
		root := rt.localRoot
		if root == "/" {
			rel := strings.TrimPrefix(clean, "/")
			return rt, rel, true
		}
		if clean == root {
			return rt, "", true
		}
		prefix := root + "/"
		if strings.HasPrefix(clean, prefix) {
			return rt, strings.TrimPrefix(clean, prefix), true
		}
	}
	return nil, "", false
}

func (rt *gitWorkspaceRuntime) overlayEntry(rel string) (client.GitOverlayEntry, bool) {
	if rt == nil {
		return client.GitOverlayEntry{}, false
	}
	e, ok := rt.overlay[rel]
	return e, ok
}

func (rt *gitWorkspaceRuntime) cleanNode(rel string) (client.GitTreeNode, bool) {
	if rt == nil {
		return client.GitTreeNode{}, false
	}
	n, ok := rt.nodes[rel]
	return n, ok
}

func (fs *Dat9FS) gitEntry(localPath string, incrementLookup bool) (*InodeEntry, bool) {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok {
		return nil, false
	}
	if rel == "" {
		return fs.gitInode(localPath, true, 0, 0o755, true, incrementLookup), true
	}
	if e, ok := rt.overlayEntry(rel); ok {
		if e.Op == "whiteout" {
			return nil, true
		}
		return fs.gitOverlayInode(rt, rel, e, incrementLookup), true
	}
	if n, ok := rt.cleanNode(rel); ok {
		return fs.gitTreeInode(localPath, n, incrementLookup), true
	}
	if rt.hasImpliedDir(rel) {
		return fs.gitInode(localPath, true, 0, 0o755, true, incrementLookup), true
	}
	return nil, true
}

func (rt *gitWorkspaceRuntime) hasImpliedDir(rel string) bool {
	if rel == "" {
		return true
	}
	if _, ok := rt.children[rel]; ok {
		return true
	}
	prefix := rel + "/"
	for p, e := range rt.overlay {
		if e.Op != "whiteout" && strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) gitTreeInode(localPath string, n client.GitTreeNode, incrementLookup bool) *InodeEntry {
	mode, hasMode, isDir := gitNodeMode(n)
	size := n.SizeBytes
	if isDir {
		size = 0
	}
	return fs.gitInode(localPath, isDir, size, mode, hasMode, incrementLookup)
}

func (fs *Dat9FS) gitOverlayInode(rt *gitWorkspaceRuntime, rel string, e client.GitOverlayEntry, incrementLookup bool) *InodeEntry {
	localPath := path.Join(rt.localRoot, rel)
	if rt.localRoot == "/" {
		localPath = "/" + rel
	}
	if e.Op == "chmod" {
		if n, ok := rt.cleanNode(rel); ok {
			base := fs.gitTreeInode(localPath, n, incrementLookup)
			if parsed, ok := parseGitMode(e.Mode); ok && base != nil {
				fs.inodes.SetModeState(base.Ino, parsed, true)
				base.Mode = parsed
				base.HasMode = true
			}
			return base
		}
	}
	isDir := e.Kind == "dir"
	mode := uint32(0o644)
	hasMode := true
	if e.Kind == "symlink" || e.Op == "symlink" {
		mode = symlinkMode()
	} else if isDir {
		mode = 0o755
	}
	if parsed, ok := parseGitMode(e.Mode); ok {
		mode = parsed
	}
	size := e.SizeBytes
	if len(e.Content) > 0 {
		size = int64(len(e.Content))
	}
	if isDir {
		size = 0
	}
	return fs.gitInode(localPath, isDir, size, mode, hasMode, incrementLookup)
}

func (fs *Dat9FS) gitInode(localPath string, isDir bool, size int64, mode uint32, hasMode bool, incrementLookup bool) *InodeEntry {
	var ino uint64
	if incrementLookup {
		ino = fs.inodes.Lookup(localPath, isDir, size, time.Now())
	} else {
		ino = fs.inodes.EnsureInode(localPath, isDir, size, time.Now())
	}
	fs.inodes.SetModeState(ino, mode, hasMode)
	entry, _ := fs.inodes.GetEntry(ino)
	return entry
}

func gitNodeMode(n client.GitTreeNode) (mode uint32, hasMode bool, isDir bool) {
	switch n.Kind {
	case "dir":
		return 0o755, true, true
	case "symlink":
		return symlinkMode(), true, false
	}
	if parsed, ok := parseGitMode(n.Mode); ok {
		return parsed, true, false
	}
	return 0o644, true, false
}

func parseGitMode(raw string) (uint32, bool) {
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, false
	}
	mode := uint32(v)
	if mode&uint32(syscall.S_IFMT) != 0 {
		return mode, true
	}
	return mode & 0o777, true
}

func (fs *Dat9FS) listGitDir(ctx context.Context, dirPath string) ([]DirEntry, bool, error) {
	_ = ctx
	rt, rel, ok := fs.gitWorkspaceForPath(dirPath)
	if !ok {
		return nil, false, nil
	}
	if rel != "" {
		if e, ok := rt.overlayEntry(rel); ok {
			if e.Op == "whiteout" {
				return nil, true, os.ErrNotExist
			}
			if e.Kind != "dir" {
				return nil, true, syscall.ENOTDIR
			}
		} else if n, ok := rt.cleanNode(rel); ok {
			if n.Kind != "dir" {
				return nil, true, syscall.ENOTDIR
			}
		} else if !rt.hasImpliedDir(rel) {
			return nil, true, os.ErrNotExist
		}
	}
	entriesByName := make(map[string]DirEntry)
	whiteouts := make(map[string]struct{})
	for _, n := range rt.children[rel] {
		localPath := path.Join(rt.localRoot, n.Path)
		if rt.localRoot == "/" {
			localPath = "/" + n.Path
		}
		entry := fs.gitTreeInode(localPath, n, false)
		if entry == nil {
			continue
		}
		entriesByName[n.Name] = DirEntry{
			Name:        n.Name,
			Ino:         entry.Ino,
			Mode:        dirEntryMode(entry.IsDir, entry.HasMode, entry.Mode),
			Size:        entry.Size,
			Mtime:       entry.Mtime,
			AttrMode:    entry.Mode,
			HasMode:     entry.HasMode,
			IsDir:       entry.IsDir,
			HasMetadata: true,
		}
	}
	prefix := ""
	if rel != "" {
		prefix = rel + "/"
	}
	for p, e := range rt.overlay {
		if prefix != "" && !strings.HasPrefix(p, prefix) {
			continue
		}
		childRel := strings.TrimPrefix(p, prefix)
		if childRel == "" || strings.Contains(childRel, "/") {
			continue
		}
		if e.Op == "whiteout" {
			whiteouts[childRel] = struct{}{}
			delete(entriesByName, childRel)
			continue
		}
		entry := fs.gitOverlayInode(rt, p, e, false)
		entriesByName[childRel] = DirEntry{
			Name:        childRel,
			Ino:         entry.Ino,
			Mode:        dirEntryMode(entry.IsDir, entry.HasMode, entry.Mode),
			Size:        entry.Size,
			Mtime:       entry.Mtime,
			AttrMode:    entry.Mode,
			HasMode:     entry.HasMode,
			IsDir:       entry.IsDir,
			HasMetadata: true,
		}
	}
	for p, e := range rt.overlay {
		if e.Op == "whiteout" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(p, prefix) {
			continue
		}
		childRel := strings.TrimPrefix(p, prefix)
		if idx := strings.Index(childRel, "/"); idx > 0 {
			name := childRel[:idx]
			if _, whiteout := whiteouts[name]; whiteout {
				continue
			}
			if _, exists := entriesByName[name]; exists {
				continue
			}
			localPath := dirEntryChildPath(dirPath, name)
			entry := fs.gitInode(localPath, true, 0, 0o755, true, false)
			entriesByName[name] = DirEntry{Name: name, Ino: entry.Ino, Mode: dirEntryMode(true, true, 0o755), IsDir: true, HasMode: true, AttrMode: 0o755, HasMetadata: true}
		}
	}
	entries := make([]DirEntry, 0, len(entriesByName))
	for _, e := range entriesByName {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, true, nil
}

func (fs *Dat9FS) readGitFile(ctx context.Context, localPath string, offset, size int64) ([]byte, error) {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || rel == "" {
		return nil, os.ErrNotExist
	}
	if e, ok := rt.overlayEntry(rel); ok {
		if e.Op == "whiteout" {
			return nil, os.ErrNotExist
		}
		if e.Kind == "dir" {
			return nil, syscall.EISDIR
		}
		if e.Op != "chmod" || len(e.Content) > 0 {
			return sliceRead(e.Content, offset, size), nil
		}
	}
	n, ok := rt.cleanNode(rel)
	if !ok {
		return nil, os.ErrNotExist
	}
	if n.Kind == "dir" || n.Kind == "submodule" {
		return nil, syscall.EISDIR
	}
	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		return nil, err
	}
	gitDir, err := fs.localOverlay.abs(path.Join(rt.localRoot, ".git"))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "blob", n.ObjectSHA)
	data, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return sliceRead(data, offset, size), nil
}

func sliceRead(data []byte, offset, size int64) []byte {
	if offset < 0 {
		return nil
	}
	if offset >= int64(len(data)) {
		return nil
	}
	end := int64(len(data))
	if size >= 0 {
		end = offset + size
	}
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	if end < offset {
		return nil
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out
}

func (fs *Dat9FS) ensureGitStateRestored(ctx context.Context, rt *gitWorkspaceRuntime) error {
	if rt == nil || fs.localOverlay == nil {
		return nil
	}
	fs.git.mu.Lock()
	if rt.restored {
		fs.git.mu.Unlock()
		return nil
	}
	fs.git.mu.Unlock()
	gitDir, err := fs.localOverlay.abs(path.Join(rt.localRoot, ".git"))
	if err != nil {
		return err
	}
	if _, err := os.Stat(gitDir); err == nil {
		fs.git.mu.Lock()
		rt.restored = true
		fs.git.mu.Unlock()
		return nil
	}
	state, err := fs.client.GetGitState(ctx, rt.workspace.WorkspaceID)
	if err != nil {
		return err
	}
	if len(state.Content) == 0 {
		return fmt.Errorf("git workspace %s has no .git checkpoint", rt.workspace.WorkspaceID)
	}
	if err := extractGitArchive(state.Content, gitDir); err != nil {
		return err
	}
	fs.git.mu.Lock()
	rt.restored = true
	fs.git.mu.Unlock()
	return nil
}

func (fs *Dat9FS) ensureGitStateForLocalPath(ctx context.Context, localPath string) error {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok {
		return nil
	}
	if rel == ".git" || strings.HasPrefix(rel, ".git/") {
		return fs.ensureGitStateRestored(ctx, rt)
	}
	return nil
}

func extractGitArchive(content []byte, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	gz, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if hdr.Name == "" || strings.HasPrefix(hdr.Name, "/") || strings.Contains(hdr.Name, "..") {
			return fmt.Errorf("unsafe git archive path %q", hdr.Name)
		}
		target := filepath.Join(dst, filepath.FromSlash(hdr.Name))
		if !strings.HasPrefix(target, dst+string(filepath.Separator)) && target != dst {
			return fmt.Errorf("unsafe git archive path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		}
	}
}

func (fs *Dat9FS) checkpointGitStateForPath(ctx context.Context, localPath string) error {
	if fs == nil || fs.git == nil || fs.client == nil || fs.localOverlay == nil {
		return nil
	}
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || (rel != ".git" && !strings.HasPrefix(rel, ".git/")) {
		return nil
	}
	gitDir, err := fs.localOverlay.abs(path.Join(rt.localRoot, ".git"))
	if err != nil {
		return err
	}
	if _, err := os.Stat(gitDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	fs.git.checkpoint.Lock()
	defer fs.git.checkpoint.Unlock()

	content, err := archiveLocalGitDir(gitDir)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(content)
	_, err = fs.client.UpsertGitState(ctx, rt.workspace.WorkspaceID, client.GitStateRequest{
		CheckpointCommit: rt.workspace.HeadCommit,
		StorageType:      "tar.gz",
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(content)),
		Content:          content,
	})
	return err
}

func archiveLocalGitDir(gitDir string) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	err := filepath.WalkDir(gitDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == gitDir {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(gitDir, p)
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(p)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (fs *Dat9FS) putGitOverlay(ctx context.Context, workspaceID string, req client.GitOverlayEntryRequest) (*client.GitOverlayEntry, error) {
	if req.Op == "" {
		req.Op = "upsert"
	}
	if req.Kind == "" {
		req.Kind = "file"
	}
	if req.SizeBytes == 0 && len(req.Content) > 0 {
		req.SizeBytes = int64(len(req.Content))
	}
	entry, err := fs.client.PutGitOverlayEntry(ctx, workspaceID, req)
	if err != nil {
		return nil, err
	}
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	for _, rt := range fs.git.workspaces {
		if rt.workspace.WorkspaceID == workspaceID {
			rt.overlay[entry.Path] = *entry
			break
		}
	}
	return entry, nil
}

func (fs *Dat9FS) flushGitHandleLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fh == nil || fh.Layer != PathLayerGitWorkspace {
		return gofuse.OK
	}
	if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		if !fh.HasPendingMode {
			return gofuse.OK
		}
		req := client.GitOverlayEntryRequest{
			Path:          fh.GitRelPath,
			Op:            "chmod",
			Kind:          fh.GitKind,
			Mode:          gitModeForHandle(fh),
			BaseObjectSHA: fh.GitBaseObjectSHA,
			SizeBytes:     fh.OrigSize,
		}
		fh.Unlock()
		_, err := fs.putGitOverlay(ctx, fh.GitWorkspaceID, req)
		fh.Lock()
		if err != nil {
			return httpToFuseStatus(err)
		}
		fs.inodes.UpdateMode(fh.Ino, fh.PendingMode&0o777)
		fh.GitMode = req.Mode
		clearPendingModeLocked(fh)
		return gofuse.OK
	}
	dataView := fh.Dirty.bytesView()
	data := make([]byte, len(dataView))
	copy(data, dataView)
	req := client.GitOverlayEntryRequest{
		Path:          fh.GitRelPath,
		Op:            "upsert",
		Kind:          fh.GitKind,
		Mode:          gitModeForHandle(fh),
		BaseObjectSHA: fh.GitBaseObjectSHA,
		Content:       data,
		SizeBytes:     int64(len(data)),
	}
	fh.Unlock()
	_, err := fs.putGitOverlay(ctx, fh.GitWorkspaceID, req)
	fh.Lock()
	if err != nil {
		return httpToFuseStatus(err)
	}
	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	fh.IsNew = false
	if fh.HasPendingMode {
		fs.inodes.UpdateMode(fh.Ino, fh.PendingMode&0o777)
		fh.GitMode = req.Mode
		clearPendingModeLocked(fh)
	}
	fs.inodes.UpdateSize(fh.Ino, int64(len(data)))
	fs.inodes.UpdateMtime(fh.Ino, time.Now())
	fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
	return gofuse.OK
}

func gitModeForHandle(fh *FileHandle) string {
	if fh == nil {
		return "100644"
	}
	if fh.GitKind == "symlink" {
		return "120000"
	}
	if fh.HasPendingMode {
		entryMode := fh.PendingMode & 0o777
		if entryMode&0o111 != 0 {
			return "100755"
		}
		return "100644"
	}
	if fh.GitMode != "" {
		return fh.GitMode
	}
	return "100644"
}

func (fs *Dat9FS) putGitWhiteout(ctx context.Context, localPath string) gofuse.Status {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || rel == "" {
		return gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path: rel,
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		return httpToFuseStatus(err)
	}
	fs.inodes.Remove(localPath)
	fs.invalidateReadCacheAndTargets(localPath)
	fs.dirCache.Invalidate(parentDir(localPath))
	fs.cacheNegativePath(localPath)
	return gofuse.OK
}

func (fs *Dat9FS) putGitDirectory(ctx context.Context, localPath string, mode uint32) (*InodeEntry, gofuse.Status) {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || rel == "" {
		return nil, gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:      rel,
		Op:        "upsert",
		Kind:      "dir",
		Mode:      gitFileModeString(mode, true),
		SizeBytes: 0,
	}); err != nil {
		return nil, httpToFuseStatus(err)
	}
	entry := fs.gitInode(localPath, true, 0, mode&0o777, true, true)
	fs.dirCache.Invalidate(parentDir(localPath))
	return entry, gofuse.OK
}

func (fs *Dat9FS) putGitSymlink(ctx context.Context, localPath, target string) (*InodeEntry, gofuse.Status) {
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || rel == "" {
		return nil, gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:      rel,
		Op:        "symlink",
		Kind:      "symlink",
		Mode:      "120000",
		Content:   []byte(target),
		SizeBytes: int64(len(target)),
	}); err != nil {
		return nil, httpToFuseStatus(err)
	}
	entry := fs.gitInode(localPath, false, int64(len(target)), symlinkMode(), true, true)
	fs.dirCache.Invalidate(parentDir(localPath))
	return entry, gofuse.OK
}

func (fs *Dat9FS) gitCreateHandle(ctx context.Context, localPath string, flags uint32, pid uint32, mode uint32, hasMode bool) (*FileHandle, *InodeEntry, gofuse.Status) {
	_ = ctx
	rt, rel, ok := fs.gitWorkspaceForPath(localPath)
	if !ok || rel == "" {
		return nil, nil, gofuse.ENOENT
	}
	if existing, handled := fs.gitEntry(localPath, false); handled && existing != nil && flags&uint32(syscall.O_EXCL) != 0 {
		return nil, nil, gofuse.Status(syscall.EEXIST)
	}
	ino := fs.inodes.Lookup(localPath, false, 0, time.Now())
	if hasMode {
		fs.inodes.UpdateMode(ino, mode)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return nil, nil, gofuse.EIO
	}
	wb := fs.newWriteBuffer(localPath, maxPreloadSize, 0)
	wb.touched = true
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	fh := &FileHandle{
		Ino:            ino,
		Path:           localPath,
		Layer:          PathLayerGitWorkspace,
		Flags:          flags,
		OpenPID:        pid,
		Dirty:          wb,
		IsNew:          true,
		WritePolicy:    fs.writePolicyForOpen(flags),
		GitWorkspaceID: rt.workspace.WorkspaceID,
		GitRelPath:     rel,
		GitKind:        "file",
		GitMode:        gitFileModeString(mode, false),
	}
	if hasMode {
		fs.setPendingModeLocked(fh, mode, 0)
	}
	fh.DirtySeq = fs.markDirtySize(ino, 0)
	return fh, entry, gofuse.OK
}

func (fs *Dat9FS) prepareGitOpenHandle(ctx context.Context, fh *FileHandle, flags uint32) gofuse.Status {
	rt, rel, ok := fs.gitWorkspaceForPath(fh.Path)
	if !ok || rel == "" {
		return gofuse.ENOENT
	}
	fh.Layer = PathLayerGitWorkspace
	fh.GitWorkspaceID = rt.workspace.WorkspaceID
	fh.GitRelPath = rel
	fh.GitKind = "file"
	if e, ok := rt.overlayEntry(rel); ok {
		fh.GitKind = e.Kind
		fh.GitMode = e.Mode
		fh.GitBaseObjectSHA = e.BaseObjectSHA
	} else if n, ok := rt.cleanNode(rel); ok {
		fh.GitKind = n.Kind
		fh.GitMode = n.Mode
		fh.GitBaseObjectSHA = n.ObjectSHA
	}
	if fh.GitKind == "dir" || fh.GitKind == "submodule" {
		return gofuse.Status(syscall.EISDIR)
	}
	accMode := flags & syscall.O_ACCMODE
	if accMode != syscall.O_WRONLY && accMode != syscall.O_RDWR {
		return gofuse.OK
	}

	if flags&syscall.O_TRUNC != 0 {
		fh.Dirty = fs.newWriteBuffer(fh.Path, maxPreloadSize, 0)
		_ = fh.Dirty.Truncate(0)
		fh.ZeroBase = true
		fh.DirtySeq = fs.markDirtySize(fh.Ino, 0)
		fs.inodes.UpdateSize(fh.Ino, 0)
		return gofuse.OK
	}

	size := fh.OrigSize
	data, err := fs.readGitFile(ctx, fh.Path, 0, size)
	if err != nil {
		return gitReadErrToFuseStatus(err)
	}
	if size < 0 {
		size = int64(len(data))
		fh.OrigSize = size
		fs.inodes.UpdateSize(fh.Ino, size)
	}
	bufMax := size * 2
	if bufMax < maxPreloadSize {
		bufMax = maxPreloadSize
	}
	fh.Dirty = fs.newWriteBuffer(fh.Path, bufMax, 0)
	if len(data) > 0 {
		if _, err := fh.Dirty.Write(0, data); err != nil {
			return gofuse.Status(syscall.EFBIG)
		}
	} else {
		_ = fh.Dirty.Truncate(size)
	}
	fh.Dirty.ClearDirty()
	return gofuse.OK
}

func gitFileModeString(mode uint32, isDir bool) string {
	if isDir {
		return "040000"
	}
	if mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		return "120000"
	}
	if mode&0o111 != 0 {
		return "100755"
	}
	return "100644"
}

func gitReadErrToFuseStatus(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if errors.Is(err, os.ErrNotExist) {
		return gofuse.ENOENT
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return gofuse.Status(errno)
	}
	return httpToFuseStatus(err)
}

func (fs *Dat9FS) setGitAttr(ctx context.Context, input *gofuse.SetAttrIn, entry *InodeEntry, out *gofuse.AttrOut) gofuse.Status {
	if entry == nil {
		return gofuse.ENOENT
	}
	rt, rel, ok := fs.gitWorkspaceForPath(entry.Path)
	if !ok || rel == "" {
		return gofuse.Status(syscall.ENOTSUP)
	}
	if input.Valid&gofuse.FATTR_MODE != 0 {
		mode := input.Mode & 0o777
		entryMode := mode
		if entryIsSymlink(entry) {
			entryMode |= uint32(syscall.S_IFLNK)
		}
		hasDirtyHandle := false
		modeGen := fs.nextPendingModeGen()
		for _, h := range fs.fileHandlesForInode(input.NodeId) {
			h.Lock()
			if h.Layer == PathLayerGitWorkspace && h.Dirty != nil {
				hasDirtyHandle = true
				fs.setPendingModeLocked(h, mode, modeGen)
				if !h.HasPreviousMode {
					if entry.HasMode {
						h.PreviousMode = entry.Mode
					}
					h.HasPreviousMode = true
					h.PreviousModeKnown = entry.HasMode
				}
			}
			h.Unlock()
		}
		if !hasDirtyHandle {
			req := client.GitOverlayEntryRequest{
				Path:          rel,
				Op:            "chmod",
				Kind:          "file",
				Mode:          gitFileModeString(entryMode, entry.IsDir),
				BaseObjectSHA: gitBaseObjectSHA(rt, rel),
			}
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, req); err != nil {
				return httpToFuseStatus(err)
			}
		}
		entry.Mode = entryMode
		entry.HasMode = true
		fs.inodes.UpdateMode(input.NodeId, entryMode)
	}
	if mtime, ok := input.GetMTime(); ok {
		entry.Mtime = mtime
		fs.inodes.UpdateMtime(input.NodeId, mtime)
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		newSize := int64(input.Size)
		if entry.IsDir {
			return gofuse.Status(syscall.EISDIR)
		}
		if input.Valid&gofuse.FATTR_FH != 0 {
			if fh, ok := fs.fileHandles.Get(input.Fh); ok && fh.Layer == PathLayerGitWorkspace && fh.Dirty != nil {
				fh.Lock()
				if err := fs.truncateWritableHandleLocked(fh, newSize); err != nil {
					fh.Unlock()
					return gofuse.Status(syscall.EFBIG)
				}
				fh.Unlock()
			}
		} else if newSize != entry.Size {
			readSize := entry.Size
			data, err := fs.readGitFile(ctx, entry.Path, 0, readSize)
			if err != nil {
				return gitReadErrToFuseStatus(err)
			}
			if readSize < 0 {
				entry.Size = int64(len(data))
				fs.inodes.UpdateSize(input.NodeId, entry.Size)
			}
			switch {
			case newSize < int64(len(data)):
				data = data[:newSize]
			case newSize > int64(len(data)):
				grown := make([]byte, newSize)
				copy(grown, data)
				data = grown
			}
			req := client.GitOverlayEntryRequest{
				Path:          rel,
				Op:            "upsert",
				Kind:          "file",
				Mode:          gitFileModeString(entry.Mode, false),
				BaseObjectSHA: gitBaseObjectSHA(rt, rel),
				Content:       data,
				SizeBytes:     int64(len(data)),
			}
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, req); err != nil {
				return httpToFuseStatus(err)
			}
		}
		entry.Size = newSize
		fs.inodes.UpdateSize(input.NodeId, newSize)
	}
	if refreshed, handled := fs.gitEntry(entry.Path, false); handled && refreshed != nil {
		entry = refreshed
	}
	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

func gitBaseObjectSHA(rt *gitWorkspaceRuntime, rel string) string {
	if rt == nil {
		return ""
	}
	if e, ok := rt.overlayEntry(rel); ok && e.BaseObjectSHA != "" {
		return e.BaseObjectSHA
	}
	if n, ok := rt.cleanNode(rel); ok {
		return n.ObjectSHA
	}
	return ""
}

func (fs *Dat9FS) renameGitPath(ctx context.Context, input *gofuse.RenameIn, oldP, newP string) (bool, gofuse.Status) {
	oldRT, oldRel, oldOK := fs.gitWorkspaceForPath(oldP)
	newRT, newRel, newOK := fs.gitWorkspaceForPath(newP)
	if !oldOK && !newOK {
		return false, gofuse.OK
	}
	if !oldOK || !newOK || oldRT.workspace.WorkspaceID != newRT.workspace.WorkspaceID || oldRel == "" || newRel == "" {
		return true, gofuse.Status(syscall.EXDEV)
	}
	oldEntry, handled := fs.gitEntry(oldP, false)
	if !handled || oldEntry == nil {
		return true, gofuse.ENOENT
	}
	if oldEntry.IsDir {
		if st := fs.renameGitDir(ctx, oldRT, oldRel, newRel); st != gofuse.OK {
			return true, st
		}
	} else {
		if st := fs.copyGitFileOverlay(ctx, oldRT, oldRel, newRel); st != gofuse.OK {
			return true, st
		}
	}
	if _, err := fs.putGitOverlay(ctx, oldRT.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path: oldRel,
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		return true, httpToFuseStatus(err)
	}
	fs.finishLocalRename(input, oldP, newP)
	fs.dirCache.InvalidatePrefix(oldP)
	fs.dirCache.InvalidatePrefix(newP)
	return true, gofuse.OK
}

func (fs *Dat9FS) renameGitDir(ctx context.Context, rt *gitWorkspaceRuntime, oldRel, newRel string) gofuse.Status {
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path: newRel,
		Op:   "upsert",
		Kind: "dir",
		Mode: "040000",
	}); err != nil {
		return httpToFuseStatus(err)
	}
	prefix := oldRel + "/"
	seen := map[string]struct{}{oldRel: {}}
	for rel, n := range rt.nodes {
		if rel == oldRel || !strings.HasPrefix(rel, prefix) {
			continue
		}
		seen[rel] = struct{}{}
		targetRel := newRel + strings.TrimPrefix(rel, oldRel)
		if n.Kind == "dir" {
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
				Path: targetRel,
				Op:   "upsert",
				Kind: "dir",
				Mode: "040000",
			}); err != nil {
				return httpToFuseStatus(err)
			}
			continue
		}
		if st := fs.copyGitFileOverlay(ctx, rt, rel, targetRel); st != gofuse.OK {
			return st
		}
	}
	for rel, e := range rt.overlay {
		if rel == oldRel || !strings.HasPrefix(rel, prefix) {
			continue
		}
		if _, ok := seen[rel]; ok {
			continue
		}
		targetRel := newRel + strings.TrimPrefix(rel, oldRel)
		req := client.GitOverlayEntryRequest{
			Path:           targetRel,
			Op:             e.Op,
			Kind:           e.Kind,
			Mode:           e.Mode,
			StorageType:    e.StorageType,
			StorageRef:     e.StorageRef,
			StorageRefHash: e.StorageRefHash,
			ChecksumSHA256: e.ChecksumSHA256,
			SizeBytes:      e.SizeBytes,
			BaseObjectSHA:  e.BaseObjectSHA,
			Content:        e.Content,
		}
		if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, req); err != nil {
			return httpToFuseStatus(err)
		}
	}
	for rel := range seen {
		if rel == oldRel {
			continue
		}
		if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
			Path: rel,
			Op:   "whiteout",
			Kind: "file",
		}); err != nil {
			return httpToFuseStatus(err)
		}
	}
	for rel := range rt.overlay {
		if rel == oldRel || strings.HasPrefix(rel, prefix) {
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
				Path: rel,
				Op:   "whiteout",
				Kind: "file",
			}); err != nil {
				return httpToFuseStatus(err)
			}
		}
	}
	return gofuse.OK
}

func (fs *Dat9FS) copyGitFileOverlay(ctx context.Context, rt *gitWorkspaceRuntime, oldRel, newRel string) gofuse.Status {
	oldLocal := path.Join(rt.localRoot, oldRel)
	if rt.localRoot == "/" {
		oldLocal = "/" + oldRel
	}
	size := int64(0)
	mode := "100644"
	kind := "file"
	base := gitBaseObjectSHA(rt, oldRel)
	if e, ok := rt.overlayEntry(oldRel); ok {
		size = e.SizeBytes
		mode = e.Mode
		kind = e.Kind
		if e.BaseObjectSHA != "" {
			base = e.BaseObjectSHA
		}
	} else if n, ok := rt.cleanNode(oldRel); ok {
		size = n.SizeBytes
		mode = n.Mode
		kind = n.Kind
	}
	data, err := fs.readGitFile(ctx, oldLocal, 0, size)
	if err != nil {
		return gitReadErrToFuseStatus(err)
	}
	if kind == "" {
		kind = "file"
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:          newRel,
		Op:            "upsert",
		Kind:          kind,
		Mode:          mode,
		BaseObjectSHA: base,
		Content:       data,
		SizeBytes:     int64(len(data)),
	}); err != nil {
		return httpToFuseStatus(err)
	}
	return gofuse.OK
}
