package client

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathfilter"
)

// ArchiveFormat selects the archive container for ArchiveDir.
type ArchiveFormat string

const (
	ArchiveFormatTarGz ArchiveFormat = "tar.gz"
	ArchiveFormatZip   ArchiveFormat = "zip"
)

// ArchiveOptions configures a ArchiveDir run.
//
// Filtering uses the same three pattern forms as pkg/fuse local-policy:
//   - **/x/**   matches any path containing the x subpath (e.g. **/node_modules/**)
//   - prefix/** matches everything under a prefix (e.g. dist/**)
//   - name      exact name or glob (e.g. *.log, go.mod)
//
// Exclude drops matching paths; include keeps ONLY matching paths when
// non-empty; override (typically populated from a profile's [remote] rules)
// restores a path that exclude would otherwise drop.
type ArchiveOptions struct {
	Format   ArchiveFormat
	Exclude  []string
	Include  []string
	Override []string
	Flat     bool
	Jobs     int
}

const archiveDefaultJobs = 16

// archiveEntry is a per-leaf work item for the parallel download/serial-write
// archive pipeline.
type archiveEntry struct {
	rel    string // relative path from the source root
	remote string // absolute remote path
	root   string // archive root name prepended to rel for in-archive names
	size   int64
	mode   uint32
}

// ArchiveDir walks the remote directory tree rooted at remoteDir and writes a
// compressed archive (tar.gz by default, zip via ArchiveOptions.Format) to w.
// The walk is client-driven (the server stays file-level): ListCtx enumerates
// directories and ReadStream fetches each leaf, streamed straight into the
// archive writer with no temp file.
//
// Filtering:
//   - Exclude drops paths matching any exclude pattern.
//   - Include, when non-empty, keeps ONLY paths matching an include pattern.
//   - Override restores a path that exclude would drop (used to honor a
//     profile's [remote] rules).
//
// Directory pruning happens at BFS time: when a directory's relative path
// matches an exclude (and no override restores it), its subtree is not
// enqueued, so excluded subtrees incur no extra ListCtx round-trips.
func (c *Client) ArchiveDir(ctx context.Context, remoteDir string, w io.Writer, opts ArchiveOptions) error {
	format := opts.Format
	if format == "" {
		format = ArchiveFormatTarGz
	}
	if format != ArchiveFormatTarGz && format != ArchiveFormatZip {
		return fmt.Errorf("unsupported archive format %q: must be tar.gz or zip", format)
	}
	if err := pathfilter.Validate(opts.Exclude, opts.Include, opts.Override); err != nil {
		return err
	}
	matcher := pathfilter.NewMatcher(opts.Include, opts.Exclude, opts.Override)

	root := normalizeArchiveRoot(remoteDir)
	archiveRoot := archiveBasename(root)
	jobs := opts.Jobs
	if jobs <= 0 {
		jobs = archiveDefaultJobs
	}

	switch format {
	case ArchiveFormatTarGz:
		return c.archiveTarGz(ctx, root, archiveRoot, w, matcher, opts.Flat, jobs)
	case ArchiveFormatZip:
		return c.archiveZip(ctx, root, archiveRoot, w, matcher, opts.Flat, jobs)
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

func (c *Client) archiveTarGz(ctx context.Context, root, archiveRoot string, w io.Writer, matcher pathfilter.Matcher, flat bool, jobs int) error {
	gw := gzip.NewWriter(w)
	defer func() { _ = gw.Close() }()
	tw := tar.NewWriter(gw)
	defer func() { _ = tw.Close() }()

	dirs, files, err := c.collectArchiveTree(ctx, root, archiveRoot, matcher, flat)
	if err != nil {
		return fmt.Errorf("walk %q: %w", root, err)
	}

	for _, d := range dirs {
		name := archiveDirName(d.root, d.rel, flat)
		if name == "" {
			continue
		}
		hdr := &tar.Header{
			Name:     name,
			Mode:     archiveTarMode(d.mode, true),
			Typeflag: tar.TypeDir,
			ModTime:  archiveModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("write dir header %q: %w", name, err)
		}
	}

	var writeMu sync.Mutex
	if err := c.archiveParallel(ctx, files, jobs, func(ctx context.Context, e archiveEntry) error {
		rc, err := c.ReadStream(ctx, e.remote)
		if err != nil {
			return fmt.Errorf("open %q: %w", e.remote, err)
		}
		defer func() { _ = rc.Close() }()
		name := archiveName(e.root, e.rel, flat)
		if name == "" {
			return nil
		}
		hdr := &tar.Header{
			Name:     name,
			Mode:     archiveTarMode(e.mode, false),
			Size:     e.size,
			Typeflag: tar.TypeReg,
			ModTime:  archiveModTime(),
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("write header %q: %w", name, err)
		}
		if _, err := io.Copy(tw, io.LimitReader(rc, e.size)); err != nil {
			return fmt.Errorf("copy body %q: %w", name, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("archive transfer: %w", err)
	}
	_ = jobs
	return nil
}

func (c *Client) archiveZip(ctx context.Context, root, archiveRoot string, w io.Writer, matcher pathfilter.Matcher, flat bool, jobs int) error {
	zw := zip.NewWriter(w)
	defer func() { _ = zw.Close() }()

	dirs, files, err := c.collectArchiveTree(ctx, root, archiveRoot, matcher, flat)
	if err != nil {
		return fmt.Errorf("walk %q: %w", root, err)
	}

	for _, d := range dirs {
		name := archiveDirName(d.root, d.rel, flat)
		if name == "" {
			continue
		}
		hdr := &zip.FileHeader{Name: name, Method: zip.Store}
		hdr.SetMode(archiveZipMode(d.mode, true) | 0o750)
		if _, err := zw.CreateHeader(hdr); err != nil {
			return fmt.Errorf("zip create dir %q: %w", name, err)
		}
	}

	var writeMu sync.Mutex
	if err := c.archiveParallel(ctx, files, jobs, func(ctx context.Context, e archiveEntry) error {
		rc, err := c.ReadStream(ctx, e.remote)
		if err != nil {
			return fmt.Errorf("open %q: %w", e.remote, err)
		}
		defer func() { _ = rc.Close() }()
		name := archiveName(e.root, e.rel, flat)
		if name == "" {
			return nil
		}
		hdr := &zip.FileHeader{Name: name, Method: zip.Deflate}
		hdr.SetMode(archiveZipMode(e.mode, false))
		writeMu.Lock()
		defer writeMu.Unlock()
		out, err := zw.CreateHeader(hdr)
		if err != nil {
			return fmt.Errorf("zip create %q: %w", name, err)
		}
		if _, err := io.Copy(out, io.LimitReader(rc, e.size)); err != nil {
			return fmt.Errorf("zip copy %q: %w", name, err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("archive transfer: %w", err)
	}
	return nil
}

// collectArchiveTree walks the remote tree once, applying the matcher to prune
// excluded directories at BFS time and collecting surviving leaf entries.
func (c *Client) collectArchiveTree(ctx context.Context, root, archiveRoot string, matcher pathfilter.Matcher, flat bool) (dirs []archiveEntry, files []archiveEntry, err error) {
	dirs = append(dirs, archiveEntry{rel: "", remote: root, root: archiveRoot, mode: 0o755})
	err = c.walkRemoteTreeBFS(ctx, root, func(rel string, info FileInfo) error {
		if rel == "" {
			return nil
		}
		if !matcher.Match(rel) {
			return nil
		}
		entry := archiveEntry{
			rel:    rel,
			remote: joinArchiveRemote(root, rel),
			root:   archiveRoot,
		}
		if info.HasMode {
			entry.mode = info.Mode
		}
		if info.IsDir {
			dirs = append(dirs, entry)
			return nil
		}
		entry.size = info.Size
		files = append(files, entry)
		return nil
	})
	return dirs, files, err
}

// walkRemoteTreeBFS walks a remote directory tree breadth-first via ListCtx.
func (c *Client) walkRemoteTreeBFS(ctx context.Context, root string, visit func(rel string, info FileInfo) error) error {
	queue := []string{""}
	for len(queue) > 0 {
		rel := queue[0]
		queue = queue[1:]
		absDir := root
		if rel != "" {
			absDir = joinArchiveRemote(root, rel)
		}
		entries, err := c.ListCtx(ctx, absDir)
		if err != nil {
			return fmt.Errorf("list %q: %w", absDir, err)
		}
		for _, e := range entries {
			childRel := e.Name
			if rel != "" {
				childRel = rel + "/" + e.Name
			}
			if err := visit(childRel, e); err != nil {
				return err
			}
			if e.IsDir {
				queue = append(queue, childRel)
			}
		}
	}
	return nil
}

// archiveParallel runs op for each entry with bounded concurrency. The first
// error is returned; sibling ops are not cancelled (mirrors parallelTransfer).
func (c *Client) archiveParallel(ctx context.Context, items []archiveEntry, jobs int, op func(context.Context, archiveEntry) error) error {
	if len(items) == 0 {
		return nil
	}
	if jobs <= 0 {
		jobs = archiveDefaultJobs
	}
	sem := make(chan struct{}, jobs)
	var wg sync.WaitGroup
	var failuresMu sync.Mutex
	var failures []error
	record := func(err error) {
		failuresMu.Lock()
		failures = append(failures, err)
		failuresMu.Unlock()
	}
launch:
	for _, item := range items {
		select {
		case <-ctx.Done():
			record(ctx.Err())
			break launch
		default:
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			record(ctx.Err())
			break launch
		}
		wg.Add(1)
		go func(e archiveEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := op(ctx, e); err != nil {
				record(err)
			}
		}(item)
	}
	wg.Wait()
	if len(failures) > 0 {
		return failures[0]
	}
	return nil
}

func normalizeArchiveRoot(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		p = "/"
	}
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		p = "/"
	}
	return p
}

func archiveBasename(p string) string {
	stripped := strings.TrimPrefix(p, "/")
	if idx := strings.LastIndex(stripped, "/"); idx >= 0 {
		stripped = stripped[idx+1:]
	}
	if stripped == "" {
		return "root"
	}
	return stripped
}

func joinArchiveRemote(root, rel string) string {
	if root == "/" {
		return "/" + rel
	}
	return root + "/" + rel
}

func archiveName(root, rel string, flat bool) string {
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return ""
	}
	if flat {
		base := path.Base(rel)
		if base == "." || base == "/" {
			return ""
		}
		return base
	}
	return root + "/" + rel
}

func archiveDirName(root, rel string, flat bool) string {
	if flat {
		return ""
	}
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return root + "/"
	}
	return root + "/" + rel + "/"
}

func archiveTarMode(mode uint32, isDir bool) int64 {
	return int64(archiveZipMode(mode, isDir))
}

func archiveZipMode(mode uint32, isDir bool) fs.FileMode {
	m := fs.FileMode(mode & 0o777)
	if m == 0 {
		if isDir {
			m = 0o755
		} else {
			m = 0o644
		}
	}
	if isDir {
		m |= fs.ModeDir
	}
	return m
}

func archiveModTime() time.Time { return time.Now() }