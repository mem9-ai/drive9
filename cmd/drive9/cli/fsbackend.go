package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/objectfs"
)

type fsHandle struct {
	Loc     Location
	Backend Backend
	Client  *client.Client
	Path    string
}

// fsHandleForArg is the chokepoint for ls/cat/rm/mkdir/stat/find/grep/chmod
// and the symlink link path. Bare /path is promoted to the current drive9
// context. KindObject never becomes a raw string on pkg/client.
func fsHandleForArg(defaultClient *client.Client, raw string) (fsHandle, error) {
	return fsHandleForArgOpts(defaultClient, raw, false)
}

func fsHandleForArgOpts(defaultClient *client.Client, raw string, forCopy bool) (fsHandle, error) {
	loc, err := Parse(raw)
	if err != nil {
		return fsHandle{}, err
	}
	if !forCopy {
		loc = promoteBareFSArg(loc)
	}
	switch loc.Kind {
	case KindStdin:
		if !forCopy {
			return fsHandle{}, fmt.Errorf("%q is only valid as an fs cp source or destination", raw)
		}
		return fsHandle{Loc: loc, Path: "-"}, nil
	case KindObject:
		b, err := openObject(context.Background(), loc)
		if err != nil {
			return fsHandle{}, err
		}
		return fsHandle{Loc: loc, Backend: b, Path: loc.Path}, nil
	case KindDrive9:
		c := defaultClient
		if loc.Context != "" {
			c, err = newFSClientForContext(loc.Context)
			if err != nil {
				return fsHandle{}, err
			}
		}
		return fsHandle{
			Loc:     loc,
			Backend: newDrive9FS(c),
			Client:  c,
			Path:    loc.Path,
		}, nil
	default: // KindLocal after promote: leftover relative / C:/ → current Client + raw
		if forCopy {
			return fsHandle{Loc: loc, Backend: newLocalFS(), Path: loc.Local}, nil
		}
		loc.Path = raw
		return fsHandle{Loc: loc, Client: defaultClient, Backend: newDrive9FS(defaultClient), Path: raw}, nil
	}
}

func requireCapOnHandle(h fsHandle, cap Capability, verb string) error {
	if h.Loc.Kind == KindObject {
		return RequireCap(h.Backend, cap, verb)
	}
	return nil
}

// drive9FS wraps a drive9 HTTP client. Errors from the client are returned
// with %w so client.IsNotFound / StatusError continue to work.
type drive9FS struct {
	c        *client.Client
	identity string
}

func newDrive9FS(c *client.Client) *drive9FS {
	id := ""
	if c != nil {
		id = "drive9\n" + c.BaseURL() + "\n" + c.APIKey()
	}
	return &drive9FS{c: c, identity: id}
}

func (a *drive9FS) Scheme() Scheme { return SchemeDrive9 }

func (a *drive9FS) Caps() Capability {
	return CapStat | CapList | CapRead | CapRangeRead |
		CapWrite | CapDelete | CapMkdir | CapRenameSame |
		CapCopySame | CapAppend | CapChmod | CapSymlink |
		CapHardlink | CapTags | CapDescription | CapSearch |
		CapLayer | CapMultipart
}

func (a *drive9FS) Close() error     { return nil }
func (a *drive9FS) Identity() string { return a.identity }
func (a *drive9FS) Client() *client.Client {
	return a.c
}

func (a *drive9FS) Stat(ctx context.Context, loc Location) (*FileInfo, error) {
	st, err := a.c.StatCtx(ctx, loc.Path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", loc.Raw, err)
	}
	name := path.Base(strings.TrimSuffix(loc.Path, "/"))
	if name == "" || name == "." {
		name = "/"
	}
	return &FileInfo{
		Name:    name,
		Path:    loc.Path,
		Size:    st.Size,
		IsDir:   st.IsDir,
		Mtime:   st.Mtime,
		Mode:    st.Mode,
		HasMode: st.HasMode,
	}, nil
}

func (a *drive9FS) List(ctx context.Context, loc Location, opts ListOpts) (ListPage, error) {
	if opts.Cursor != "" {
		return ListPage{}, fmt.Errorf("list %s: cursor pagination is not supported", loc.Raw)
	}
	if opts.Recursive {
		var out []FileInfo
		err := walkRemoteTreeBFS(ctx, a.c, loc.Path, func(rel string, e client.FileInfo) error {
			out = append(out, drive9EntryInfo(loc.Path, rel, e))
			return nil
		})
		if err != nil {
			return ListPage{}, fmt.Errorf("list %s: %w", loc.Raw, err)
		}
		return ListPage{Entries: out}, nil
	}
	entries, err := a.c.ListCtx(ctx, loc.Path)
	if err != nil {
		return ListPage{}, fmt.Errorf("list %s: %w", loc.Raw, err)
	}
	out := make([]FileInfo, 0, len(entries))
	for _, e := range entries {
		out = append(out, drive9EntryInfo(loc.Path, e.Name, e))
	}
	return ListPage{Entries: out}, nil
}

func drive9EntryInfo(root, rel string, e client.FileInfo) FileInfo {
	info := FileInfo{
		Name:    e.Name,
		Path:    path.Join(strings.TrimSuffix(root, "/"), rel),
		Size:    e.Size,
		IsDir:   e.IsDir,
		Mode:    e.Mode,
		HasMode: e.HasMode,
	}
	if e.Mtime != 0 {
		info.Mtime = time.Unix(e.Mtime, 0)
	}
	if e.IsDir && !strings.HasSuffix(info.Path, "/") {
		info.Path += "/"
	}
	return info
}

func (a *drive9FS) OpenRead(ctx context.Context, loc Location) (io.ReadCloser, error) {
	rc, err := a.c.ReadStream(ctx, loc.Path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, err)
	}
	return rc, nil
}

func (a *drive9FS) OpenReadRange(ctx context.Context, loc Location, offset, length int64) (io.ReadCloser, error) {
	rc, err := a.c.ReadStreamRange(ctx, loc.Path, offset, length)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, err)
	}
	return rc, nil
}

func (a *drive9FS) OpenWrite(ctx context.Context, loc Location, opts WriteOpts) (WriteHandle, error) {
	// WriteStreamWithSummary needs a seekable ReaderAt for large files, so
	// spool to a tempfile instead of buffering the whole object in RAM.
	f, err := os.CreateTemp("", "drive9-cp-*")
	if err != nil {
		return nil, fmt.Errorf("write %s: %w", loc.Raw, err)
	}
	return &drive9Write{ctx: ctx, a: a, loc: loc, opts: opts, f: f}, nil
}

func (a *drive9FS) Remove(ctx context.Context, loc Location, recursive bool) error {
	if recursive {
		if err := a.c.RemoveAllCtx(ctx, loc.Path); err != nil {
			return fmt.Errorf("rm %s: %w", loc.Raw, err)
		}
		return nil
	}
	if err := a.c.DeleteCtx(ctx, loc.Path); err != nil {
		return fmt.Errorf("rm %s: %w", loc.Raw, err)
	}
	return nil
}

func (a *drive9FS) Mkdir(ctx context.Context, loc Location) error {
	if err := a.c.Mkdir(loc.Path); err != nil {
		return fmt.Errorf("mkdir %s: %w", loc.Raw, err)
	}
	return nil
}

func (a *drive9FS) Rename(ctx context.Context, old, new Location) error {
	if err := a.c.RenameCtx(ctx, old.Path, new.Path); err != nil {
		return fmt.Errorf("mv %s: %w", old.Raw, err)
	}
	return nil
}

func (a *drive9FS) Copy(ctx context.Context, src, dst Location) error {
	if err := a.c.CopyCtx(ctx, src.Path, dst.Path); err != nil {
		return fmt.Errorf("cp %s: %w", src.Raw, err)
	}
	return nil
}

type drive9Write struct {
	ctx  context.Context
	a    *drive9FS
	loc  Location
	opts WriteOpts
	f    *os.File
}

func (w *drive9Write) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

func (w *drive9Write) Close() error {
	name := w.f.Name()
	defer func() {
		_ = w.f.Close()
		_ = os.Remove(name)
	}()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("write %s: %w", w.loc.Raw, err)
	}
	fi, err := w.f.Stat()
	if err != nil {
		return fmt.Errorf("write %s: %w", w.loc.Raw, err)
	}
	w.a.c.Warm(w.ctx)
	var opts []client.WriteOption
	if w.opts.Tags != nil {
		opts = append(opts, client.WithTags(w.opts.Tags))
	}
	if w.opts.Description != "" {
		opts = append(opts, client.WithDescription(w.opts.Description))
	}
	_, err = w.a.c.WriteStreamWithSummary(w.ctx, w.loc.Path, w.f, fi.Size(), nil, opts...)
	if err != nil {
		return fmt.Errorf("write %s: %w", w.loc.Raw, err)
	}
	return nil
}

func (w *drive9Write) Abort(context.Context) error {
	name := w.f.Name()
	_ = w.f.Close()
	return os.Remove(name)
}

// localFS reads and writes local files. Identity is process-global.
type localFS struct{}

func newLocalFS() *localFS { return &localFS{} }

func (a *localFS) Scheme() Scheme { return SchemeFile }
func (a *localFS) Caps() Capability {
	return CapStat | CapList | CapRead | CapRangeRead |
		CapWrite | CapDelete | CapMkdir
}
func (a *localFS) Close() error     { return nil }
func (a *localFS) Identity() string { return "file://localhost" }

func (a *localFS) path(loc Location) string {
	// Walk sets Path to each child while leaving Local as the walk root.
	if loc.Path != "" {
		return loc.Path
	}
	if loc.Local != "" {
		return loc.Local
	}
	return loc.Path
}

func (a *localFS) Stat(_ context.Context, loc Location) (*FileInfo, error) {
	fi, err := os.Stat(a.path(loc))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("stat %s: %w", loc.Raw, ErrNotFound)
		}
		return nil, err
	}
	return &FileInfo{
		Name:    fi.Name(),
		Path:    a.path(loc),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		Mtime:   fi.ModTime(),
		Mode:    uint32(fi.Mode().Perm()),
		HasMode: true,
	}, nil
}

func (a *localFS) List(_ context.Context, loc Location, opts ListOpts) (ListPage, error) {
	if opts.Cursor != "" {
		return ListPage{}, nil
	}
	root := a.path(loc)
	if opts.Recursive {
		var entries []FileInfo
		err := filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if p == root {
				return nil
			}
			entries = append(entries, FileInfo{
				Name:  fi.Name(),
				Path:  p,
				Size:  fi.Size(),
				IsDir: fi.IsDir(),
				Mtime: fi.ModTime(),
			})
			return nil
		})
		if err != nil {
			return ListPage{}, err
		}
		return ListPage{Entries: entries}, nil
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		return ListPage{}, err
	}
	out := make([]FileInfo, 0, len(ents))
	for _, e := range ents {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, FileInfo{
			Name:  e.Name(),
			Path:  filepath.Join(root, e.Name()),
			Size:  info.Size(),
			IsDir: e.IsDir(),
			Mtime: info.ModTime(),
		})
	}
	return ListPage{Entries: out}, nil
}

func (a *localFS) OpenRead(_ context.Context, loc Location) (io.ReadCloser, error) {
	f, err := os.Open(a.path(loc))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("read %s: %w", loc.Raw, ErrNotFound)
		}
		return nil, err
	}
	return f, nil
}

func (a *localFS) OpenReadRange(_ context.Context, loc Location, offset, length int64) (io.ReadCloser, error) {
	f, err := os.Open(a.path(loc))
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(f, length), Closer: f}, nil
}

func (a *localFS) OpenWrite(_ context.Context, loc Location, _ WriteOpts) (WriteHandle, error) {
	p := a.path(loc)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(p)
	if err != nil {
		return nil, err
	}
	return &fileWrite{f: f, path: p}, nil
}

func (a *localFS) Remove(_ context.Context, loc Location, recursive bool) error {
	p := a.path(loc)
	if recursive {
		return os.RemoveAll(p)
	}
	return os.Remove(p)
}

func (a *localFS) Mkdir(_ context.Context, loc Location) error {
	return os.MkdirAll(a.path(loc), 0o755)
}

type fileWrite struct {
	f    *os.File
	path string
}

func (w *fileWrite) Write(p []byte) (int, error) { return w.f.Write(p) }
func (w *fileWrite) Close() error                { return w.f.Close() }
func (w *fileWrite) Abort(context.Context) error {
	_ = w.f.Close()
	return os.Remove(w.path)
}

// objectBackend adapts pkg/objectfs to the CLI Backend interface.
type objectBackend struct {
	fs  *objectfs.FS
	loc Location
}

func openObject(ctx context.Context, loc Location) (*objectBackend, error) {
	inner, err := objectfs.Open(ctx, toObjectLocation(loc))
	if err != nil {
		return nil, mapObjectErr(err)
	}
	return &objectBackend{fs: inner, loc: loc}, nil
}

func toObjectLocation(loc Location) objectfs.Location {
	return objectfs.Location{
		Raw:     loc.Raw,
		Scheme:  objectfs.Scheme(loc.Scheme),
		Bucket:  loc.Bucket,
		Path:    loc.Path,
		DirHint: loc.DirHint,
		Query:   loc.Query,
	}
}

func fromObjectInfo(fi objectfs.FileInfo) FileInfo {
	return FileInfo{
		Name:  fi.Name,
		Path:  fi.Path,
		Size:  fi.Size,
		IsDir: fi.IsDir,
		Mtime: fi.Mtime,
	}
}

func (a *objectBackend) Scheme() Scheme { return a.loc.Scheme }
func (a *objectBackend) Caps() Capability {
	return CapStat | CapList | CapRead | CapRangeRead |
		CapWrite | CapDelete | CapMkdir | CapRenameSame |
		CapCopySame | CapMultipart
}
func (a *objectBackend) Close() error     { return a.fs.Close() }
func (a *objectBackend) Identity() string { return a.fs.Identity() }

func (a *objectBackend) Stat(ctx context.Context, loc Location) (*FileInfo, error) {
	fi, err := a.fs.Stat(ctx, toObjectLocation(loc))
	if err != nil {
		return nil, mapObjectErr(err)
	}
	out := fromObjectInfo(*fi)
	return &out, nil
}

func (a *objectBackend) List(ctx context.Context, loc Location, opts ListOpts) (ListPage, error) {
	page, err := a.fs.List(ctx, toObjectLocation(loc), objectfs.ListOpts{
		Recursive: opts.Recursive,
		Cursor:    opts.Cursor,
		Limit:     opts.Limit,
	})
	if err != nil {
		return ListPage{}, mapObjectErr(err)
	}
	out := make([]FileInfo, 0, len(page.Entries))
	for _, e := range page.Entries {
		out = append(out, fromObjectInfo(e))
	}
	return ListPage{Entries: out, NextCursor: page.NextCursor}, nil
}

func (a *objectBackend) OpenRead(ctx context.Context, loc Location) (io.ReadCloser, error) {
	rc, err := a.fs.OpenRead(ctx, toObjectLocation(loc))
	if err != nil {
		return nil, mapObjectErr(err)
	}
	return rc, nil
}

func (a *objectBackend) OpenReadRange(ctx context.Context, loc Location, offset, length int64) (io.ReadCloser, error) {
	rc, err := a.fs.OpenReadRange(ctx, toObjectLocation(loc), offset, length)
	if err != nil {
		return nil, mapObjectErr(err)
	}
	return rc, nil
}

func (a *objectBackend) OpenWrite(ctx context.Context, loc Location, opts WriteOpts) (WriteHandle, error) {
	wh, err := a.fs.OpenWrite(ctx, toObjectLocation(loc), objectfs.WriteOpts{
		Size:      opts.Size,
		Overwrite: opts.Overwrite,
	})
	if err != nil {
		return nil, mapObjectErr(err)
	}
	return wh, nil
}

func (a *objectBackend) Remove(ctx context.Context, loc Location, recursive bool) error {
	return mapObjectErr(a.fs.Remove(ctx, toObjectLocation(loc), recursive))
}

func (a *objectBackend) Mkdir(ctx context.Context, loc Location) error {
	return mapObjectErr(a.fs.Mkdir(ctx, toObjectLocation(loc)))
}

func (a *objectBackend) Copy(ctx context.Context, src, dst Location) error {
	return mapObjectErr(a.fs.Copy(ctx, toObjectLocation(src), toObjectLocation(dst)))
}

func (a *objectBackend) Rename(ctx context.Context, old, new Location) error {
	return mapObjectErr(a.fs.Rename(ctx, toObjectLocation(old), toObjectLocation(new)))
}
