package objectfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
)

// FS is an object-store filesystem rooted at a bucket.
type FS struct {
	f   fs.Fs
	loc Location
}

// Open builds a backend for loc. The rclone Fs is always the bucket root;
// loc.Path is the object key.
func Open(ctx context.Context, loc Location) (*FS, error) {
	f, err := OpenFsBucket(ctx, loc)
	if err != nil {
		return nil, err
	}
	return &FS{f: f, loc: loc}, nil
}

func (a *FS) Scheme() Scheme { return a.loc.Scheme }
func (a *FS) Close() error   { return nil }

// Identity is scheme+account+bucket+endpoint+region so two keys in the
// same bucket (or Azure container+account) share a server-side copy identity.
func (a *FS) Identity() string {
	endpoint, region, account := "", "", ""
	if a.loc.Query != nil {
		endpoint = a.loc.Query[QueryEndpoint]
		region = a.loc.Query[QueryRegion]
		account = a.loc.Query[QueryAccount]
	}
	return string(CanonicalScheme(a.loc.Scheme)) + "\n" + account + "\n" + a.loc.Bucket + "\n" + endpoint + "\n" + region
}

func (a *FS) Fs() fs.Fs { return a.f }

func (a *FS) remoteOf(loc Location) string {
	return strings.Trim(loc.Path, "/")
}

func (a *FS) Stat(ctx context.Context, loc Location) (*FileInfo, error) {
	remote := a.remoteOf(loc)
	if remote != "" && !loc.DirHint && !strings.HasSuffix(loc.Path, "/") {
		obj, err := a.f.NewObject(ctx, remote)
		if err == nil {
			return fileInfoFromObj(obj), nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorNotAFile) {
			return nil, fmt.Errorf("stat %s: %w", loc.Raw, err)
		}
	}
	entries, err := a.f.List(ctx, remote)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) || errors.Is(err, fs.ErrorIsFile) {
			return nil, fmt.Errorf("stat %s: %w", loc.Raw, ErrNotFound)
		}
		return nil, fmt.Errorf("stat %s: %w", loc.Raw, err)
	}
	// S3 List of a missing prefix is empty + nil. Do not treat that as a
	// directory or `cp dest` will write dest/basename instead of dest.
	if len(entries) == 0 && !loc.DirHint && remote != "" && !strings.HasSuffix(loc.Path, "/") {
		if dir := parentDirEntry(ctx, a.f, remote); dir {
			name := path.Base(remote)
			return &FileInfo{Name: name, Path: loc.Path, IsDir: true}, nil
		}
		return nil, fmt.Errorf("stat %s: %w", loc.Raw, ErrNotFound)
	}
	name := path.Base(remote)
	if name == "" || name == "." {
		name = loc.Bucket
	}
	return &FileInfo{
		Name:  name,
		Path:  loc.Path,
		IsDir: true,
	}, nil
}

// List returns one complete directory page. rclone's List already
// follows S3 continuation tokens internally; we do not expose a cursor.
func (a *FS) List(ctx context.Context, loc Location, opts ListOpts) (ListPage, error) {
	if opts.Cursor != "" {
		return ListPage{}, fmt.Errorf("list %s: cursor pagination is not supported", loc.Raw)
	}
	dir := a.remoteOf(loc)
	var entries fs.DirEntries
	var err error
	if opts.Recursive {
		err = walk.Walk(ctx, a.f, dir, true, -1, func(_ string, e fs.DirEntries, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			entries = append(entries, e...)
			return nil
		})
	} else {
		entries, err = a.f.List(ctx, dir)
	}
	if err != nil {
		// Local backends may wrap "not a directory"; object backends use
		// ErrorIsFile. Either way, a file key should list as itself.
		if obj, oerr := a.f.NewObject(ctx, dir); oerr == nil {
			return ListPage{Entries: []FileInfo{*fileInfoFromObj(obj)}}, nil
		}
		return ListPage{}, fmt.Errorf("list %s: %w", loc.Raw, err)
	}
	// S3/GCS/Azure List of a file key is empty+nil, not ErrorIsFile.
	if len(entries) == 0 && dir != "" && !loc.DirHint && !strings.HasSuffix(loc.Path, "/") {
		if obj, oerr := a.f.NewObject(ctx, dir); oerr == nil {
			return ListPage{Entries: []FileInfo{*fileInfoFromObj(obj)}}, nil
		}
	}
	out := make([]FileInfo, 0, len(entries))
	for _, e := range entries {
		out = append(out, fileInfoFromEntry(e))
	}
	return ListPage{Entries: out}, nil
}

func (a *FS) OpenRead(ctx context.Context, loc Location) (io.ReadCloser, error) {
	if err := rejectVersionID(loc); err != nil {
		return nil, err
	}
	obj, err := a.f.NewObject(ctx, a.remoteOf(loc))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, mapNotFound(err))
	}
	rc, err := obj.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, err)
	}
	return rc, nil
}

func (a *FS) OpenReadRange(ctx context.Context, loc Location, offset, length int64) (io.ReadCloser, error) {
	if err := rejectVersionID(loc); err != nil {
		return nil, err
	}
	if length <= 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	if offset < 0 || offset > (1<<63-1)-length {
		return nil, fmt.Errorf("read %s: invalid range", loc.Raw)
	}
	obj, err := a.f.NewObject(ctx, a.remoteOf(loc))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, mapNotFound(err))
	}
	rc, err := obj.Open(ctx, &fs.RangeOption{Start: offset, End: offset + length - 1})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", loc.Raw, err)
	}
	return rc, nil
}

func (a *FS) OpenWrite(ctx context.Context, loc Location, opts WriteOpts) (WriteHandle, error) {
	remote := a.remoteOf(loc)
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	size := opts.Size
	go func() {
		info := object.NewStaticObjectInfo(remote, time.Now(), size, true, nil, a.f)
		_, err := a.f.Put(ctx, pr, info)
		if err != nil {
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
		errc <- err
	}()
	return &writeHandle{pw: pw, errc: errc}, nil
}

func (a *FS) Remove(ctx context.Context, loc Location, recursive bool) error {
	if !recursive {
		obj, err := a.f.NewObject(ctx, a.remoteOf(loc))
		if err != nil {
			return fmt.Errorf("rm %s: %w", loc.Raw, mapNotFound(err))
		}
		if err := obj.Remove(ctx); err != nil {
			return fmt.Errorf("rm %s: %w", loc.Raw, err)
		}
		return nil
	}
	remote := a.remoteOf(loc)
	if !loc.DirHint && !strings.HasSuffix(loc.Path, "/") {
		obj, err := a.f.NewObject(ctx, remote)
		if err == nil {
			if err := obj.Remove(ctx); err != nil {
				return fmt.Errorf("rm %s: %w", loc.Raw, err)
			}
			return nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorNotAFile) && !errors.Is(err, fs.ErrorIsDir) {
			return fmt.Errorf("rm %s: %w", loc.Raw, mapNotFound(err))
		}
	}
	if err := operations.Purge(ctx, a.f, remote); err != nil {
		return fmt.Errorf("rm %s: %w", loc.Raw, mapNotFound(err))
	}
	return nil
}

func (a *FS) Mkdir(ctx context.Context, loc Location) error {
	dir := a.remoteOf(loc)
	if err := a.f.Mkdir(ctx, dir); err != nil {
		return fmt.Errorf("mkdir %s: %w", loc.Raw, err)
	}
	return nil
}

func (a *FS) Copy(ctx context.Context, src, dst Location) error {
	srcObj, err := a.f.NewObject(ctx, a.remoteOf(src))
	if err != nil {
		return fmt.Errorf("copy %s: %w", src.Raw, mapNotFound(err))
	}
	_, err = operations.Copy(ctx, a.f, nil, a.remoteOf(dst), srcObj)
	if err != nil {
		return fmt.Errorf("copy %s: %w", src.Raw, err)
	}
	return nil
}

func (a *FS) Rename(ctx context.Context, old, new Location) error {
	if strings.HasSuffix(old.Path, "/") || old.DirHint {
		return fmt.Errorf("mv: directory rename is not supported on object stores")
	}
	if a.remoteOf(old) == a.remoteOf(new) {
		return nil
	}
	if err := a.Copy(ctx, old, new); err != nil {
		return err
	}
	return a.Remove(ctx, old, false)
}

func parentDirEntry(ctx context.Context, f fs.Fs, remote string) bool {
	parent := path.Dir(remote)
	if parent == "." {
		parent = ""
	}
	ents, err := f.List(ctx, parent)
	if err != nil {
		return false
	}
	base := path.Base(remote)
	for _, e := range ents {
		if _, ok := e.(fs.Directory); !ok {
			continue
		}
		name := path.Base(strings.TrimSuffix(e.Remote(), "/"))
		if name == base {
			return true
		}
	}
	return false
}

func rejectVersionID(loc Location) error {
	if loc.Query[QueryVersionID] != "" {
		return fmt.Errorf("versionId is not supported on this backend: %s", loc.Raw)
	}
	return nil
}

func fileInfoFromObj(o fs.Object) *FileInfo {
	return &FileInfo{
		Name:  path.Base(o.Remote()),
		Path:  o.Remote(),
		Size:  o.Size(),
		Mtime: o.ModTime(context.Background()),
	}
}

func fileInfoFromEntry(e fs.DirEntry) FileInfo {
	info := FileInfo{
		Name:  path.Base(e.Remote()),
		Path:  e.Remote(),
		Size:  e.Size(),
		Mtime: e.ModTime(context.Background()),
	}
	if _, ok := e.(fs.Directory); ok {
		info.IsDir = true
	}
	return info
}

func mapNotFound(err error) error {
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) {
		return ErrNotFound
	}
	return err
}

type writeHandle struct {
	pw        *io.PipeWriter
	errc      chan error
	closeOnce sync.Once
	closeErr  error
}

func (w *writeHandle) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *writeHandle) finish(writeErr error) error {
	w.closeOnce.Do(func() {
		if writeErr != nil {
			_ = w.pw.CloseWithError(writeErr)
		} else {
			_ = w.pw.Close()
		}
		w.closeErr = <-w.errc
	})
	return w.closeErr
}

func (w *writeHandle) Close() error {
	return w.finish(nil)
}

func (w *writeHandle) Abort(context.Context) error {
	_ = w.finish(context.Canceled)
	return nil
}
