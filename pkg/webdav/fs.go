package webdav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"golang.org/x/net/webdav"
)

// fileSystem implements webdav.FileSystem over a drive9 client.
type fileSystem struct {
	client *client.Client
}

var _ webdav.FileSystem = (*fileSystem)(nil)

func (f *fileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return f.client.MkdirCtx(ctx, normPath(name))
}

func (f *fileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	p := normPath(name)

	// For create/truncate: return a writable file handle.
	if flag&(os.O_CREATE|os.O_TRUNC) != 0 {
		return &writeFile{client: f.client, path: p, ctx: ctx}, nil
	}

	// Stat to determine if it's a directory or file.
	stat, err := f.client.StatCtx(ctx, p)
	if err != nil {
		return nil, mapError(err)
	}

	if stat.IsDir {
		entries, err := f.client.ListCtx(ctx, p)
		if err != nil {
			return nil, mapError(err)
		}
		return &dirFile{path: p, stat: stat, entries: entries}, nil
	}

	// Read entire file content. For the lightweight skills/config use case
	// this is acceptable; large file streaming can be optimized later.
	data, err := f.client.ReadCtx(ctx, p)
	if err != nil {
		return nil, mapError(err)
	}

	return &readFile{
		path:   p,
		stat:   stat,
		Reader: bytes.NewReader(data),
	}, nil
}

func (f *fileSystem) RemoveAll(ctx context.Context, name string) error {
	return f.client.RemoveAllCtx(ctx, normPath(name))
}

func (f *fileSystem) Rename(ctx context.Context, oldName, newName string) error {
	return f.client.RenameCtx(ctx, normPath(oldName), normPath(newName))
}

func (f *fileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	p := normPath(name)
	stat, err := f.client.StatCtx(ctx, p)
	if err != nil {
		return nil, mapError(err)
	}
	return &fileInfo{name: path.Base(p), stat: stat}, nil
}

// normPath normalizes a WebDAV path to a drive9 path.
// WebDAV paths are absolute (e.g. "/foo/bar"); drive9 expects the same.
func normPath(p string) string {
	p = path.Clean(p)
	if p == "." || p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// mapError converts client errors to os-level errors that webdav.Handler expects.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case 404:
			return os.ErrNotExist
		case 409:
			return os.ErrExist
		case 403:
			return os.ErrPermission
		}
	}
	return err
}

// fileInfo implements os.FileInfo for drive9 stat results.
type fileInfo struct {
	name string
	stat *client.StatResult
}

var _ os.FileInfo = (*fileInfo)(nil)

func (fi *fileInfo) Name() string      { return fi.name }
func (fi *fileInfo) Size() int64       { return fi.stat.Size }
func (fi *fileInfo) IsDir() bool       { return fi.stat.IsDir }
func (fi *fileInfo) Sys() interface{}  { return nil }
func (fi *fileInfo) ModTime() time.Time { return fi.stat.Mtime }
func (fi *fileInfo) Mode() os.FileMode {
	if fi.stat.IsDir {
		return os.ModeDir | 0o755
	}
	return 0o644
}

// dirFile is a webdav.File for directories.
type dirFile struct {
	path    string
	stat    *client.StatResult
	entries []client.FileInfo
	pos     int
}

var _ webdav.File = (*dirFile)(nil)

func (d *dirFile) Close() error                             { return nil }
func (d *dirFile) Read([]byte) (int, error)                 { return 0, fmt.Errorf("is a directory") }
func (d *dirFile) Write([]byte) (int, error)                { return 0, fmt.Errorf("is a directory") }
func (d *dirFile) Seek(int64, int) (int64, error)           { return 0, fmt.Errorf("is a directory") }

func (d *dirFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: path.Base(d.path), stat: d.stat}, nil
}

func (d *dirFile) Readdir(count int) ([]fs.FileInfo, error) {
	if d.pos >= len(d.entries) {
		if count <= 0 {
			return nil, nil
		}
		return nil, io.EOF
	}

	end := len(d.entries)
	if count > 0 && d.pos+count < end {
		end = d.pos + count
	}

	infos := make([]fs.FileInfo, 0, end-d.pos)
	for _, e := range d.entries[d.pos:end] {
		var mtime time.Time
		if e.Mtime > 0 {
			mtime = time.Unix(e.Mtime, 0)
		}
		infos = append(infos, &fileInfo{
			name: e.Name,
			stat: &client.StatResult{
				Size:  e.Size,
				IsDir: e.IsDir,
				Mtime: mtime,
			},
		})
	}
	d.pos = end

	if count > 0 && d.pos >= len(d.entries) {
		return infos, io.EOF
	}
	return infos, nil
}

// readFile is a webdav.File for reading file content.
type readFile struct {
	path string
	stat *client.StatResult
	*bytes.Reader
}

var _ webdav.File = (*readFile)(nil)

func (r *readFile) Close() error                             { return nil }
func (r *readFile) Write([]byte) (int, error)                { return 0, fmt.Errorf("read-only") }
func (r *readFile) Readdir(int) ([]fs.FileInfo, error)       { return nil, fmt.Errorf("not a directory") }
func (r *readFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: path.Base(r.path), stat: r.stat}, nil
}

// writeFile is a webdav.File that buffers writes and flushes on Close.
type writeFile struct {
	client *client.Client
	path   string
	ctx    context.Context
	buf    bytes.Buffer
}

var _ webdav.File = (*writeFile)(nil)

func (w *writeFile) Read([]byte) (int, error)           { return 0, fmt.Errorf("write-only") }
func (w *writeFile) Readdir(int) ([]fs.FileInfo, error)  { return nil, fmt.Errorf("not a directory") }
func (w *writeFile) Seek(int64, int) (int64, error)      { return 0, fmt.Errorf("not seekable") }

func (w *writeFile) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *writeFile) Close() error {
	return w.client.WriteCtx(w.ctx, w.path, w.buf.Bytes())
}

func (w *writeFile) Stat() (os.FileInfo, error) {
	return &fileInfo{
		name: path.Base(w.path),
		stat: &client.StatResult{Size: int64(w.buf.Len())},
	}, nil
}
