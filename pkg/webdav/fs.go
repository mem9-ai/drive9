package webdav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"golang.org/x/net/webdav"
)

// fileSystem implements webdav.FileSystem over a drive9 client.
type fileSystem struct {
	client     *client.Client
	remoteRoot string // normalized remote root path, default "/"
	props      *deadPropStore
	put        putConfig
}

// putConfig bounds one PUT's staging and commit.
type putConfig struct {
	// bufferBytes is how much of the body may be held in memory; the rest
	// is staged to a temporary file.
	bufferBytes int64
	// stagingDir is where the overflow is staged ("" = os.TempDir()).
	stagingDir string
	// timeout bounds the commit Close performs.
	timeout time.Duration
}

func (cfg putConfig) bufferLimit() int64 {
	if cfg.bufferBytes > 0 {
		return cfg.bufferBytes
	}
	return DefaultPutBufferBytes
}

func (cfg putConfig) commitTimeout() time.Duration {
	if cfg.timeout > 0 {
		return cfg.timeout
	}
	return DefaultPutTimeout
}

// putRequest carries what the HTTP layer knows about one PUT and the file
// layer must not guess: the declared body length and the body's own read
// outcome, so a truncated upload is never committed, and the commit result,
// which the HTTP layer turns into a truthful status (x/net/webdav answers 405
// for every write/close error).
type putRequest struct {
	expected int64 // declared body length; -1 when the request did not say
	body     *putBody
	mu       sync.Mutex
	err      error
}

// putBody observes the request body's reads. x/net/webdav copies the body and
// still calls File.Close on the write file after the copy failed, so without
// this record a connection that died mid-body would commit the bytes that did
// arrive — a truncated image published as if complete.
type putBody struct {
	rc  io.ReadCloser
	mu  sync.Mutex
	err error
}

func (b *putBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err != nil && err != io.EOF {
		b.mu.Lock()
		if b.err == nil {
			b.err = err
		}
		b.mu.Unlock()
	}
	return n, err
}

func (b *putBody) Close() error { return b.rc.Close() }

// failure reports the first read error the body produced, if any.
func (b *putBody) failure() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (p *putRequest) bodyFailure() error {
	if p == nil {
		return nil
	}
	return p.body.failure()
}

// putRequestKey is the request-context key carrying the *putRequest.
type putRequestKey struct{}

func putRequestFrom(ctx context.Context) *putRequest {
	req, _ := ctx.Value(putRequestKey{}).(*putRequest)
	return req
}

func (p *putRequest) expectedLen() int64 {
	if p == nil {
		return -1
	}
	return p.expected
}

// fail records the first failure of this PUT.
func (p *putRequest) fail(err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = err
	}
}

// failure reports the recorded commit error, if any.
func (p *putRequest) failure() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

var _ webdav.FileSystem = (*fileSystem)(nil)

// remotePath maps a local WebDAV path to the remote drive9 path.
func (f *fileSystem) remotePath(localPath string) string {
	return mountpath.ToRemote(f.remoteRoot, normPath(localPath))
}

func (f *fileSystem) deadProps() *deadPropStore {
	if f.props == nil {
		f.props = newDeadPropStore()
	}
	return f.props
}

func (f *fileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return mapError(f.client.MkdirCtx(ctx, f.remotePath(name), uint32(perm)&0o777))
}

func (f *fileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	local := normPath(name)
	remote := mountpath.ToRemote(f.remoteRoot, local)

	// For create/truncate: return a writable file handle.
	if flag&(os.O_CREATE|os.O_TRUNC) != 0 {
		if local == "/" {
			return nil, os.ErrInvalid
		}
		parentRemote := mountpath.ToRemote(f.remoteRoot, path.Dir(local))
		parent, err := f.client.StatCtx(ctx, parentRemote)
		if err != nil {
			return nil, mapError(err)
		}
		if !parent.IsDir {
			return nil, os.ErrInvalid
		}
		return &writeFile{
			client: f.client,
			path:   remote,
			props:  f.deadProps(),
			put:    f.put,
			req:    putRequestFrom(ctx),
		}, nil
	}

	// Stat to determine if it's a directory or file.
	stat, err := f.client.StatCtx(ctx, remote)
	if err != nil {
		return nil, mapError(err)
	}

	if stat.IsDir {
		entries, err := f.client.ListCtx(ctx, remote)
		if err != nil {
			return nil, mapError(err)
		}
		return &dirFile{path: local, propPath: remote, props: f.deadProps(), stat: stat, entries: entries}, nil
	}

	// Read entire file content. For the lightweight skills/config use case
	// this is acceptable; large file streaming can be optimized later.
	data, err := f.client.ReadCtx(ctx, remote)
	if err != nil {
		return nil, mapError(err)
	}

	return &readFile{
		path:     local,
		propPath: remote,
		props:    f.deadProps(),
		stat:     stat,
		Reader:   bytes.NewReader(data),
	}, nil
}

func (f *fileSystem) RemoveAll(ctx context.Context, name string) error {
	local := normPath(name)
	if local == "/" {
		return os.ErrInvalid
	}
	remote := mountpath.ToRemote(f.remoteRoot, local)
	if err := mapError(f.client.RemoveAllCtx(ctx, remote)); err != nil {
		return err
	}
	f.deadProps().deleteTree(remote)
	return nil
}

func (f *fileSystem) Rename(ctx context.Context, oldName, newName string) error {
	oldLocal, newLocal := normPath(oldName), normPath(newName)
	if oldLocal == "/" || newLocal == "/" {
		return os.ErrInvalid
	}
	oldRemote := mountpath.ToRemote(f.remoteRoot, oldLocal)
	newRemote := mountpath.ToRemote(f.remoteRoot, newLocal)
	if err := mapError(f.client.RenameCtx(ctx, oldRemote, newRemote)); err != nil {
		return err
	}
	f.deadProps().renameTree(oldRemote, newRemote)
	return nil
}

func (f *fileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	local := normPath(name)
	remote := mountpath.ToRemote(f.remoteRoot, local)
	stat, err := f.client.StatCtx(ctx, remote)
	if err != nil {
		return nil, mapError(err)
	}
	return &fileInfo{name: path.Base(local), stat: stat}, nil
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
		case 400:
			return os.ErrInvalid
		case 409:
			return os.ErrExist
		case 401, 403:
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

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.stat.Size }
func (fi *fileInfo) IsDir() bool        { return fi.stat.IsDir }
func (fi *fileInfo) Sys() interface{}   { return nil }
func (fi *fileInfo) ModTime() time.Time { return fi.stat.Mtime }
func (fi *fileInfo) Mode() os.FileMode {
	if fi.stat.IsDir {
		return os.ModeDir | 0o755
	}
	return 0o644
}

type deadPropStore struct {
	mu    sync.Mutex
	props map[string]map[xml.Name]webdav.Property
}

func newDeadPropStore() *deadPropStore {
	return &deadPropStore{props: make(map[string]map[xml.Name]webdav.Property)}
}

func (s *deadPropStore) deadProps(name string) (map[xml.Name]webdav.Property, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.props[name]
	if len(current) == 0 {
		return nil, nil
	}
	out := make(map[xml.Name]webdav.Property, len(current))
	for k, v := range current {
		out[k] = v
	}
	return out, nil
}

func (s *deadPropStore) patch(name string, patches []webdav.Proppatch) ([]webdav.Propstat, error) {
	if s == nil {
		pstat := webdav.Propstat{Status: http.StatusOK}
		for _, patch := range patches {
			for _, p := range patch.Props {
				pstat.Props = append(pstat.Props, webdav.Property{XMLName: p.XMLName})
			}
		}
		return []webdav.Propstat{pstat}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.props[name]
	for _, patch := range patches {
		for _, p := range patch.Props {
			if current == nil && !patch.Remove {
				current = make(map[xml.Name]webdav.Property)
				s.props[name] = current
			}
			if patch.Remove {
				delete(current, p.XMLName)
				continue
			}
			current[p.XMLName] = p
		}
	}
	if len(current) == 0 {
		delete(s.props, name)
	}

	pstat := webdav.Propstat{Status: http.StatusOK}
	for _, patch := range patches {
		for _, p := range patch.Props {
			pstat.Props = append(pstat.Props, webdav.Property{XMLName: p.XMLName})
		}
	}
	return []webdav.Propstat{pstat}, nil
}

func (s *deadPropStore) deleteTree(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := treePrefix(name)
	for p := range s.props {
		if p == name || strings.HasPrefix(p, prefix) {
			delete(s.props, p)
		}
	}
}

func (s *deadPropStore) renameTree(oldName, newName string) {
	if s == nil || oldName == newName {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	oldPrefix := treePrefix(oldName)
	updates := make(map[string]map[xml.Name]webdav.Property)
	for p, props := range s.props {
		switch {
		case p == oldName:
			updates[newName] = props
			delete(s.props, p)
		case strings.HasPrefix(p, oldPrefix):
			updates[newName+strings.TrimPrefix(p, oldName)] = props
			delete(s.props, p)
		}
	}
	for p, props := range updates {
		s.props[p] = props
	}
}

func treePrefix(name string) string {
	if strings.HasSuffix(name, "/") {
		return name
	}
	return name + "/"
}

// dirFile is a webdav.File for directories.
type dirFile struct {
	path     string
	propPath string
	props    *deadPropStore
	stat     *client.StatResult
	entries  []client.FileInfo
	pos      int
}

var _ webdav.File = (*dirFile)(nil)
var _ webdav.DeadPropsHolder = (*dirFile)(nil)

func (d *dirFile) Close() error                   { return nil }
func (d *dirFile) Read([]byte) (int, error)       { return 0, fmt.Errorf("is a directory") }
func (d *dirFile) Write([]byte) (int, error)      { return 0, fmt.Errorf("is a directory") }
func (d *dirFile) Seek(int64, int) (int64, error) { return 0, fmt.Errorf("is a directory") }

func (d *dirFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: path.Base(d.path), stat: d.stat}, nil
}

func (d *dirFile) DeadProps() (map[xml.Name]webdav.Property, error) {
	return d.props.deadProps(d.propPath)
}

func (d *dirFile) Patch(patches []webdav.Proppatch) ([]webdav.Propstat, error) {
	return d.props.patch(d.propPath, patches)
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
	return infos, nil
}

// readFile is a webdav.File for reading file content.
type readFile struct {
	path     string
	propPath string
	props    *deadPropStore
	stat     *client.StatResult
	*bytes.Reader
}

var _ webdav.File = (*readFile)(nil)
var _ webdav.DeadPropsHolder = (*readFile)(nil)

func (r *readFile) Close() error                       { return nil }
func (r *readFile) Write([]byte) (int, error)          { return 0, fmt.Errorf("read-only") }
func (r *readFile) Readdir(int) ([]fs.FileInfo, error) { return nil, fmt.Errorf("not a directory") }
func (r *readFile) Stat() (os.FileInfo, error) {
	return &fileInfo{name: path.Base(r.path), stat: r.stat}, nil
}

func (r *readFile) DeadProps() (map[xml.Name]webdav.Property, error) {
	return r.props.deadProps(r.propPath)
}

func (r *readFile) Patch(patches []webdav.Proppatch) ([]webdav.Propstat, error) {
	return r.props.patch(r.propPath, patches)
}

// writeFile is a webdav.File that stages a write and commits it on Close.
//
// Staging is bounded: the first PutBufferBytes of the body stay in memory and
// the overflow moves to an unlinked temporary file, so a large PUT costs disk
// rather than RSS and its failure point is a write error, not an OOM kill far
// from the request. Close is the single commit point — the body is committed
// whole or not at all, in one Drive9 request, and a nil return means the
// server acknowledged the new revision.
type writeFile struct {
	client *client.Client
	path   string
	props  *deadPropStore
	put    putConfig
	req    *putRequest

	// buf holds the staged prefix; past the memory bound file takes over
	// and buf is empty.
	buf  bytes.Buffer
	file *os.File
	name string // staged file's name (already unlinked where the OS allows)
	size int64

	// werr latches the first staging failure: a body that was not fully
	// recorded must not be committed later.
	werr   error
	closed bool
}

var _ webdav.File = (*writeFile)(nil)
var _ webdav.DeadPropsHolder = (*writeFile)(nil)

func (w *writeFile) Read([]byte) (int, error)           { return 0, fmt.Errorf("write-only") }
func (w *writeFile) Readdir(int) ([]fs.FileInfo, error) { return nil, fmt.Errorf("not a directory") }
func (w *writeFile) Seek(int64, int) (int64, error)     { return 0, fmt.Errorf("not seekable") }

// Write stages p, spilling to disk once the body outgrows the memory bound.
func (w *writeFile) Write(p []byte) (int, error) {
	if w.werr != nil {
		return 0, w.werr
	}
	if w.file == nil && w.size+int64(len(p)) > w.put.bufferLimit() {
		if err := w.spill(); err != nil {
			return 0, w.fail(err)
		}
	}
	var (
		n   int
		err error
	)
	if w.file != nil {
		n, err = w.file.Write(p)
	} else {
		n, err = w.buf.Write(p)
	}
	w.size += int64(n)
	if err != nil {
		return n, w.fail(fmt.Errorf("webdav: stage PUT body: %w", err))
	}
	return n, nil
}

// spill moves the staged prefix into a temporary file and switches the rest of
// the body to it. The file is unlinked immediately where the OS allows it, so
// a crashed process leaves no PUT bodies behind; the name is kept for systems
// that cannot unlink an open file.
func (w *writeFile) spill() error {
	dir := w.put.stagingDir
	if dir == "" {
		dir = os.TempDir()
	}
	file, err := os.CreateTemp(dir, "drive9-webdav-put-*")
	if err != nil {
		return fmt.Errorf("webdav: stage PUT body: %w", err)
	}
	w.name = file.Name()
	_ = os.Remove(w.name)
	if _, err := file.Write(w.buf.Bytes()); err != nil {
		_ = file.Close()
		_ = os.Remove(w.name)
		return fmt.Errorf("webdav: stage PUT body: %w", err)
	}
	w.buf.Reset()
	w.file = file
	return nil
}

// Close commits the staged body. It is the PUT's commit point: the body is
// written in one request, and only a server acknowledgement (a committed
// revision) returns nil. Anything else records the failure and leaves the
// target untouched.
func (w *writeFile) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer w.cleanup()

	if w.werr != nil {
		return w.werr
	}
	// A body shorter than the request declared is an aborted upload: the
	// bytes that did arrive are a truncated image, never a commit.
	if expected := w.req.expectedLen(); expected >= 0 && w.size != expected {
		return w.fail(fmt.Errorf("webdav: PUT body ended after %d of %d declared bytes", w.size, expected))
	}
	if err := w.req.bodyFailure(); err != nil {
		return w.fail(fmt.Errorf("webdav: PUT body aborted after %d bytes: %w", w.size, err))
	}

	// webdav.File.Close has no context parameter. Do not retain the OpenFile
	// request context here: clients can finish sending a PUT body and close
	// the connection before the server flushes the staged bytes to Drive9.
	// The commit therefore runs on its own context, but a BOUNDED one — an
	// untimed background context let an unresponsive server park the close
	// (and the PUT) forever.
	ctx, cancel := context.WithTimeout(context.Background(), w.put.commitTimeout())
	defer cancel()

	if w.file != nil {
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			return w.fail(fmt.Errorf("webdav: read staged PUT body: %w", err))
		}
		// The reader is the staged file itself: the streaming upload keeps
		// memory bounded by the client's transfer window instead of holding
		// the whole body again.
		return w.fail(mapError(w.client.WriteStream(ctx, w.path, w.file, w.size, nil)))
	}
	return w.fail(mapError(w.client.WriteCtx(ctx, w.path, w.buf.Bytes())))
}

// cleanup releases the staged file. A committed or failed PUT leaves nothing
// behind in the staging directory.
func (w *writeFile) cleanup() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.name != "" {
		_ = os.Remove(w.name)
		w.name = ""
	}
}

// fail records the PUT's failure for the HTTP layer and returns it.
func (w *writeFile) fail(err error) error {
	w.req.fail(err)
	return err
}

func (w *writeFile) Stat() (os.FileInfo, error) {
	return &fileInfo{
		name: path.Base(w.path),
		stat: &client.StatResult{Size: w.size},
	}, nil
}

func (w *writeFile) DeadProps() (map[xml.Name]webdav.Property, error) {
	return w.props.deadProps(w.path)
}

func (w *writeFile) Patch(patches []webdav.Proppatch) ([]webdav.Propstat, error) {
	return w.props.patch(w.path, patches)
}
