// Package backend implements the Dat9Backend, which satisfies AGFS's FileSystem interface.
// P0 uses TiDB (MySQL protocol) + local blob storage as a stand-in for db9/fs9.
package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/oklog/ulid/v2"
)

const smallFileThreshold = 1 << 20 // 1MB

// Dat9Backend implements filesystem.FileSystem with the inode model.
type Dat9Backend struct {
	store   *meta.Store
	blobDir string
	s3      s3client.S3Client // nil when S3 is not configured
	mu      sync.Mutex
	entropy io.Reader
}

func New(store *meta.Store, blobDir string) (*Dat9Backend, error) {
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return nil, fmt.Errorf("create blob dir: %w", err)
	}
	return &Dat9Backend{
		store:   store,
		blobDir: blobDir,
		entropy: ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
	}, nil
}

// NewWithS3 creates a Dat9Backend with S3 support for large file uploads.
func NewWithS3(store *meta.Store, blobDir string, s3 s3client.S3Client) (*Dat9Backend, error) {
	b, err := New(store, blobDir)
	if err != nil {
		return nil, err
	}
	b.s3 = s3
	return b, nil
}

func (b *Dat9Backend) Store() *meta.Store { return b.store }

func (b *Dat9Backend) genID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	id, err := ulid.New(ulid.Timestamp(time.Now()), b.entropy)
	if err != nil {
		// Fallback: reset entropy on exhaustion
		b.entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
		id = ulid.MustNew(ulid.Timestamp(time.Now()), b.entropy)
	}
	return id.String()
}

func (b *Dat9Backend) Create(path string) error {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return err
	}

	fileID := b.genID()
	storageRef := "/blobs/" + fileID
	now := time.Now()

	if err := b.writeBlob(storageRef, nil); err != nil {
		return err
	}
	if err := b.store.InsertFile(&meta.File{
		FileID: fileID, StorageType: meta.StorageDB9, StorageRef: storageRef,
		SizeBytes: 0, Revision: 1, Status: meta.StatusConfirmed,
		CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		return err
	}
	if err := b.store.EnsureParentDirs(path, b.genID); err != nil {
		return err
	}
	return b.store.InsertNode(&meta.FileNode{
		NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
		Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
	})
}

func (b *Dat9Backend) Mkdir(path string, perm uint32) error {
	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return err
	}
	if err := b.store.EnsureParentDirs(dirPath, b.genID); err != nil {
		return err
	}
	return b.store.InsertNode(&meta.FileNode{
		NodeID: b.genID(), Path: dirPath, ParentPath: pathutil.ParentPath(dirPath),
		Name: pathutil.BaseName(dirPath), IsDirectory: true, CreatedAt: time.Now(),
	})
}

func (b *Dat9Backend) Remove(path string) error {
	path = normalizePath(path)
	node, err := b.store.GetNode(path)
	if err != nil {
		return err
	}
	if node.IsDirectory {
		return b.store.DeleteEmptyDir(path)
	}
	deleted, err := b.store.DeleteFileWithRefCheck(path)
	if err != nil {
		return err
	}
	if deleted != nil {
		b.deleteBlob(deleted.StorageRef)
	}
	return nil
}

func (b *Dat9Backend) RemoveAll(path string) error {
	path = normalizePath(path)
	node, err := b.store.GetNode(path)
	if err != nil {
		return err
	}
	if !node.IsDirectory {
		return b.Remove(path)
	}
	orphaned, err := b.store.DeleteDirRecursive(path)
	if err != nil {
		return err
	}
	for _, f := range orphaned {
		b.deleteBlob(f.StorageRef)
	}
	return nil
}

func (b *Dat9Backend) Read(path string, offset int64, size int64) ([]byte, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	nf, err := b.store.Stat(path)
	if err != nil {
		return nil, err
	}
	if nf.Node.IsDirectory {
		return nil, fmt.Errorf("is a directory: %s", path)
	}
	if nf.File == nil {
		return nil, fmt.Errorf("no file entity for path: %s", path)
	}

	data, err := b.readBlob(nf.File.StorageRef)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if offset >= int64(len(data)) {
			return nil, io.EOF
		}
		data = data[offset:]
	}
	if size >= 0 && size < int64(len(data)) {
		data = data[:size]
	}
	return data, nil
}

func (b *Dat9Backend) Write(path string, data []byte, offset int64, flags filesystem.WriteFlag) (int64, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return 0, err
	}

	existing, err := b.store.Stat(path)
	if err == meta.ErrNotFound {
		if flags&filesystem.WriteFlagCreate == 0 {
			return 0, meta.ErrNotFound
		}
		return b.createAndWrite(path, data)
	}
	if err != nil {
		return 0, err
	}
	if existing.Node.IsDirectory {
		return 0, fmt.Errorf("is a directory: %s", path)
	}
	if flags&filesystem.WriteFlagExclusive != 0 {
		return 0, fmt.Errorf("file already exists: %s", path)
	}
	return b.overwriteFile(existing, data, offset, flags)
}

func (b *Dat9Backend) createAndWrite(path string, data []byte) (int64, error) {
	fileID := b.genID()
	storageRef := "/blobs/" + fileID
	now := time.Now()

	if err := b.writeBlob(storageRef, data); err != nil {
		return 0, err
	}

	contentType := detectContentType(path, data)
	checksum := sha256sum(data)
	contentText := extractText(data, contentType)

	if err := b.store.InsertFile(&meta.File{
		FileID: fileID, StorageType: meta.StorageDB9, StorageRef: storageRef,
		ContentType: contentType, SizeBytes: int64(len(data)),
		ChecksumSHA256: checksum, Revision: 1, Status: meta.StatusConfirmed,
		ContentText: contentText, CreatedAt: now, ConfirmedAt: &now,
	}); err != nil {
		return 0, err
	}
	if err := b.store.EnsureParentDirs(path, b.genID); err != nil {
		return 0, err
	}
	if err := b.store.InsertNode(&meta.FileNode{
		NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
		Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
	}); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func (b *Dat9Backend) overwriteFile(nf *meta.NodeWithFile, data []byte, offset int64, flags filesystem.WriteFlag) (int64, error) {
	if nf.File == nil {
		return 0, fmt.Errorf("no file entity")
	}

	var finalData []byte
	if flags&filesystem.WriteFlagAppend != 0 {
		existing, err := b.readBlob(nf.File.StorageRef)
		if err != nil {
			return 0, fmt.Errorf("read existing blob for append: %w", err)
		}
		finalData = append(existing, data...)
	} else if flags&filesystem.WriteFlagTruncate != 0 || offset <= 0 {
		finalData = data
	} else {
		existing, err := b.readBlob(nf.File.StorageRef)
		if err != nil {
			return 0, fmt.Errorf("read existing blob for offset write: %w", err)
		}
		if offset > int64(len(existing)) {
			existing = append(existing, make([]byte, offset-int64(len(existing)))...)
		}
		end := offset + int64(len(data))
		finalData = append(existing[:offset], data...)
		if end < int64(len(existing)) {
			finalData = append(finalData, existing[end:]...)
		}
	}

	oldRef := nf.File.StorageRef
	newRef := "/blobs/" + b.genID()

	if err := b.writeBlob(newRef, finalData); err != nil {
		return 0, err
	}

	contentType := detectContentType(nf.Node.Path, finalData)
	checksum := sha256sum(finalData)
	contentText := extractText(finalData, contentType)

	if err := b.store.UpdateFileContent(
		nf.File.FileID, meta.StorageDB9, newRef,
		contentType, checksum, contentText, int64(len(finalData)),
	); err != nil {
		return 0, err
	}
	if oldRef != newRef {
		b.deleteBlob(oldRef)
	}
	return int64(len(data)), nil
}

func (b *Dat9Backend) ReadDir(path string) ([]filesystem.FileInfo, error) {
	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return nil, err
	}
	entries, err := b.store.ListDir(dirPath)
	if err != nil {
		return nil, err
	}

	var infos []filesystem.FileInfo
	for _, e := range entries {
		info := filesystem.FileInfo{
			Name: e.Node.Name, IsDir: e.Node.IsDirectory, Mode: 0o644,
		}
		if e.Node.IsDirectory {
			info.Mode = 0o755
		}
		if e.File != nil {
			info.Size = e.File.SizeBytes
			if e.File.ConfirmedAt != nil {
				info.ModTime = *e.File.ConfirmedAt
			} else {
				info.ModTime = e.File.CreatedAt
			}
		} else {
			info.ModTime = e.Node.CreatedAt
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (b *Dat9Backend) Stat(path string) (*filesystem.FileInfo, error) {
	path = normalizePath(path)
	nf, err := b.store.Stat(path)
	if err != nil {
		return nil, err
	}
	info := &filesystem.FileInfo{
		Name: nf.Node.Name, IsDir: nf.Node.IsDirectory, Mode: 0o644,
	}
	if nf.Node.IsDirectory {
		info.Mode = 0o755
	}
	if nf.File != nil {
		info.Size = nf.File.SizeBytes
		if nf.File.ConfirmedAt != nil {
			info.ModTime = *nf.File.ConfirmedAt
		} else {
			info.ModTime = nf.File.CreatedAt
		}
	} else {
		info.ModTime = nf.Node.CreatedAt
	}
	return info, nil
}

func (b *Dat9Backend) Rename(oldPath, newPath string) error {
	oldPath = normalizePath(oldPath)
	newPath = normalizePath(newPath)

	node, err := b.store.GetNode(oldPath)
	if err != nil {
		return err
	}
	if node.IsDirectory {
		if err := b.store.EnsureParentDirs(newPath, b.genID); err != nil {
			return err
		}
		_, err := b.store.RenameDir(oldPath, newPath)
		return err
	}
	if err := b.store.EnsureParentDirs(newPath, b.genID); err != nil {
		return err
	}
	return b.store.UpdateNodePath(oldPath, newPath, pathutil.ParentPath(newPath), pathutil.BaseName(newPath))
}

func (b *Dat9Backend) Chmod(path string, mode uint32) error { return nil }

func (b *Dat9Backend) Open(path string) (io.ReadCloser, error) {
	data, err := b.Read(path, 0, -1)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *Dat9Backend) OpenWrite(path string) (io.WriteCloser, error) {
	return &writeCloser{backend: b, path: path}, nil
}

// --- CapabilityProvider ---

func (b *Dat9Backend) GetCapabilities() filesystem.Capabilities {
	caps := filesystem.DefaultCapabilities()
	if b.s3 != nil {
		caps.IsObjectStore = true
	}
	return caps
}

func (b *Dat9Backend) GetPathCapabilities(path string) filesystem.Capabilities {
	return b.GetCapabilities()
}

// Verify interface compliance.
var _ filesystem.CapabilityProvider = (*Dat9Backend)(nil)

// CopyFile performs a zero-copy cp (new file_node pointing to same file_id).
func (b *Dat9Backend) CopyFile(srcPath, dstPath string) error {
	srcPath, err := pathutil.Canonicalize(srcPath)
	if err != nil {
		return err
	}
	dstPath, err = pathutil.Canonicalize(dstPath)
	if err != nil {
		return err
	}
	srcNode, err := b.store.GetNode(srcPath)
	if err != nil {
		return err
	}
	if srcNode.IsDirectory {
		return fmt.Errorf("cannot copy directory with CopyFile: %s", srcPath)
	}
	if err := b.store.EnsureParentDirs(dstPath, b.genID); err != nil {
		return err
	}
	return b.store.InsertNode(&meta.FileNode{
		NodeID: b.genID(), Path: dstPath, ParentPath: pathutil.ParentPath(dstPath),
		Name: pathutil.BaseName(dstPath), FileID: srcNode.FileID, CreatedAt: time.Now(),
	})
}

// --- blob storage ---

func (b *Dat9Backend) blobPath(ref string) string {
	name := strings.TrimPrefix(ref, "/blobs/")
	return filepath.Join(b.blobDir, name)
}

func (b *Dat9Backend) writeBlob(ref string, data []byte) error {
	if data == nil {
		data = []byte{}
	}
	return os.WriteFile(b.blobPath(ref), data, 0o644)
}

func (b *Dat9Backend) readBlob(ref string) ([]byte, error) {
	return os.ReadFile(b.blobPath(ref))
}

func (b *Dat9Backend) deleteBlob(ref string) {
	if b.s3 != nil && !strings.HasPrefix(ref, "/blobs/") {
		// S3 key (e.g. "blobs/ULID") — delete from S3
		_ = b.s3.DeleteObject(context.Background(), ref)
		return
	}
	_ = os.Remove(b.blobPath(ref))
}

// --- writeCloser ---

type writeCloser struct {
	backend *Dat9Backend
	path    string
	buf     bytes.Buffer
}

func (w *writeCloser) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *writeCloser) Close() error {
	_, err := w.backend.Write(w.path, w.buf.Bytes(), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	return err
}

// --- helpers ---

func normalizePath(path string) string {
	if pathutil.IsDir(path) {
		p, err := pathutil.CanonicalizeDir(path)
		if err != nil {
			return path
		}
		return p
	}
	p, err := pathutil.Canonicalize(path)
	if err != nil {
		return path
	}
	return p
}

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func detectContentType(path string, data []byte) string {
	ext := pathutil.Ext(path)
	if ext != "" {
		if ct := mime.TypeByExtension(ext); ct != "" {
			return ct
		}
	}
	if len(data) > 0 && isTextContent(data) {
		return "text/plain"
	}
	return "application/octet-stream"
}

func isTextContent(data []byte) bool {
	for _, b := range data {
		if b == 0 {
			return false
		}
	}
	return true
}

func extractText(data []byte, contentType string) string {
	if !strings.HasPrefix(contentType, "text/") &&
		contentType != "application/json" &&
		contentType != "application/xml" &&
		contentType != "application/yaml" {
		return ""
	}
	if len(data) > smallFileThreshold {
		return ""
	}
	return string(data)
}

func (b *Dat9Backend) ExecSQL(ctx context.Context, query string) ([]map[string]interface{}, error) {
	return b.store.ExecSQL(ctx, query)
}
