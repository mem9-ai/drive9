package fuse

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultDiskReadCacheMaxSize   = 1 << 30
	defaultDiskReadCacheFreeRatio = 0.10
	diskReadCacheEntryExt         = ".drc"
)

var diskReadCacheTempSeq atomic.Uint64

type DiskReadCacheOptions struct {
	Dir       string
	MaxSize   int64
	FreeRatio float64
}

type DiskReadCacheKey struct {
	FileID   string
	Path     string
	Revision int64
	Offset   int64
	Length   int64
}

func (k DiskReadCacheKey) valid() bool {
	return k.FileID != "" && k.Revision > 0 && k.Offset >= 0 && k.Length > 0
}

func (k DiskReadCacheKey) digest() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d\x00%d", k.FileID, k.Revision, k.Offset, k.Length)))
	return hex.EncodeToString(sum[:])
}

func (k DiskReadCacheKey) flightKey() string {
	return fmt.Sprintf("disk:%s:%d:%d:%d", k.FileID, k.Revision, k.Offset, k.Length)
}

type diskReadCacheHeader struct {
	FileID    string `json:"file_id"`
	Path      string `json:"path,omitempty"`
	Revision  int64  `json:"revision"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
	Size      int64  `json:"size"`
	Checksum  uint32 `json:"crc32"`
	CreatedAt int64  `json:"created_at_unix_nano"`
}

type diskReadCacheEntry struct {
	key  DiskReadCacheKey
	path string
	size int64
	elem *list.Element
}

type diskReadCachePending struct {
	key  DiskReadCacheKey
	data []byte
	seq  uint64
}

type DiskReadCache struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	dir       string
	maxSize   int64
	freeRatio float64
	items     map[string]*diskReadCacheEntry
	pending   map[string]diskReadCachePending
	order     *list.List
	size      int64
	closed    bool
}

func NewDiskReadCache(opts DiskReadCacheOptions) (*DiskReadCache, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("disk read cache: dir is required")
	}
	if opts.MaxSize <= 0 {
		opts.MaxSize = defaultDiskReadCacheMaxSize
	}
	if opts.FreeRatio < 0 {
		opts.FreeRatio = 0
	} else if opts.FreeRatio == 0 {
		opts.FreeRatio = defaultDiskReadCacheFreeRatio
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(opts.Dir, 0o700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	cache := &DiskReadCache{
		ctx:       ctx,
		cancel:    cancel,
		dir:       opts.Dir,
		maxSize:   opts.MaxSize,
		freeRatio: opts.FreeRatio,
		items:     make(map[string]*diskReadCacheEntry),
		pending:   make(map[string]diskReadCachePending),
		order:     list.New(),
	}
	if err := cache.recoverIndex(); err != nil {
		return nil, err
	}
	cache.evictLocked()
	return cache, nil
}

func (c *DiskReadCache) Get(key DiskReadCacheKey) ([]byte, bool) {
	if c == nil || !key.valid() {
		return nil, false
	}
	digest := key.digest()
	c.mu.Lock()
	if pending, ok := c.pending[digest]; ok {
		data := make([]byte, len(pending.data))
		copy(data, pending.data)
		c.mu.Unlock()
		return data, true
	}
	entry, ok := c.items[digest]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	path := entry.path
	c.order.MoveToFront(entry.elem)
	c.mu.Unlock()

	data, err := readDiskReadCacheFile(path, key)
	if err != nil {
		c.removeDigest(digest)
		return nil, false
	}
	return data, true
}

func (c *DiskReadCache) Put(key DiskReadCacheKey, data []byte) {
	if c == nil || int64(len(data)) != key.Length {
		return
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	c.PutOwned(key, stored)
}

func (c *DiskReadCache) PutAsync(key DiskReadCacheKey, data []byte) {
	if c == nil || !key.valid() || int64(len(data)) != key.Length || int64(len(data)) > c.maxSize {
		return
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	digest := key.digest()
	seq := diskReadCacheTempSeq.Add(1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.pending[digest] = diskReadCachePending{key: key, data: stored, seq: seq}
	c.wg.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wg.Done()
		if c.ctx.Err() == nil {
			c.putOwned(key, stored, seq)
		}
		c.clearPending(digest, seq)
	}()
}

func (c *DiskReadCache) PutOwned(key DiskReadCacheKey, data []byte) {
	c.putOwned(key, data, 0)
}

func (c *DiskReadCache) putOwned(key DiskReadCacheKey, data []byte, pendingSeq uint64) {
	if c == nil || !key.valid() || int64(len(data)) != key.Length || int64(len(data)) > c.maxSize {
		return
	}
	digest := key.digest()
	path := c.pathForDigest(digest)
	tmp := filepath.Join(c.dir, fmt.Sprintf("%s.%d.%d.tmp", digest, os.Getpid(), diskReadCacheTempSeq.Add(1)))
	if err := writeDiskReadCacheFile(tmp, key, data); err != nil {
		_ = os.Remove(tmp)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = os.Remove(tmp)
		return
	}
	if pendingSeq != 0 {
		pending, ok := c.pending[digest]
		if !ok || pending.seq != pendingSeq {
			_ = os.Remove(tmp)
			return
		}
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return
	}
	fileSize, err := diskReadCacheFileSize(path)
	if err != nil {
		_ = os.Remove(path)
		return
	}

	if existing, ok := c.items[digest]; ok {
		c.size -= existing.size
		existing.path = path
		existing.size = fileSize
		c.size += existing.size
		c.order.MoveToFront(existing.elem)
	} else {
		entry := &diskReadCacheEntry{key: key, path: path, size: fileSize}
		entry.elem = c.order.PushFront(entry)
		c.items[digest] = entry
		c.size += entry.size
	}
	c.evictLocked()
}

func (c *DiskReadCache) clearPending(digest string, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pending, ok := c.pending[digest]; ok && pending.seq == seq {
		delete(c.pending, digest)
	}
}

func (c *DiskReadCache) InvalidateFile(fileID string) {
	if c == nil || fileID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for digest, entry := range c.items {
		if entry.key.FileID == fileID {
			c.removeEntryLocked(digest, entry)
		}
	}
	for digest, pending := range c.pending {
		if pending.key.FileID == fileID {
			delete(c.pending, digest)
		}
	}
}

func (c *DiskReadCache) InvalidatePathPrefix(prefix string) {
	if c == nil || prefix == "" {
		return
	}
	filePrefix := pathDiskReadCacheFileID(prefix)
	c.mu.Lock()
	defer c.mu.Unlock()
	for digest, entry := range c.items {
		if strings.HasPrefix(entry.key.Path, prefix) || strings.HasPrefix(entry.key.FileID, filePrefix) {
			c.removeEntryLocked(digest, entry)
		}
	}
	for digest, pending := range c.pending {
		if strings.HasPrefix(pending.key.Path, prefix) || strings.HasPrefix(pending.key.FileID, filePrefix) {
			delete(c.pending, digest)
		}
	}
}

func (c *DiskReadCache) InvalidateAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for digest, entry := range c.items {
		c.removeEntryLocked(digest, entry)
	}
	for digest := range c.pending {
		delete(c.pending, digest)
	}
}

func (c *DiskReadCache) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cancel()
	for digest := range c.pending {
		delete(c.pending, digest)
	}
	c.mu.Unlock()
	c.wg.Wait()
}

func (c *DiskReadCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *DiskReadCache) pathForDigest(digest string) string {
	return filepath.Join(c.dir, digest+diskReadCacheEntryExt)
}

func (c *DiskReadCache) recoverIndex() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	type recovered struct {
		digest string
		entry  *diskReadCacheEntry
		mtime  time.Time
	}
	var recoveredEntries []recovered
	for _, dirEntry := range entries {
		if dirEntry.IsDir() {
			continue
		}
		name := dirEntry.Name()
		fullPath := filepath.Join(c.dir, name)
		if strings.HasSuffix(name, ".tmp") || strings.Contains(name, ".tmp.") {
			_ = os.Remove(fullPath)
			continue
		}
		if !strings.HasSuffix(name, diskReadCacheEntryExt) {
			continue
		}
		header, err := readDiskReadCacheHeader(fullPath)
		if err != nil {
			_ = os.Remove(fullPath)
			continue
		}
		key := DiskReadCacheKey{
			FileID:   header.FileID,
			Path:     header.Path,
			Revision: header.Revision,
			Offset:   header.Offset,
			Length:   header.Length,
		}
		if !key.valid() || header.Size < 0 {
			_ = os.Remove(fullPath)
			continue
		}
		info, err := dirEntry.Info()
		if err != nil {
			_ = os.Remove(fullPath)
			continue
		}
		recoveredEntries = append(recoveredEntries, recovered{
			digest: strings.TrimSuffix(name, diskReadCacheEntryExt),
			entry:  &diskReadCacheEntry{key: key, path: fullPath, size: info.Size()},
			mtime:  info.ModTime(),
		})
	}
	sort.Slice(recoveredEntries, func(i, j int) bool {
		return recoveredEntries[i].mtime.Before(recoveredEntries[j].mtime)
	})
	for _, item := range recoveredEntries {
		item.entry.elem = c.order.PushFront(item.entry)
		c.items[item.digest] = item.entry
		c.size += item.entry.size
	}
	return nil
}

func (c *DiskReadCache) evictLocked() {
	for c.size > c.maxSize && c.order.Len() > 0 {
		entry := c.order.Back().Value.(*diskReadCacheEntry)
		c.removeEntryLocked(entry.key.digest(), entry)
	}
	if c.freeRatio <= 0 {
		return
	}
	for c.order.Len() > 0 && diskFreeRatio(c.dir) < c.freeRatio {
		entry := c.order.Back().Value.(*diskReadCacheEntry)
		c.removeEntryLocked(entry.key.digest(), entry)
	}
}

func (c *DiskReadCache) removeDigest(digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.items[digest]; ok {
		c.removeEntryLocked(digest, entry)
	}
}

func (c *DiskReadCache) removeEntryLocked(digest string, entry *diskReadCacheEntry) {
	delete(c.items, digest)
	if entry.elem != nil {
		c.order.Remove(entry.elem)
	}
	c.size -= entry.size
	if c.size < 0 {
		c.size = 0
	}
	_ = os.Remove(entry.path)
}

func writeDiskReadCacheFile(path string, key DiskReadCacheKey, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	header := diskReadCacheHeader{
		FileID:    key.FileID,
		Path:      key.Path,
		Revision:  key.Revision,
		Offset:    key.Offset,
		Length:    key.Length,
		Size:      int64(len(data)),
		Checksum:  crc32.ChecksumIEEE(data),
		CreatedAt: time.Now().UnixNano(),
	}
	headerData, err := json.Marshal(header)
	if err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(headerData); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write([]byte("\n")); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func diskReadCacheFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func readDiskReadCacheFile(path string, key DiskReadCacheKey) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReader(file)
	headerData, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var header diskReadCacheHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerData), &header); err != nil {
		return nil, err
	}
	if header.FileID != key.FileID || header.Revision != key.Revision || header.Offset != key.Offset || header.Length != key.Length {
		return nil, fmt.Errorf("disk read cache: key mismatch")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != header.Size || crc32.ChecksumIEEE(data) != header.Checksum {
		return nil, fmt.Errorf("disk read cache: checksum mismatch")
	}
	return data, nil
}

func readDiskReadCacheHeader(path string) (diskReadCacheHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return diskReadCacheHeader{}, err
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReader(file)
	headerData, err := reader.ReadBytes('\n')
	if err != nil {
		return diskReadCacheHeader{}, err
	}
	var header diskReadCacheHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerData), &header); err != nil {
		return diskReadCacheHeader{}, err
	}
	return header, nil
}

func diskFreeRatio(dir string) float64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil || stat.Blocks == 0 {
		return 1
	}
	return float64(stat.Bavail) / float64(stat.Blocks)
}

func pathDiskReadCacheFileID(p string) string {
	return "path:" + p
}
