package fuse

import (
	"crypto/sha256"
	"crypto/subtle"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const extentDiskChecksumLen = sha256.Size

type diskCacheSpan struct {
	fileKey string
	off     int64
	length  int64
}

type extentBlockDiskCache struct {
	mu       sync.Mutex
	dirs     []string
	maxBytes int64
	curBytes int64
	order    []string
	sizes    map[string]int64
	spans    map[string][]diskCacheSpan // blockKey -> cached ranges (JuiceFS block ReadAt)
}

func newExtentBlockDiskCache(dirs []string, maxBytes int64) *extentBlockDiskCache {
	if maxBytes <= 0 {
		maxBytes = 1 << 30
	}
	return &extentBlockDiskCache{
		dirs:     dirs,
		maxBytes: maxBytes,
		sizes:    make(map[string]int64),
		spans:    make(map[string][]diskCacheSpan),
	}
}

func extentHashPick(dirs []string, key string) string {
	if len(dirs) == 0 {
		return ""
	}
	if len(dirs) == 1 {
		return dirs[0]
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return dirs[h.Sum64()%uint64(len(dirs))]
}

func extentDiskCacheKey(blockKey string, blockOff, length int64) string {
	return strings.ReplaceAll(blockKey, "/", "_") + "_" + strconv.FormatInt(blockOff, 10) + "_" + strconv.FormatInt(length, 10)
}

func (fs *Dat9FS) ensureExtentDiskCache() *extentBlockDiskCache {
	if fs == nil {
		return nil
	}
	if fs.extentDisk != nil {
		return fs.extentDisk
	}
	fs.extentOpenMu.Lock()
	defer fs.extentOpenMu.Unlock()
	if fs.extentDisk != nil {
		return fs.extentDisk
	}
	dirs := fs.extentRawDirs()
	if len(dirs) == 0 {
		return nil
	}
	max := int64(0)
	if fs.opts != nil {
		max = fs.opts.DiskReadCacheSize
		if max <= 0 {
			max = fs.opts.CacheSize
		}
	}
	if max <= 0 {
		max = 1 << 30
	}
	fs.extentDisk = newExtentBlockDiskCache(dirs, max)
	return fs.extentDisk
}

func (c *extentBlockDiskCache) path(key string) string {
	if c == nil || len(c.dirs) == 0 {
		return ""
	}
	dir := extentHashPick(c.dirs, key)
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	shard := strconv.FormatUint(uint64(h.Sum32()%256), 16)
	if len(shard) == 1 {
		shard = "0" + shard
	}
	return filepath.Join(dir, shard, key)
}

func (c *extentBlockDiskCache) get(blockKey string, blockOff, length int64) ([]byte, bool) {
	if c == nil || blockKey == "" || length <= 0 {
		return nil, false
	}
	if data, ok := c.readSpan(extentDiskCacheKey(blockKey, blockOff, length), 0, length); ok {
		c.mu.Lock()
		c.touchLocked(extentDiskCacheKey(blockKey, blockOff, length), length)
		c.mu.Unlock()
		return data, true
	}
	fileKey, storedOff, storedLen, ok := c.findCovering(blockKey, blockOff, length)
	if !ok {
		return nil, false
	}
	data, ok := c.readSpan(fileKey, blockOff-storedOff, length)
	if !ok {
		return nil, false
	}
	c.mu.Lock()
	c.touchLocked(fileKey, storedLen)
	c.mu.Unlock()
	return data, true
}

func (c *extentBlockDiskCache) readSpan(fileKey string, rel, length int64) ([]byte, bool) {
	raw, err := os.ReadFile(c.path(fileKey))
	if err != nil {
		return nil, false
	}
	payload := int64(len(raw)) - extentDiskChecksumLen
	if payload < 0 || rel < 0 || rel+length > payload {
		return nil, false
	}
	sum := sha256.Sum256(raw[extentDiskChecksumLen:])
	if subtle.ConstantTimeCompare(sum[:], raw[:extentDiskChecksumLen]) != 1 {
		_ = os.Remove(c.path(fileKey))
		return nil, false
	}
	out := make([]byte, length)
	copy(out, raw[extentDiskChecksumLen+int(rel):extentDiskChecksumLen+int(rel+length)])
	return out, true
}

func (c *extentBlockDiskCache) findCovering(blockKey string, blockOff, length int64) (string, int64, int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	end := blockOff + length
	for _, sp := range c.spans[blockKey] {
		if sp.off <= blockOff && sp.off+sp.length >= end {
			return sp.fileKey, sp.off, sp.length, true
		}
	}
	return "", 0, 0, false
}

func (c *extentBlockDiskCache) put(blockKey string, blockOff, length int64, data []byte) {
	if c == nil || blockKey == "" || int64(len(data)) != length || length <= 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(c.path(extentDiskCacheKey(blockKey, blockOff, length))), 0o700); err != nil {
		return
	}
	key := extentDiskCacheKey(blockKey, blockOff, length)
	sum := sha256.Sum256(data)
	buf := make([]byte, extentDiskChecksumLen+len(data))
	copy(buf, sum[:])
	copy(buf[extentDiskChecksumLen:], data)
	tmp := c.path(key) + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, c.path(key)); err != nil {
		_ = os.Remove(tmp)
		return
	}
	c.mu.Lock()
	c.touchLocked(key, length)
	c.addSpanLocked(blockKey, key, blockOff, length)
	c.evictLocked(0)
	c.mu.Unlock()
}

func (c *extentBlockDiskCache) addSpanLocked(blockKey, fileKey string, off, length int64) {
	if c.spans == nil {
		c.spans = make(map[string][]diskCacheSpan)
	}
	list := c.spans[blockKey]
	for i, sp := range list {
		if sp.fileKey == fileKey {
			list[i] = diskCacheSpan{fileKey: fileKey, off: off, length: length}
			c.spans[blockKey] = list
			return
		}
	}
	c.spans[blockKey] = append(list, diskCacheSpan{fileKey: fileKey, off: off, length: length})
}

func (c *extentBlockDiskCache) touchLocked(key string, length int64) {
	if _, ok := c.sizes[key]; !ok {
		c.order = append(c.order, key)
		c.sizes[key] = length
		c.curBytes += length
	} else {
		for i, k := range c.order {
			if k == key {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
		c.order = append(c.order, key)
	}
}

func (c *extentBlockDiskCache) evictLocked(need int64) {
	for c.curBytes+need > c.maxBytes && len(c.order) > 0 {
		idx := 0
		if len(c.order) > 1 {
			i := rand.IntN(len(c.order))
			j := rand.IntN(len(c.order))
			idx = i
			if j < i {
				idx = j
			}
		}
		key := c.order[idx]
		c.order = append(c.order[:idx], c.order[idx+1:]...)
		if sz, ok := c.sizes[key]; ok {
			c.curBytes -= sz
			delete(c.sizes, key)
		}
		c.dropSpanFileLocked(key)
		_ = os.Remove(c.path(key))
	}
}

func (c *extentBlockDiskCache) dropSpanFileLocked(fileKey string) {
	for bk, list := range c.spans {
		kept := list[:0]
		for _, sp := range list {
			if sp.fileKey != fileKey {
				kept = append(kept, sp)
			}
		}
		if len(kept) == 0 {
			delete(c.spans, bk)
		} else {
			c.spans[bk] = kept
		}
	}
}
