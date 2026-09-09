package fuse

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type extentStagingMeta struct {
	BlockKey  string            `json:"block_key"`
	PutURL    string            `json:"put_url"`
	Headers   map[string]string `json:"headers,omitempty"`
	Path      string            `json:"path"`
	FileOff   int64             `json:"file_off"`
	Len       int64             `json:"len"`
	Checksum  string            `json:"checksum,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

func (fs *Dat9FS) extentStagingDir() string {
	dir := fs.extentCacheDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "rawstaging")
}

func (fs *Dat9FS) extentRawDir() string {
	dir := fs.extentCacheDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "raw")
}

func extentStagingName(blockKey string) string {
	return strings.ReplaceAll(blockKey, "/", "_")
}

func (fs *Dat9FS) stageExtentBlocks(blocks []client.LandedBlock) error {
	dir := fs.extentStagingDir()
	if dir == "" || len(blocks) == 0 {
		return nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, blk := range blocks {
		if blk.Op.BlockKey == "" || len(blk.Data) == 0 {
			continue
		}
		base := filepath.Join(dir, extentStagingName(blk.Op.BlockKey))
		dataPath := base + ".data"
		metaPath := base + ".json"
		if err := os.WriteFile(dataPath, blk.Data, 0o600); err != nil {
			return err
		}
		meta := extentStagingMeta{
			BlockKey:  blk.Op.BlockKey,
			PutURL:    blk.PutURL,
			Headers:   blk.Headers,
			Path:      blk.Path,
			FileOff:   blk.Op.FileOff,
			Len:       blk.Op.Len,
			Checksum:  blk.Op.ChecksumSHA256,
			CreatedAt: time.Now().UTC(),
		}
		raw, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		if err := os.WriteFile(metaPath, raw, 0o600); err != nil {
			return err
		}
		if fd, err := os.OpenFile(dataPath, os.O_RDWR, 0); err == nil {
			_ = fd.Sync()
			_ = fd.Close()
		}
		if fd, err := os.OpenFile(metaPath, os.O_RDWR, 0); err == nil {
			_ = fd.Sync()
			_ = fd.Close()
		}
	}
	if dirfd, err := os.Open(dir); err == nil {
		_ = dirfd.Sync()
		_ = dirfd.Close()
	}
	return nil
}

func (fs *Dat9FS) promoteExtentStaging(blockKey string, data []byte, blockOff, length int64) {
	if blockKey == "" {
		return
	}
	fs.extentDiskCachePut(blockKey, blockOff, length, data)
	dir := fs.extentStagingDir()
	if dir == "" {
		return
	}
	base := filepath.Join(dir, extentStagingName(blockKey))
	_ = os.Remove(base + ".data")
	_ = os.Remove(base + ".json")
}

func (fs *Dat9FS) replayExtentStaging(ctx context.Context) error {
	_ = ctx
	// Writeback staging is unused (FlushExtent Writeback:false). Leftover
	// rawstaging files from older builds blocked mount with expired
	// presign retries and surfaced mkdir EAGAIN to community.fio.
	dir := fs.extentStagingDir()
	if dir == "" {
		return nil
	}
	_ = os.RemoveAll(dir)
	return nil
}
