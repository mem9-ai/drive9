package extent

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
)

// EngineConfig configures the mounted engine.
type EngineConfig struct {
	// Client is the mount's API client.
	Client *client.Client
	// RemoteRoot is the namespace subtree served through the engine.
	RemoteRoot string
	// CacheDir arms the chunk store's local disk cache (empty = memory
	// only).
	CacheDir string
	// CacheSize bounds the disk cache in bytes (0 = default).
	CacheSize uint64
	// FreeSpaceRatio is the disk-cache free-space floor (JuiceFS
	// free-space-ratio): eviction and staging both respect it so the
	// cache never fills the volume. 0 = JuiceFS default (10%).
	FreeSpaceRatio float32
	// Writeback enables the ENGINE's own writeback pipeline: chunk blocks
	// below WritebackThresholdSize land in the on-disk stage cache and are
	// uploaded asynchronously by the chunk store (JuiceFS's native path),
	// instead of the bridge's synchronous commit staging.
	Writeback bool
	// WritebackThresholdSize is the block-size cutoff for staging a block
	// as a writeback block (JuiceFS default: BlockSize+1, i.e. every block
	// that is not the full block size).
	WritebackThresholdSize int
	// WritebackWait bounds the meta-side wait for asynchronously uploaded
	// blocks (0 uses a safe default).
	WritebackWait time.Duration
	// MaxStageWrite bounds concurrent staging writes (0 = JuiceFS default:
	// 40 on Linux). Caps the in-flight writeback byte footprint feeding the
	// shared cache budget.
	MaxStageWrite int
	// MaxUpload bounds the chunk store's concurrent staging-file uploads
	// (0 = defaultMaxUpload). The writeback drain is paced by this worker
	// count, but values above 4 have only been validated to expose
	// staging-eviction races — treat anything higher as experimental.
	MaxUpload int
	// ReadOnly mounts never mutate the remote.
	ReadOnly bool
}

const (
	// defaultMaxUpload is the direct in-memory staging concurrency used
	// when JuiceFS native Writeback is disabled.
	defaultMaxUpload = 16
	// defaultWritebackMaxUpload bounds upload workers when native
	// rawstaging Writeback is explicitly enabled (avoids free-space
	// staging-eviction races under disk pressure).
	defaultWritebackMaxUpload = 4
	defaultMaxDownload        = 16
)

// envMaxUpload overrides the default upload concurrency when
// DRIVE9_EXTENT_MAX_UPLOAD is set.
func envMaxUpload(nativeWriteback bool) int {
	if v := os.Getenv("DRIVE9_EXTENT_MAX_UPLOAD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	if nativeWriteback {
		return defaultWritebackMaxUpload
	}
	return defaultMaxUpload
}

// upstreamChunkDefaultsForTest exposes the chunk-store defaults the engine
// installs (test hook: they must match JuiceFS's own mount defaults).
func upstreamChunkDefaultsForTest(blockSize int) chunk.Config {
	return chunk.Config{
		BlockSize:      blockSize,
		CacheFullBlock: true,
		OSCache:        true,
		Readahead:      8 * blockSize,
		Prefetch:       1,
		BufferSize:     300 << 20,
	}
}

// Engine bundles the bridge, chunk store and VFS — the construction the
// mount wiring and the tests share. The community engine is used exactly
// as upstream ships it; every drive9-specific behavior lives in the Bridge
// and its object storage.
type Engine struct {
	Bridge *Bridge
	Store  chunk.ChunkStore
	VFS    *vfs.VFS
}

// NewEngine builds a ready-to-use engine.
func NewEngine(cfg EngineConfig) *Engine {
	cacheSize := uint64(cfg.CacheSize)
	var spoolBudget int64
	if cfg.CacheDir != "" && cacheSize >= 16<<20 {
		reserve := cacheSize / 4
		if reserve > 256<<20 {
			reserve = 256 << 20
		}
		spoolBudget = int64(reserve)
		cacheSize -= reserve
	}
	freeRatio := float64(cfg.FreeSpaceRatio)
	if freeRatio == 0 {
		freeRatio = 0.10
	}
	nativeWriteback := cfg.Writeback && cfg.CacheDir != "" && spoolBudget > 0
	maxUpload := cfg.MaxUpload
	if maxUpload <= 0 {
		maxUpload = envMaxUpload(nativeWriteback)
	}
	bcfg := Config{
		Client:          cfg.Client,
		RemoteRoot:      cfg.RemoteRoot,
		NativeWriteback: nativeWriteback,
		WritebackWait:   cfg.WritebackWait,
		CacheDir:        cfg.CacheDir,
		SpoolBudget:     spoolBudget,
		FreeSpaceRatio:  freeRatio,
		MaxUpload:       maxUpload,
	}
	b := NewBridge(bcfg)
	blockSize := blockSizeMiB << 20
	putTimeout := time.Minute
	if !nativeWriteback {
		putTimeout = 10 * time.Minute
	}
	chunkConf := chunk.Config{
		BlockSize:  blockSize << 0,
		CacheDir:   cfg.CacheDir,
		CacheSize:  cacheSize,
		CacheMode:  0o600,
		AutoCreate: true,
		Compress:   "none",
		// Upstream mount defaults (cmd/mount.go): cache FULL blocks too
		// (cache-partial-only is off upstream), keep the OS page cache,
		// read ahead 8 blocks per session, and prefetch one block.
		// Leaving CacheFullBlock false — the zero value — meant a whole
		// 4 MiB block was never written to the disk cache, i.e. the
		// engine's read cache silently did nothing for the common case,
		// and Readahead 0 disabled JuiceFS's own readahead entirely.
		CacheFullBlock: true,
		OSCache:        true,
		Readahead:      8 * blockSize,
		Prefetch:       1,
		MaxUpload:      maxUpload,
		MaxDownload:    defaultMaxDownload,
		// BufferSize is JuiceFS's TOTAL read/write buffering budget, and it
		// is not just a soft throttle: the data reader derives its global
		// read-ahead budget from it (80% of this value gates every read
		// buffer allocation, vfs/reader.go), so a huge number here grants
		// the mount an enormous in-memory read budget. Upstream mounts
		// 300 MiB; with the native writeback pipeline blocks go to the disk
		// stage instead of accumulating in memory, so the upstream default
		// is the right shape for this plane too.
		BufferSize: 300 << 20,
		GetTimeout: time.Minute,
		PutTimeout: putTimeout,
		// Writeback (JuiceFS native pipeline): blocks below the threshold
		// are staged to the disk cache and uploaded asynchronously, with
		// the cache's own eviction/free-space accounting bounding the
		// retained bytes. Requires a real cache directory (memory mode
		// disables it in SelfCheck).
		Writeback:              cfg.Writeback && cfg.CacheDir != "",
		WritebackThresholdSize: cfg.WritebackThresholdSize,
		FreeSpace:              cfg.FreeSpaceRatio,
		// Bound how many blocks may sit in the stage area at once: the
		// writeback byte budget is the shared cache quota (CacheSize +
		// FreeSpace), and this caps the in-flight staging concurrency
		// that feeds it (JuiceFS falls back to a direct upload when the
		// stage area is full — errStageFull).
		MaxStageWrite: cfg.MaxStageWrite,
	}
	// SelfCheck is upstream's normalization step (cmd/mount.go calls it
	// before NewCachedStore): it applies the eviction/checksum defaults and
	// disables writeback when the cache is memory-only. Skipping it left
	// the cache dir un-scoped ("unknown cache eviction policy" warnings)
	// and the writeback setup unvalidated.
	//
	// An EMPTY uuid on purpose: upstream appends it as a cache-directory
	// namespace so several volumes can share one cache dir, but this
	// engine already owns a per-mount directory (the mount's scoped cache
	// root, whose hash covers server + mountpoint + remote root +
	// credential). Appending a fixed per-plane UUID would only add a
	// redundant path segment (.../<scope>/extent/<uuid>), so the cache
	// stays exactly .../<scope>/extent.
	chunkConf.SelfCheck("")
	store := chunk.NewCachedStore(b.Storage(), chunkConf, nil)
	conf := &vfs.Config{
		Meta:            &meta.Config{Retries: 1},
		Format:          b.VolumeFormat(),
		Chunk:           &chunkConf,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
	}
	v := vfs.NewVFS(conf, b, store, nil, nil)
	return &Engine{Bridge: b, Store: store, VFS: v}
}

// FlushFile commits any slice still pending on the engine's asynchronous
// commit thread for the file (the bridge's Close/Flush also attempt this,
// but fsync/close paths call it explicitly to bound landing time).
func (e *Engine) FlushFile(ctx context.Context, ino uint64) error {
	return e.Bridge.FlushFile(ctx, meta.Ino(ino))
}

// FlushFileFinal is FlushFile for the last close of a descriptor: it may
// also materialize a still-empty file's name (see Bridge.FlushFileFinal).
func (e *Engine) FlushFileFinal(ctx context.Context, ino uint64) error {
	return e.Bridge.FlushFileFinal(ctx, meta.Ino(ino))
}

// ResolvePath maps a mount-local path onto a bridge inode, registering the
// path↔inode mapping. ENOENT when the target does not exist server-side
// and has no local (fresh, uncommitted) mapping either.
func (e *Engine) ResolvePath(ctx context.Context, local string) (uint64, syscall.Errno) {
	ino, errno := e.Bridge.ResolveLocal(ctx, local)
	return uint64(ino), errno
}

// LocalAttr reports the bridge's view of a mount-local path (local size
// wins over the server's while uncommitted writes are pending).
func (e *Engine) LocalAttr(ctx context.Context, local string) (size int64, ok bool) {
	return e.Bridge.LocalSizeOf(local)
}

// RootIno is the engine's root inode.
const RootIno = uint64(rootIno)

// SplitParent splits a mount-local path into parent path and base name.
func SplitParent(local string) (parent, name string) {
	local = strings.TrimSuffix(local, "/")
	i := strings.LastIndex(local, "/")
	if i <= 0 {
		return "/", strings.TrimPrefix(local, "/")
	}
	return local[:i], local[i+1:]
}
