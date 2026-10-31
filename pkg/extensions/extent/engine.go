package extent

import (
	"context"
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
	b := NewBridge(Config{Client: cfg.Client, RemoteRoot: cfg.RemoteRoot})
	cacheSize := cfg.CacheSize
	if cacheSize == 0 {
		cacheSize = 1 << 30
	}
	chunkConf := chunk.Config{
		BlockSize:   blockSizeMiB << 20,
		CacheDir:    cfg.CacheDir,
		CacheSize:   cacheSize,
		CacheMode:   0o600,
		AutoCreate:  true,
		Compress:    "none",
		MaxUpload:   4,
		MaxDownload: 4,
		// The bridge stages uploads in memory at commit time; a generous
		// buffer keeps the writer's AllocMemory-based throttle out of the
		// way. TODO(calibrate): revisit for production mounts.
		BufferSize: 32 << 30,
		Readahead:  0,
		GetTimeout: time.Minute,
		PutTimeout: time.Minute,
	}
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
