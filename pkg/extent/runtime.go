package extent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sirupsen/logrus"
)

// Runtime is the JuiceFS data plane used by Dat9FS and the compact executor.
// FUSE Open/SetAttr/Write/Read/Flush/Release go through VFS. Reader/Writer
// remain for the CLI sequential reader and the Phase 0 IO harness.
type Runtime struct {
	Meta      jfsmeta.Meta
	Transport jfsmeta.Drive9Transport
	VFS       *vfs.VFS
	Reader    vfs.DataReader
	Writer    vfs.DataWriter
	Store     chunk.ChunkStore
	ChunkConf chunk.Config
	Storage   object.ObjectStorage
	// SessionID is the JuiceFS session this runtime created (the meta client
	// writes it back into the config drive9 owns). Unlink and rename report it as
	// the owner of a sustained inode, so a file that is removed or replaced while
	// a handle is still open is NOT reclaimed at mutation time: the node and its
	// blocks stay until the session that held it ends. See jfsUnlinkTx's opened
	// branch for why session end, not fd close, is when the row goes.
	SessionID uint64
}

type RuntimeConfig struct {
	// CacheDir is the drive9-side cache root for this runtime; JuiceFS keeps
	// its chunk cache in the jfs/ subdirectory. It defaults under the drive9
	// user cache dir. Empty means an in-memory cache, whose size CacheBytes
	// controls.
	CacheDir string
	// CacheBytes caps the local block cache; 0 means the mount default (1 GiB).
	// The server-side fallback compactor reads one chunk at a time and serves
	// no user reads, so it asks for a much smaller cache instead of reserving a
	// mount-sized one per tenant.
	CacheBytes int64
	// CacheKey namespaces the cache root per filesystem (tenant). Slice ids
	// restart at 1 in every tenant and the cache key carries no filesystem
	// identity, so two tenants sharing CacheDir would otherwise overwrite and
	// serve each other's blocks. Use CacheKeyForPrefix.
	CacheKey  string
	Transport jfsmeta.Drive9Transport
	Storage   object.ObjectStorage
}

// extentChunkCacheBytes is the mount-side block cache cap: JuiceFS --cache-size
// default is 1024 MiB (cmd/flags.go).
const extentChunkCacheBytes = 1 << 30

// juiceChunkBufferSize is JuiceFS --buffer-size default (cmd/flags.go).
// writer.Write throttles when process AllocMemory exceeds this; 32MiB (the
// SelfCheck floor) makes every sqlite write sleep 10–100ms.
const juiceChunkBufferSize = 300 << 20

// juiceMetaConf is JuiceFS meta.Config for an HTTP engine. Locks go to the
// authoritative metadata service (the drive9 engine's setlk/getlk/flock ops,
// which run in one TiDB transaction against jfs_plock/jfs_flock), so two mounts
// of the same filesystem really do exclude each other. --open-cache is 0s in
// JuiceFS because SQL/Redis GetAttr is local; JuiceFS documents enabling it
// when metadata is remote.
func juiceMetaConf() *jfsmeta.Config {
	conf := jfsmeta.DefaultConf()
	conf.NoBGJob = true
	// MaxDeletes must stay > 0: baseMeta.deleteSlice is the only path that
	// hands a dead slice to the server's delete_slice op (block GC). With 0,
	// the CAS loser of a compaction leaks the blob it uploaded, and slice GC
	// from truncate/overwrite leaks too. NoBGJob still keeps cleanupDeleted-
	// Files/Slices/Trash off (drive9 drains those server-side).
	conf.MaxDeletes = 10
	conf.Heartbeat = 12 * time.Second
	conf.OpenCache = time.Second
	conf.OpenCacheLimit = 10000
	conf.AtimeMode = jfsmeta.StrictAtime
	return conf
}

// applyChunkCacheDir is JuiceFS `mount --cache-dir DIR`: the local block cache
// the chunk store reads through.
func applyChunkCacheDir(conf *chunk.Config, cacheDir string) {
	if cacheDir == "" || conf == nil {
		return
	}
	conf.CacheDir = path.Join(cacheDir, "jfs")
	if conf.BufferSize < juiceChunkBufferSize {
		conf.BufferSize = juiceChunkBufferSize
	}
	if conf.Prefetch == 0 {
		conf.Prefetch = 1
	}
	conf.CacheFullBlock = true
	if conf.FreeSpace == 0 {
		conf.FreeSpace = 0.1
	}
	if conf.CacheMode == 0 {
		conf.CacheMode = 0o600
	}
}

// registerJuiceMetaMsg is JuiceFS cmd/mount.go registerMetaMsg. CompactChunk
// must upload the new blob (vfs.Compact) so baseMeta.compactChunk can CAS.
// Returning ErrCompactDelegated left chunks at thousands of slices: the
// backup claim_compact loop leased one task and never retried.
func registerJuiceMetaMsg(m jfsmeta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	if m == nil || store == nil || chunkConf == nil {
		return
	}
	m.OnMsg(jfsmeta.DeleteSlice, func(...interface{}) error { return nil })
	m.OnMsg(jfsmeta.CompactChunk, func(args ...interface{}) error {
		slices, id, tier, err := juiceCompactArgs(args)
		if err != nil {
			return err
		}
		return vfs.Compact(*chunkConf, store, slices, id, tier)
	})
}

func juiceCompactArgs(args []interface{}) ([]jfsmeta.Slice, uint64, uint8, error) {
	if len(args) < 3 {
		return nil, 0, 0, fmt.Errorf("compact: need slices, id, tier")
	}
	slices, ok := args[0].([]jfsmeta.Slice)
	if !ok {
		return nil, 0, 0, fmt.Errorf("compact: slices type %T", args[0])
	}
	id, ok := args[1].(uint64)
	if !ok {
		return nil, 0, 0, fmt.Errorf("compact: id type %T", args[1])
	}
	tier, _ := args[2].(uint8)
	return slices, id, tier, nil
}

func quietJuiceFSLogs() {
	for _, name := range []string{"juicefs", "object", "meta", "chunk", "vfs"} {
		utils.GetLogger(name).SetLevel(logrus.ErrorLevel)
	}
	utils.SetLogLevel(logrus.ErrorLevel)
}

// extentCacheRoot resolves the drive9-side cache root for the extent data
// plane: the caller's mount-scoped directory, else drive9's user cache dir
// (the same default the other drive9 caches use).
func extentCacheRoot(configured string) string {
	if configured != "" {
		return configured
	}
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "drive9")
}

// extentChunkCacheRoot is the JuiceFS cache root for one filesystem: the
// mount-scoped root plus the per-filesystem cache key. JuiceFS appends its own
// format UUID below it, so the final path is
// <configured>/jfs/<cacheKey>/<uuid>/raw.
func extentChunkCacheRoot(configured, cacheKey string) string {
	root := extentCacheRoot(configured)
	if cacheKey == "" {
		return root
	}
	return filepath.Join(root, cacheKey)
}

// CacheKeyForPrefix derives the per-filesystem cache key from the data
// credential prefix (server-side `t/<tenant>/`). Slice ids and the JuiceFS
// format UUID are identical across tenants, so the local cache tree must be
// namespaced by something tenant-stable; the credential prefix is stable
// across API key rotation.
func CacheKeyForPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(prefix))
	return hex.EncodeToString(sum[:8])
}

func NewRuntime(cfg RuntimeConfig) (rt *Runtime, err error) {
	// NewRuntime takes ownership of cfg.Storage: every error path releases it
	// (object.Shutdown(nil) is a no-op), so the caller never has to compensate
	// for a half-built runtime — a client that opened a GCS store would
	// otherwise leak its HTTP client.
	defer func() {
		if err != nil {
			object.Shutdown(cfg.Storage)
		}
	}()
	if cfg.Transport == nil {
		return nil, fmt.Errorf("extent runtime: missing meta transport")
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("extent runtime: missing object storage")
	}
	quietJuiceFSLogs()
	conf := juiceMetaConf()
	m := jfsmeta.NewDrive9Meta(conf, cfg.Transport)
	format := &jfsmeta.Format{
		Name:      "drive9",
		UUID:      "drive9-extent",
		Storage:   "file",
		BlockSize: 4 << 20,
		TrashDays: 0,
	}
	if err := m.Init(format, false); err != nil {
		_ = m.Init(format, true)
	}
	if _, err := m.Load(false); err != nil {
		return nil, fmt.Errorf("load extent format: %w", err)
	}
	if err := m.NewSession(true); err != nil {
		return nil, fmt.Errorf("extent session: %w", err)
	}
	// The block cache is a read cache: with Writeback off below, a write holds
	// no staged state here, so its size only trades local memory against
	// object-store reads.
	cacheBytes := cfg.CacheBytes
	if cacheBytes == 0 {
		cacheBytes = extentChunkCacheBytes
	}
	chunkConf := chunk.Config{
		BlockSize:      4 << 20,
		CacheSize:      uint64(cacheBytes),
		CacheDir:       "memory",
		MaxUpload:      20,
		MaxDownload:    200,
		Prefetch:       1,
		BufferSize:     juiceChunkBufferSize,
		GetTimeout:     time.Minute,
		PutTimeout:     time.Minute,
		AutoCreate:     true,
		CacheFullBlock: true,
		// Chunk-store staging stays off (upstream default): staging would let
		// Finish return before the object PUT, and drive9's fsync/close-sync/
		// write-sync contracts need the block in object storage first.
		Writeback: false,
	}
	if cfg.CacheDir != "" {
		// The cache dir is still the local block cache for reads (and the
		// memory-throttle/put-timeout knobs live there too). The per-filesystem
		// key keeps tenants apart.
		applyChunkCacheDir(&chunkConf, extentChunkCacheRoot(cfg.CacheDir, cfg.CacheKey))
	}
	loaded := m.GetFormat()
	chunkConf.SelfCheck(loaded.UUID)
	store := chunk.NewCachedStore(cfg.Storage, chunkConf, nil)
	registerJuiceMetaMsg(m, store, &chunkConf)
	vfsConf := &vfs.Config{
		Meta:            conf,
		Format:          loaded,
		Chunk:           &chunkConf,
		HideInternal:    true,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
		FuseOpts:        &vfs.FuseOptions{},
	}
	reader := vfs.NewDataReader(vfsConf, m, store)
	writer := vfs.NewDataWriter(vfsConf, m, store, reader)
	v := newIsolatedVFS(vfsConf, m, store)
	return &Runtime{
		Meta:      m,
		Transport: cfg.Transport,
		SessionID: conf.Sid,
		VFS:       v,
		Reader:    reader,
		Writer:    writer,
		Store:     store,
		ChunkConf: chunkConf,
		Storage:   cfg.Storage,
	}, nil
}

// CloseRuntime releases what a Runtime holds: the object store (for a store
// that owns a client, such as GCS) and the JuiceFS session (which flushes stats
// and stops the delete-slice tasks). Nothing else is stoppable through the
// ChunkStore interface — the cached store's cache monitor goroutine keeps
// running — so callers that build runtimes repeatedly (the server's compact
// fallback) must reuse one Runtime per tenant instead.
func CloseRuntime(rt *Runtime) error {
	if rt == nil {
		return nil
	}
	// Close the session first: session-owned background work may still be using
	// the store. Then release the store, which for GCS owns an HTTP client.
	var err error
	if rt.Meta != nil {
		err = rt.Meta.CloseSession()
	}
	object.Shutdown(rt.Storage)
	return err
}

var vfsStateMu sync.Mutex

// newIsolatedVFS builds vfs.NewVFS without loading a leftover JuiceFS
// /tmp/state<ppid>.json from an unrelated mount of the same parent pid.
func newIsolatedVFS(conf *vfs.Config, m jfsmeta.Meta, store chunk.ChunkStore) *vfs.VFS {
	vfsStateMu.Lock()
	defer vfsStateMu.Unlock()
	prev, had := os.LookupEnv("_FUSE_STATE_PATH")
	p := filepath.Join(os.TempDir(), fmt.Sprintf("drive9-jfs-vfs-%d-%d.json", os.Getpid(), time.Now().UnixNano()))
	_ = os.Setenv("_FUSE_STATE_PATH", p)
	defer func() {
		if had {
			_ = os.Setenv("_FUSE_STATE_PATH", prev)
		} else {
			_ = os.Unsetenv("_FUSE_STATE_PATH")
		}
		_ = os.Remove(p)
		_ = os.Remove(p + ".bak")
	}()
	return vfs.NewVFS(conf, m, store, nil, nil)
}
