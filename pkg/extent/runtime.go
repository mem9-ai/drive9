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
}

type RuntimeConfig struct {
	// CacheDir is the drive9-side cache root for this runtime; JuiceFS keeps
	// its chunk cache and writeback staging in the jfs/ subdirectory. It
	// defaults under the drive9 user cache dir when Writeback is requested.
	CacheDir string
	// CacheKey namespaces the cache root per filesystem (tenant). Slice ids
	// restart at 1 in every tenant and the cache key carries no filesystem
	// identity, so two tenants sharing CacheDir would otherwise overwrite and
	// serve each other's blocks. Use CacheKeyForPrefix.
	CacheKey string
	// Writeback stages writes locally and uploads them in the background
	// (JuiceFS --writeback). FUSE mounts always enable it.
	Writeback bool
	Transport jfsmeta.Drive9Transport
	Storage   object.ObjectStorage
}

// juiceFuseOpts is JuiceFS `-o writeback_cache`. VFS.Read of O_WRONLY
// handles is EBADF unless EnableWriteback is set; the kernel cap is
// negotiated separately in Dat9FS go-fuse MountOptions.
func juiceFuseOpts() *vfs.FuseOptions {
	return &vfs.FuseOptions{EnableWriteback: true}
}

// juiceWritebackBufferSize is JuiceFS --buffer-size default (cmd/flags.go).
// writer.Write throttles when process AllocMemory exceeds this; 32MiB (the
// SelfCheck floor) makes every sqlite write sleep 10–100ms.
const juiceWritebackBufferSize = 300 << 20

// juiceMetaConf is JuiceFS meta.Config for an HTTP engine. Locks stay
// in-process (memLockMeta). --open-cache is 0s in JuiceFS because SQL/Redis
// GetAttr is local; JuiceFS documents enabling it when metadata is remote.
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

// applyChunkCacheDir is JuiceFS `mount --cache-dir DIR`. UploadDelay stays 0
// (JuiceFS default); fsync returns after local stage and the same goroutine
// may PUT afterwards. fuse-crash-recovery remounts the same cache dir so
// scanStaging uploads leftover blocks.
func applyChunkCacheDir(conf *chunk.Config, cacheDir string) {
	if cacheDir == "" || conf == nil {
		return
	}
	conf.CacheDir = path.Join(cacheDir, "jfs")
	conf.UploadDelay = 0
	if conf.BufferSize < juiceWritebackBufferSize {
		conf.BufferSize = juiceWritebackBufferSize
	}
	if conf.MaxStageWrite == 0 {
		conf.MaxStageWrite = 1000
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
// (the same default the other drive9 caches use). JuiceFS writeback needs
// local staging space, so the root is never left empty.
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
// <configured>/jfs/<cacheKey>/<uuid>/{raw,rawstaging}.
func extentChunkCacheRoot(configured, cacheKey string) string {
	root := extentCacheRoot(configured)
	if cacheKey == "" {
		return root
	}
	return filepath.Join(root, cacheKey)
}

// CacheKeyForPrefix derives the per-filesystem cache key from the data
// credential prefix (server-side `t/<tenant>/`). Slice ids and the JuiceFS
// format UUID are identical across tenants, so the local cache/staging tree
// must be namespaced by something tenant-stable; the credential prefix is
// stable across API key rotation.
func CacheKeyForPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(prefix))
	return hex.EncodeToString(sum[:8])
}

func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if cfg.Transport == nil {
		return nil, fmt.Errorf("extent runtime: missing meta transport")
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("extent runtime: missing object storage")
	}
	quietJuiceFSLogs()
	conf := juiceMetaConf()
	m := wrapLockMeta(jfsmeta.NewDrive9Meta(conf, cfg.Transport))
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
	chunkConf := chunk.Config{
		BlockSize:      4 << 20,
		CacheSize:      1 << 30,
		CacheDir:       "memory",
		MaxUpload:      20,
		MaxDownload:    200,
		MaxStageWrite:  1000,
		Prefetch:       1,
		BufferSize:     juiceWritebackBufferSize,
		GetTimeout:     time.Minute,
		PutTimeout:     time.Minute,
		AutoCreate:     true,
		CacheFullBlock: true,
		Writeback:      cfg.Writeback,
	}
	if cfg.CacheDir != "" || cfg.Writeback {
		// Writeback is never left without a cache root: an empty dir would
		// make chunk.SelfCheck fall back to memory mode and silently drop
		// writeback. The per-filesystem key keeps tenants apart.
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
		// JuiceFS fuse Open/Read: writeback_cache needs a reader on
		// O_WRONLY handles so kernel page-cache writeback can Read.
		FuseOpts: juiceFuseOpts(),
	}
	reader := vfs.NewDataReader(vfsConf, m, store)
	writer := vfs.NewDataWriter(vfsConf, m, store, reader)
	v := newIsolatedVFS(vfsConf, m, store)
	return &Runtime{
		Meta:      m,
		Transport: cfg.Transport,
		VFS:       v,
		Reader:    reader,
		Writer:    writer,
		Store:     store,
		ChunkConf: chunkConf,
		Storage:   cfg.Storage,
	}, nil
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
