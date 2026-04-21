package fuse

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

// VaultFS is a read-only FUSE filesystem that exposes the vault secrets
// readable by the bound capability/owner credential as a two-level tree:
//
//	<mount>/<secret-name>/<field-name>
//
// Layout decisions (V2e sealed contract rows A–I):
//   - Row C — read-only: every mutating op (Mkdir, Unlink, Create, Write,
//     Setattr, Rename, Rmdir, …) returns EROFS via the embedded default
//     RawFileSystem. We do NOT inherit the writable Dat9FS path.
//   - Row D — scope visibility: root readdir is the result of
//     ListReadableVaultSecrets(); the server enforces per-token scope
//     (Invariant #7), and we do not augment or filter client-side.
//   - Row E — new-op after revoke: each readdir/open re-issues the
//     consumption call, so a 403 from the server cleanly maps to EACCES.
//   - Row F — existing FD after revoke must NEVER serve V2: at Open() time
//     we materialise the field bytes once into the handle. Subsequent Read()
//     calls are served from that snapshot — no re-fetch on the hot path.
//     If the server later rotates the value (V2), the open FD continues to
//     serve V1 (or returns EIO if its snapshot was lost), but never V2.
//   - Row G — settle after put: per-secret field maps live behind a TTL
//     cache (DirTTL); after expiry the next readdir/open fetches fresh.
//   - Row H — TTL: enforced by vaultCache.refreshAfter == DirTTL.
//   - Row I — empty-scope = EACCES: Mount refuses to start if the bearer
//     has zero readable secrets at probe time.
//
// Inode allocation: we use a stable map keyed by absolute path. Inode 1 is
// the root; every distinct secret directory and field file gets a fresh
// inode the first time the kernel asks about it. We never recycle.
type VaultFS struct {
	gofuse.RawFileSystem

	client *client.Client
	dirTTL time.Duration

	mu          sync.Mutex
	pathToInode map[string]uint64
	inodeToPath map[uint64]string
	nextInode   uint64

	// rootCache caches the secret-name list at the root.
	rootCache       []string
	rootCacheExpiry time.Time

	// secretCache caches per-secret field maps so that Lookup → GetAttr →
	// Open from the kernel for a single field share one server round-trip.
	secretCache map[string]secretCacheEntry

	// openFiles snapshots field bytes at Open() time so that revoke during
	// an open FD never produces a post-revoke value (Row F).
	openFiles map[uint64]*vaultOpenFile
	openSeq   uint64

	uid uint32
	gid uint32
}

type secretCacheEntry struct {
	fields map[string]string
	expiry time.Time
}

type vaultOpenFile struct {
	data []byte
}

// NewVaultFS builds a read-only vault filesystem bound to c. dirTTL controls
// how long secret-list / field-map results are cached before refresh.
func NewVaultFS(c *client.Client, dirTTL time.Duration) *VaultFS {
	if dirTTL <= 0 {
		dirTTL = 5 * time.Second
	}
	fs := &VaultFS{
		RawFileSystem: gofuse.NewDefaultRawFileSystem(),
		client:        c,
		dirTTL:        dirTTL,
		pathToInode:   make(map[string]uint64),
		inodeToPath:   make(map[uint64]string),
		nextInode:     2, // 1 is reserved for root
		secretCache:   make(map[string]secretCacheEntry),
		openFiles:     make(map[uint64]*vaultOpenFile),
		uid:           uint32(os.Getuid()),
		gid:           uint32(os.Getgid()),
	}
	fs.pathToInode["/"] = 1
	fs.inodeToPath[1] = "/"
	return fs
}

// inodeForPath returns a stable inode for path, allocating one if needed.
func (fs *VaultFS) inodeForPath(p string) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if ino, ok := fs.pathToInode[p]; ok {
		return ino
	}
	ino := fs.nextInode
	fs.nextInode++
	fs.pathToInode[p] = ino
	fs.inodeToPath[ino] = p
	return ino
}

func (fs *VaultFS) pathForInode(ino uint64) (string, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	p, ok := fs.inodeToPath[ino]
	return p, ok
}

// listSecrets returns the cached or freshly-fetched list of readable secrets.
func (fs *VaultFS) listSecrets(ctx context.Context) ([]string, error) {
	fs.mu.Lock()
	if time.Now().Before(fs.rootCacheExpiry) && fs.rootCache != nil {
		out := append([]string(nil), fs.rootCache...)
		fs.mu.Unlock()
		return out, nil
	}
	fs.mu.Unlock()

	secrets, err := fs.client.ListReadableVaultSecrets(ctx)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	fs.rootCache = append([]string(nil), secrets...)
	fs.rootCacheExpiry = time.Now().Add(fs.dirTTL)
	fs.mu.Unlock()
	return secrets, nil
}

// readSecret returns the cached or freshly-fetched field map for secretName.
func (fs *VaultFS) readSecret(ctx context.Context, secretName string) (map[string]string, error) {
	fs.mu.Lock()
	if entry, ok := fs.secretCache[secretName]; ok && time.Now().Before(entry.expiry) {
		out := make(map[string]string, len(entry.fields))
		for k, v := range entry.fields {
			out[k] = v
		}
		fs.mu.Unlock()
		return out, nil
	}
	fs.mu.Unlock()

	fields, err := fs.client.ReadVaultSecret(ctx, secretName)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	fs.secretCache[secretName] = secretCacheEntry{
		fields: fields,
		expiry: time.Now().Add(fs.dirTTL),
	}
	fs.mu.Unlock()
	return fields, nil
}

// splitPath turns "/secret/field" into ("secret", "field"). Empty components
// are returned for shorter paths.
func splitVaultPath(p string) (secret, field string) {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func vaultJoin(parts ...string) string {
	out := "/"
	for _, p := range parts {
		if p == "" {
			continue
		}
		if !strings.HasSuffix(out, "/") {
			out += "/"
		}
		out += p
	}
	return out
}

// httpStatusToFuse maps an HTTP error from the vault client into a FUSE
// status, mirroring httpToFuseStatus but local to the vault filesystem so
// the writable filesystem's wider mapping doesn't bleed in.
func httpStatusToVaultFuse(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusNotFound:
			return gofuse.ENOENT
		case http.StatusForbidden, http.StatusUnauthorized:
			return gofuse.EACCES
		case http.StatusBadRequest:
			return gofuse.Status(syscall.EINVAL)
		case http.StatusGatewayTimeout, http.StatusRequestTimeout:
			return gofuse.Status(syscall.ETIMEDOUT)
		}
		if se.StatusCode >= 500 {
			return gofuse.EIO
		}
		return gofuse.EIO
	}
	return gofuse.EIO
}

// fillDirAttr populates an Attr struct for a directory inode.
func (fs *VaultFS) fillDirAttr(out *gofuse.Attr, ino uint64) {
	out.Ino = ino
	out.Mode = syscall.S_IFDIR | 0o555
	out.Nlink = 2
	out.Uid = fs.uid
	out.Gid = fs.gid
}

// fillFileAttr populates an Attr struct for a regular field inode.
func (fs *VaultFS) fillFileAttr(out *gofuse.Attr, ino uint64, size int64) {
	out.Ino = ino
	out.Mode = syscall.S_IFREG | 0o444
	out.Nlink = 1
	out.Size = uint64(size)
	out.Uid = fs.uid
	out.Gid = fs.gid
}

// Init records the server pointer (currently unused; kept for symmetry with
// Dat9FS so future inode-notify wiring is a one-liner).
func (fs *VaultFS) Init(server *gofuse.Server) {
	_ = server
}

func (fs *VaultFS) StatFs(cancel <-chan struct{}, header *gofuse.InHeader, out *gofuse.StatfsOut) gofuse.Status {
	out.Bsize = 4096
	out.NameLen = 255
	return gofuse.OK
}

// ----- read path -----

func (fs *VaultFS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	parentPath, ok := fs.pathForInode(header.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()

	switch parentPath {
	case "/":
		// Looking up a secret name under root.
		secrets, err := fs.listSecrets(ctx)
		if err != nil {
			return httpStatusToVaultFuse(err)
		}
		for _, s := range secrets {
			if s == name {
				p := vaultJoin(name)
				ino := fs.inodeForPath(p)
				fs.fillDirAttr(&out.Attr, ino)
				out.NodeId = ino
				out.SetEntryTimeout(fs.dirTTL)
				out.SetAttrTimeout(fs.dirTTL)
				return gofuse.OK
			}
		}
		return gofuse.ENOENT
	default:
		secretName, sub := splitVaultPath(parentPath)
		if secretName == "" || sub != "" {
			// We only have two levels.
			return gofuse.ENOENT
		}
		fields, err := fs.readSecret(ctx, secretName)
		if err != nil {
			return httpStatusToVaultFuse(err)
		}
		val, ok := fields[name]
		if !ok {
			return gofuse.ENOENT
		}
		p := vaultJoin(secretName, name)
		ino := fs.inodeForPath(p)
		fs.fillFileAttr(&out.Attr, ino, int64(len(val)))
		out.NodeId = ino
		out.SetEntryTimeout(fs.dirTTL)
		out.SetAttrTimeout(fs.dirTTL)
		return gofuse.OK
	}
}

func (fs *VaultFS) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	p, ok := fs.pathForInode(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	out.SetTimeout(fs.dirTTL)
	if p == "/" {
		fs.fillDirAttr(&out.Attr, input.NodeId)
		return gofuse.OK
	}
	secretName, fieldName := splitVaultPath(p)
	if fieldName == "" {
		fs.fillDirAttr(&out.Attr, input.NodeId)
		return gofuse.OK
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	fields, err := fs.readSecret(ctx, secretName)
	if err != nil {
		return httpStatusToVaultFuse(err)
	}
	val, ok := fields[fieldName]
	if !ok {
		return gofuse.ENOENT
	}
	fs.fillFileAttr(&out.Attr, input.NodeId, int64(len(val)))
	return gofuse.OK
}

func (fs *VaultFS) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	if _, ok := fs.pathForInode(input.NodeId); !ok {
		return gofuse.ENOENT
	}
	return gofuse.OK
}

func (fs *VaultFS) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDirCommon(cancel, input, out, false)
}

func (fs *VaultFS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	return fs.readDirCommon(cancel, input, out, true)
}

func (fs *VaultFS) readDirCommon(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList, plus bool) gofuse.Status {
	p, ok := fs.pathForInode(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()

	type entry struct {
		name string
		mode uint32
		ino  uint64
		size int64
	}
	var entries []entry

	if p == "/" {
		secrets, err := fs.listSecrets(ctx)
		if err != nil {
			return httpStatusToVaultFuse(err)
		}
		for _, s := range secrets {
			ino := fs.inodeForPath(vaultJoin(s))
			entries = append(entries, entry{name: s, mode: syscall.S_IFDIR, ino: ino})
		}
	} else {
		secretName, sub := splitVaultPath(p)
		if secretName == "" || sub != "" {
			return gofuse.ENOENT
		}
		fields, err := fs.readSecret(ctx, secretName)
		if err != nil {
			return httpStatusToVaultFuse(err)
		}
		for k, v := range fields {
			ino := fs.inodeForPath(vaultJoin(secretName, k))
			entries = append(entries, entry{name: k, mode: syscall.S_IFREG, ino: ino, size: int64(len(v))})
		}
	}

	for i := int(input.Offset); i < len(entries); i++ {
		e := entries[i]
		de := gofuse.DirEntry{Name: e.name, Ino: e.ino, Mode: e.mode}
		if plus {
			eout := out.AddDirLookupEntry(de)
			if eout == nil {
				break
			}
			if e.mode == syscall.S_IFDIR {
				fs.fillDirAttr(&eout.Attr, e.ino)
			} else {
				fs.fillFileAttr(&eout.Attr, e.ino, e.size)
			}
			eout.NodeId = e.ino
			eout.SetEntryTimeout(fs.dirTTL)
			eout.SetAttrTimeout(fs.dirTTL)
		} else {
			if !out.AddDirEntry(de) {
				break
			}
		}
	}
	return gofuse.OK
}

func (fs *VaultFS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	// Reject any write/truncate mode at the door (Row C).
	if input.Flags&uint32(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE) != 0 {
		return gofuse.EROFS
	}
	p, ok := fs.pathForInode(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	secretName, fieldName := splitVaultPath(p)
	if secretName == "" || fieldName == "" {
		return gofuse.EISDIR
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	// Bypass the cache for the open call: the snapshot we take here is the
	// only data the FD will ever serve (Row F), so we want it as fresh as
	// the bearer can legitimately read RIGHT NOW. Subsequent revocation
	// must not produce a V2 value via this FD.
	fields, err := fs.client.ReadVaultSecret(ctx, secretName)
	if err != nil {
		return httpStatusToVaultFuse(err)
	}
	val, ok := fields[fieldName]
	if !ok {
		return gofuse.ENOENT
	}
	snapshot := []byte(val)

	fs.mu.Lock()
	fs.openSeq++
	fh := fs.openSeq
	fs.openFiles[fh] = &vaultOpenFile{data: snapshot}
	// Refresh the secretCache so neighbouring lookups see a coherent size.
	fs.secretCache[secretName] = secretCacheEntry{
		fields: fields,
		expiry: time.Now().Add(fs.dirTTL),
	}
	fs.mu.Unlock()

	out.Fh = fh
	// DIRECT_IO so the kernel page cache can't mix V1 bytes from one FD
	// with V2 bytes a future open might see. Each open is its own snapshot.
	out.OpenFlags = gofuse.FOPEN_DIRECT_IO
	return gofuse.OK
}

func (fs *VaultFS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	fs.mu.Lock()
	of, ok := fs.openFiles[input.Fh]
	fs.mu.Unlock()
	if !ok {
		return nil, gofuse.EBADF
	}
	off := int64(input.Offset)
	if off >= int64(len(of.data)) {
		return gofuse.ReadResultData(nil), gofuse.OK
	}
	end := off + int64(len(buf))
	if end > int64(len(of.data)) {
		end = int64(len(of.data))
	}
	return gofuse.ReadResultData(of.data[off:end]), gofuse.OK
}

func (fs *VaultFS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	fs.mu.Lock()
	delete(fs.openFiles, input.Fh)
	fs.mu.Unlock()
}

func (fs *VaultFS) ReleaseDir(input *gofuse.ReleaseIn) {}

func (fs *VaultFS) Forget(nodeId uint64, nlookup uint64) {}

// ----- write path: every mutation rejected with EROFS (Row C) -----

func (fs *VaultFS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	return 0, gofuse.EROFS
}
func (fs *VaultFS) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Symlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo string, linkName string, out *gofuse.EntryOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Link(cancel <-chan struct{}, input *gofuse.LinkIn, name string, out *gofuse.EntryOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Mknod(cancel <-chan struct{}, input *gofuse.MknodIn, name string, out *gofuse.EntryOut) gofuse.Status {
	return gofuse.EROFS
}
func (fs *VaultFS) Fallocate(cancel <-chan struct{}, input *gofuse.FallocateIn) gofuse.Status {
	return gofuse.EROFS
}
