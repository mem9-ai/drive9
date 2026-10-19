package extent

// Pure-function rows ported from v1 pkg/fuse/extent_policy_test.go: the
// JuiceFS write-offset append rules, open-flag mapping, and the fuse-context
// interrupt semantics.

import (
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestJuiceWriteOffset.
func TestPortJuiceWriteOffset(t *testing.T) {
	if got := juiceWriteOffset(0, 216, true); got != 216 {
		t.Fatalf("O_APPEND syscall write off=%d, want EOF 216", got)
	}
	if got := juiceWriteOffset(100, 216, true); got != 100 {
		t.Fatalf("O_APPEND with a kernel offset=%d, want 100", got)
	}
	if got := juiceWriteOffset(0, 216, false); got != 0 {
		t.Fatalf("non-append write off=%d, want 0", got)
	}
}

// v1 TestExtentOpenFlags.
func TestPortExtentOpenFlags(t *testing.T) {
	if got := extentOpenFlags(true); got != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("keepCache open flags = %#x, want FOPEN_KEEP_CACHE", got)
	}
	// Drop-cache is now an explicit facade marker (a plain 0 could not be
	// distinguished from "no opinion" and cancelled close-to-open
	// invalidation between mounts).
	if got := extentOpenFlags(false); got != fs.OpenFlagDropKernelCache {
		t.Fatalf("default open flags = %#x, want the drop-cache marker", got)
	}
}

// v1 TestJfsCtxCancelHonorsFuseCancel: JuiceFS Canceled() ignores the FUSE
// interrupt for the first second, then sees it.
func TestPortJfsCtxCancelHonorsFuseCancel(t *testing.T) {
	cancel := make(chan struct{})
	ctx := &jfsFuseContext{cancel: cancel, started: time.Now().Add(-2 * time.Second)}
	if ctx.Canceled() {
		t.Fatal("context canceled before FUSE cancel")
	}
	close(cancel)
	if !ctx.Canceled() {
		t.Fatal("JuiceFS Canceled() must see FUSE cancel after the first second")
	}
	// Fresh context: the interrupt is ignored during the first second.
	fresh := make(chan struct{})
	close(fresh)
	young := &jfsFuseContext{cancel: fresh, started: time.Now()}
	if young.Canceled() {
		t.Fatal("JuiceFS Canceled() must ignore FUSE cancel for the first second")
	}
}

// v1 TestJfsCtxCancelLockBlockValue: blocking lock contexts see interrupts
// immediately.
func TestPortJfsCtxCancelLockBlockValue(t *testing.T) {
	cancel := make(chan struct{})
	ctx := &jfsFuseContext{cancel: cancel, lockBlock: true, started: time.Now()}
	if ctx.Canceled() {
		t.Fatal("blocking lock context canceled before FUSE cancel")
	}
	close(cancel)
	if !ctx.Canceled() {
		t.Fatal("blocking lock context must see FUSE cancel immediately")
	}
}

// ---------------------------------------------------------------------------
// Further rows from v1 pkg/fuse/extent_policy_test.go (d39a6d1b3): the
// ShouldUseExtentPath glob family, the full juiceTypeToStatMode type table
// with the posixPermissionModeMask=0o7777 fix, clampExtentTTL, the TTL-hint /
// keep-cache surface, remote-path prefixing, mknod special classification,
// and the WAL-index bookkeeping. Facade-only rows (inode tables, rename
// finish, dir-cache listings, EntryOut timeouts) live above the extension
// boundary and are not portable here.

// v1 TestMatchExtentPattern table, including the "*-wal" suffix row and the
// whitespace-trimmed pattern row.
func TestPortMatchExtentPatternV1Table(t *testing.T) {
	cases := []struct {
		pat, path string
		want      bool
	}{
		{"*", "/foo.db", true},
		{"*.db", "/a/foo.db", true},
		{"*.DB", "/a/foo.db", true},
		{"*.db", "/a/foo.txt", false},
		{"*.db", "/a/foo.db-wal", false},
		{"*-wal", "/x.db-wal", true},
		{"foo.db", "/foo.db", true},
		{"FOO.DB", "/foo.db", true},
		{"  *.db  ", "/a/foo.db", true}, // pattern-side whitespace trims
		{"data*", "/x/database", true},
		{"", "/whatever", false},
	}
	for _, c := range cases {
		if got := matchExtentPattern(c.pat, c.path); got != c.want {
			t.Errorf("matchExtentPattern(%q,%q)=%v want %v", c.pat, c.path, got, c.want)
		}
	}
}

// v1 TestShouldUseExtentPath + TestShouldUseExtentPathSkipsDirectories.
func TestPortShouldUseExtentPathV1Rows(t *testing.T) {
	pats := []string{"*.db", "*-wal"}
	if !ShouldUseExtentPath(pats, "/tmp/x.db") {
		t.Fatal("*.db should match")
	}
	if !ShouldUseExtentPath(pats, "/tmp/x.db-wal") {
		t.Fatal("*-wal should match")
	}
	if ShouldUseExtentPath(pats, "/tmp/x.txt") {
		t.Fatal("txt must not match")
	}
	if !ShouldUseExtentPath([]string{"*"}, "/anything") {
		t.Fatal("* should match everything")
	}
	if ShouldUseExtentPath([]string{"*"}, "/dir/") {
		t.Fatal("directories never take the extent path")
	}
	if ShouldUseExtentPath(nil, "/app/app.db") {
		t.Fatal("no patterns must disable")
	}
}

// v1 TestJuiceTypeToStatMode full family: every JuiceFS type maps to its
// stat kind, and an unknown type falls back to a regular file.
func TestPortJuiceTypeToStatModeFullFamily(t *testing.T) {
	rows := []struct {
		typ  uint8
		kind uint32
	}{
		{jfsmeta.TypeFile, syscall.S_IFREG},
		{jfsmeta.TypeDirectory, syscall.S_IFDIR},
		{jfsmeta.TypeSymlink, syscall.S_IFLNK},
		{jfsmeta.TypeFIFO, syscall.S_IFIFO},
		{jfsmeta.TypeBlockDev, syscall.S_IFBLK},
		{jfsmeta.TypeCharDev, syscall.S_IFCHR},
		{jfsmeta.TypeSocket, syscall.S_IFSOCK},
		{0xFE, syscall.S_IFREG}, // unknown type degrades to regular file
	}
	for _, r := range rows {
		if got := juiceTypeToStatMode(r.typ, 0o644); got&syscall.S_IFMT != r.kind {
			t.Errorf("type %d mode = %o, want kind %o", r.typ, got, r.kind)
		}
	}
	if got := juiceTypeToStatMode(jfsmeta.TypeFile, 0o642); got&0o777 != 0o642 {
		t.Fatalf("file perm = %o, want 0642", got&0o777)
	}
}

// posixPermissionModeMask is exactly 0o7777 (the 12 permission bits incl.
// setuid/setgid/sticky): the mask keeps special bits for POSIX chmod parity
// while never letting the uint16 perm bleed into the stat type nibble.
func TestPortPosixPermissionModeMask(t *testing.T) {
	if posixPermissionModeMask != 0o7777 {
		t.Fatalf("posixPermissionModeMask = %o, want 0o7777", posixPermissionModeMask)
	}
	// setuid+setgid+sticky survive the mapping.
	if got := juiceTypeToStatMode(jfsmeta.TypeFile, 0o4755) & 0o7777; got != 0o4755 {
		t.Fatalf("special bits = %o, want 04755 preserved", got)
	}
	// Bits above 0o7777 must be dropped, not spliced into the kind nibble.
	full := juiceTypeToStatMode(jfsmeta.TypeDirectory, 0o17777)
	if full&syscall.S_IFMT != syscall.S_IFDIR {
		t.Fatalf("kind nibble contaminated by perm: %o", full)
	}
	if full&0o7777 != 0o7777 {
		t.Fatalf("perm = %o, want masked to 0o7777", full&0o7777)
	}
	if got := juiceTypeToStatMode(jfsmeta.TypeFile, 0o10000); got != syscall.S_IFREG {
		t.Fatalf("out-of-range perm leaked into mode: %o", got)
	}
}

// v1 clampExtentTTL / TestExtentAttrTimeoutZeroWhenHardlinked family: the
// mount's configured TTL is an upper bound; a deliberate zero stays zero; the
// extent cadence is the JuiceFS --attr-cache default (1s).
func TestPortClampExtentTTLFamily(t *testing.T) {
	if juiceFSAttrTimeout != time.Second {
		t.Fatalf("juiceFSAttrTimeout = %v, want the JuiceFS 1.0s attr-cache default", juiceFSAttrTimeout)
	}
	if got := clampExtentTTL(juiceFSAttrTimeout, 5*time.Second); got != juiceFSAttrTimeout {
		t.Fatalf("clamp with longer configured = %v, want %v", got, juiceFSAttrTimeout)
	}
	if got := clampExtentTTL(juiceFSAttrTimeout, juiceFSAttrTimeout); got != juiceFSAttrTimeout {
		t.Fatalf("clamp with equal configured = %v, want %v", got, juiceFSAttrTimeout)
	}
	if got := clampExtentTTL(juiceFSAttrTimeout, 100*time.Millisecond); got != 100*time.Millisecond {
		t.Fatalf("configured below extent ttl must win: %v", got)
	}
	if got := clampExtentTTL(0, time.Second); got != 0 {
		t.Fatalf("deliberate zero must stay zero: %v", got)
	}
	if got := clampExtentTTL(juiceFSAttrTimeout, 0); got != 0 {
		t.Fatalf("configured zero clamps everything to zero: %v", got)
	}
}

// v1 TestExtentFillEntryOutUsesJuiceFSAttrCache, at the new boundary: the
// TTL hint exposes the JuiceFS cadence for bound/matching paths only, and
// DirKeepCacheOff turns off kernel listing cache for bound directories.
func TestPortEntryTTLHintAndDirKeepCache(t *testing.T) {
	e, err := New(Options{ExtentPaths: []string{"*.db"}})
	if err != nil {
		t.Fatal(err)
	}
	if ttl, ok := e.EntryTTLHint("/plain.txt"); ok || ttl != 0 {
		t.Fatalf("non-matching path hint = (%v,%v), want none", ttl, ok)
	}
	if ttl, ok := e.EntryTTLHint("/app/app.db"); !ok || ttl != clampExtentTTL(juiceFSAttrTimeout, juiceFSAttrTimeout) {
		t.Fatalf("matching path hint = (%v,%v), want the JuiceFS attr timeout", ttl, ok)
	}
	e.rememberExtentIno("/bound", 9)
	if ttl, ok := e.EntryTTLHint("/bound"); !ok || ttl != juiceFSAttrTimeout {
		t.Fatalf("bound path hint = (%v,%v), want the JuiceFS attr timeout", ttl, ok)
	}
	if !e.DirKeepCacheOff("/bound") {
		t.Fatal("bound directory must re-list on every open")
	}
	if e.DirKeepCacheOff("/app") {
		t.Fatal("unbound directory must keep the kernel listing cache")
	}
	off, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if ttl, ok := off.EntryTTLHint("/app/app.db"); ok || ttl != 0 {
		t.Fatalf("disabled mount hint = (%v,%v), want none", ttl, ok)
	}
	if off.DirKeepCacheOff("/any") {
		t.Fatal("disabled mount must not disable listing cache")
	}
}

// v1 TestRemotePathPrefixesExtentProjection: engine-facing paths carry the
// remote-root prefix; a root mount passes through.
func TestPortRemotePathPrefixesExtentProjection(t *testing.T) {
	e, err := New(Options{RemoteRoot: "/drive9-fuse-supervise-1"})
	if err != nil {
		t.Fatal(err)
	}
	got := e.remotePath("/cache-invalidation-x/rename-src.txt")
	if want := "/drive9-fuse-supervise-1/cache-invalidation-x/rename-src.txt"; got != want {
		t.Fatalf("remotePath = %q, want %q", got, want)
	}
	if got := e.remotePath("/"); got != "/drive9-fuse-supervise-1" {
		t.Fatalf("root remotePath = %q, want the prefix itself", got)
	}
	rooted, err := New(Options{RemoteRoot: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if got := rooted.remotePath("/cache-invalidation-x/rename-src.txt"); got != "/cache-invalidation-x/rename-src.txt" {
		t.Fatalf("root mount remotePath = %q, want passthrough", got)
	}
}

// v1 TestJuiceDirCreateModeUsesStickyOwner / metadata-only specials: only
// FIFO/CHR/BLK/SOCK creations are metadata-only specials.
func TestPortMetadataOnlySpecialMode(t *testing.T) {
	for _, mode := range []uint32{syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK} {
		if !metadataOnlySpecialMode(mode | 0o644) {
			t.Errorf("mode %o must classify as metadata-only special", mode|0o644)
		}
	}
	for _, mode := range []uint32{syscall.S_IFREG, syscall.S_IFDIR} {
		if metadataOnlySpecialMode(mode | 0o755) {
			t.Errorf("mode %o must not classify as metadata-only special", mode|0o755)
		}
	}
}

// v1 TestExtentKeepOpenWALIndexSize: a still-open sqlite -shm index reports
// the mapping floor; closed or non-shm paths report the raw size.
func TestPortKeepOpenWALIndexSize(t *testing.T) {
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	const shm = "/sqlite/mptest-wal-multiwrite01.db-shm"
	attr := fs.Attr{Size: 0}
	e.keepOpenWALIndexSize(shm, &attr)
	if attr.Size != 0 {
		t.Fatalf("closed shm size = %d, want 0", attr.Size)
	}
	e.trackLive(shm)
	e.keepOpenWALIndexSize(shm, &attr)
	if attr.Size != sqliteWALIndexMinSize {
		t.Fatalf("open empty shm size = %d, want %d", attr.Size, sqliteWALIndexMinSize)
	}
	nonZero := fs.Attr{Size: 4096}
	e.keepOpenWALIndexSize(shm, &nonZero)
	if nonZero.Size != 4096 {
		t.Fatalf("non-zero size overwritten: %d", nonZero.Size)
	}
	plain := fs.Attr{Size: 0}
	e.keepOpenWALIndexSize("/sqlite/notes.txt", &plain)
	if plain.Size != 0 {
		t.Fatalf("non-shm path touched: %d", plain.Size)
	}
}

// v1 WAL-index path shape: "<db>-shm" only, never a bare "-shm".
func TestPortIsSQLiteWALIndexPath(t *testing.T) {
	rows := []struct {
		path string
		want bool
	}{
		{"/repo/workload.db-shm", true},
		{"/repo/workload-shm", true},
		{"/repo/-shm", false},
		{"/repo/workload.db-wal", false},
		{"/repo/notes.txt", false},
	}
	for _, r := range rows {
		if got := isSQLiteWALIndexPath(r.path); got != r.want {
			t.Errorf("isSQLiteWALIndexPath(%q) = %v, want %v", r.path, got, r.want)
		}
	}
}

// v1 inode/miss bookkeeping: a remembered ino clears the miss, forgetIno
// drops both kinds of evidence, retargetLive moves the open-handle marker,
// subtreeMirrored walks ancestors, and the negative cache is bounded.
func TestPortInoAndMissBookkeeping(t *testing.T) {
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.missRemember("/f")
	if !e.missCached("/f") {
		t.Fatal("miss not cached")
	}
	e.rememberExtentIno("/f", 7)
	if e.missCached("/f") {
		t.Fatal("remembered ino must clear the miss")
	}
	if ino, ok := e.cachedIno("/f"); !ok || ino != 7 {
		t.Fatalf("cached ino = %d %v, want 7", ino, ok)
	}
	if !e.subtreeMirrored("/f/child/grand") {
		t.Fatal("ancestor ino must mark the subtree mirrored")
	}
	if e.subtreeMirrored("/other/child") {
		t.Fatal("unrelated subtree reported mirrored")
	}
	e.missRemember("/g")
	e.forgetIno("/f")
	if _, ok := e.cachedIno("/f"); ok {
		t.Fatal("forgetIno must drop the ino mapping")
	}
	if e.subtreeMirrored("/f/child") {
		t.Fatal("subtree still mirrored after forgetIno")
	}

	e.trackLive("/a")
	e.retargetLive("/a", "/b")
	if e.hasLive("/a") || !e.hasLive("/b") {
		t.Fatal("retargetLive must move the open-handle marker")
	}

	for i := 0; i < extentMissCap; i++ {
		e.missRemember("/miss" + strconv.Itoa(i))
	}
	if got := len(e.misses); got >= extentMissCap {
		t.Fatalf("negative cache unbounded: %d entries (cap %d)", got, extentMissCap)
	}
}

// v1 TestExtentOpenInoFollowsLayoutNotGlob: the declaration glob decides
// where new files are created; the data plane of an existing file is decided
// by the server-reported content layout alone.
func TestPortOpenInoFollowsLayoutNotGlob(t *testing.T) {
	cases := []struct {
		name   string
		layout string
		ino    string
		want   uint64
		wantOK bool
	}{
		{name: "single layout ignores the glob", layout: "single", ino: "", want: 0, wantOK: false},
		{name: "extent layout wins", layout: "extent", ino: "42", want: 42, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stats atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				stats.Add(1)
				w.Header().Set("X-Dat9-Content-Layout", tc.layout)
				if tc.ino != "" {
					w.Header().Set("X-Dat9-Extent-Ino", tc.ino)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			e, err := New(Options{ExtentPaths: []string{"*.db"}, Client: client.New(srv.URL, "")})
			if err != nil {
				t.Fatal(err)
			}
			if !ShouldUseExtentPath(e.opts.ExtentPaths, "/orphan.db") {
				t.Fatal("test premise broken: the glob should match this path")
			}
			ino, ok := e.statIno(context.Background(), "/orphan.db")
			if ok != tc.wantOK || ino != tc.want {
				t.Fatalf("statIno = (%d, %v), want (%d, %v)", ino, ok, tc.want, tc.wantOK)
			}
			if got := stats.Load(); got != 1 {
				t.Fatalf("stat probes = %d, want 1", got)
			}
		})
	}
}

// mashenjun's row: credentials resolve through the handle when the
// context carries none — JuiceFS VFS operations run as the requester.
func TestPortExtentHandleCallerFallback(t *testing.T) {
	h := &extentHandle{caller: fs.Caller{Uid: 1001, Gid: 1001, Pid: 42}}
	if c := h.callerOf(context.Background()); c.Uid != 1001 || c.Pid != 42 {
		t.Fatalf("handle caller fallback = %+v, want the opener's credentials", c)
	}
	// A context-carried caller (namespace ops) wins.
	ctx := contextWithCaller(context.Background(), fs.Caller{Uid: 1002})
	if c := h.callerOf(ctx); c.Uid != 1002 {
		t.Fatalf("context caller = %+v, want 1002", c)
	}
	// The zero context-carried caller falls back to the opener's.
	zero := contextWithCaller(context.Background(), fs.Caller{})
	if c := h.callerOf(zero); c.Uid != 1001 {
		t.Fatalf("zero context caller fell through to %+v, want the opener's 1001", c)
	}
}
