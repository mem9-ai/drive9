package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

// fakeVaultServer is the test double for /v1/vault/read[/<name>].
//
// Witness tests for V2e contract rows interact with VaultFS through this
// server so that revocation, value-rotation, and TTL behaviour can be
// driven deterministically — no live tenant required. Auth failures use 401
// to match pkg/server/vault.go.
type fakeVaultServer struct {
	mu sync.Mutex

	// Underlying state.
	secrets   []string
	fields    map[string]map[string]string
	revoked   bool
	listCalls int32
	readCalls int32
}

func newFakeVault() *fakeVaultServer {
	return &fakeVaultServer{fields: map[string]map[string]string{}}
}

func (f *fakeVaultServer) setSecrets(secrets []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.secrets = append([]string(nil), secrets...)
}

func (f *fakeVaultServer) setField(secret, field, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fields[secret] == nil {
		f.fields[secret] = map[string]string{}
	}
	f.fields[secret][field] = value
}

func (f *fakeVaultServer) revoke() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = true
}

func (f *fakeVaultServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/vault/read", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		revoked := f.revoked
		secrets := append([]string(nil), f.secrets...)
		f.mu.Unlock()
		atomic.AddInt32(&f.listCalls, 1)
		if revoked {
			http.Error(w, `{"error":"revoked"}`, http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"secrets": secrets})
	})
	mux.HandleFunc("/v1/vault/read/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/v1/vault/read/")
		f.mu.Lock()
		revoked := f.revoked
		fields, hasSecret := f.fields[name]
		copyFields := map[string]string{}
		for k, v := range fields {
			copyFields[k] = v
		}
		f.mu.Unlock()
		atomic.AddInt32(&f.readCalls, 1)
		if revoked {
			http.Error(w, `{"error":"revoked"}`, http.StatusUnauthorized)
			return
		}
		if !hasSecret {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(copyFields)
	})
	return mux
}

func newTestVaultFS(t *testing.T, ttl time.Duration) (*VaultFS, *fakeVaultServer, func()) {
	t.Helper()
	fv := newFakeVault()
	srv := httptest.NewServer(fv.handler())
	c := client.NewWithToken(srv.URL, "test-token")
	vfs := NewVaultFS(c, ttl)
	return vfs, fv, srv.Close
}

// lookupRoot performs Lookup(root, name) and returns the resulting NodeId.
func lookupRoot(t *testing.T, vfs *VaultFS, name string) (uint64, gofuse.Status) {
	t.Helper()
	hdr := &gofuse.InHeader{NodeId: 1}
	out := &gofuse.EntryOut{}
	st := vfs.Lookup(nil, hdr, name, out)
	return out.NodeId, st
}

func lookupChild(t *testing.T, vfs *VaultFS, parentIno uint64, name string) (uint64, uint64, gofuse.Status) {
	t.Helper()
	hdr := &gofuse.InHeader{NodeId: parentIno}
	out := &gofuse.EntryOut{}
	st := vfs.Lookup(nil, hdr, name, out)
	// out.Size — Attr is embedded in EntryOut; the explicit selector trips
	// staticcheck QF1008.
	return out.NodeId, out.Size, st
}

// ---------------------------------------------------------------------------
// Row C — read-only enforcement.
// Witness: every mutating op returns EROFS even when the path looks plausible.
// ---------------------------------------------------------------------------

func TestVaultMountReadOnly(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 5*time.Second)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "key", "v1")

	mkdirIn := &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: 1}}
	st := vfs.Mkdir(nil, mkdirIn, "newdir", &gofuse.EntryOut{})
	if st != gofuse.EROFS {
		t.Fatalf("mkdir under root status = %v, want EROFS", st)
	}

	st = vfs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "alpha")
	if st != gofuse.EROFS {
		t.Fatalf("unlink under root status = %v, want EROFS", st)
	}

	st = vfs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "alpha")
	if st != gofuse.EROFS {
		t.Fatalf("rmdir status = %v, want EROFS", st)
	}

	createIn := &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}}
	st = vfs.Create(nil, createIn, "x", &gofuse.CreateOut{})
	if st != gofuse.EROFS {
		t.Fatalf("create status = %v, want EROFS", st)
	}

	writeIn := &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: 1}}
	_, st = vfs.Write(nil, writeIn, []byte("hi"))
	if st != gofuse.EROFS {
		t.Fatalf("write status = %v, want EROFS", st)
	}

	setattrIn := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: 1}}}
	st = vfs.SetAttr(nil, setattrIn, &gofuse.AttrOut{})
	if st != gofuse.EROFS {
		t.Fatalf("setattr status = %v, want EROFS", st)
	}

	// Open with write flags must also be refused.
	alphaIno, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("lookup alpha status = %v, want OK", st)
	}
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "key")
	if !st.Ok() {
		t.Fatalf("lookup alpha/key status = %v, want OK", st)
	}
	openIn := &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Flags: 0x0001 /* O_WRONLY */}
	st = vfs.Open(nil, openIn, &gofuse.OpenOut{})
	if st != gofuse.EROFS {
		t.Fatalf("open(O_WRONLY) on field status = %v, want EROFS", st)
	}
}

// ---------------------------------------------------------------------------
// Row D — delegated scope visibility.
// Witness: root listing returns exactly the server-published set; lookup of a
// secret outside that set returns ENOENT (Invariant #7 — server is the
// authority, but the client must not invent extra entries either).
// ---------------------------------------------------------------------------

func TestVaultMountScopeVisibility(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 5*time.Second)
	defer cleanup()
	fv.setSecrets([]string{"visible"})
	fv.setField("visible", "f1", "x")
	fv.setField("hidden", "f2", "y") // populated but NOT in the published list

	ino, st := lookupRoot(t, vfs, "visible")
	if !st.Ok() {
		t.Fatalf("lookup visible status = %v, want OK", st)
	}
	if ino == 0 {
		t.Fatalf("lookup visible returned zero NodeId")
	}

	// "hidden" must not be reachable via root readdir even though the field
	// map is internally populated — the bearer cannot see it.
	_, st = lookupRoot(t, vfs, "hidden")
	if st != gofuse.ENOENT {
		t.Fatalf("lookup hidden status = %v, want ENOENT (outside published scope)", st)
	}
}

// ---------------------------------------------------------------------------
// Row E — new operation after revoke must surface as EACCES.
// Witness: there is NO TTL-sized fail-open window. Each FUSE op hits the
// server, so the very NEXT op after revoke returns EACCES — no wait, no
// Eventually. An in-process cache would violate this row (see the
// docstring on VaultFS.listSecrets for why there is no such cache).
// ---------------------------------------------------------------------------

func TestVaultMountNewOpAfterRevokeEACCES(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 5*time.Second)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "k", "v1")

	// Warm up: succeeds. This also populates any hypothetical cache — if
	// one ever regresses into the code, the post-revoke assertion below
	// will catch it.
	_, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("warmup lookup status = %v, want OK", st)
	}
	alphaIno, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("lookup alpha status = %v, want OK", st)
	}
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "k")
	if !st.Ok() {
		t.Fatalf("lookup alpha/k status = %v, want OK", st)
	}

	// Revoke at the server.
	fv.revoke()

	// IMMEDIATE — the NEXT op is already EACCES. No Eventually, no TTL wait.
	_, st = lookupRoot(t, vfs, "alpha")
	if st != gofuse.EACCES {
		t.Fatalf("next root Lookup after revoke status = %v, want EACCES (no TTL fail-open)", st)
	}

	_, _, st = lookupChild(t, vfs, alphaIno, "k")
	if st != gofuse.EACCES {
		t.Fatalf("next field Lookup after revoke status = %v, want EACCES (no TTL fail-open)", st)
	}

	// GetAttr on a previously-known field inode must also refuse.
	attrIn := &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: keyIno}}
	attrSt := vfs.GetAttr(nil, attrIn, &gofuse.AttrOut{})
	if attrSt != gofuse.EACCES {
		t.Fatalf("GetAttr on field inode after revoke status = %v, want EACCES", attrSt)
	}
}

// ---------------------------------------------------------------------------
// Row F — existing FD after revoke must NEVER serve a post-revoke value.
// Witness: open snapshot is V1; server rotates to V2 and revokes; reads from
// the held FD return V1 forever, never V2 (and never a post-revoke fetch).
// ---------------------------------------------------------------------------

func TestVaultMountRevokeExistingFD_NeverServesPostRevokeValue(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 100*time.Millisecond)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "k", "V1")

	alphaIno, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("lookup alpha status = %v, want OK", st)
	}
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "k")
	if !st.Ok() {
		t.Fatalf("lookup alpha/k status = %v, want OK", st)
	}

	openIn := &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Flags: 0}
	openOut := &gofuse.OpenOut{}
	st = vfs.Open(nil, openIn, openOut)
	if !st.Ok() {
		t.Fatalf("open pre-revoke status = %v, want OK", st)
	}
	fh := openOut.Fh

	// Mutate value and revoke at the server.
	fv.setField("alpha", "k", "V2-NEVER-SERVE")
	fv.revoke()

	// Hammer reads on the held FD over a window > TTL. Each must return
	// V1 (the open-time snapshot). The held FD must NEVER produce V2,
	// regardless of how long we wait.
	deadline := time.Now().Add(500 * time.Millisecond) // > 5×TTL
	for time.Now().Before(deadline) {
		buf := make([]byte, 64)
		readIn := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Fh: fh, Offset: 0, Size: uint32(len(buf))}
		rr, st := vfs.Read(nil, readIn, buf)
		if !st.Ok() {
			t.Fatalf("read on held FD post-revoke status = %v, want OK (snapshot lives in memory)", st)
		}
		out, _ := rr.Bytes(buf)
		if strings.Contains(string(out), "V2") {
			t.Fatalf("held FD served post-revoke value %q; must NEVER contain V2", string(out))
		}
		if string(out) != "V1" {
			t.Fatalf("held FD read = %q, want V1", string(out))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Row G — state-after-settle. After a server-side put, eventual readdir/
// lookup reflects the new value once TTL expires.
// ---------------------------------------------------------------------------

func TestVaultMountSettleAfterPut(t *testing.T) {
	const ttl = 100 * time.Millisecond
	vfs, fv, cleanup := newTestVaultFS(t, ttl)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "k", "before")

	alphaIno, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("lookup alpha status = %v, want OK", st)
	}
	_, sz, st := lookupChild(t, vfs, alphaIno, "k")
	if !st.Ok() {
		t.Fatalf("lookup alpha/k status = %v, want OK", st)
	}
	if sz != uint64(len("before")) {
		t.Fatalf("initial size = %d, want %d", sz, len("before"))
	}

	// Update the value server-side (a "put").
	fv.setField("alpha", "k", "after-put")

	// Eventually, the new size is visible (Row G witness — settle window
	// = TTL + safety margin per pre-PR contract v4 row G).
	deadline := time.Now().Add(ttl + 5*time.Second)
	for {
		_, sz, st := lookupChild(t, vfs, alphaIno, "k")
		if st.Ok() && sz == uint64(len("after-put")) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("value put did not settle within TTL window (last sz = %d, st = %v)", sz, st)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Row H — no in-process cache: every lookup hits the server.
// Witness: N lookups produce N server calls (DirTTL is a kernel hint only,
// not an in-process auth cache — see Row E rationale in listSecrets).
// ---------------------------------------------------------------------------

func TestVaultMountNoInProcessCache(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 5*time.Second)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "k", "x")

	before := atomic.LoadInt32(&fv.listCalls)
	const N = 5
	for i := 0; i < N; i++ {
		_, st := lookupRoot(t, vfs, "alpha")
		if !st.Ok() {
			t.Fatalf("lookup #%d status = %v, want OK", i, st)
		}
	}
	after := atomic.LoadInt32(&fv.listCalls)
	if got := after - before; got != int32(N) {
		t.Fatalf("listCalls delta = %d, want %d (every lookup must hit the server, no in-process cache)", got, N)
	}
}

// ---------------------------------------------------------------------------
// Row I — revoked credential at probe time is rejected (server returns 401).
// Empty scope (zero secrets, valid token) is NOT rejected — see vault_mount.go.
// ---------------------------------------------------------------------------

func TestVaultMountRevokedCredentialRejected(t *testing.T) {
	fv := newFakeVault()
	fv.setSecrets([]string{"alpha"})
	fv.revoke()
	srv := httptest.NewServer(fv.handler())
	defer srv.Close()

	tmp := t.TempDir()
	opts := &VaultMountOptions{
		Server:     srv.URL,
		Token:      "revoked-token",
		MountPoint: tmp,
		DirTTL:     100 * time.Millisecond,
	}
	// Exercise the probe directly — the server returns 401 for revoked tokens,
	// which must propagate as a non-nil error before any FUSE wiring.
	_, _, err := probeVaultMount(opts)
	if err == nil {
		t.Fatalf("probeVaultMount with revoked credential returned nil error, want non-nil")
	}
}

// ---------------------------------------------------------------------------
// Row I — empty scope (zero secrets) must NOT be rejected for either
// principal kind. Owner: normal new-tenant startup. Delegated: valid grant
// whose scope targets secrets that don't exist yet. Revoked/malformed tokens
// are caught by the server returning 401, not by counting secrets.
// ---------------------------------------------------------------------------

func TestVaultMountEmptyScopeAllowed(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(url string, tmp string) *VaultMountOptions
	}{
		{"owner", func(url, tmp string) *VaultMountOptions {
			return &VaultMountOptions{Server: url, APIKey: "owner-key", MountPoint: tmp, DirTTL: 100 * time.Millisecond}
		}},
		{"delegated", func(url, tmp string) *VaultMountOptions {
			return &VaultMountOptions{Server: url, Token: "delegated-token", MountPoint: tmp, DirTTL: 100 * time.Millisecond}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fv := newFakeVault()
			srv := httptest.NewServer(fv.handler())
			defer srv.Close()

			tmp := t.TempDir()
			// Exercise the probe directly, not the full MountVault path:
			// MountVault blocks on server.Wait() once the FUSE mount succeeds
			// (which it can on Linux CI with /dev/fuse available), while this
			// test only cares about the probe verdict — empty scope must NOT
			// be rejected with EACCES for either owner or delegated callers.
			_, secrets, err := probeVaultMount(tc.opts(srv.URL, tmp))
			if err != nil {
				t.Fatalf("%s: probeVaultMount with empty vault returned %v, want nil", tc.name, err)
			}
			if len(secrets) != 0 {
				t.Fatalf("%s: expected empty secret list, got %v", tc.name, secrets)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Read must guard input.Offset (uint64) against math.MaxInt64 overflow and
// uint64 wraparound when computing end = offset + len(buf). A hostile
// ^uint64(0) offset or a near-max offset + buf_len wrap must be rejected as
// empty read, never panic with a negative slice index.
// ---------------------------------------------------------------------------

func TestVaultMountReadOffsetOverflowSafe(t *testing.T) {
	vfs, fv, cleanup := newTestVaultFS(t, 5*time.Second)
	defer cleanup()
	fv.setSecrets([]string{"alpha"})
	fv.setField("alpha", "k", "payload")

	alphaIno, st := lookupRoot(t, vfs, "alpha")
	if !st.Ok() {
		t.Fatalf("lookup alpha status = %v, want OK", st)
	}
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "k")
	if !st.Ok() {
		t.Fatalf("lookup alpha/k status = %v, want OK", st)
	}

	openIn := &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Flags: 0}
	openOut := &gofuse.OpenOut{}
	if st := vfs.Open(nil, openIn, openOut); !st.Ok() {
		t.Fatalf("open status = %v, want OK", st)
	}
	fh := openOut.Fh

	for _, offset := range []uint64{
		^uint64(0),               // math.MaxUint64
		^uint64(0) - 10,          // near-max; offset + len(buf) wraps
		uint64(1) << 63,          // math.MaxInt64+1 (first offset that overflows int64)
	} {
		buf := make([]byte, 64)
		readIn := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Fh: fh, Offset: offset, Size: uint32(len(buf))}
		rr, rst := vfs.Read(nil, readIn, buf)
		if !rst.Ok() {
			t.Fatalf("Read(offset=%#x) status = %v, want OK (must not panic or error)", offset, rst)
		}
		out, _ := rr.Bytes(buf)
		if len(out) != 0 {
			t.Fatalf("Read(offset=%#x) returned %d bytes, want 0 (past-EOF)", offset, len(out))
		}
	}
}
