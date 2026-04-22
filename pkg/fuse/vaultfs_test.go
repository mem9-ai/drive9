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
	"github.com/stretchr/testify/require"
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
	return out.NodeId, out.Attr.Size, st
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
	require.Equal(t, gofuse.EROFS, st, "mkdir under root must be EROFS")

	st = vfs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "alpha")
	require.Equal(t, gofuse.EROFS, st, "unlink under root must be EROFS")

	st = vfs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "alpha")
	require.Equal(t, gofuse.EROFS, st, "rmdir must be EROFS")

	createIn := &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}}
	st = vfs.Create(nil, createIn, "x", &gofuse.CreateOut{})
	require.Equal(t, gofuse.EROFS, st, "create must be EROFS")

	writeIn := &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: 1}}
	_, st = vfs.Write(nil, writeIn, []byte("hi"))
	require.Equal(t, gofuse.EROFS, st, "write must be EROFS")

	setattrIn := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{InHeader: gofuse.InHeader{NodeId: 1}}}
	st = vfs.SetAttr(nil, setattrIn, &gofuse.AttrOut{})
	require.Equal(t, gofuse.EROFS, st, "setattr must be EROFS")

	// Open with write flags must also be refused.
	alphaIno, st := lookupRoot(t, vfs, "alpha")
	require.True(t, st.Ok(), "lookup alpha should succeed")
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "key")
	require.True(t, st.Ok(), "lookup alpha/key should succeed")
	openIn := &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Flags: 0x0001 /* O_WRONLY */}
	st = vfs.Open(nil, openIn, &gofuse.OpenOut{})
	require.Equal(t, gofuse.EROFS, st, "open(O_WRONLY) on field must be EROFS")
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
	require.True(t, st.Ok())
	require.NotZero(t, ino)

	// "hidden" must not be reachable via root readdir even though the field
	// map is internally populated — the bearer cannot see it.
	_, st = lookupRoot(t, vfs, "hidden")
	require.Equal(t, gofuse.ENOENT, st, "secret outside published scope must be ENOENT at root lookup")
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
	require.True(t, st.Ok())
	alphaIno, st := lookupRoot(t, vfs, "alpha")
	require.True(t, st.Ok())
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "k")
	require.True(t, st.Ok())

	// Revoke at the server.
	fv.revoke()

	// IMMEDIATE — the NEXT op is already EACCES. No Eventually, no TTL wait.
	_, st = lookupRoot(t, vfs, "alpha")
	require.Equal(t, gofuse.EACCES, st, "next root Lookup after revoke must be EACCES (no TTL fail-open)")

	_, _, st = lookupChild(t, vfs, alphaIno, "k")
	require.Equal(t, gofuse.EACCES, st, "next field Lookup after revoke must be EACCES (no TTL fail-open)")

	// GetAttr on a previously-known field inode must also refuse.
	attrIn := &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: keyIno}}
	attrSt := vfs.GetAttr(nil, attrIn, &gofuse.AttrOut{})
	require.Equal(t, gofuse.EACCES, attrSt, "GetAttr on field inode after revoke must be EACCES")
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
	require.True(t, st.Ok())
	keyIno, _, st := lookupChild(t, vfs, alphaIno, "k")
	require.True(t, st.Ok())

	openIn := &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: keyIno}, Flags: 0}
	openOut := &gofuse.OpenOut{}
	st = vfs.Open(nil, openIn, openOut)
	require.True(t, st.Ok(), "open should succeed pre-revoke")
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
		require.True(t, st.Ok(), "read on held FD must not error post-revoke (snapshot lives in memory)")
		out, _ := rr.Bytes(buf)
		require.NotContains(t, string(out), "V2", "held FD must NEVER serve a post-revoke value")
		require.Equal(t, "V1", string(out))
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
	require.True(t, st.Ok())
	_, sz, st := lookupChild(t, vfs, alphaIno, "k")
	require.True(t, st.Ok())
	require.EqualValues(t, len("before"), sz)

	// Update the value server-side (a "put").
	fv.setField("alpha", "k", "after-put")

	// Eventually, the new size is visible (Row G witness — settle window
	// = TTL + safety margin per pre-PR contract v4 row G).
	require.Eventually(t, func() bool {
		_, sz, st := lookupChild(t, vfs, alphaIno, "k")
		return st.Ok() && sz == uint64(len("after-put"))
	}, ttl+5*time.Second, 25*time.Millisecond, "value put must settle within TTL window")
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
		require.True(t, st.Ok())
	}
	after := atomic.LoadInt32(&fv.listCalls)
	require.Equal(t, int32(N), after-before, "every lookup must hit the server (no in-process cache)")
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
	err := MountVault(opts)
	require.Error(t, err, "revoked credential must reject mount")
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
			err := MountVault(tc.opts(srv.URL, tmp))
			// FUSE mount will fail in test env, but the probe must NOT
			// produce an EACCES rejection for empty scope.
			if err != nil {
				require.NotContains(t, err.Error(), "EACCES",
					"%s with empty vault must NOT be rejected with EACCES", tc.name)
			}
		})
	}
}
