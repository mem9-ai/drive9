package server

import (
	"errors"
	"testing"

	"github.com/mem9-ai/dat9/pkg/meta"
)

func TestAuthorizeFSOwnerAllowsAll(t *testing.T) {
	scope := &TenantScope{}
	if err := scope.AuthorizeFS(FSOpDelete, "/main/secrets.txt"); err != nil {
		t.Fatalf("owner AuthorizeFS error = %v, want nil", err)
	}
}

func TestAuthorizeFSScopedAllowsMatchingOpAndPrefix(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpList: true, FSOpWrite: true},
		}},
	}

	if err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/input.txt"); err != nil {
		t.Fatalf("AuthorizeFS allowed path error = %v, want nil", err)
	}
	if err := scope.AuthorizeFS(FSOpWrite, ":/scratch/run-1/out.txt"); err != nil {
		t.Fatalf("AuthorizeFS drive-style path error = %v, want nil", err)
	}
}

func TestAuthorizeFSDeniesOutsidePrefix(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	err := scope.AuthorizeFS(FSOpRead, "/scratch/run-2/input.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFS error = %v, want ErrFSAccessDenied", err)
	}
}

func TestAuthorizeFSUsesSegmentBoundary(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/foo",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	if err := scope.AuthorizeFS(FSOpRead, "/foo/bar.txt"); err != nil {
		t.Fatalf("AuthorizeFS /foo/bar.txt error = %v, want nil", err)
	}
	err := scope.AuthorizeFS(FSOpRead, "/foobar/secrets.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFS /foobar error = %v, want ErrFSAccessDenied", err)
	}
}

func TestAuthorizeFSRejectsEscapingPathBeforeMatching(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/../main/secrets.txt")
	if !errors.Is(err, ErrFSInvalidPath) {
		t.Fatalf("AuthorizeFS escaped path error = %v, want ErrFSInvalidPath", err)
	}
}

func TestAuthorizeFSPairRequiresBothPaths(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{
			{Prefix: "/source", Ops: map[FSOp]bool{FSOpRead: true}},
			{Prefix: "/dest", Ops: map[FSOp]bool{FSOpWrite: true}},
		},
	}

	if err := scope.AuthorizeFSPair(FSOpRead, "/source/a.txt", FSOpWrite, "/dest/a.txt"); err != nil {
		t.Fatalf("AuthorizeFSPair allowed paths error = %v, want nil", err)
	}
	err := scope.AuthorizeFSPair(FSOpRead, "/source/a.txt", FSOpWrite, "/other/a.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFSPair dst error = %v, want ErrFSAccessDenied", err)
	}
}

func TestParseFSScopeOpsRejectsSearchWithoutRead(t *testing.T) {
	if _, err := parseFSScopeOps("search"); err == nil {
		t.Fatal("parseFSScopeOps(search) error = nil, want error")
	}
	if _, err := parseFSScopeOps("read,search"); err != nil {
		t.Fatalf("parseFSScopeOps(read,search) error = %v, want nil", err)
	}
}

func TestFSScopeFromMetaRejectsEmptyPrefix(t *testing.T) {
	if _, err := fsScopesFromMeta(nil); err != nil {
		t.Fatalf("fsScopesFromMeta(nil) error = %v, want nil", err)
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: "", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(empty prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: "   ", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(blank prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: ":", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(bare colon prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: ":/", Ops: "read"}}); err != nil {
		t.Fatalf("fsScopesFromMeta(explicit root prefix) error = %v, want nil", err)
	}
}

func TestIsScopedBusinessPathAllowed(t *testing.T) {
	denied := []string{
		"/v1/fs:batch-stat",
		"/v1/fs:batch-read-small",
		"/v1/fs/main.txt",
		"/v1/uploads/initiate",
		"/v1/uploads",
		"/v1/uploads/upload-1/complete",
		"/v2/uploads/upload-1/parts",
		"/v1/sql",
		"/v1/fork",
		"/v1/events",
		"/v1/journals",
		"/v1/vault/secrets",
	}
	for _, p := range denied {
		if isScopedBusinessPathAllowed(p) {
			t.Fatalf("isScopedBusinessPathAllowed(%q) = true, want false", p)
		}
	}
}

func BenchmarkAuthorizeFSOwnerToken(b *testing.B) {
	scope := &TenantScope{}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/main/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeFSSingleZone(b *testing.B) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeFSTenZones(b *testing.B) {
	scope := &TenantScope{IsScoped: true}
	for _, prefix := range []string{"/zone0", "/zone1", "/zone2", "/zone3", "/zone4", "/zone5", "/zone6", "/zone7", "/zone8", "/zone9"} {
		scope.FSScopes = append(scope.FSScopes, FSScope{
			Prefix: prefix,
			Ops:    map[FSOp]bool{FSOpRead: true},
		})
	}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/zone9/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}
