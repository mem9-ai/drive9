package extent

import (
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

func TestMatchExtentPattern(t *testing.T) {
	cases := []struct {
		pattern, path string
		want          bool
	}{
		{"*", "/anything/file.txt", true},
		{"*.db", "/a/b/x.DB", true}, // suffix, case-insensitive
		{"*.db", "/a/b/x.db-wal", false},
		{"data*", "/x/database", true}, // path.Match on base
		{"wal", "/deep/dir/WAL", true}, // exact name, case-insensitive
		{"wal", "/deep/dir/wal2", false},
		{"", "/whatever", false},
	}
	for _, c := range cases {
		if got := matchExtentPattern(c.pattern, c.path); got != c.want {
			t.Errorf("match(%q, %q) = %v, want %v", c.pattern, c.path, got, c.want)
		}
	}
}

func TestShouldUseExtentPath(t *testing.T) {
	pats := []string{"*.db", "media"}
	if !ShouldUseExtentPath(pats, "/app/app.db") {
		t.Fatal("*.db should match")
	}
	if !ShouldUseExtentPath(pats, "/media") {
		t.Fatal("exact name should match")
	}
	if ShouldUseExtentPath(pats, "/media/") {
		t.Fatal("directories never take the extent path")
	}
	if ShouldUseExtentPath(nil, "/app/app.db") {
		t.Fatal("no patterns must disable")
	}
}

func TestJuiceTypeToStatMode(t *testing.T) {
	if m := juiceTypeToStatMode(jfsmeta.TypeDirectory, 0o755); m != syscall.S_IFDIR|0o755 {
		t.Fatalf("dir mode = %o", m)
	}
	if m := juiceTypeToStatMode(jfsmeta.TypeSymlink, 0o777); m != syscall.S_IFLNK|0o777 {
		t.Fatalf("symlink mode = %o", m)
	}
	if m := juiceTypeToStatMode(jfsmeta.TypeFile, 0o644); m != syscall.S_IFREG|0o644 {
		t.Fatalf("file mode = %o", m)
	}
}

func TestClampExtentTTL(t *testing.T) {
	if got := clampExtentTTL(juiceFSAttrTimeout, 5*time.Second); got != juiceFSAttrTimeout {
		t.Fatalf("clamp = %v", got)
	}
	if got := clampExtentTTL(juiceFSAttrTimeout, 100*time.Millisecond); got != 100*time.Millisecond {
		t.Fatalf("configured below extent ttl must win: %v", got)
	}
	if got := clampExtentTTL(0, time.Second); got != 0 {
		t.Fatalf("deliberate zero must stay zero: %v", got)
	}
}
