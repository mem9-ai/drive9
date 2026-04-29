package mountpath

import (
	"testing"
)

func TestNormalizeRoot(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "/", false},
		{"/", "/", false},
		{"/foo/bar", "/foo/bar", false},
		{"/foo/bar/", "/foo/bar", false},
		{"/foo//bar", "/foo/bar", false},
		{"/foo/../bar", "/bar", false},
		{"/foo/./bar", "/foo/bar", false},
		{"foo", "", true}, // not absolute
	}
	for _, tt := range tests {
		got, err := NormalizeRoot(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("NormalizeRoot(%q) error=%v, wantErr=%v", tt.in, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("NormalizeRoot(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestToRemote(t *testing.T) {
	tests := []struct {
		root, local, want string
	}{
		{"/", "/", "/"},
		{"/", "/a.txt", "/a.txt"},
		{"/", "/a/b/c", "/a/b/c"},
		{"/foo/bar", "/", "/foo/bar"},
		{"/foo/bar", "/a.txt", "/foo/bar/a.txt"},
		{"/foo/bar", "/sub/dir", "/foo/bar/sub/dir"},
		// ".." in local path is clamped to "/"
		{"/foo/bar", "/../escape", "/foo/bar/escape"},
		{"/foo/bar", "/../../..", "/foo/bar"},
		// Unnormalized local paths
		{"/foo", "//a//b", "/foo/a/b"},
		{"/foo", "/./a", "/foo/a"},
	}
	for _, tt := range tests {
		got := ToRemote(tt.root, tt.local)
		if got != tt.want {
			t.Errorf("ToRemote(%q, %q) = %q, want %q", tt.root, tt.local, got, tt.want)
		}
	}
}

func TestToLocal(t *testing.T) {
	tests := []struct {
		root, remote string
		wantLocal    string
		wantOK       bool
	}{
		{"/", "/", "/", true},
		{"/", "/a.txt", "/a.txt", true},
		{"/", "/a/b", "/a/b", true},
		{"/foo/bar", "/foo/bar", "/", true},
		{"/foo/bar", "/foo/bar/a.txt", "/a.txt", true},
		{"/foo/bar", "/foo/bar/sub/dir", "/sub/dir", true},
		// Outside scope
		{"/foo/bar", "/foo", "", false},
		{"/foo/bar", "/foo/baz", "", false},
		{"/foo/bar", "/other", "", false},
		// Prefix attack: /foo/bar2 should not match /foo/bar
		{"/foo/bar", "/foo/bar2", "", false},
	}
	for _, tt := range tests {
		gotLocal, gotOK := ToLocal(tt.root, tt.remote)
		if gotOK != tt.wantOK {
			t.Errorf("ToLocal(%q, %q) ok=%v, want %v", tt.root, tt.remote, gotOK, tt.wantOK)
			continue
		}
		if gotLocal != tt.wantLocal {
			t.Errorf("ToLocal(%q, %q) = %q, want %q", tt.root, tt.remote, gotLocal, tt.wantLocal)
		}
	}
}
