package pathutil

import "testing"

func TestCanonicalize(t *testing.T) {
	tests := []struct {
		in   string
		want string
		err  bool
	}{
		{"", "/", false},
		{"/", "/", false},
		{"/data/file.txt", "/data/file.txt", false},
		{"data/file.txt", "/data/file.txt", false},
		{"/data//file.txt", "/data/file.txt", false},
		{"/data/../file.txt", "", true},
		{"/data/./file.txt", "", true},
		{"/data/file\x00.txt", "", true},
		{"/data/file\x01.txt", "", true},
		{"/data\\file.txt", "", true},
		{"/data/file.txt/", "/data/file.txt", false},
	}
	for _, tt := range tests {
		got, err := Canonicalize(tt.in)
		if tt.err && err == nil {
			t.Errorf("Canonicalize(%q) expected error", tt.in)
		}
		if !tt.err && err != nil {
			t.Errorf("Canonicalize(%q) unexpected error: %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("Canonicalize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestCanonicalizeDir(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"/", "/"},
		{"/data", "/data/"},
		{"/data/", "/data/"},
		{"/data/sub", "/data/sub/"},
	}
	for _, tt := range tests {
		got, err := CanonicalizeDir(tt.in)
		if err != nil {
			t.Errorf("CanonicalizeDir(%q) error: %v", tt.in, err)
		}
		if got != tt.want {
			t.Errorf("CanonicalizeDir(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParentPath(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"/", "/"},
		{"/data/file.txt", "/data/"},
		{"/data/", "/"},
		{"/a/b/c/", "/a/b/"},
		{"/a", "/"},
	}
	for _, tt := range tests {
		got := ParentPath(tt.in)
		if got != tt.want {
			t.Errorf("ParentPath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestBaseName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"/data/file.txt", "file.txt"},
		{"/data/", "data"},
		{"/", "/"},
	}
	for _, tt := range tests {
		got := BaseName(tt.in)
		if got != tt.want {
			t.Errorf("BaseName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestIsDir(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"/", true},
		{"/data/", true},
		{"/data/file.txt", false},
	}
	for _, tt := range tests {
		got := IsDir(tt.in)
		if got != tt.want {
			t.Errorf("IsDir(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestExt(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"/data/file.txt", ".txt"},
		{"/data/file", ""},
		{"/data/file.tar.gz", ".gz"},
	}
	for _, tt := range tests {
		got := Ext(tt.in)
		if got != tt.want {
			t.Errorf("Ext(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
