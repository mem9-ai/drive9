package fuse

import "testing"

func TestMatchExtentPattern(t *testing.T) {
	cases := []struct {
		pat, path string
		want      bool
	}{
		{"*", "/foo.db", true},
		{"*.db", "/a/foo.db", true},
		{"*.db", "/a/foo.txt", false},
		{"*-wal", "/x.db-wal", true},
		{"foo.db", "/foo.db", true},
	}
	for _, tc := range cases {
		if got := matchExtentPattern(tc.pat, tc.path); got != tc.want {
			t.Fatalf("matchExtentPattern(%q,%q)=%v want %v", tc.pat, tc.path, got, tc.want)
		}
	}
}

func TestShouldUseExtentPath(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*.db", "*-wal"}}}
	if !fs.shouldUseExtentPath("/tmp/x.db") {
		t.Fatal("expected *.db match")
	}
	if fs.shouldUseExtentPath("/tmp/x.txt") {
		t.Fatal("did not expect txt match")
	}
	fs.opts.ExtentPaths = []string{"*"}
	if !fs.shouldUseExtentPath("/anything") {
		t.Fatal("expected * match")
	}
}
