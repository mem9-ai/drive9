package cli

import "testing"

func TestResolve(t *testing.T) {
	tests := []struct {
		cwd  string
		path string
		want string
	}{
		{"/", "file.txt", "/file.txt"},
		{"/data/", "file.txt", "/data/file.txt"},
		{"/data/", "/abs/path", "/abs/path"},
		{"/data/", "sub/file", "/data/sub/file"},
		{"/", "/", "/"},
	}
	for _, tt := range tests {
		got := resolve(tt.cwd, tt.path)
		if got != tt.want {
			t.Errorf("resolve(%q, %q) = %q, want %q", tt.cwd, tt.path, got, tt.want)
		}
	}
}
