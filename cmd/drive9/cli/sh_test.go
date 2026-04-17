package cli

import (
	"reflect"
	"testing"
)

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

func TestResolveRmArgs(t *testing.T) {
	tests := []struct {
		name string
		cwd  string
		args []string
		want []string
	}{
		{
			name: "preserves_recursive_flag",
			cwd:  "/data/",
			args: []string{"-r", "dir/"},
			want: []string{"-r", "/data/dir"},
		},
		{
			name: "double_dash_stops_flag_parsing",
			cwd:  "/data/",
			args: []string{"-r", "--", "-scratch"},
			want: []string{"-r", "--", "/data/-scratch"},
		},
		{
			name: "unknown_flag_is_left_for_rm",
			cwd:  "/data/",
			args: []string{"-f", "dir/"},
			want: []string{"-f", "/data/dir"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveRmArgs(tt.cwd, tt.args)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("resolveRmArgs(%q, %v) = %v, want %v", tt.cwd, tt.args, got, tt.want)
			}
		})
	}
}
