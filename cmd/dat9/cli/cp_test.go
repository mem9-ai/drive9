package cli

import "testing"

func TestIsRemote(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{":/data/file.txt", true},
		{":/", true},
		{"/data/file.txt", false},
		{"/tmp/local.txt", false},
		{"local.txt", false},
		{"./local.txt", false},
		{"-", false},
	}
	for _, tt := range tests {
		got := isRemote(tt.path)
		if got != tt.want {
			t.Errorf("isRemote(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
