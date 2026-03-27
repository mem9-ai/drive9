package cli

import "testing"

func TestParseRemote(t *testing.T) {
	tests := []struct {
		input    string
		wantCtx  string
		wantPath string
		wantOK   bool
	}{
		{":/data/file.txt", "", "/data/file.txt", true},
		{":/", "", "/", true},
		{"test1:/TODO.md", "test1", "/TODO.md", true},
		{"mydb:/data/file.txt", "mydb", "/data/file.txt", true},
		{"/data/file.txt", "", "", false},
		{"/tmp/local.txt", "", "", false},
		{"local.txt", "", "", false},
		{"./local.txt", "", "", false},
		{"-", "", "", false},
		{"C:/tmp/a.txt", "", "", false},
		{"D:/Users/test", "", "", false},
	}
	for _, tt := range tests {
		rp, ok := ParseRemote(tt.input)
		if ok != tt.wantOK {
			t.Errorf("ParseRemote(%q) ok=%v, want %v", tt.input, ok, tt.wantOK)
			continue
		}
		if ok {
			if rp.Context != tt.wantCtx {
				t.Errorf("ParseRemote(%q) context=%q, want %q", tt.input, rp.Context, tt.wantCtx)
			}
			if rp.Path != tt.wantPath {
				t.Errorf("ParseRemote(%q) path=%q, want %q", tt.input, rp.Path, tt.wantPath)
			}
		}
	}
}
