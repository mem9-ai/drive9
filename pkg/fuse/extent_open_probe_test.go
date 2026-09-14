package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// P1-4: the profile glob decides where new files are created, never which data
// plane an existing file is read through. A single-layout file that matches the
// glob (plus an orphan jfs edge of the same name) used to be bound to the
// extent inode by a by-name probe on Open.
func TestExtentOpenInoFollowsLayoutNotGlob(t *testing.T) {
	cases := []struct {
		name   string
		layout string
		ino    string
		want   uint64
		wantOK bool
	}{
		{name: "single layout ignores the glob", layout: "single", ino: "", want: 0, wantOK: false},
		{name: "extent layout wins", layout: "extent", ino: "42", want: 42, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Dat9-Content-Layout", tc.layout)
				if tc.ino != "" {
					w.Header().Set("X-Dat9-Extent-Ino", tc.ino)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			fs := &Dat9FS{
				inodes: NewInodeToPath(),
				opts:   &MountOptions{ExtentPaths: []string{"*.db"}},
				client: newTestClient(srv.URL),
			}
			if !fs.shouldUseExtentPath("/orphan.db") {
				t.Fatal("test premise broken: the profile glob should match this path")
			}
			ino, ok := fs.extentOpenIno(context.Background(), 7, "/orphan.db")
			if ok != tc.wantOK || ino != tc.want {
				t.Fatalf("extentOpenIno = (%d, %v), want (%d, %v)", ino, ok, tc.want, tc.wantOK)
			}
		})
	}
}
