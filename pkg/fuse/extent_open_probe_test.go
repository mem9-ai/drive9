package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
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

func TestExtentStatInoRestoresMirroredDirectoryWithoutFileLayout(t *testing.T) {
	for _, test := range []struct {
		name   string
		isDir  bool
		wantOK bool
	}{
		{name: "mirrored directory", isDir: true, wantOK: true},
		{name: "ordinary file", isDir: false, wantOK: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("X-Dat9-IsDir", fmt.Sprintf("%t", test.isDir))
				writer.Header().Set("X-Dat9-Extent-Ino", "42")
				writer.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			fs := &Dat9FS{client: newTestClient(server.URL)}
			ino, ok := fs.extentStatIno(t.Context(), "/directory")
			wantIno := uint64(0)
			if test.wantOK {
				wantIno = 42
			}
			if ok != test.wantOK || ino != wantIno {
				t.Fatalf("extentStatIno = (%d, %t), want (%d, %t)", ino, ok, wantIno, test.wantOK)
			}
		})
	}
}

func TestProjectedExtentInoSeparatesDirectoriesFromOrdinaryFiles(t *testing.T) {
	for _, test := range []struct {
		name   string
		stat   *client.StatResult
		want   uint64
		wantOK bool
	}{
		{name: "missing"},
		{name: "directory mirror", stat: &client.StatResult{IsDir: true, ExtentIno: 41}, want: 41, wantOK: true},
		{name: "extent file", stat: &client.StatResult{ContentLayout: client.ContentLayoutExtent, ExtentIno: 42}, want: 42, wantOK: true},
		{name: "ordinary file with stale inode", stat: &client.StatResult{ContentLayout: client.ContentLayoutSingle, ExtentIno: 43}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := projectedExtentIno(test.stat)
			if got != test.want || ok != test.wantOK {
				t.Fatalf("projectedExtentIno = (%d, %t), want (%d, %t)", got, ok, test.want, test.wantOK)
			}
		})
	}
}
