package cli

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathfilter"
)

// seedRemoteTree populates a mockTreeServer with a directory listing tree
// rooted at root, plus inline file bodies for each leaf.
func seedRemoteTree(mock *mockTreeServer, root string, files map[string]string) {
	dirs := map[string][]client.FileInfo{}
	dirSeen := map[string]map[string]bool{} // dir path -> set of child names already added
	addChild := func(dir, name string, info client.FileInfo) {
		if dirSeen[dir] == nil {
			dirSeen[dir] = map[string]bool{}
		}
		if dirSeen[dir][name] {
			return
		}
		dirSeen[dir][name] = true
		dirs[dir] = append(dirs[dir], info)
	}
	for p, body := range files {
		rel := strings.TrimPrefix(p, root)
		rel = strings.TrimPrefix(rel, "/")
		dir := root
		segs := strings.Split(rel, "/")
		for i, seg := range segs {
			isLast := i == len(segs)-1
			if isLast {
				addChild(dir, seg, client.FileInfo{Name: seg, Size: int64(len(body)), IsDir: false, HasMode: true, Mode: 0o644})
				mock.fileBodies[p] = []byte(body)
				break
			}
			childDir := strings.TrimRight(dir, "/") + "/" + seg
			addChild(dir, seg, client.FileInfo{Name: seg, IsDir: true, HasMode: true, Mode: 0o755})
			if dirs[childDir] == nil {
				dirs[childDir] = []client.FileInfo{}
			}
			dir = childDir
		}
	}
	mock.listEntries = dirs
}

func tarEntries(t *testing.T, p string) []string {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("open %q: %v", p, err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip new reader: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		name := hdr.Name
		if hdr.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/") + "/"
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func tarFileBody(t *testing.T, p, entryName, want string) {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("open %q: %v", p, err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip new reader: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		if hdr.Name != entryName {
			continue
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read body %q: %v", hdr.Name, err)
		}
		if string(body) != want {
			t.Fatalf("tar entry %q body = %q, want %q", hdr.Name, string(body), want)
		}
		return
	}
	t.Fatalf("tar entry %q not found", entryName)
}

func TestArchiveTarGzBasicTree(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/README.md":        "hello world\n",
		"/proj/src/main.go":      "package main\n",
		"/proj/src/util/util.go": "package util\n",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	if err := Archive(c, []string{":/proj", out}); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	got := tarEntries(t, out)
	want := []string{
		"proj/",
		"proj/README.md",
		"proj/src/",
		"proj/src/main.go",
		"proj/src/util/",
		"proj/src/util/util.go",
	}
	sort.Strings(want)
	if !equalAsSets(got, want) {
		t.Fatalf("tar entries = %v, want %v", got, want)
	}
	tarFileBody(t, out, "proj/src/util/util.go", "package util\n")
}

func TestArchiveStdoutProducesValidTarGz(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/a.txt": "AAA",
		"/proj/b.txt": "BBB",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	// Capture stdout.
oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	errCh := make(chan error, 1)
	go func() {
		if err := Archive(c, []string{":/proj", "--stdout"}); err != nil {
			errCh <- err
			return
		}
		close(errCh)
	}()
	go func() {
		<-errCh
		_ = w.Close()
	}()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy pipe: %v", err)
	}
	os.Stdout = oldStdout

	gz, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		names = append(names, hdr.Name)
	}
	sort.Strings(names)
	if !contains(names, "proj/a.txt") || !contains(names, "proj/b.txt") {
		t.Fatalf("stdout tar entries = %v; expected a.txt and b.txt", names)
	}
}

func TestArchiveExcludeSkipsNodeModules(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/src/app.go":           "package src\n",
		"/proj/node_modules/react/index.js": "module.exports\n",
		"/proj/node_modules/react/foo.js":   "foo\n",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	if err := Archive(c, []string{":/proj", out, "--exclude", "**/node_modules/**"}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	got := tarEntries(t, out)
	for _, name := range got {
		if strings.Contains(name, "node_modules") {
			t.Fatalf("excluded node_modules leaked into archive: %q", name)
		}
	}
	if !contains(got, "proj/src/app.go") {
		t.Fatalf("app.go missing from archive: %v", got)
	}
}

func TestArchiveProfileCodingAgentSkipsDefaults(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/main.go":                  "package main\n",
		"/proj/dist/bundle.js":           "bundle\n",
		"/proj/node_modules/react/x.js":  "x\n",
		"/proj/.git/HEAD":                "ref: refs/heads/main\n",
		"/proj/.cache/foo":               "cached\n",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	if err := Archive(c, []string{":/proj", out, "--profile", "coding-agent"}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	got := tarEntries(t, out)
	for _, name := range got {
		for _, bad := range []string{"node_modules", "/dist/", ".git/", ".cache/"} {
			if strings.Contains(name, bad) {
				t.Fatalf("coding-agent profile should skip %q but found %q", bad, name)
			}
		}
	}
	if !contains(got, "proj/main.go") {
		t.Fatalf("main.go missing: %v", got)
	}
}

func TestArchiveZipFormat(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/a.txt": "AAA",
		"/proj/b/c.txt": "CCC",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.zip")
	if err := Archive(c, []string{":/proj", out, "--format", "zip"}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatalf("zip open: %v", err)
	}
	defer func() { _ = zr.Close() }()
	var names []string
	for _, f := range zr.File {
		names = append(names, f.Name)
	}
	sort.Strings(names)
	if !contains(names, "proj/a.txt") || !contains(names, "proj/b/c.txt") {
		t.Fatalf("zip entries = %v; expected a.txt and b/c.txt", names)
	}
}

func TestArchiveFlatStripsHierarchy(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/src/deep/nested/a.go": "package nested\n",
		"/proj/b.go":                 "package main\n",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	if err := Archive(c, []string{":/proj", out, "--flat"}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	got := tarEntries(t, out)
	// Flat: only file basenames, no directory entries.
	for _, name := range got {
		if strings.HasSuffix(name, "/") {
			t.Fatalf("flat mode should not emit dirs, got %q", name)
		}
		if strings.Contains(name, "/") {
			t.Fatalf("flat mode should emit basenames only, got %q", name)
		}
	}
	if !contains(got, "a.go") || !contains(got, "b.go") {
		t.Fatalf("flat tar entries = %v; want a.go and b.go", got)
	}
}

func TestArchiveIncludeWhitelist(t *testing.T) {
	mock := newMockTreeServer()
	seedRemoteTree(mock, "/proj", map[string]string{
		"/proj/src/app.go":    "package src\n",
		"/proj/src/util.go":   "package src\n",
		"/proj/README.md":     "# readme\n",
		"/proj/docs/guide.md": "# guide\n",
	})
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	// Include only markdown files (exact glob at root level: *.md). To match
	// nested files use prefix/**.
	if err := Archive(c, []string{":/proj", out, "--include", "docs/**", "--include", "README.md"}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	got := tarEntries(t, out)
	if !contains(got, "proj/docs/guide.md") {
		t.Fatalf("include should keep docs/guide.md: %v", got)
	}
	if !contains(got, "proj/README.md") {
		t.Fatalf("include should keep README.md: %v", got)
	}
	for _, name := range got {
		if strings.HasSuffix(name, ".go") {
			t.Fatalf("include whitelist should drop .go files, got %q", name)
		}
	}
}

func TestArchiveRejectsLocalSource(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Archive(c, []string{"./local-dir", "out.tar.gz"})
	if err == nil {
		t.Fatal("expected error for local source")
	}
	if !strings.Contains(err.Error(), "remote path") {
		t.Fatalf("expected remote-path error, got: %v", err)
	}
}

func TestArchiveInvalidFormat(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "")
	err := Archive(c, []string{":/proj", "out.tar.gz", "--format", "rar"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "tar.gz or zip") {
		t.Fatalf("expected format error, got: %v", err)
	}
}

func TestBuildArchiveOptionsProfileExcludeMerge(t *testing.T) {
	opts, err := buildArchiveOptions("coding-agent", nil, []string{"**/secret/**"}, "tar.gz", false, 16)
	if err != nil {
		t.Fatalf("buildArchiveOptions: %v", err)
	}
	m := pathfilter.NewMatcher(opts.Include, opts.Exclude, opts.Override)
	// Coding-agent excludes node_modules; combined with --exclude secret.
	if !m.HasExclude() {
		t.Fatal("matcher should have excludes")
	}
	if m.Match("proj/node_modules/react/x.js") {
		t.Fatal("node_modules should be excluded via profile")
	}
	if m.Match("proj/secret/key.pem") {
		t.Fatal("secret should be excluded via --exclude")
	}
	if !m.Match("proj/src/app.go") {
		t.Fatal("app.go should pass")
	}
}

func TestBuildArchiveOptionsOverrideRestores(t *testing.T) {
	// Build a profile on disk with a [remote] override that restores a path
	// the [local] exclude would drop.
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	profileDir := filepath.Join(dir, ".drive9", "profiles")
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	profileBody := `[local]
**/node_modules/**

[remote]
proj/node_modules/.package-lock.json
`
	if err := os.WriteFile(filepath.Join(profileDir, "test-override"), []byte(profileBody), 0o644); err != nil {
		t.Fatalf("write profile: %v", err)
	}

	opts, err := buildArchiveOptions("test-override", nil, nil, "tar.gz", false, 16)
	if err != nil {
		t.Fatalf("buildArchiveOptions: %v", err)
	}
	m := pathfilter.NewMatcher(opts.Include, opts.Exclude, opts.Override)
	if m.Match("proj/node_modules/react/x.js") {
		t.Fatal("node_modules should be excluded")
	}
	if !m.Match("proj/node_modules/.package-lock.json") {
		t.Fatal("override should restore package-lock.json")
	}
}

func TestArchiveEmptyDirPreserved(t *testing.T) {
	mock := newMockTreeServer()
	// Tree with an empty subdir.
	mock.listEntries = map[string][]client.FileInfo{
		"/proj":   {{Name: "src", IsDir: true, HasMode: true, Mode: 0o755}, {Name: "a.txt", Size: 3, IsDir: false, HasMode: true, Mode: 0o644}},
		"/proj/src": {}, // empty dir
	}
	mock.fileBodies = map[string][]byte{"/proj/a.txt": []byte("AAA")}
	srv := mock.httpServer(t)
	defer srv.Close()

	c := client.New(srv.URL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)

	out := filepath.Join(t.TempDir(), "proj.tar.gz")
	ctx := context.Background()
	f, err := os.Create(out)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.ArchiveDir(ctx, "/proj", f, client.ArchiveOptions{}); err != nil {
		_ = f.Close()
		t.Fatalf("ArchiveDir: %v", err)
	}
	_ = f.Close()
	got := tarEntries(t, out)
	if !contains(got, "proj/src/") {
		t.Fatalf("empty dir src/ should be preserved: %v", got)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}