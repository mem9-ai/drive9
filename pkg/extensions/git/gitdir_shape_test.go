package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"net/http/httptest"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

// A linked worktree's registered gitdir_rel must name the worktree admin
// directory git itself uses. Every other shape addresses a directory git reads
// configuration or executables from, so resolution has to refuse it — including
// for rows that were registered before the shape was validated.
func TestLinkedWorktreeGitDirRejectsNonAdminShape(t *testing.T) {
	common := filepath.Join(t.TempDir(), "repo", ".git")

	// A trailing separator is normalized away by the path clean, so it names the
	// same admin directory.
	accepted := []string{".git/worktrees/w1", "worktrees/w1", "worktrees/wt.1", "worktrees/w1/"}
	for _, rel := range accepted {
		got, err := linkedWorktreeGitDir(common, rel)
		if err != nil {
			t.Fatalf("linkedWorktreeGitDir(%q) = %v, want success", rel, err)
		}
		if want := filepath.Join(common, "worktrees", filepath.Base(got)); got != want {
			t.Fatalf("linkedWorktreeGitDir(%q) = %q, want %q", rel, got, want)
		}
	}

	refused := []string{
		".git/hooks",  // the restore would publish a checkpoint over the hooks git runs
		"hooks",       // same directory, written without the .git prefix
		".git/config", // git reads configuration from here
		".git/objects",
		"worktrees",                // not an admin directory itself
		"worktrees/w1/extra",       // deeper than the admin directory
		"worktrees/../../hooks",    // climbs out and back in
		"../hooks",                 // escapes the common .git
		"/etc/hooks",               // absolute
		".git/worktrees/../hooks",  // ".." segment
		"worktrees/",               // no name
		"worktrees/w1\x00",         // NUL
		"worktrees/w1\\pre-commit", // backslash separator
		".git/worktrees",           // no name
	}
	for _, rel := range refused {
		if got, err := linkedWorktreeGitDir(common, rel); err == nil {
			t.Fatalf("linkedWorktreeGitDir(%q) = %q, want a refusal", rel, got)
		}
	}
}

// restoreWorkspaceState must refuse a workspace whose gitdir_rel is not a
// worktree admin directory before it restores the common repository, so nothing
// is published into the targeted directory.
func TestRestoreRefusesNonWorktreeGitDirBeforeTouchingCommon(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "https://example.test/repo.git", strings.Repeat("a", 40))
	// A tenant-supplied gitdir_rel that would publish the linked checkpoint at
	// <common>/.git/hooks.
	b.SeedGitWorktree("linked", "/repo/wt", "https://example.test/repo.git", strings.Repeat("a", 40), "main", ".git/hooks")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)

	localRoot := t.TempDir()
	g, err := New(Options{Client: client.New(ts.URL, ""), LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	mainWS := &workspace{id: "main", root: "/repo", headCommit: strings.Repeat("a", 40), repoURL: "https://example.test/repo.git"}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt", headCommit: strings.Repeat("a", 40), repoURL: "https://example.test/repo.git",
		workspaceKind: "linked", commonID: "main", gitDirRel: ".git/hooks",
	}
	g.workspaces = []*workspace{mainWS, linkedWS}

	err = g.restoreWorkspaceState(context.Background(), linkedWS)
	if err == nil {
		t.Fatal("restore accepted a gitdir_rel outside worktrees/<name>")
	}
	if !strings.Contains(err.Error(), "worktrees/<name>") {
		t.Fatalf("restore error = %v, want the gitdir shape refusal", err)
	}
	// The common repository was never materialized, so nothing could have been
	// written into the targeted directory.
	if _, statErr := os.Stat(filepath.Join(localRoot, "overlay", "repo", ".git")); !os.IsNotExist(statErr) {
		t.Fatalf("common git dir exists after a refused restore (stat err = %v)", statErr)
	}
}

// A checkpoint archive is untrusted input: any entry that is not safely inside
// the extraction root is refused instead of silently skipped, because a
// partially extracted admin directory would still be adopted by the restore.
func TestExtractGitArchiveRefusesEntriesOutsideExtractionRoot(t *testing.T) {
	build := func(entries ...string) []byte {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		tw := tar.NewWriter(gz)
		for _, name := range entries {
			body := []byte("#!/bin/sh\nexit 0\n")
			if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
				t.Fatalf("write header %q: %v", name, err)
			}
			if _, err := tw.Write(body); err != nil {
				t.Fatalf("write body %q: %v", name, err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}

	for _, name := range []string{"../pre-commit", "a/../../pre-commit", ".git/config", ".git/hooks/pre-commit"} {
		dst := t.TempDir()
		if err := extractGitArchive(build(name), dst); err == nil {
			t.Fatalf("extractGitArchive accepted entry %q", name)
		}
		if entries, readErr := os.ReadDir(dst); readErr != nil {
			t.Fatal(readErr)
		} else if len(entries) != 0 {
			t.Fatalf("extractGitArchive wrote %d entries for a refused archive", len(entries))
		}
	}

	// An ordinary admin-directory archive still extracts.
	dst := t.TempDir()
	if err := extractGitArchive(build("HEAD", "hooks/pre-commit"), dst); err != nil {
		t.Fatalf("extractGitArchive refused a normal archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "HEAD")); err != nil {
		t.Fatalf("HEAD not extracted: %v", err)
	}
}
