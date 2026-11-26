package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

// A linked worktree's registered gitdir_rel must name the worktree admin
// directory git itself uses, and it must name THIS worktree. Every other shape
// addresses a directory git reads configuration or executables from, so
// resolution has to refuse it — including for rows that were registered while the
// value was free-form.
func TestLinkedWorktreeGitDirRejectsNonAdminShape(t *testing.T) {
	common := filepath.Join(t.TempDir(), "repo", ".git")

	accepted := []struct{ rel, name string }{
		{".git/worktrees/w1", "w1"},
		{"worktrees/w1", "w1"},
		{"worktrees/wt.1", "wt.1"},
	}
	for _, tc := range accepted {
		got, err := linkedWorktreeGitDir(common, tc.rel, tc.name)
		if err != nil {
			t.Fatalf("linkedWorktreeGitDir(%q, %q) = %v, want success", tc.rel, tc.name, err)
		}
		if want := filepath.Join(common, "worktrees", tc.name); got != want {
			t.Fatalf("linkedWorktreeGitDir(%q, %q) = %q, want %q", tc.rel, tc.name, got, want)
		}
	}

	refused := []struct{ rel, name string }{
		{".git/hooks", "hooks"},   // the restore would publish a checkpoint over the hooks git runs
		{"hooks", "hooks"},        // same directory, written without the .git prefix
		{".git/config", "config"}, // git reads configuration from here
		{".git/objects", "objects"},
		{"worktrees", "w1"},                  // not an admin directory itself
		{"worktrees/w1/extra", "w1"},         // deeper than the admin directory
		{"worktrees/../../hooks", "hooks"},   // climbs out and back in
		{"../hooks", "hooks"},                // escapes the common .git
		{"/etc/hooks", "hooks"},              // absolute
		{".git/worktrees/../hooks", "hooks"}, // ".." segment
		{"worktrees/", ""},                   // no name
		{"worktrees/w1\x00", "w1"},           // NUL
		{"worktrees/w1\\pre-commit", "w1"},   // backslash separator
		{".git/worktrees", ""},               // no name
		{"worktrees//w1", "w1"},              // empty segment, which the API also rejects
		{"worktrees/w1/", "w1"},              // trailing separator: the same rule on both sides
		{"worktrees/w1", "w2"},               // a sibling worktree's admin directory
		{"worktrees/w1", ""},                 // the stored name is what makes it this worktree's
	}
	for _, tc := range refused {
		if got, err := linkedWorktreeGitDir(common, tc.rel, tc.name); err == nil {
			t.Fatalf("linkedWorktreeGitDir(%q, %q) = %q, want a refusal", tc.rel, tc.name, got)
		}
	}
}

// restoreWorkspaceState must refuse a workspace whose gitdir_rel is not a
// worktree admin directory before it materializes the common repository. The
// common's checkpoint is seeded here and a restore of it creates the overlay
// directory, so an ordering regression that restored it first turns this red.
func TestRestoreRefusesNonWorktreeGitDirBeforeTouchingCommon(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "https://example.test/repo.git", strings.Repeat("a", 40))
	// A tenant-supplied gitdir_rel that would publish the linked checkpoint at
	// <common>/.git/hooks.
	b.SeedGitWorktree("linked", "/repo/wt", "https://example.test/repo.git", strings.Repeat("a", 40), "main", ".git/hooks")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	seedGitState(t, ts.URL, "main")

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
	if !strings.Contains(err.Error(), "worktrees/") {
		t.Fatalf("restore error = %v, want the gitdir shape refusal", err)
	}
	// The common repository was never materialized: its overlay directory is the
	// first thing a restore creates, and the staging directory lands inside it.
	if _, statErr := os.Stat(filepath.Join(localRoot, "overlay", "repo")); !os.IsNotExist(statErr) {
		t.Fatalf("common repository was materialized by a refused restore (stat err = %v)", statErr)
	}
}

// The worktree admin path must be a real directory. The common repository's own
// checkpoint is tenant-controlled, so it can plant "worktrees -> refs"; a restore
// would then delete and replace the branch store instead of an admin directory.
func TestRestoreRefusesSymlinkedWorktreesBeforeSwap(t *testing.T) {
	localRoot := t.TempDir()
	commonGitDir := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(filepath.Join(commonGitDir, "refs", "heads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("refs", filepath.Join(commonGitDir, "worktrees")); err != nil {
		t.Fatal(err)
	}
	headPath := filepath.Join(commonGitDir, "refs", "heads", "heads")
	if err := os.WriteFile(headPath, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "https://example.test/repo.git", strings.Repeat("a", 40))
	b.SeedGitWorktree("linked", "/repo/wt", "https://example.test/repo.git", strings.Repeat("a", 40), "main", "worktrees/heads")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)

	g, err := New(Options{Client: client.New(ts.URL, ""), LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	mainWS := &workspace{id: "main", root: "/repo", headCommit: strings.Repeat("a", 40), repoURL: "https://example.test/repo.git"}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt", headCommit: strings.Repeat("a", 40), repoURL: "https://example.test/repo.git",
		workspaceKind: "linked", commonID: "main", gitDirRel: "worktrees/heads", worktreeName: "heads",
	}
	g.workspaces = []*workspace{mainWS, linkedWS}

	if err := g.restoreWorkspaceState(context.Background(), linkedWS); err == nil {
		t.Fatal("restore accepted a symlinked worktrees directory")
	}
	// The branch store the link pointed at is untouched.
	got, readErr := os.ReadFile(headPath)
	if readErr != nil {
		t.Fatalf("refs/heads/heads was removed by the refused restore: %v", readErr)
	}
	if string(got) != "ref: refs/heads/main\n" {
		t.Fatalf("refs/heads/heads = %q, want the original content", got)
	}
}

// A checkpoint archive is untrusted input: any entry that is not safely inside
// the extraction root is refused instead of silently skipped, because a
// partially extracted admin directory would still be adopted by the restore.
func TestExtractGitArchiveRefusesEntriesOutsideExtractionRoot(t *testing.T) {
	for _, name := range []string{"../pre-commit", "a/../../pre-commit", "/abs/pre-commit", ".git/config", ".git/hooks/pre-commit"} {
		dst := t.TempDir()
		if err := extractGitArchive(tarArchive(t, tarEntry{name: name, body: "#!/bin/sh\nexit 0\n", mode: 0o755}), dst); err == nil {
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
	archive := tarArchive(t,
		tarEntry{name: "HEAD", body: "ref: refs/heads/main\n"},
		tarEntry{name: "hooks/pre-commit", body: "#!/bin/sh\n", mode: 0o755},
	)
	if err := extractGitArchive(archive, dst); err != nil {
		t.Fatalf("extractGitArchive refused a normal archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "HEAD")); err != nil {
		t.Fatalf("HEAD not extracted: %v", err)
	}
}

// A link of its own is what makes a lexical path check meaningless: the reported
// chain built from ".."-named components, and the same chain built from ordinary
// names, both resolve the final write outside the extraction root. Neither may
// land anything, and a plain link is refused too — the restore and every later git
// command follow it long after the sanitizer ran.
func TestExtractGitArchiveRefusesSymlinks(t *testing.T) {
	chains := []struct {
		name   string
		links  []tarEntry
		finals []tarEntry
	}{
		{
			name: "..-named links",
			links: []tarEntry{
				{symlink: "..self", target: "."},
				{symlink: "..up", target: "..self/.."},
				{symlink: "..up/..self", target: "."},
				{symlink: "..common", target: "..up/..self/.."},
			},
			finals: []tarEntry{{name: "..common/hooks/pre-commit", body: "#!/bin/sh\necho pwned\n", mode: 0o755}},
		},
		{
			name: "ordinary names",
			links: []tarEntry{
				{symlink: "self", target: "."},
				{symlink: "up", target: "self/.."},
				{symlink: "up/self", target: "."},
				{symlink: "common", target: "up/self/.."},
			},
			finals: []tarEntry{{name: "common/hooks/pre-commit", body: "#!/bin/sh\necho pwned\n", mode: 0o755}},
		},
		{
			name:   "in-root link to the config",
			links:  []tarEntry{{symlink: "config", target: "../config"}},
			finals: []tarEntry{{name: "HEAD", body: "ref: refs/heads/main\n"}},
		},
	}
	for _, tc := range chains {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			dst := filepath.Join(parent, "worktrees", "tmp")
			entries := append(append([]tarEntry{}, tc.links...), tc.finals...)
			if err := extractGitArchive(tarArchive(t, entries...), dst); err == nil {
				t.Fatal("extractGitArchive accepted a symlink entry")
			}
			for _, outside := range []string{
				filepath.Join(parent, "hooks", "pre-commit"),
				filepath.Join(parent, "worktrees", "hooks", "pre-commit"),
				filepath.Join(parent, "config"),
			} {
				if _, err := os.Stat(outside); err == nil {
					t.Fatalf("archive wrote outside the extraction root: %s", outside)
				}
			}
		})
	}
}

// A repository that carries a symlink still checkpoints and restores: the writer
// drops the link instead of failing the upload, so no released client is stranded.
func TestCheckpointArchiveDropsLinks(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	if err := os.Symlink("HEAD", filepath.Join(src, ".git", "link-to-head")); err != nil {
		t.Fatal(err)
	}

	archive, err := archiveLocalGitStateDir(filepath.Join(src, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitStateDir: %v", err)
	}
	dst := t.TempDir()
	if err := extractGitArchive(archive, dst); err != nil {
		t.Fatalf("a checkpoint that dropped its link must still restore: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "link-to-head")); !os.IsNotExist(err) {
		t.Fatalf("link-to-head was archived (lstat err = %v), want it dropped", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "HEAD")); err != nil {
		t.Fatalf("HEAD missing from the restored archive: %v", err)
	}
}

// A symbolic HEAD (core.preferSymlinkRefs) is the one link a checkpoint has to
// keep: it is recorded in the regular-file form git reads as the same ref, so the
// restored gitdir still points at the workspace's branch.
func TestCheckpointArchiveRecordsSymbolicHead(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	headPath := filepath.Join(src, ".git", "HEAD")
	if err := os.Remove(headPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("refs/heads/main", headPath); err != nil {
		t.Fatal(err)
	}

	archive, err := archiveLocalGitStateDir(filepath.Join(src, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitStateDir: %v", err)
	}
	dst := t.TempDir()
	if err := extractGitArchive(archive, dst); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	info, err := os.Lstat(filepath.Join(dst, "HEAD"))
	if err != nil {
		t.Fatalf("HEAD missing from the archive: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("HEAD was archived as a link; the restore refuses those")
	}
	body, err := os.ReadFile(filepath.Join(dst, "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ref: refs/heads/main\n" {
		t.Fatalf("HEAD = %q, want the symbolic ref", body)
	}
	// The recorded form is what git reads as the same symbolic ref. (The extracted
	// tree has no object store of its own — the restore supplies it — so this
	// asserts the ref file rather than running git against a half-built repo.)
	if out := run(t, src, "symbolic-ref", "HEAD"); out != "refs/heads/main" {
		t.Fatalf("source symbolic-ref = %q", out)
	}
}

// The final check has to revalidate the common repository too: a reload can point
// the workspace at another common workspace while the restore runs, and the earlier
// resolution captured the old one.
func TestSwapLinkedGitDirRejectsChangedCommon(t *testing.T) {
	localRoot := t.TempDir()
	g := &Git{localRoot: localRoot}
	mainWS := &workspace{id: "main", root: "/repo", workspaceKind: "main"}
	otherWS := &workspace{id: "main-2", root: "/repo-2", workspaceKind: "main"}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt",
		workspaceKind: "linked", commonID: "main", gitDirRel: "worktrees/wt", worktreeName: "wt",
	}
	g.workspaces = []*workspace{mainWS, otherWS, linkedWS}

	expectedCommon := g.gitDirFor("/repo")
	expectedGitDir := filepath.Join(expectedCommon, "worktrees", "wt")
	if err := os.MkdirAll(expectedGitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	prepared := t.TempDir()

	// The row moved to another common repository: the prepared path belongs to the
	// old one, so the swap is refused.
	linkedWS.setLinkedMeta("linked", "main-2", "worktrees/wt", "wt")
	err := g.swapLinkedGitDir(linkedWS, expectedCommon, expectedGitDir, prepared)
	if err == nil {
		t.Fatal("a common repository change during restore was accepted")
	}
	if !strings.Contains(err.Error(), "common repository changed") {
		t.Fatalf("error = %v, want the common repository refusal", err)
	}

	// A missing common workspace is refused the same way.
	linkedWS.setLinkedMeta("linked", "gone", "worktrees/wt", "wt")
	if err := g.swapLinkedGitDir(linkedWS, expectedCommon, expectedGitDir, prepared); err == nil {
		t.Fatal("a missing common workspace was accepted")
	}

	// A changed admin path is refused too.
	linkedWS.setLinkedMeta("linked", "main", "worktrees/other", "other")
	if err := g.swapLinkedGitDir(linkedWS, expectedCommon, expectedGitDir, prepared); err == nil {
		t.Fatal("a changed worktree gitdir was accepted")
	}
}

// The check and the swap have to be one frozen step: a reload that lands between
// them would change what the swap deletes. The mechanism is asserted directly —
// while the linked identity is frozen, a reload waits.
func TestWorkspaceIdentityIsFrozenForTheSwap(t *testing.T) {
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt",
		workspaceKind: "linked", commonID: "main", gitDirRel: "worktrees/wt", worktreeName: "wt",
	}

	started := make(chan struct{})
	release := make(chan struct{})
	frozen := make(chan struct{})
	go func() {
		defer close(frozen)
		_ = linkedWS.withFrozenLinkedTarget(func(_, _, _, _ string) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	reloaded := make(chan struct{})
	go func() {
		linkedWS.setLinkedMeta("linked", "main", "worktrees/other", "other")
		close(reloaded)
	}()
	select {
	case <-reloaded:
		t.Fatal("a reload applied its linked identity while the swap section was frozen")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	<-frozen
	select {
	case <-reloaded:
	case <-time.After(10 * time.Second):
		t.Fatal("the reload never applied after the frozen section ended")
	}
	if got := linkedWS.gitDirRelPath(); got != "worktrees/other" {
		t.Fatalf("reload did not apply after the swap section: %q", got)
	}
}

// An unchanged workspace swaps the prepared directory in.
func TestSwapLinkedGitDirReplacesTheAdminDir(t *testing.T) {
	localRoot := t.TempDir()
	g := &Git{localRoot: localRoot}
	mainWS := &workspace{id: "main", root: "/repo", workspaceKind: "main"}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt",
		workspaceKind: "linked", commonID: "main", gitDirRel: "worktrees/wt", worktreeName: "wt",
	}
	g.workspaces = []*workspace{mainWS, linkedWS}
	commonGitDir := g.gitDirFor("/repo")
	gitDir := filepath.Join(commonGitDir, "worktrees", "wt")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "stale"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	prepared := filepath.Join(t.TempDir(), "prepared")
	if err := os.MkdirAll(prepared, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prepared, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := g.swapLinkedGitDir(linkedWS, commonGitDir, gitDir, prepared); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "HEAD")); err != nil {
		t.Fatalf("prepared directory was not swapped in: %v", err)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "stale")); !os.IsNotExist(err) {
		t.Fatalf("the previous admin directory survived the swap (stat err = %v)", err)
	}
	// The linked root's .git pointer now names the admin directory.
	pointer, err := os.ReadFile(filepath.Join(localRoot, "overlay", "repo", "wt", ".git"))
	if err != nil {
		t.Fatalf("read worktree pointer: %v", err)
	}
	if !strings.Contains(string(pointer), gitDir) {
		t.Fatalf("worktree pointer = %q, want it to name %q", pointer, gitDir)
	}
}

// tarEntry is one member of a test checkpoint archive: a regular file, or a
// symlink when symlink is set.
type tarEntry struct {
	name    string
	body    string
	mode    int64
	symlink string
	target  string
}

func tarArchive(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range entries {
		hdr := &tar.Header{Name: entry.name, Mode: entry.mode, Typeflag: tar.TypeReg, Size: int64(len(entry.body))}
		if entry.symlink != "" {
			hdr.Name = entry.symlink
			hdr.Typeflag = tar.TypeSymlink
			hdr.Size = 0
			hdr.Linkname = entry.target
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write header %q: %v", hdr.Name, err)
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(entry.body)); err != nil {
				t.Fatalf("write body %q: %v", hdr.Name, err)
			}
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

// seedGitState uploads a minimal checkpoint for a workspace, so a restore of it
// would start writing the overlay before failing on the archive contents.
func seedGitState(t *testing.T, baseURL, workspaceID string) {
	t.Helper()
	archive := tarArchive(t,
		tarEntry{name: "HEAD", body: "ref: refs/heads/main\n"},
		tarEntry{name: "config", body: "[core]\n\trepositoryformatversion = 0\n"},
	)
	sum := sha256.Sum256(archive)
	_, err := client.New(baseURL, "").UpsertGitState(context.Background(), workspaceID, client.GitStateRequest{
		CheckpointCommit: strings.Repeat("a", 40),
		StorageType:      gitStateStorageTarGzNoObjects,
		ChecksumSHA256:   fmt.Sprintf("%x", sum),
		SizeBytes:        int64(len(archive)),
		Content:          archive,
	})
	if err != nil {
		t.Fatalf("seed git state for %s: %v", workspaceID, err)
	}
}
