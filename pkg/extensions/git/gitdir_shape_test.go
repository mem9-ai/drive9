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
	"os/exec"
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

// The on-disk checks in the resolver are what protect a mount whose local overlay was
// written by a previous build, so they are pinned directly and at the restore level.
func TestLinkedWorktreeGitDirRefusesSymlinkedComponents(t *testing.T) {
	root := t.TempDir()
	common := filepath.Join(root, "repo", ".git")
	if err := os.MkdirAll(filepath.Join(common, "refs", "heads"), 0o755); err != nil {
		t.Fatal(err)
	}

	// "worktrees" as a link into the branch store.
	if err := os.Symlink("refs", filepath.Join(common, "worktrees")); err != nil {
		t.Fatal(err)
	}
	if got, err := linkedWorktreeGitDir(common, "worktrees/heads", "heads"); err == nil {
		t.Fatalf("linkedWorktreeGitDir = %q, want a refusal for a symlinked worktrees dir", got)
	}
	if err := os.Remove(filepath.Join(common, "worktrees")); err != nil {
		t.Fatal(err)
	}

	// "worktrees" real, the admin dir itself a link.
	if err := os.MkdirAll(filepath.Join(common, "worktrees"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(common, "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../hooks", filepath.Join(common, "worktrees", "wt")); err != nil {
		t.Fatal(err)
	}
	if got, err := linkedWorktreeGitDir(common, "worktrees/wt", "wt"); err == nil {
		t.Fatalf("linkedWorktreeGitDir = %q, want a refusal for a symlinked admin dir", got)
	}

	// A real admin directory still resolves.
	if err := os.Remove(filepath.Join(common, "worktrees", "wt")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(common, "worktrees", "wt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got, err := linkedWorktreeGitDir(common, "worktrees/wt", "wt"); err != nil || got != filepath.Join(common, "worktrees", "wt") {
		t.Fatalf("linkedWorktreeGitDir = %q, %v; want the real admin dir", got, err)
	}
}

// A restore with a checkpoint available, on a mount whose overlay already carries the
// planted link: the refusal has to be the symlink one, not "checkpoint not available".
func TestRestoreRefusesSymlinkedWorktreesWithACheckpoint(t *testing.T) {
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
	// A checkpoint exists, so the restore cannot stop at "not available".
	seedGitState(t, ts.URL, "linked")
	seedGitState(t, ts.URL, "main")

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

	err = g.restoreWorkspaceState(context.Background(), linkedWS)
	if err == nil {
		t.Fatal("restore accepted a symlinked worktrees directory")
	}
	// The specific refusal, not any message that happens to mention "worktrees":
	// the shape rule alone would produce a different one.
	if !strings.Contains(err.Error(), "is not a worktrees directory") {
		t.Fatalf("restore error = %v, want the symlinked worktrees refusal", err)
	}
	got, readErr := os.ReadFile(headPath)
	if readErr != nil {
		t.Fatalf("refs/heads/heads was removed by the refused restore: %v", readErr)
	}
	if string(got) != "ref: refs/heads/main\n" {
		t.Fatalf("refs/heads/heads = %q, want the original content", got)
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
// names, both resolve a write outside the extraction root. A link entry is dropped
// rather than created, so nothing lands outside and an older checkpoint still
// restores.
func TestExtractGitArchiveDropsSymlinks(t *testing.T) {
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
			if err := extractGitArchive(tarArchive(t, entries...), dst); err != nil {
				t.Fatalf("a dropped link must not fail the extraction: %v", err)
			}
			// No link was created: the name may exist as a real directory (a later
			// entry can create one), but never as a symlink.
			for _, link := range tc.links {
				info, err := os.Lstat(filepath.Join(dst, link.symlink))
				if err == nil && info.Mode()&os.ModeSymlink != 0 {
					t.Fatalf("link %q was extracted", link.symlink)
				}
				if err != nil && !os.IsNotExist(err) {
					t.Fatalf("lstat %q: %v", link.symlink, err)
				}
			}
			// ...and nothing landed outside the extraction root.
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
	// git itself reads the extracted form as the same symbolic ref. Run it against the
	// EXTRACTED directory: on the source repository this could never fail for the
	// archive. git only accepts the directory as a git dir with its object and ref
	// stores present, and symbolic-ref needs nothing in them.
	for _, dir := range []string{"objects", "refs/heads"} {
		if err := os.MkdirAll(filepath.Join(dst, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if out := gitOut(t, "--git-dir", dst, "symbolic-ref", "HEAD"); out != "refs/heads/main" {
		t.Fatalf("extracted symbolic-ref = %q, want refs/heads/main", out)
	}
}

// gitOut runs git and returns its trimmed stdout.
func gitOut(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, stderr.String())
	}
	return strings.TrimSpace(string(out))
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

// The registry section must not take the registry again, and the swap must be inside
// it. This stalls the section on the common workspace's write lock — i.e. after the
// registry read lock is held — queues a registry writer there, releases, and requires
// both the swap and the writer to finish. With the post-swap verification moved back
// inside the section its second read lock queues behind that writer, which is waiting
// for the section, and the swap never returns.
func TestSwapLinkedGitDirCompletesWithAQueuedRegistryWriter(t *testing.T) {
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
	prepared := t.TempDir()
	if err := os.WriteFile(filepath.Join(prepared, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Stalling the common's root read holds the swap inside the registry section.
	mainWS.mu.Lock() //nolint:staticcheck // the test holds the workspace lock deliberately
	mainStalled := true
	releaseMain := func() {
		if mainStalled {
			mainStalled = false
			mainWS.mu.Unlock()
		}
	}
	t.Cleanup(releaseMain)

	swapErr := make(chan error, 1)
	go func() { swapErr <- g.swapLinkedGitDir(linkedWS, commonGitDir, gitDir, prepared) }()

	// Wait until the swap holds the registry: a failed TryLock (write) means a reader
	// holds it, which in this test can only be the section.
	registryHeld := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !g.mu.TryLock() {
			registryHeld = true
			break
		}
		g.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	if !registryHeld {
		t.Fatal("the swap never entered its registry section")
	}

	// Queue a writer for the registry while the section is provably held.
	writerSeen := make(chan struct{}, 1)
	writerDone := make(chan struct{})
	writerSawRegistry := make(chan int, 1)
	go func() {
		writerSeen <- struct{}{}
		g.mu.Lock()
		writerSawRegistry <- len(g.workspaces)
		g.mu.Unlock()
		close(writerDone)
	}()
	<-writerSeen
	queued := false
	if time.Now().After(deadline) {
		// A slow first poll must not shorten the second one.
		deadline = time.Now().Add(10 * time.Second)
	}
	for time.Now().Before(deadline) {
		if g.mu.TryRLock() {
			g.mu.RUnlock()
			time.Sleep(time.Millisecond)
			continue
		}
		queued = true
		break
	}
	if !queued {
		t.Fatal("the queued writer never reached the registry lock")
	}

	releaseMain()
	select {
	case err := <-swapErr:
		if err != nil {
			t.Fatalf("swap: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the swap never completed with a registry writer queued; nothing inside the section may take the registry again")
	}
	select {
	case <-writerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the queued registry writer never completed")
	}
	if got := <-writerSawRegistry; got != 2 {
		t.Fatalf("the queued writer saw %d workspaces, want 2", got)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "HEAD")); err != nil {
		t.Fatalf("prepared directory was not swapped in: %v", err)
	}
}

// An older checkpoint records a symbolic HEAD as a link (core.preferSymlinkRefs).
// The extractor drops links, but HEAD has to become the regular-file form: dropping
// it left the clone's HEAD in place, which is the remote's default branch and not the
// branch the workspace was on.
func TestExtractGitArchiveRecordsAHeadLink(t *testing.T) {
	dst := t.TempDir()
	if err := extractGitArchive(tarArchive(t, tarEntry{symlink: "HEAD", target: "refs/heads/feature"}), dst); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	info, err := os.Lstat(filepath.Join(dst, "HEAD"))
	if err != nil {
		t.Fatalf("HEAD missing from the extraction: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("HEAD was extracted as a link")
	}
	body, err := os.ReadFile(filepath.Join(dst, "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ref: refs/heads/feature\n" {
		t.Fatalf("HEAD = %q, want the symbolic ref in regular-file form", body)
	}
	// A target that cannot be a single ref line is refused, not written.
	if err := extractGitArchive(tarArchive(t, tarEntry{symlink: "HEAD", target: "refs/heads/a\nb"}), t.TempDir()); err == nil {
		t.Fatal("extractGitArchive recorded a HEAD target that cannot be a ref line")
	}
}

// The restore-level case for a standalone workspace: the checkpoint's branch wins over
// the branch the remote clone happens to default to.
func TestRestoreKeepsASymbolicHeadFromAnOlderCheckpoint(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, src, "rev-parse", "HEAD")
	run(t, src, "branch", "feature")

	localRoot := t.TempDir()
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "file://"+src, head)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	// The shape the previous writer produced for core.preferSymlinkRefs = true: the
	// branch a clone does not create locally travels in the archive with its ref.
	seedGitStateArchive(t, ts.URL, "main", tarArchive(t,
		tarEntry{symlink: "HEAD", target: "refs/heads/feature"},
		tarEntry{name: "refs/heads/feature", body: head + "\n"},
	))

	g, err := New(Options{Client: client.New(ts.URL, ""), LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	ws := &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}
	g.workspaces = []*workspace{ws}
	if err := g.restoreWorkspaceState(context.Background(), ws); err != nil {
		t.Fatalf("restore: %v", err)
	}
	repo := filepath.Join(localRoot, "overlay", "repo")
	body, err := os.ReadFile(filepath.Join(repo, ".git", "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ref: refs/heads/feature\n" {
		t.Fatalf("restored HEAD = %q, want the checkpoint's branch", body)
	}
	if out := run(t, repo, "symbolic-ref", "--short", "HEAD"); out != "feature" {
		t.Fatalf("restored branch = %q, want feature", out)
	}
}

// The same case for a linked workspace, where the dropped HEAD was fatal: the admin
// directory has no other HEAD, so the restore failed with "missing a usable HEAD".
func TestRestoreKeepsALinkedSymbolicHeadFromAnOlderCheckpoint(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, src, "rev-parse", "HEAD")

	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(filepath.Dir(repo), 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, filepath.Dir(repo), "clone", "-q", "file://"+src, repo)
	run(t, repo, "worktree", "add", "-q", "wt", "-b", "b1")

	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "file://"+src, head)
	b.SeedGitWorktree("linked", "/repo/wt", "file://"+src, head, "main", ".git/worktrees/wt")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := g.checkpointWorkspace(ctx, &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}, filepath.Join(repo, ".git")); err != nil {
		t.Fatalf("main checkpoint: %v", err)
	}
	// The linked checkpoint an older build wrote: HEAD as a link, commondir relative.
	seedGitStateArchive(t, ts.URL, "linked", tarArchive(t,
		tarEntry{symlink: "HEAD", target: "refs/heads/b1"},
		tarEntry{name: "commondir", body: "../..\n"},
	))
	if err := os.RemoveAll(filepath.Join(localRoot, "overlay")); err != nil {
		t.Fatal(err)
	}

	restored := &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}
	linked := &workspace{
		id: "linked", root: "/repo/wt", headCommit: head, repoURL: "file://" + src,
		workspaceKind: "linked", commonID: "main", gitDirRel: ".git/worktrees/wt", worktreeName: "wt",
	}
	g.workspaces = []*workspace{restored, linked}
	if err := g.restoreWorkspaceState(ctx, restored); err != nil {
		t.Fatalf("main restore: %v", err)
	}
	if err := g.restoreWorkspaceState(ctx, linked); err != nil {
		t.Fatalf("linked restore of a checkpoint whose HEAD is a link: %v", err)
	}
	adminDir := filepath.Join(localRoot, "overlay", "repo", ".git", "worktrees", "wt")
	body, err := os.ReadFile(filepath.Join(adminDir, "HEAD"))
	if err != nil {
		t.Fatalf("admin HEAD: %v", err)
	}
	if string(body) != "ref: refs/heads/b1\n" {
		t.Fatalf("admin HEAD = %q, want the checkpoint's branch", body)
	}
	if out := run(t, filepath.Join(localRoot, "overlay", "repo", "wt"), "symbolic-ref", "--short", "HEAD"); out != "b1" {
		t.Fatalf("linked branch = %q, want b1", out)
	}
}

// The swap resolves the admin target itself, after the earlier check ran and before it
// deletes anything. This pins that resolution: with the path a link into the branch
// store the swap has to refuse, and the store must be untouched.
func TestSwapLinkedGitDirRefusesASymlinkedWorktreesDir(t *testing.T) {
	localRoot := t.TempDir()
	g := &Git{localRoot: localRoot}
	mainWS := &workspace{id: "main", root: "/repo", workspaceKind: "main"}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt",
		workspaceKind: "linked", commonID: "main", gitDirRel: "worktrees/wt", worktreeName: "wt",
	}
	g.workspaces = []*workspace{mainWS, linkedWS}
	commonGitDir := g.gitDirFor("/repo")
	if err := os.MkdirAll(filepath.Join(commonGitDir, "refs", "heads"), 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(commonGitDir, "refs", "heads", "keep")
	if err := os.WriteFile(marker, []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("refs", filepath.Join(commonGitDir, "worktrees")); err != nil {
		t.Fatal(err)
	}
	prepared := t.TempDir()
	if err := os.WriteFile(filepath.Join(prepared, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := g.swapLinkedGitDir(linkedWS, commonGitDir, filepath.Join(commonGitDir, "worktrees", "wt"), prepared)
	if err == nil {
		t.Fatal("the swap accepted a symlinked worktrees directory")
	}
	if !strings.Contains(err.Error(), "is not a worktrees directory") {
		t.Fatalf("swap error = %v, want the symlinked worktrees refusal", err)
	}
	if _, readErr := os.ReadFile(marker); readErr != nil {
		t.Fatalf("the branch store the link pointed at was modified: %v", readErr)
	}
	// The refusal has to come BEFORE the swap deletes or renames anything: through the
	// link, the prepared directory would land in the branch store as refs/wt.
	if _, statErr := os.Stat(filepath.Join(commonGitDir, "refs", "wt")); !os.IsNotExist(statErr) {
		t.Fatalf("the prepared directory was swapped through the link (stat err = %v)", statErr)
	}
	if _, statErr := os.Stat(prepared); statErr != nil {
		t.Fatalf("the prepared directory was consumed by a refused swap: %v", statErr)
	}
}

// The post-swap verification has to run outside the registry section and still report a
// row that moved before it ran. The writer below is queued while the section holds the
// registry, so it applies the new identity before the verification can read it: the swap
// therefore finishes (the prepared directory is in place) and the result is an error,
// not a success for a mount that no longer owns the path.
func TestSwapLinkedGitDirReportsATargetThatMovedAfterTheSection(t *testing.T) {
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
	prepared := t.TempDir()
	if err := os.WriteFile(filepath.Join(prepared, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Stalling the common's root read holds the swap inside the registry section.
	mainWS.mu.Lock() //nolint:staticcheck // the test holds the workspace lock deliberately
	mainStalled := true
	releaseMain := func() {
		if mainStalled {
			mainStalled = false
			mainWS.mu.Unlock()
		}
	}
	t.Cleanup(releaseMain)

	swapErr := make(chan error, 1)
	go func() {
		swapErr <- g.swapLinkedGitDir(linkedWS, commonGitDir, gitDir, prepared)
	}()
	registryHeld := false
	waitDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(waitDeadline) {
		if !g.mu.TryLock() {
			registryHeld = true
			break
		}
		g.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	if !registryHeld {
		t.Fatal("the swap never entered its registry section")
	}

	// Queue a reload that moves the workspace to another admin directory.
	writerDone := make(chan struct{})
	go func() {
		g.mu.Lock()
		linkedWS.setLinkedMeta("linked", "main", "worktrees/other", "other")
		g.mu.Unlock()
		close(writerDone)
	}()
	queued := false
	for time.Now().Before(waitDeadline) {
		if g.mu.TryRLock() {
			g.mu.RUnlock()
			time.Sleep(time.Millisecond)
			continue
		}
		queued = true
		break
	}
	if !queued {
		t.Fatal("the queued reload never reached the registry lock")
	}
	releaseMain()
	err := <-swapErr
	if err == nil {
		t.Fatal("the swap reported success while the row moved under it")
	}
	if !strings.Contains(err.Error(), "target changed while it was restored") {
		t.Fatalf("swap error = %v, want the moved-target refusal", err)
	}
	select {
	case <-writerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the queued reload never completed")
	}
	// The swap itself did complete: the prepared directory is at the path it approved.
	if _, statErr := os.Stat(filepath.Join(gitDir, "HEAD")); statErr != nil {
		t.Fatalf("the prepared directory was not swapped in: %v", statErr)
	}
	if got := linkedWS.gitDirRelPath(); got != "worktrees/other" {
		t.Fatalf("the reload did not apply: %q", got)
	}
}

// The common repository's checkpoint is tenant-supplied, so it can carry a regular
// file where a worktree admin directory belongs. The restore materializes the common
// repository first and then resolves the admin path again inside the swap, right before
// it deletes anything: that resolution is what refuses the planted path, and the file
// it planted is left where it is.
func TestRestoreRefusesAPlantedWorktreeAdminPath(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, src, "rev-parse", "HEAD")

	localRoot := t.TempDir()
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "file://"+src, head)
	b.SeedGitWorktree("linked", "/repo/wt", "file://"+src, head, "main", ".git/worktrees/wt")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	// "worktrees/wt" is a file, so the admin path exists but is not a directory.
	seedGitStateArchive(t, ts.URL, "main", tarArchive(t,
		tarEntry{name: "HEAD", body: "ref: refs/heads/main\n"},
		tarEntry{name: "config", body: "[core]\n\trepositoryformatversion = 0\n"},
		tarEntry{name: "worktrees/wt", body: "planted"},
	))
	seedGitState(t, ts.URL, "linked")

	g, err := New(Options{Client: client.New(ts.URL, ""), LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	mainWS := &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}
	linkedWS := &workspace{
		id: "linked", root: "/repo/wt", headCommit: head, repoURL: "file://" + src,
		workspaceKind: "linked", commonID: "main", gitDirRel: ".git/worktrees/wt", worktreeName: "wt",
	}
	g.workspaces = []*workspace{mainWS, linkedWS}
	err = g.restoreWorkspaceState(context.Background(), linkedWS)
	if err == nil {
		t.Fatal("restore accepted a common repository whose admin path is a regular file")
	}
	if !strings.Contains(err.Error(), "worktree admin path") || !strings.Contains(err.Error(), "is not a directory") {
		t.Fatalf("restore error = %v, want the planted admin path refusal", err)
	}
	planted := filepath.Join(localRoot, "overlay", "repo", ".git", "worktrees", "wt")
	body, readErr := os.ReadFile(planted)
	if readErr != nil {
		t.Fatalf("the planted admin path was replaced instead of refused: %v", readErr)
	}
	if string(body) != "planted" {
		t.Fatalf("planted admin path = %q, want it untouched", body)
	}
	// The linked root was never pointed at the planted shape.
	if _, statErr := os.Stat(filepath.Join(localRoot, "overlay", "repo", "wt", ".git")); !os.IsNotExist(statErr) {
		t.Fatalf("the linked root was written despite the refusal (stat err = %v)", statErr)
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
	seedGitStateArchive(t, baseURL, workspaceID, tarArchive(t,
		tarEntry{name: "HEAD", body: "ref: refs/heads/main\n"},
		tarEntry{name: "config", body: "[core]\n\trepositoryformatversion = 0\n"},
	))
}

// seedGitStateArchive uploads an exact checkpoint archive, so a test can produce the
// shapes an older build wrote (a symbolic HEAD recorded as a link, a planted path).
func seedGitStateArchive(t *testing.T, baseURL, workspaceID string, archive []byte) {
	t.Helper()
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
