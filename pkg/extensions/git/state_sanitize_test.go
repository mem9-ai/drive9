package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeCheckpointRepo lays out a repository admin directory the way git validates
// one (HEAD plus objects/ and refs/) with the given config body. The sanitizer only
// rewrites config where git would read it, so fixtures must have that skeleton.
func writeCheckpointRepo(t *testing.T, dir, config string) {
	t.Helper()
	for _, sub := range []string{"objects", "refs"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if config == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A checkpoint archive is tenant-writable but is restored as a live .git on the
// mount host, so hooks and command-bearing config must not survive the restore.
func TestSanitizeGitCheckpointDirStripsExecutionSurface(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(dir, "hooks", "post-checkout")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ntouch /tmp/pwned\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	config := `[core]
	repositoryformatversion = 0
	filemode = true
	bare = false
	logallrefupdates = true
	fsmonitor = "touch /tmp/pwned"
	pager = "sh -c 'touch /tmp/pwned'"
	sshCommand = "ssh -oProxyCommand='touch /tmp/pwned'"
	askPass = /tmp/evil
	editor = /tmp/evil
	hooksPath = /tmp/evil-hooks
	alternateRefsCommand = "touch /tmp/pwned"
[filter "evil"]
	clean = "touch /tmp/pwned"
	smudge = "touch /tmp/pwned"
[diff "evil"]
	textconv = "touch /tmp/pwned"
[merge "evil"]
	driver = "touch /tmp/pwned"
[include]
	path = /etc/passwd
[includeIf "gitdir:/"]
	path = /etc/passwd
[url "ssh://evil.example/"]
	insteadOf = https://github.com/
[remote "origin"]
	url = https://github.com/example/repo.git
	fetch = +refs/heads/*:refs/remotes/origin/*
[remote "evil"]
	url = ext::sh -c 'touch /tmp/pwned'
	vcs = evil
	uploadpack = touch /tmp/pwned
	receivepack = touch /tmp/pwned
	proxy = touch /tmp/pwned
[branch "main"]
	remote = origin
	merge = refs/heads/main
[credential]
	helper = "!touch /tmp/pwned"
`
	cfgPath := filepath.Join(dir, "config")
	if err := os.WriteFile(cfgPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatalf("sanitize: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "hooks")); !os.IsNotExist(err) {
		t.Fatalf("hooks directory survived sanitization: %v", err)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	for _, banned := range []string{
		"fsmonitor", "pager", "sshCommand", "askPass", "editor", "hooksPath",
		"alternateRefsCommand", "filter", "textconv", "driver", "include",
		"insteadOf", "credential", "helper",
		// remote keys that name a program git runs
		"vcs", "uploadpack", "receivepack", "proxy", "ext::",
		// a checkpoint must not redirect the worktree outside the repository
		"core.worktree",
	} {
		if strings.Contains(got, banned) {
			t.Errorf("config still contains %q:\n%s", banned, got)
		}
	}
	// The remote and branch configuration a checkpoint needs must survive.
	for _, kept := range []string{
		"[core]", "repositoryformatversion = 0",
		"[remote \"origin\"]", "url = https://github.com/example/repo.git",
		"[branch \"main\"]",
	} {
		if !strings.Contains(got, kept) {
			t.Errorf("config dropped %q:\n%s", kept, got)
		}
	}
	if mode := fileMode(t, cfgPath); mode != 0o600 {
		t.Errorf("config mode = %o, want 600", mode)
	}
}

func TestSanitizeGitCheckpointDirWithoutConfig(t *testing.T) {
	dir := t.TempDir()
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatalf("sanitize without config: %v", err)
	}
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

// A backslash continuation belongs to the line before it; dropping the
// continuation of a kept value would leave git with a malformed config.
func TestSanitizeGitCheckpointConfigKeepsContinuations(t *testing.T) {
	dir := t.TempDir()
	cfg := "[remote \"origin\"]\n\turl = https://example.com/\\\n\trepo.git\n[core]\n\tfsmonitor = /tmp/evil\n"
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if !strings.Contains(got, "https://example.com/\\\n\trepo.git") {
		t.Fatalf("continuation was dropped:\n%s", got)
	}
	if strings.Contains(got, "fsmonitor") {
		t.Fatalf("executable key survived:\n%s", got)
	}
}

// Hooks never need to leave the mounting host, so the checkpoint writer skips
// them even before the restore-side sanitizer runs.
func TestCheckpointArchiveSkipsHooks(t *testing.T) {
	gitDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(gitDir, "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	content, err := archiveLocalGitStateDir(gitDir)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if strings.HasPrefix(hdr.Name, "hooks/") {
			t.Fatalf("archive carries %q", hdr.Name)
		}
	}
}

// extensions.worktreeconfig moves core.* keys into config.worktree; a
// checkpoint must not be able to hide an executable key there.
func TestSanitizeGitCheckpointDirStripsWorktreeConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("[core]\n\trepositoryformatversion = 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	worktreeConfig := "[core]\n\tfsmonitor = /tmp/evil\n\thooksPath = /tmp/evil-hooks\n"
	if err := os.WriteFile(filepath.Join(dir, "config.worktree"), []byte(worktreeConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "config.worktree"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); strings.Contains(got, "fsmonitor") || strings.Contains(got, "hooksPath") {
		t.Fatalf("config.worktree kept executable keys:\n%s", got)
	}
}

// git config tolerates a key on the section-header line; the sanitizer must not
// write one back, or it would bypass the per-section policy.
func TestSanitizeGitCheckpointConfigDropsHeaderLineKeys(t *testing.T) {
	dir := t.TempDir()
	cfg := "[core] fsmonitor = /tmp/evil\n[remote \"origin\"] url = ext::sh -c 'touch /tmp/pwned'\n[core]\n\trepositoryformatversion = 0\n"
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	for _, banned := range []string{"fsmonitor", "ext::"} {
		if strings.Contains(got, banned) {
			t.Fatalf("header-line key survived (%q):\n%s", banned, got)
		}
	}
	if !strings.Contains(got, "repositoryformatversion = 0") {
		t.Fatalf("safe section lost:\n%s", got)
	}
}

// The archive skip applies to the top-level hooks/ and objects/ directories, not
// to a ref or file that happens to be named "hooks".
func TestCheckpointArchiveKeepsRefsNamedHooks(t *testing.T) {
	gitDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(gitDir, "refs", "heads", "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "refs", "heads", "hooks", "branch"), []byte("sha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(gitDir, "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	for _, rel := range []string{"objects", "hooks", "hooks/post-checkout"} {
		if !shouldSkipGitObjectStatePath(rel, nil) {
			t.Errorf("shouldSkipGitObjectStatePath(%q) = false, want true", rel)
		}
	}
	for _, rel := range []string{"refs/heads/hooks", "refs/heads/hooks/branch", "config", "HEAD"} {
		if shouldSkipGitObjectStatePath(rel, nil) {
			t.Errorf("shouldSkipGitObjectStatePath(%q) = true, want false", rel)
		}
	}
}

// A syntactically valid header with a trailing comment used to update the
// parser's section without being emitted, so the following keys were written
// under the previous (kept) section. Parser state and emitted text must not
// diverge.
func TestSanitizeGitCheckpointConfigKeepsHeaderWithTrailingComment(t *testing.T) {
	dir := t.TempDir()
	cfg := "[core]\n[user] # comment\nfsmonitor = /tmp/evil\n"
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	// The header must be emitted, so the key stays scoped to "user" and can
	// never become core.fsmonitor after a restore.
	if !strings.Contains(got, "[user]") {
		t.Fatalf("trailing-comment header was dropped:\n%s", got)
	}
	userIdx := strings.Index(got, "[user]")
	if fsmonitorIdx := strings.Index(got, "fsmonitor"); fsmonitorIdx >= 0 && fsmonitorIdx < userIdx {
		t.Fatalf("fsmonitor emitted before the [user] header:\n%s", got)
	}
	if coreIdx := strings.Index(got, "[core]"); coreIdx >= 0 && userIdx < coreIdx {
		t.Fatalf("section order changed:\n%s", got)
	}
}

// core.worktree redirects the repository's worktree to an arbitrary host path;
// tenant-controlled checkpoint state must not be able to do that.
func TestSanitizeGitCheckpointConfigDropsCoreWorktree(t *testing.T) {
	dir := t.TempDir()
	cfg := "[core]\n\trepositoryformatversion = 0\n\tworktree = /tmp/evil-worktree\n[extensions]\n\tobjectformat = sha1\n"
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(got, "worktree") {
		t.Fatalf("core.worktree survived the sanitizer:\n%s", got)
	}
	if !strings.Contains(got, "repositoryformatversion = 0") || !strings.Contains(got, "objectformat = sha1") {
		t.Fatalf("safe keys were dropped:\n%s", got)
	}
}

// A continuation line belongs to the value that started it: when that value is
// rejected, every physical line of it must be dropped too, or a header on the
// continuation becomes live configuration under the previous section.
func TestSanitizeGitCheckpointConfigDropsRejectedValueContinuation(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  string
		// banned must not appear anywhere in the sanitized output.
		banned []string
		// kept must still appear.
		kept []string
	}{
		{
			name:   "rejected remote key continuation",
			cfg:    "[remote \"origin\"]\n\tuploadpack = ignored \\\n[core] fsmonitor = /path/to/program\n\turl = https://github.com/example/repo.git\n",
			banned: []string{"uploadpack", "fsmonitor"},
			kept:   []string{"url = https://github.com/example/repo.git"},
		},
		{
			name:   "rejected helper-scheme url continuation",
			cfg:    "[remote \"origin\"]\n\turl = ext::sh -c 'x' \\\n[core] pager = /tmp/evil\n",
			banned: []string{"ext::", "pager"},
		},
		{
			name:   "rejected allowlist key continuation",
			cfg:    "[core]\n\tpager = less \\\n[remote \"origin\"]\n\turl = https://github.com/example/repo.git\n",
			banned: []string{"pager"},
		},
		{
			name:   "dropped section continuation",
			cfg:    "[filter \"evil\"]\n\tclean = cmd \\\n[core]\n\tfsmonitor = /tmp/evil\n",
			banned: []string{"filter", "clean", "fsmonitor"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config")
			if err := os.WriteFile(path, []byte(tc.cfg), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := sanitizeGitCheckpointConfig(path); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got := string(data)
			for _, banned := range tc.banned {
				if strings.Contains(got, banned) {
					t.Fatalf("rejected value leaked %q:\n%s", banned, got)
				}
			}
			for _, kept := range tc.kept {
				if !strings.Contains(got, kept) {
					t.Fatalf("kept value %q missing:\n%s", kept, got)
				}
			}
		})
	}
}

// A backslash inside an inline comment does not continue the value: git ends it
// at the unquoted '#'/';'. Treating it as a continuation swallowed the next
// physical line verbatim, which is a path for "[core] fsmonitor = ...".
func TestSanitizeGitCheckpointConfigIgnoresCommentContinuation(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  string
	}{
		{
			name: "hash comment",
			cfg:  "[user]\n\tname = example # \\\n[core] fsmonitor = /path/to/program\n",
		},
		{
			name: "semicolon comment",
			cfg:  "[user]\n\tname = example ; \\\n[core] pager = /path/to/program\n",
		},
		{
			name: "escaped trailing backslash",
			cfg:  "[user]\n\tname = example\\\\\n[core] fsmonitor = /path/to/program\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config")
			if err := os.WriteFile(path, []byte(tc.cfg), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := sanitizeGitCheckpointConfig(path); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			got := string(data)
			for _, banned := range []string{"fsmonitor", "pager"} {
				if strings.Contains(got, banned) {
					t.Fatalf("false continuation leaked %q:\n%s", banned, got)
				}
			}
			if !strings.Contains(got, "name = example") {
				t.Fatalf("kept user value missing:\n%s", got)
			}
		})
	}
}

// A quoted inline comment keeps a real continuation working, and an escaped
// backslash ends the value.
func TestGitConfigValueContinues(t *testing.T) {
	for _, tc := range []struct {
		line string
		want bool
	}{
		{`url = https://example.com/\`, true},
		{`url = https://example.com/\\`, false},
		{`name = example # \`, false},
		{`name = example ; \`, false},
		{`name = "a # b" \`, true},
		{`name = "trailing\\"`, false},
		{"", false},
	} {
		var scan gitConfigValueState
		if got := scan.continues(tc.line, true); got != tc.want {
			t.Errorf("continues(%q) = %v, want %v", tc.line, got, tc.want)
		}
	}
}

// Git keeps its quote flag across a backslash-newline, so the quote a value opens
// on one physical line can be closed by the next. Scanning each line with a fresh
// quote state reported the comment's trailing backslash as a continuation and
// then emitted the following "[core] fsmonitor" line verbatim.
func TestGitConfigValueStateCarriesQuoteAcrossContinuation(t *testing.T) {
	var scan gitConfigValueState
	if !scan.continues(`name = "example\`, true) {
		t.Fatal("opening quote with a trailing backslash must continue the value")
	}
	if scan.continues(`" # \`, false) {
		t.Fatal("the continuation line closes the quote, so its comment backslash cannot continue the value")
	}
	// A completed value carries nothing into the next one.
	if scan.continues(`next = value\`, true) != true {
		t.Fatal("a fresh value must start with a clean quote state")
	}
	if scan.continues(`tail`, false) {
		t.Fatal("continuation ended without a trailing backslash")
	}
	if scan.inQuotes {
		t.Fatal("quote state must not survive the end of a value")
	}
}

// The full shape of the reported bypass: the closed quote ends the value, the
// comment's backslash does not continue it, and the smuggled key is dropped.
func TestSanitizeGitCheckpointConfigCarriesQuoteStateAcrossLines(t *testing.T) {
	const cfg = `[user]
	name = "example\
" # \
[core]
	fsmonitor = /path/to/program
	repositoryformatversion = 0
`
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(got, "fsmonitor") {
		t.Fatalf("fsmonitor survived a quote-carrying continuation:\n%s", got)
	}
	if strings.Contains(got, "/path/to/program") {
		t.Fatalf("command path survived a quote-carrying continuation:\n%s", got)
	}
	if !strings.Contains(got, "repositoryformatversion = 0") {
		t.Fatalf("kept core keys were dropped:\n%s", got)
	}
}

// git itself must agree with the sanitizer's reading: the emitted file has to
// parse, and the command-bearing key must be absent from git's own key list.
func TestSanitizeGitCheckpointConfigQuoteContinuationMatchesGit(t *testing.T) {
	gitBin, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git is not installed")
	}
	const cfg = `[user]
	name = "example\
" # \
[core]
	fsmonitor = /path/to/program
	repositoryformatversion = 0
`
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(gitBin, "config", "--file", path, "--list").CombinedOutput()
	if err != nil {
		t.Fatalf("git rejected the sanitized config: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "fsmonitor") {
		t.Fatalf("git still sees fsmonitor:\n%s", out)
	}
	if !strings.Contains(string(out), "core.repositoryformatversion=0") {
		t.Fatalf("git lost the kept core key:\n%s", out)
	}
}

// With extensions.worktreeConfig set, git reads a linked worktree's core.* keys
// from <gitdir>/worktrees/<name>/config.worktree. Sanitizing only the top-level
// config left that file live, so a checkpoint could still name a program to run.
func TestSanitizeGitCheckpointDirStripsLinkedWorktreeConfig(t *testing.T) {
	dir := t.TempDir()
	adminDir := filepath.Join(dir, "worktrees", "wt-1")
	if err := os.MkdirAll(adminDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(adminDir, "gitdir"), []byte("/repo/.git/worktrees/wt-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy := "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n\tfilemode = true\n"
	if err := os.WriteFile(filepath.Join(adminDir, "config"), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	worktreeConfig := "[core]\n\tfsmonitor = /tmp/evil\n\tpager = /tmp/evil-pager\n\tsshCommand = /tmp/evil-ssh\n\tfilemode = true\n"
	if err := os.WriteFile(filepath.Join(adminDir, "config.worktree"), []byte(worktreeConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"config", "config.worktree"} {
		data, err := os.ReadFile(filepath.Join(adminDir, name))
		if err != nil {
			t.Fatalf("read worktree %s: %v", name, err)
		}
		got := string(data)
		for _, banned := range []string{"fsmonitor", "pager", "sshCommand", "/tmp/evil"} {
			if strings.Contains(got, banned) {
				t.Fatalf("worktree %s kept executable surface %q:\n%s", name, banned, got)
			}
		}
		if !strings.Contains(got, "filemode = true") {
			t.Fatalf("worktree %s lost its safe core key:\n%s", name, got)
		}
	}
	// The admin directory itself must survive: git needs it to resolve the
	// worktree, and dropping it would break a legitimate restore.
	if _, err := os.Stat(filepath.Join(adminDir, "gitdir")); err != nil {
		t.Fatalf("worktree admin dir was removed: %v", err)
	}
}

// `git submodule` commands run git inside the submodule's repository, so its
// gitdir carries the same executable surface as the superproject's.
func TestSanitizeGitCheckpointDirStripsSubmoduleConfig(t *testing.T) {
	dir := t.TempDir()
	moduleDir := filepath.Join(dir, "modules", "dep")
	// A real submodule gitdir: HEAD plus the objects/ and refs/ directories git
	// validates before it will run anything from the repository.
	for _, sub := range []string{"objects", "refs", "hooks"} {
		if err := os.MkdirAll(filepath.Join(moduleDir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(moduleDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n\thooksPath = /tmp/evil-hooks\n"
	if err := os.WriteFile(filepath.Join(moduleDir, "config"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(moduleDir, "hooks", "post-checkout"), []byte("#!/bin/sh\ntouch /tmp/pwned\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(moduleDir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"fsmonitor", "hooksPath"} {
		if strings.Contains(string(data), banned) {
			t.Fatalf("submodule config kept %q:\n%s", banned, string(data))
		}
	}
	if _, err := os.Stat(filepath.Join(moduleDir, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("submodule hook survived the sanitizer (err=%v)", err)
	}
}

// git resolves worktrees/<name> and modules/<name> by name, so a link there would
// point git — and the sanitizer's rewrites — at a directory outside the restored
// tree. Links are dropped rather than followed, and the target keeps its bytes.
func TestSanitizeGitCheckpointDirDropsAdminDirLinks(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	hostile := "[core]\n\tfsmonitor = /tmp/evil\n"
	// A linked worktree admin directory and a linked submodule path component.
	target := filepath.Join(gitDir, "real", "wt-1")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(target, "config.worktree")
	if err := os.WriteFile(victim, []byte(hostile), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, parent := range []string{filepath.Join(gitDir, "worktrees"), filepath.Join(gitDir, "modules", "libs")} {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(target, filepath.Join(gitDir, "worktrees", "wt-1")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Join("..", "..", "real"), filepath.Join(gitDir, "modules", "libs", "foo")); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	for _, link := range []string{
		filepath.Join(gitDir, "worktrees", "wt-1"),
		filepath.Join(gitDir, "modules", "libs", "foo"),
	} {
		if _, err := os.Lstat(link); !os.IsNotExist(err) {
			t.Fatalf("admin directory link %s survived (err=%v)", link, err)
		}
	}
	if data, err := os.ReadFile(victim); err != nil || string(data) != hostile {
		t.Fatalf("link target was rewritten: data=%q err=%v", string(data), err)
	}
}

// A branch may be named "config". Its loose ref and reflog are not configuration,
// and the restore must leave both exactly as they were.
func TestSanitizeGitCheckpointDirPreservesBranchNamedConfig(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n")
	const oid = "0123456789abcdef0123456789abcdef01234567"
	const reflog = "0000000000000000000000000000000000000000 " + oid + " A U Thor <a@example.com> 1 +0000\tcommit: x\n"
	for path, body := range map[string]string{
		filepath.Join(gitDir, "refs", "heads", "config"):         oid + "\n",
		filepath.Join(gitDir, "logs", "refs", "heads", "config"): reflog,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(gitDir, "refs", "heads", "config")); err != nil || string(data) != oid+"\n" {
		t.Fatalf("branch named config was rewritten: data=%q err=%v", string(data), err)
	}
	if data, err := os.ReadFile(filepath.Join(gitDir, "logs", "refs", "heads", "config")); err != nil || string(data) != reflog {
		t.Fatalf("reflog for the branch named config was rewritten: data=%q err=%v", string(data), err)
	}
	data, err := os.ReadFile(filepath.Join(gitDir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("the repository config was not sanitized:\n%s", string(data))
	}
}

// With a branch called config/topic, refs/heads/config is a directory. The walk must
// not try to read it as a config file (EISDIR would abort the restore).
func TestSanitizeGitCheckpointDirToleratesBranchDirectoryNamedConfig(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	topic := filepath.Join(gitDir, "refs", "heads", "config", "topic")
	if err := os.MkdirAll(filepath.Dir(topic), 0o755); err != nil {
		t.Fatal(err)
	}
	const oid = "fedcba9876543210fedcba9876543210fedcba98\n"
	if err := os.WriteFile(topic, []byte(oid), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatalf("sanitize aborted on a branch directory named config: %v", err)
	}
	if data, err := os.ReadFile(topic); err != nil || string(data) != oid {
		t.Fatalf("branch config/topic was rewritten: data=%q err=%v", string(data), err)
	}
}

// A submodule may itself be named "hooks". Its gitdir is modules/.../hooks, and
// its parent is a plain namespace directory, so the parent's hooks child is a
// repository that must survive the walk.
func TestSanitizeGitCheckpointDirPreservesSubmoduleNamedHooks(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "libs", "hooks")
	for _, dir := range []string{filepath.Join(sub, "objects"), filepath.Join(sub, "refs"), filepath.Join(sub, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "config"), []byte("[core]\n\tfsmonitor = /tmp/evil\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sub, "HEAD")); err != nil {
		t.Fatalf("submodule named hooks was deleted: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(sub, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("submodule named hooks kept fsmonitor:\n%s", string(data))
	}
	// Its own hooks directory is a hooks directory and still goes away.
	if _, err := os.Stat(filepath.Join(sub, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("hooks inside the submodule survived: %v", err)
	}
}

// A HEAD (or config) marker written into modules/ must not make the walk treat the
// namespace as a repository and skip a submodule below it.
func TestSanitizeGitCheckpointDirIgnoresNamespaceMarkers(t *testing.T) {
	gitDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(gitDir, "modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "modules", "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "modules", "config"), []byte("[core]\n\tfilemode = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(gitDir, "modules", "objects")
	for _, dir := range []string{filepath.Join(sub, "objects"), filepath.Join(sub, "refs"), filepath.Join(sub, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "config"), []byte("[core]\n\tfsmonitor = /tmp/evil\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(sub, "config"))
	if err != nil {
		t.Fatalf("submodule named objects was skipped: %v", err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("submodule named objects kept fsmonitor:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(sub, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("submodule named objects kept its hook: %v", err)
	}
}

// A config that is itself a symlink would make the rewrite edit its target
// instead of the file git reads.
func TestSanitizeGitCheckpointConfigDropsSymlinkedConfig(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "shared.conf")
	const shared = "[core]\n\trepositoryformatversion = 0\n"
	if err := os.WriteFile(target, []byte(shared), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "config")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := sanitizeGitCheckpointConfig(link); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("symlinked config survived (err=%v)", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != shared {
		t.Fatalf("sanitizer rewrote the symlink target:\n%s", string(data))
	}
}

// extensions.worktreeConfig decides whether git reads any per-worktree config at
// all, so a restore must not let a checkpoint switch it on.
func TestSanitizeGitCheckpointConfigDropsWorktreeConfigExtension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	cfg := "[core]\n\trepositoryformatversion = 1\n[extensions]\n\tworktreeconfig = true\n\tobjectformat = sha256\n"
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(strings.ToLower(got), "worktreeconfig") {
		t.Fatalf("extensions.worktreeConfig survived:\n%s", got)
	}
	if !strings.Contains(got, "objectformat = sha256") {
		t.Fatalf("safe extension was dropped:\n%s", got)
	}
}

// A submodule's admin path is <gitdir>/modules/<name>, and <name> is the
// submodule's path in the superproject — so it may contain slashes and is not a
// single directory entry. The walk has to descend through those components.
func TestSanitizeGitCheckpointDirStripsNestedSubmoduleName(t *testing.T) {
	dir := t.TempDir()
	moduleDir := filepath.Join(dir, "modules", "libs", "foo")
	writeCheckpointRepo(t, moduleDir, "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n")
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(moduleDir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("nested submodule config kept fsmonitor:\n%s", string(data))
	}
}

// Nested repositories are not bounded by a fixed depth: a submodule chain or a
// submodule of a submodule keeps nesting, and every level is still git config
// git will read.
func TestSanitizeGitCheckpointDirSanitizesDeepRepositoryNesting(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "modules", "a", "modules", "b", "modules", "c", "modules", "d", "modules", "e")
	writeCheckpointRepo(t, deep, "[core]\n\tpager = /tmp/evil-pager\n")
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(deep, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("deeply nested config kept pager:\n%s", string(data))
	}
}

// A submodule may carry one of git's own directory names ("objects", "info",
// "refs"). Its admin directory is then modules/<that name> and is a repository
// whose config must be sanitized, so the inert-name skip only applies inside a
// repository's own directory.
func TestSanitizeGitCheckpointDirStripsInertNamedSubmodule(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"objects", "info", "refs"} {
		writeCheckpointRepo(t, filepath.Join(dir, "modules", name), "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n")
	}
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"objects", "info", "refs"} {
		data, err := os.ReadFile(filepath.Join(dir, "modules", name, "config"))
		if err != nil {
			t.Fatalf("read modules/%s/config: %v", name, err)
		}
		if strings.Contains(string(data), "fsmonitor") {
			t.Fatalf("submodule named %q kept fsmonitor:\n%s", name, string(data))
		}
	}
}

// The inert skip still applies inside a repository: its own objects/ tree is not
// walked (and must not be emptied), while a repository nested under modules/ is.
func TestSanitizeGitCheckpointDirKeepsRepoObjectsIntact(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	objDir := filepath.Join(dir, "objects", "ab")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objDir, "cdef"), []byte("object"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(objDir, "cdef")); err != nil || string(data) != "object" {
		t.Fatalf("objects/ content was touched: data=%q err=%v", string(data), err)
	}
}

// Git parses a config that starts with a UTF-8 byte-order mark, so the sanitizer
// has to strip the mark before deciding whether the file is configuration. Without
// that, a BOM-prefixed config skipped sanitising entirely and its core.fsmonitor
// stayed live.
func TestSanitizeGitCheckpointConfigStripsBOM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	body := append([]byte{0xEF, 0xBB, 0xBF}, []byte("[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n")...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointConfig(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("BOM-prefixed config kept fsmonitor:\n%s", string(data))
	}
	if !strings.Contains(string(data), "repositoryformatversion = 0") {
		t.Fatalf("BOM-prefixed config lost its safe core key:\n%s", string(data))
	}
}

// A long chain of nested submodules must be walked once per directory: sanitising a
// directory's registries and then descending into them again multiplies the work
// per level, so a checkpoint with a deep chain could make the restore crawl.
func TestSanitizeGitCheckpointDirVisitsDeepChainOnce(t *testing.T) {
	dir := t.TempDir()
	deep := dir
	for i := 0; i < 24; i++ {
		deep = filepath.Join(deep, "modules", fmt.Sprintf("m%d", i))
		writeCheckpointRepo(t, deep, "[core]\n\tfsmonitor = /tmp/evil\n\tpager = /tmp/evil-pager\n")
	}
	start := time.Now()
	if err := sanitizeGitCheckpointDir(dir); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("sanitising a 24-level chain took %s; the walk is no longer linear", elapsed)
	}
	data, err := os.ReadFile(filepath.Join(deep, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") || strings.Contains(string(data), "pager") {
		t.Fatalf("deepest config kept executable keys:\n%s", string(data))
	}
}

// A worktree's commondir names the directory git reads the shared config and default
// hooks from. A checkpoint can put them in a sibling directory that has no HEAD of
// its own, where the repository predicate does not apply, so the pointer is followed
// and that directory is sanitized too.
func TestSanitizeGitCheckpointDirSanitizesCommonDir(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "dep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte("shared\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(sub, "shared")
	for _, dir := range []string{filepath.Join(shared, "objects"), filepath.Join(shared, "refs"), filepath.Join(shared, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(shared, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(shared, "config"))
	if err != nil {
		t.Fatalf("common dir config: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("common dir config kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(shared, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("common dir hooks survived: %v", err)
	}
}

// A commondir that leaves the restored tree would point git at an arbitrary host
// directory, so it is dropped: the restore writes the canonical relative pointer for
// a linked worktree afterwards.
func TestSanitizeGitCheckpointDirDropsCommondirOutsideTree(t *testing.T) {
	outside := t.TempDir()
	victim := filepath.Join(outside, "config")
	const hostile = "[core]\n\tpager = /tmp/evil-pager\n"
	if err := os.WriteFile(victim, []byte(hostile), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{victim, "../../../../../../tmp"} {
		gitDir := t.TempDir()
		writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
		if err := os.WriteFile(filepath.Join(gitDir, "commondir"), []byte(value+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := sanitizeGitCheckpointDir(gitDir); err != nil {
			t.Fatalf("commondir %q: %v", value, err)
		}
		if _, err := os.Lstat(filepath.Join(gitDir, "commondir")); !os.IsNotExist(err) {
			t.Fatalf("commondir %q survived (err=%v)", value, err)
		}
	}
	if data, err := os.ReadFile(victim); err != nil || string(data) != hostile {
		t.Fatalf("a directory outside the tree was rewritten: data=%q err=%v", string(data), err)
	}
}

// Git accepts a repository whose refs entry is an executable regular file and then
// reads its config, so the repository predicate must not require a refs directory.
func TestSanitizeGitCheckpointDirSanitizesExecutableRefsFileRepo(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "dep")
	for _, dir := range []string{filepath.Join(sub, "objects"), filepath.Join(sub, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	refs := filepath.Join(sub, "refs")
	if err := os.WriteFile(refs, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(refs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(sub, "config"))
	if err != nil {
		t.Fatalf("repository with an executable refs file was skipped: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("repository with an executable refs file kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(sub, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("repository with an executable refs file kept its hook: %v", err)
	}
}

// A submodule branch may be named "worktrees/team/hooks": its loose ref and reflog
// live below refs/ and logs/, and neither is an admin directory.
func TestSanitizeGitCheckpointDirPreservesBranchNamedWorktrees(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	sub := filepath.Join(gitDir, "modules", "dep")
	writeCheckpointRepo(t, sub, "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = /tmp/evil\n")
	const oid = "0123456789abcdef0123456789abcdef01234567"
	const reflog = "0000000000000000000000000000000000000000 " + oid + " A U Thor <a@example.com> 1 +0000\tcommit: x\n"
	for path, body := range map[string]string{
		filepath.Join(sub, "refs", "heads", "worktrees", "team", "hooks"):         oid + "\n",
		filepath.Join(sub, "logs", "refs", "heads", "worktrees", "team", "hooks"): reflog,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(sub, "refs", "heads", "worktrees", "team", "hooks")); err != nil || string(data) != oid+"\n" {
		t.Fatalf("branch ref was rewritten: data=%q err=%v", string(data), err)
	}
	if data, err := os.ReadFile(filepath.Join(sub, "logs", "refs", "heads", "worktrees", "team", "hooks")); err != nil || string(data) != reflog {
		t.Fatalf("branch reflog was rewritten: data=%q err=%v", string(data), err)
	}
	data, err := os.ReadFile(filepath.Join(sub, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("submodule config was not sanitized:\n%s", string(data))
	}
}

// Git strips only the trailing line terminator from commondir, so a leading space is
// part of the directory name. A sanitizer that trims it rewrites a different
// directory than git reads.
func TestSanitizeGitCheckpointDirResolvesCommondirLikeGit(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "dep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A leading space is preserved by git, which resolves " shared" not "shared".
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte(" shared\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(sub, " shared")
	for _, dir := range []string{filepath.Join(shared, "objects"), filepath.Join(shared, "refs"), filepath.Join(shared, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(shared, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(shared, "config"))
	if err != nil {
		t.Fatalf("space-prefixed common dir config: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("space-prefixed common dir kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(shared, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("space-prefixed common dir kept its hook: %v", err)
	}
}

// Git resolves commondir with realpath, so a symlink in the middle of the path makes
// it read a directory the sanitizer would not pick if it resolved lexically.
func TestSanitizeGitCheckpointDirResolvesCommondirThroughSymlinks(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "dep")
	for _, dir := range []string{filepath.Join(sub, "objects", "real", "child"), filepath.Join(sub, "objects", "real", "shared"), filepath.Join(sub, "objects", "real", "shared", "hooks"), filepath.Join(sub, "refs")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte("objects/link/../shared\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// objects/link is a symlink to objects/real/child, so git resolves
	// objects/link/../shared to objects/real/shared. A lexical clean picks
	// objects/shared instead.
	if err := os.Symlink(filepath.Join("real", "child"), filepath.Join(sub, "objects", "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	// git resolves objects/link -> objects/real/child, so objects/link/../shared is
	// objects/real/shared; a lexical clean would pick objects/shared instead.
	real := filepath.Join(sub, "objects", "real", "shared")
	if err := os.WriteFile(filepath.Join(real, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(real, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(real, "config"))
	if err != nil {
		t.Fatalf("realpath common dir config: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("realpath common dir kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(real, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("realpath common dir kept its hook: %v", err)
	}
}

// A namespace directory that carries repository markers must not hide a complete
// submodule gitdir placed below one of its storage-shaped names: git rejects the
// namespace's empty HEAD, so the child is the only repository there.
func TestSanitizeGitCheckpointDirKeepsWalkingNamespaceStorageNames(t *testing.T) {
	gitDir := t.TempDir()
	ns := filepath.Join(gitDir, "modules", "ns")
	for _, dir := range []string{filepath.Join(ns, "objects"), filepath.Join(ns, "refs")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(ns, "HEAD"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(ns, "objects", "dep")
	writeCheckpointRepo(t, child, "[core]\n\tfsmonitor = /tmp/evil\n")
	if err := os.MkdirAll(filepath.Join(child, "hooks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(child, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(child, "config"))
	if err != nil {
		t.Fatalf("child gitdir below an objects directory was skipped: %v", err)
	}
	if strings.Contains(string(data), "fsmonitor") {
		t.Fatalf("child gitdir below an objects directory kept fsmonitor:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(child, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("child gitdir below an objects directory kept its hook: %v", err)
	}
}

// A common store can be visited before the repository that points at it: os.ReadDir
// sorts names, so modules/a-shared is walked (and, having no HEAD, not sanitized)
// before modules/z-wt resolves its commondir to it. Having been walked is not proof
// that its administrative files were handled.
func TestSanitizeGitCheckpointDirSanitizesCommonDirWalkedFirst(t *testing.T) {
	gitDir := t.TempDir()
	shared := filepath.Join(gitDir, "modules", "a-shared")
	for _, dir := range []string{filepath.Join(shared, "objects"), filepath.Join(shared, "refs"), filepath.Join(shared, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(shared, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// z-wt sorts after a-shared, so the common store is walked first.
	worktree := filepath.Join(gitDir, "modules", "z-wt")
	if err := os.MkdirAll(worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "commondir"), []byte("../a-shared\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(shared, "config"))
	if err != nil {
		t.Fatalf("common store config: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("common store walked first kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(shared, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("common store walked first kept its hook: %v", err)
	}
}

// Branches "topic/HEAD" and "topic/commondir" satisfy the repository predicate, so a
// submodule's refs/heads/topic is classified as an admin directory. Its loose refs —
// including the one named commondir, whose object id is not a path — must keep their
// bytes, and so must the matching reflog.
func TestSanitizeGitCheckpointDirPreservesBranchNamedCommondir(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	sub := filepath.Join(gitDir, "modules", "dep")
	writeCheckpointRepo(t, sub, "[core]\n\trepositoryformatversion = 0\n")
	const oid = "0123456789abcdef0123456789abcdef01234567"
	const reflog = "0000000000000000000000000000000000000000 " + oid + " A U Thor <a@example.com> 1 +0000\tcommit: x\n"
	paths := map[string]string{
		filepath.Join(sub, "refs", "heads", "topic", "HEAD"):              oid + "\n",
		filepath.Join(sub, "refs", "heads", "topic", "commondir"):         oid + "\n",
		filepath.Join(sub, "logs", "refs", "heads", "topic", "HEAD"):      reflog,
		filepath.Join(sub, "logs", "refs", "heads", "topic", "commondir"): reflog,
	}
	for path, body := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	want := paths
	for path, body := range want {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s was removed: %v", path, err)
		}
		if string(data) != body {
			t.Fatalf("%s was rewritten: got %q want %q", path, string(data), body)
		}
	}
}

// A commondir written against the gitdir's FINAL path does not resolve while the tree
// is still staged, but appears as soon as the rename publishes it. Absolute pointers
// are dropped instead of trusted.
func TestSanitizeGitCheckpointDirDropsAbsoluteCommondir(t *testing.T) {
	gitDir := t.TempDir()
	finalGitDir := filepath.Join(t.TempDir(), ".git")
	sub := filepath.Join(gitDir, "modules", "dep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte(filepath.Join(finalGitDir, "shared")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(sub, "commondir")); !os.IsNotExist(err) {
		t.Fatalf("absolute commondir survived (err=%v)", err)
	}
}

// A relative pointer can climb out of the staged tree and resolve against the host
// after the rename; it is dropped even when it does not resolve yet.
func TestSanitizeGitCheckpointDirDropsEscapingCommondir(t *testing.T) {
	gitDir := t.TempDir()
	sub := filepath.Join(gitDir, "modules", "dep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	escaping := filepath.Join("..", "..", "..", "..", "..", "..", "..", "tmp", "not-yet-there")
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte(escaping+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(sub, "commondir")); !os.IsNotExist(err) {
		t.Fatalf("escaping commondir survived (err=%v)", err)
	}
}

// A hook is identified by its name, so a file that opens with "ref: " cannot pass
// itself off as reference storage: git would run the shell lines after that one.
func TestSanitizeGitCheckpointDirRemovesRefPrefixedHook(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	hooks := filepath.Join(gitDir, "hooks")
	if err := os.MkdirAll(hooks, 0o755); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(hooks, "post-checkout")
	if err := os.WriteFile(hook, []byte("ref: refs/heads/main\ntouch /tmp/pwned\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A name this build does not know yet still goes when it is executable.
	unknown := filepath.Join(hooks, "future-hook")
	if err := os.WriteFile(unknown, []byte("touch /tmp/pwned\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{hook, unknown} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("executable hook %s survived (err=%v)", filepath.Base(path), err)
		}
	}
}

// Branch "topic/hooks" makes the hooks entry a regular file, and a branch
// "topic/hooks/leaf" puts a reflog under it. Neither is a program and both must keep
// their bytes, including the reflog record that is not an object id.
func TestSanitizeGitCheckpointDirPreservesBranchNamedHooks(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	sub := filepath.Join(gitDir, "modules", "dep")
	writeCheckpointRepo(t, sub, "[core]\n\trepositoryformatversion = 0\n")
	const oid = "0123456789abcdef0123456789abcdef01234567"
	const reflog = "0000000000000000000000000000000000000000 " + oid + " A U Thor <a@example.com> 1 +0000\tcommit: x\n"
	paths := map[string]string{
		filepath.Join(sub, "refs", "heads", "topic", "HEAD"):                   oid + "\n",
		filepath.Join(sub, "refs", "heads", "topic", "commondir"):              oid + "\n",
		filepath.Join(sub, "refs", "heads", "leaf", "hooks", "leaf"):           oid + "\n",
		filepath.Join(sub, "refs", "heads", "topic", "hooks", "post-checkout"): oid + "\n",
		filepath.Join(sub, "logs", "refs", "heads", "topic", "HEAD"):           reflog,
	}
	for path, body := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// refs/heads/topic is classified as an admin directory, so its "hooks" child (a
	// direct reflog namespace) must survive too.
	reflogUnderHooks := filepath.Join(sub, "logs", "refs", "heads", "topic", "hooks", "leaf")
	if err := os.MkdirAll(filepath.Dir(reflogUnderHooks), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reflogUnderHooks, []byte(reflog), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatalf("sanitize aborted on reference storage named hooks: %v", err)
	}
	for path := range paths {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("%s was removed: %v", path, err)
		}
	}
	if data, err := os.ReadFile(reflogUnderHooks); err != nil || string(data) != reflog {
		t.Fatalf("reflog under a hooks-named ref was rewritten: data=%q err=%v", string(data), err)
	}
}

// Git's realpath works on a C string, so bytes after a NUL in commondir are ignored
// while Go's path conversion rejects them. Cutting the value at the NUL is what makes
// the sanitizer resolve the directory git will actually read.
func TestSanitizeGitCheckpointDirResolvesCommondirAtNUL(t *testing.T) {
	gitDir := t.TempDir()
	shared := filepath.Join(gitDir, "shared")
	for _, dir := range []string{filepath.Join(shared, "objects"), filepath.Join(shared, "refs"), filepath.Join(shared, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(shared, "config"), []byte("[core]\n\tpager = /tmp/evil-pager\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shared, "hooks", "post-checkout"), []byte("#!/bin/sh\ntrue\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(gitDir, "modules", "dep")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "commondir"), []byte("../../shared\x00ignored\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(shared, "config"))
	if err != nil {
		t.Fatalf("NUL-terminated commondir target config: %v", err)
	}
	if strings.Contains(string(data), "pager") {
		t.Fatalf("NUL-terminated commondir target kept pager:\n%s", string(data))
	}
	if _, err := os.Stat(filepath.Join(shared, "hooks", "post-checkout")); !os.IsNotExist(err) {
		t.Fatalf("NUL-terminated commondir target kept its hook: %v", err)
	}
}

// Executability, not the file name, decides what git can run: an upper-case hook name
// (found on a case-insensitive filesystem) and a "ref: ...;id" one-liner are both
// removed, while a non-executable hook-named file is inert and keeps its bytes.
func TestSanitizeGitCheckpointDirRemovesExecutableHooksOnly(t *testing.T) {
	gitDir := t.TempDir()
	writeCheckpointRepo(t, gitDir, "[core]\n\trepositoryformatversion = 0\n")
	hooks := filepath.Join(gitDir, "hooks")
	if err := os.MkdirAll(hooks, 0o755); err != nil {
		t.Fatal(err)
	}
	executable := map[string]string{
		"POST-CHECKOUT":       "ref: refs/heads/main;id\n",
		"post-checkout":       "#!/bin/sh\ntrue\n",
		"future-hook-unknown": "#!/bin/sh\ntrue\n",
	}
	for name, body := range executable {
		if err := os.WriteFile(filepath.Join(hooks, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// A non-executable file git will not run, and which may be a branch ref.
	inert := map[string]string{
		"post-commit": "0123456789abcdef0123456789abcdef01234567\n",
		"POST-MERGE":  "ref: refs/heads/main\n",
	}
	for name, body := range inert {
		if err := os.WriteFile(filepath.Join(hooks, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := sanitizeGitCheckpointDir(gitDir); err != nil {
		t.Fatal(err)
	}
	for name := range executable {
		if _, err := os.Lstat(filepath.Join(hooks, name)); !os.IsNotExist(err) {
			t.Fatalf("executable hook %s survived (err=%v)", name, err)
		}
	}
	for name, body := range inert {
		data, err := os.ReadFile(filepath.Join(hooks, name))
		if err != nil {
			t.Fatalf("inert file %s was removed: %v", name, err)
		}
		if string(data) != body {
			t.Fatalf("inert file %s was rewritten: %q", name, string(data))
		}
	}
}
