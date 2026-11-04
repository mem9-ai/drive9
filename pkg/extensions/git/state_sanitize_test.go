package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		if got := gitConfigValueContinues(tc.line); got != tc.want {
			t.Errorf("gitConfigValueContinues(%q) = %v, want %v", tc.line, got, tc.want)
		}
	}
}
