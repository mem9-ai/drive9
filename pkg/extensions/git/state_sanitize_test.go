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
