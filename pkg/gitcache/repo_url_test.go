package gitcache

import (
	"strings"
	"testing"
)

func TestValidateRepoURLAcceptsRemoteTransports(t *testing.T) {
	for _, u := range []string{
		"https://github.com/example/repo.git",
		"http://git.example.com/example/repo",
		"ssh://git@github.com/example/repo.git",
		"git://git.example.com/repo.git",
		"git@github.com:example/repo.git",
		"user@git.example.com:team/repo.git",
		// IPv6 literals are hosts, not remote helpers.
		"ssh://[2001:db8::1]/repo.git",
		"https://[2001:db8::1]:8443/repo.git",
		"git@[2001:db8::1]:team/repo.git",
	} {
		if err := ValidateRepoURL(u); err != nil {
			t.Errorf("ValidateRepoURL(%q) = %v, want nil", u, err)
		}
	}
}

// ext:: runs the command the URL names, so it must never reach git clone.
func TestValidateRepoURLRejectsCommandTransports(t *testing.T) {
	for _, u := range []string{
		"ext::sh -c 'touch /tmp/pwned'",
		"ext::whoami",
		"EXT::touch /tmp/pwned",
		"https://example.com/x::y",
		"hg::https://example.com/repo",
		"sh::touch /tmp/pwned",
		"-oProxyCommand=touch /tmp/pwned",
		"--upload-pack=touch /tmp/pwned",
		"",
		"https://example.com/repo\ngit@evil:repo",
	} {
		if err := ValidateRepoURL(u); err == nil {
			t.Errorf("ValidateRepoURL(%q) succeeded, want rejection", u)
		}
	}
}

// A local path or file:// URL would let a tenant read a repository (and its
// history) that the server process can reach.
func TestValidateRepoURLRejectsLocalReposByDefault(t *testing.T) {
	t.Setenv(EnvGitAllowLocalRepos, "")
	for _, u := range []string{
		"file:///srv/secret.git",
		"/srv/secret.git",
		"../../srv/secret.git",
		"C:/repos/secret.git",
	} {
		if err := ValidateRepoURL(u); err == nil {
			t.Errorf("ValidateRepoURL(%q) succeeded, want rejection", u)
		}
	}
}

func TestValidateRepoURLAllowsLocalReposWhenOptedIn(t *testing.T) {
	t.Setenv(EnvGitAllowLocalRepos, "1")
	for _, u := range []string{
		"file:///srv/local.git",
		"/srv/local.git",
	} {
		if err := ValidateRepoURL(u); err != nil {
			t.Errorf("ValidateRepoURL(%q) = %v, want nil", u, err)
		}
	}
	// The opt-in never re-enables a command transport.
	if err := ValidateRepoURL("ext::touch /tmp/pwned"); err == nil {
		t.Fatal("ext:: transport accepted after opting into local repos")
	}
}

func TestGitProtocolPoliciesExcludeExtAndFile(t *testing.T) {
	for _, p := range []string{GitAllowedProtocols, GitLocalAllowedProtocols} {
		for _, banned := range []string{"ext", "file:"} {
			if strings.Contains(p, banned) && !strings.Contains(p, "http:file") {
				t.Errorf("protocol policy %q allows %q", p, banned)
			}
		}
		if strings.Contains(p, "ext") {
			t.Errorf("protocol policy %q allows ext", p)
		}
	}
	if !strings.Contains(GitLocalAllowedProtocols, "file") {
		t.Errorf("local policy %q does not allow file", GitLocalAllowedProtocols)
	}
}

// The remote.<name>.vcs / uploadpack style keys and helper-scheme URLs are
// rejected by the sanitizer using HasRemoteHelperScheme; keep the predicate and
// the URL policy consistent.
func TestHasRemoteHelperScheme(t *testing.T) {
	for _, value := range []string{
		"ext::sh -c 'touch /tmp/pwned'",
		"hg::https://example.com/repo",
		"sh::touch /tmp/pwned",
	} {
		if !HasRemoteHelperScheme(value) {
			t.Errorf("HasRemoteHelperScheme(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"https://github.com/example/repo.git",
		"ssh://[2001:db8::1]/repo.git",
		"git@[2001:db8::1]:team/repo.git",
		"file:///srv/local.git",
	} {
		if HasRemoteHelperScheme(value) {
			t.Errorf("HasRemoteHelperScheme(%q) = true, want false", value)
		}
	}
}
