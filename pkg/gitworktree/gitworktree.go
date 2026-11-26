// Package gitworktree holds the shape rules a linked git worktree's identity has
// to satisfy.
//
// Both sides that touch a linked workspace need them and they must not drift: the
// API stores a tenant-supplied gitdir_rel and the mount republishes a tenant
// checkpoint at <common>/.git/<gitdir_rel>, so a value that names anything other
// than the worktree admin directory (".git/hooks", ".git/config", ".git/refs/…")
// turns a restore into a writer for a directory git executes from or reads
// configuration from. The rule is therefore an allow-list of the shape git itself
// uses, not a block-list of directories.
package gitworktree

import (
	"fmt"
	"path/filepath"
	"strings"
)

const (
	// MaxNameBytes bounds a worktree id. git derives it from the worktree
	// directory's basename, so it is a single path segment.
	MaxNameBytes = 255
	// MaxGitDirRelBytes matches the git_workspaces.gitdir_rel column.
	MaxGitDirRelBytes = 1024
	// AdminDirPrefix is the directory under the common .git that holds linked
	// worktree admin directories.
	AdminDirPrefix = "worktrees"
)

// ValidateName validates a worktree id (git's own word for it is the worktree
// name): a single non-empty path segment, at most MaxNameBytes.
func ValidateName(name string) error {
	if name == "" {
		return fmt.Errorf("name is required")
	}
	if len(name) > MaxNameBytes {
		return fmt.Errorf("name exceeds %d bytes", MaxNameBytes)
	}
	if strings.ContainsRune(name, 0) {
		return fmt.Errorf("name contains NUL")
	}
	if strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return fmt.Errorf("name must be a single safe path segment")
	}
	return nil
}

// AdminRel validates gitdirRel as the admin directory of worktreeName and returns
// it normalized (slash form, leading ".git/" removed): always
// "worktrees/<worktreeName>".
func AdminRel(gitdirRel, worktreeName string) (string, error) {
	rel := strings.TrimSpace(gitdirRel)
	if rel == "" {
		return "", fmt.Errorf("gitdir_rel is required for a linked workspace")
	}
	if len(rel) > MaxGitDirRelBytes {
		return "", fmt.Errorf("gitdir_rel exceeds %d bytes", MaxGitDirRelBytes)
	}
	// Reject the raw value's separators before normalizing: a backslash is a path
	// separator to tar and to git on Windows, and checking after ToSlash would let it
	// through on the platform where it matters.
	if strings.ContainsAny(rel, "\\\x00") {
		return "", fmt.Errorf("gitdir_rel contains an invalid character")
	}
	slashed := filepath.ToSlash(rel)
	if strings.HasPrefix(slashed, "/") {
		return "", fmt.Errorf("gitdir_rel must be relative")
	}
	slashed = strings.TrimPrefix(slashed, ".git/")
	parts := strings.Split(slashed, "/")
	if len(parts) != 2 || parts[0] != AdminDirPrefix {
		return "", fmt.Errorf("gitdir_rel must be %s/<worktree_name>", AdminDirPrefix)
	}
	if err := ValidateName(parts[1]); err != nil {
		return "", fmt.Errorf("gitdir_rel name: %w", err)
	}
	if worktreeName == "" {
		// A linked workspace without a recorded name cannot say which admin
		// directory is its own, so it must not restore into any of them.
		return "", fmt.Errorf("worktree_name is required for a linked workspace")
	}
	if parts[1] != worktreeName {
		return "", fmt.Errorf("gitdir_rel must name the registered worktree_name %q", worktreeName)
	}
	return AdminDirPrefix + "/" + parts[1], nil
}
