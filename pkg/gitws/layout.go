// Package gitws defines the FS-native git workspace store under
// /.drive9/git-workspaces/: the on-drive9 layout, the document types
// (manifest, lifecycle state, tree index, pack and git-state metadata),
// and their parse/encode rules. Both the HTTP client (CLI-side writes)
// and the git fs extension (mount-side view transformation) speak this
// layout; nothing else may invent paths under it.
//
// Layout:
//
//	/.drive9/git-workspaces/index.json        existence registry (pkg/gitwsindex)
//	/.drive9/git-workspaces/<id>/manifest.json
//	/.drive9/git-workspaces/<id>/state        dormant | armed | deleted
//	/.drive9/git-workspaces/<id>/tree-index.json
//	/.drive9/git-workspaces/<id>/tree/<commit>/<path...>
//	/.drive9/git-workspaces/<id>/overlay/<path...>
//	/.drive9/git-workspaces/<id>/overlay-blackouts/<path...>
//	/.drive9/git-workspaces/<id>/packs/<pack-id>
//	/.drive9/git-workspaces/<id>/packs/<pack-id>.json
//	/.drive9/git-workspaces/<id>/git-state.tar.gz
//	/.drive9/git-workspaces/<id>/git-state.json
//
// The fs is the index: overlay membership is decided by stat-ing
// overlay/<path> and overlay-blackouts/<path>, with no sidecar manifest.
package gitws

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"
)

// Root is the store root under the reserved /.drive9 namespace.
const Root = "/.drive9/git-workspaces"

// Workspace lifecycle states (the <id>/state file body).
const (
	StateDormant = "dormant"
	StateArmed   = "armed"
	StateDeleted = "deleted"
)

// MaxDocBytes bounds a single JSON document read from the store (manifest,
// tree index, metadata). Tree indexes of very large repositories are the
// biggest expected document; git-state archives and packs are binary and
// stream under their own budgets.
const MaxDocBytes = 64 << 20

// ValidateID rejects workspace IDs that could escape their directory.
func ValidateID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("gitws: workspace id is empty")
	}
	if id != path.Base(id) || strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.ContainsAny(id, ".\x00") {
		return fmt.Errorf("gitws: invalid workspace id %q", id)
	}
	return nil
}

// CleanRel normalizes a workspace-relative path (no leading slash, "."
// for the root). It rejects escaping segments.
func CleanRel(rel string) (string, error) {
	rel = strings.TrimPrefix(strings.TrimSpace(rel), "/")
	if rel == "" {
		return ".", nil
	}
	if strings.Contains(rel, "\\") || strings.Contains(rel, "\x00") {
		return "", fmt.Errorf("gitws: invalid relative path %q", rel)
	}
	clean := path.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("gitws: relative path %q escapes the workspace root", rel)
	}
	return clean, nil
}

// WorkspaceDir is the store directory of one workspace.
func WorkspaceDir(id string) string { return Root + "/" + strings.TrimSpace(id) }

// ManifestPath is the workspace record document.
func ManifestPath(id string) string { return WorkspaceDir(id) + "/manifest.json" }

// StatePath is the lifecycle state file.
func StatePath(id string) string { return WorkspaceDir(id) + "/state" }

// TreeIndexPath is the tracked-path oracle written by the git tooling.
func TreeIndexPath(id string) string { return WorkspaceDir(id) + "/tree-index.json" }

// ValidateCommit rejects commit names that could escape tree/ before they
// are concatenated into store paths (a manifest-supplied head such as
// "../../manifest.json" must never redirect tree reads or writes). Git
// object ids are 40/64-char hex; anything with separators or traversal is
// refused outright.
func ValidateCommit(commit string) error {
	commit = strings.TrimSpace(commit)
	if commit == "" {
		return fmt.Errorf("gitws: commit is empty")
	}
	if isHexOID(commit) {
		return nil
	}
	return fmt.Errorf("gitws: invalid commit name %q", commit)
}

func isHexOID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

// ValidateChecksumName rejects checksums used as store file names that
// could carry path structure (the pointer document is store-writable, so
// its body reference must stay a flat hex name).
func ValidateChecksumName(checksum string) error {
	checksum = strings.TrimSpace(checksum)
	if checksum == "" || len(checksum) > 128 || !isHexOIDLower(checksum) {
		return fmt.Errorf("gitws: invalid checksum name %q", checksum)
	}
	return nil
}

func isHexOIDLower(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// TreeDir is the clean-tree content root for one commit.
func TreeDir(id, commit string) string {
	return WorkspaceDir(id) + "/tree/" + strings.TrimSpace(commit)
}

// TreePath maps a workspace-relative path onto the clean tree store.
func TreePath(id, commit, rel string) string {
	rel, err := CleanRel(rel)
	if err != nil || rel == "." {
		return TreeDir(id, commit)
	}
	return TreeDir(id, commit) + "/" + rel
}

// OverlayDir is the dirty-mirror upsert root.
func OverlayDir(id string) string { return WorkspaceDir(id) + "/overlay" }

// OverlayPath maps a workspace-relative path onto the dirty mirror.
func OverlayPath(id, rel string) string {
	rel, err := CleanRel(rel)
	if err != nil || rel == "." {
		return OverlayDir(id)
	}
	return OverlayDir(id) + "/" + rel
}

// BlackoutDir is the dirty-mirror deletion-marker root.
func BlackoutDir(id string) string { return WorkspaceDir(id) + "/overlay-blackouts" }

// BlackoutPath maps a workspace-relative path onto its deletion marker.
func BlackoutPath(id, rel string) string {
	rel, err := CleanRel(rel)
	if err != nil || rel == "." {
		return BlackoutDir(id)
	}
	return BlackoutDir(id) + "/" + rel
}

// PacksDir is the git object pack root of one workspace.
func PacksDir(id string) string { return WorkspaceDir(id) + "/packs" }

// PackPath is the binary pack body; the pack id must be a flat name.
func PackPath(id, packID string) string {
	return PacksDir(id) + "/" + strings.TrimSpace(packID)
}

// PackMetaPath is the pack's sidecar metadata document.
func PackMetaPath(id, packID string) string { return PackPath(id, packID) + ".json" }

// GitStateMetaPath is the current state pointer document: it names the
// content-addressed archive body that belongs with it, so concurrent
// checkpoints can never pair one archive with another's metadata.
func GitStateMetaPath(id string) string { return WorkspaceDir(id) + "/git-state.json" }

// GitStateDir holds the content-addressed .git state archive versions
// (tar.gz, objects excluded).
func GitStateDir(id string) string { return WorkspaceDir(id) + "/git-state" }

// GitStateBodyPath is one immutable archive version, named by its own
// checksum; the pointer selects the current one.
func GitStateBodyPath(id, checksum string) string {
	return GitStateDir(id) + "/" + strings.TrimSpace(checksum) + ".tar.gz"
}

// PackIDFor derives the content-addressed pack id (retried checkpoints
// dedupe to the same id).
func PackIDFor(content []byte) string {
	sum := sha256.Sum256(content)
	return "pack-" + hex.EncodeToString(sum[:16])
}

// ChecksumHex returns the lowercase hex sha256 of data.
func ChecksumHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// NowUTC stamps sidecar metadata documents.
func NowUTC() time.Time { return time.Now().UTC() }
