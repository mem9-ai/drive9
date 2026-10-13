// Package layerfs defines the /.drive9/layers on-filesystem layout shared
// by the layer mount extension and the CLI: marker codecs, snapshot
// walking, deep copies (fork/checkpoint), and the client-side two-phase
// commit. Everything here runs through the generic filesystem client —
// layer data never touches dedicated backend interfaces.
package layerfs

import (
	"strconv"
	"strings"
)

// The namespace (all ordinary files in the classic view):
//
//	/.drive9/layers/<layer-id>/
//	  tree/<path...>       overlay content (dirs, files, symlinks; mode is chmod)
//	  blackouts/<path...>  whiteout markers, content "<kind>:<baseRev>"
//	  cas/<path...>        commit-CAS markers, content "<baseRev>" (0 = base must not exist)
//	  attrs/<path...>      metadata-only markers, content JSON {"mode":..,"mtime":..}
//	  checkpoints/<cp-id>/{tree,blackouts,cas,attrs}/...   checkpoint snapshots
//	  commits/<commit-id>/...                              two-phase commit journals
const (
	Root              = "/.drive9/layers"
	TreeSubdir        = "tree"
	BlackoutSubdir    = "blackouts"
	BlackoutDirSubdir = "blackout-dirs"
	CasSubdir         = "cas"
	AttrsSubdir       = "attrs"
	CommitsSubdir     = "commits"
)

// Roots bundles the marker-tree prefixes of one layer (or checkpoint).
type Roots struct {
	Tree         string
	Blackouts    string
	BlackoutDirs string
	Cas          string
	Attrs        string
}

// LayerRoot returns the namespace root of a layer.
func LayerRoot(layerID string) string { return Root + "/" + layerID }

// CheckpointRoot returns the snapshot root of a layer checkpoint.
func CheckpointRoot(layerID, checkpointID string) string {
	return LayerRoot(layerID) + "/checkpoints/" + checkpointID
}

// RootsOf derives the marker roots from a layer (or checkpoint) base path.
func RootsOf(base string) Roots {
	return Roots{
		Tree:         base + "/" + TreeSubdir,
		Blackouts:    base + "/" + BlackoutSubdir,
		BlackoutDirs: base + "/" + BlackoutDirSubdir,
		Cas:          base + "/" + CasSubdir,
		Attrs:        base + "/" + AttrsSubdir,
	}
}

// MarkerPath joins a root with a server-absolute path.
func MarkerPath(root, serverPath string) string { return root + serverPath }

// Blackout decodes a blackout marker: the deleted base's revision ("-"
// when none was recorded). File and directory whiteouts live in separate
// roots so a directory whiteout never collides with its children's
// markers — one path cannot be both a marker file and a directory.
type Blackout struct {
	IsDir   bool
	BaseRev int64
	HasBase bool
}

// Encode renders the marker body.
func (b Blackout) Encode() []byte {
	if b.HasBase {
		return []byte(strconv.FormatInt(b.BaseRev, 10))
	}
	return []byte("-")
}

// DecodeBlackout parses marker content.
func DecodeBlackout(content []byte) Blackout {
	text := strings.TrimSpace(string(content))
	out := Blackout{}
	if text != "" && text != "-" {
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			out.BaseRev = n
			out.HasBase = true
		}
	}
	return out
}

// AttrsPatch decodes an attrs/<path> marker (metadata-only change).
type AttrsPatch struct {
	Mode  uint32 `json:"mode"`
	Mtime int64  `json:"mtime,omitempty"` // unix seconds; 0 = unchanged
}

// EncodeCas renders a cas marker: a positive revision pins the classic
// base the content was derived from; 0 requires the base to be absent.
func EncodeCas(baseRev int64, hasBase bool) []byte {
	if !hasBase {
		return []byte("0")
	}
	return []byte(strconv.FormatInt(baseRev, 10))
}

// DecodeCas parses a cas marker; hasBase is false for the "must be absent"
// anchor.
func DecodeCas(content []byte) (baseRev int64, hasBase bool) {
	text := strings.TrimSpace(string(content))
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, n > 0
	}
	return 0, false
}
