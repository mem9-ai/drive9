// Client-side two-phase commit: apply a layer snapshot onto the classic
// base through generic filesystem operations with per-path compare-and-set,
// journalled so a crash anywhere resumes idempotently, then flip the layer
// state server-side (the entry log is empty, so the server's commit is a
// pure state transition).
package layerfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
)

// CommitConflict reports one path the commit refused to touch.
type CommitConflict struct {
	Path         string `json:"path"`
	Reason       string `json:"reason"`
	BaseRevision int64  `json:"base_revision,omitempty"`
	WantRevision int64  `json:"want_revision,omitempty"`
}

// CommitResult reports the outcome of a commit run.
type CommitResult struct {
	Applied   int              `json:"applied"`
	Conflicts []CommitConflict `json:"conflicts,omitempty"`
}

// commitPlanEntry is one journalled apply step.
type commitPlanEntry struct {
	Seq     int    `json:"seq"`
	Path    string `json:"path"`
	Op      string `json:"op"` // upsert-file | upsert-symlink | mkdir | chmod | whiteout
	Mode    uint32 `json:"mode,omitempty"`
	Size    int64  `json:"size,omitempty"`
	BaseRev int64  `json:"base_rev"`
	HasBase bool   `json:"has_base"`
	HasCas  bool   `json:"has_cas"`
	Symlink bool   `json:"symlink,omitempty"`
	Applied bool   `json:"applied"`
}

type commitJournal struct {
	CommitID string             `json:"commit_id"`
	LayerID  string             `json:"layer_id"`
	BaseRoot string             `json:"base_root"`
	Entries  []*commitPlanEntry `json:"entries"`
}

const (
	commitInlineMax = 8 << 20
	commitChunk     = 4 << 20
)

// Commit applies the snapshot of the layer's current tree onto its base
// root. It is journalled and idempotent: a previous crashed run resumes
// before a new plan starts.
func Commit(ctx context.Context, cl *client.Client, layer *client.FSLayer) (*CommitResult, error) {
	// Resume any unfinished journal first (crash recovery).
	if err := resumeUnfinished(ctx, cl, layer.LayerID); err != nil {
		return nil, err
	}
	baseRoot := strings.TrimSuffix(layer.BaseRootPath, "/")
	snap, err := LoadSnapshot(ctx, cl, LayerRoot(layer.LayerID))
	if err != nil {
		return nil, fmt.Errorf("load layer snapshot: %w", err)
	}
	plan, conflicts := planCommit(layer.LayerID, baseRoot, snap)
	if len(conflicts) > 0 {
		return &CommitResult{Conflicts: conflicts}, nil
	}
	cid, err := newCommitID()
	if err != nil {
		return nil, err
	}
	journal := &commitJournal{CommitID: cid, LayerID: layer.LayerID, BaseRoot: baseRoot, Entries: plan}
	if err := writeJournal(ctx, cl, layer.LayerID, journal, false); err != nil {
		return nil, err
	}
	res, err := applyJournal(ctx, cl, journal)
	if err != nil || len(res.Conflicts) > 0 {
		return res, err
	}
	if err := writeJournal(ctx, cl, layer.LayerID, journal, true); err != nil {
		return nil, err
	}
	// Flip the layer state server-side: the entry log is empty, so the
	// server's commit path is a pure committed transition.
	if _, err := cl.CommitFSLayer(ctx, layer.LayerID); err != nil {
		return nil, fmt.Errorf("mark layer committed: %w", err)
	}
	return res, nil
}

// planCommit folds a snapshot into an ordered, CAS-anchored apply plan.
// Order: content upserts (parents first), symlinks, mkdirs, chmods, then
// whiteouts (children first, so a parent dir drains before it goes).
func planCommit(layerID, baseRoot string, snap *Snapshot) ([]*commitPlanEntry, []CommitConflict) {
	var plan []*commitPlanEntry
	var conflicts []CommitConflict
	inScope := func(p string) bool {
		return p == baseRoot || strings.HasPrefix(p, baseRoot+"/")
	}
	paths := make([]string, 0, len(snap.Tree))
	for p := range snap.Tree {
		paths = append(paths, p)
	}
	sort.Strings(paths) // lexical order lists parents before children
	for _, p := range paths {
		entry := snap.Tree[p]
		if !inScope(p) {
			conflicts = append(conflicts, CommitConflict{Path: p, Reason: "path outside layer base root"})
			continue
		}
		anchor, hasCas := snap.Cas[p]
		pe := &commitPlanEntry{
			Path: p, Mode: entry.Mode & 0o777, Size: entry.Size,
			BaseRev: anchor.BaseRev, HasBase: anchor.HasBase, HasCas: hasCas,
			Symlink: entry.IsSymlink,
		}
		switch {
		case entry.IsDir:
			pe.Op = "mkdir"
		case entry.IsSymlink:
			pe.Op = "upsert-symlink"
		default:
			pe.Op = "upsert-file"
		}
		plan = append(plan, pe)
	}
	attrPaths := make([]string, 0, len(snap.Attrs))
	for p := range snap.Attrs {
		attrPaths = append(attrPaths, p)
	}
	sort.Strings(attrPaths)
	for _, p := range attrPaths {
		if !inScope(p) {
			conflicts = append(conflicts, CommitConflict{Path: p, Reason: "path outside layer base root"})
			continue
		}
		plan = append(plan, &commitPlanEntry{Path: p, Op: "chmod", Mode: snap.Attrs[p].Mode & 0o7777})
	}
	blackPaths := make([]string, 0, len(snap.Blackouts))
	for p := range snap.Blackouts {
		blackPaths = append(blackPaths, p)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(blackPaths))) // children first
	for _, p := range blackPaths {
		if !inScope(p) {
			conflicts = append(conflicts, CommitConflict{Path: p, Reason: "path outside layer base root"})
			continue
		}
		b := snap.Blackouts[p]
		plan = append(plan, &commitPlanEntry{Path: p, Op: "whiteout", BaseRev: b.BaseRev, HasBase: b.HasBase})
	}
	for i, pe := range plan {
		pe.Seq = i + 1
	}
	return plan, conflicts
}

// applyJournal executes the plan entry by entry, recording an applied
// marker after each step (crash-safe resume). A CAS failure aborts with a
// conflict report; already-applied entries are skipped.
func applyJournal(ctx context.Context, cl *client.Client, journal *commitJournal) (*CommitResult, error) {
	res := &CommitResult{}
	for _, pe := range journal.Entries {
		if pe.Applied {
			res.Applied++
			continue
		}
		conflict, err := applyOne(ctx, cl, journal, pe)
		if err != nil {
			return res, err
		}
		if conflict != nil {
			res.Conflicts = append(res.Conflicts, *conflict)
			return res, nil
		}
		pe.Applied = true
		res.Applied++
		if err := markApplied(ctx, cl, journal, pe.Seq); err != nil {
			return res, err
		}
	}
	return res, nil
}

// applyOne performs one journalled step; a non-nil conflict reports a
// refusal (the plan stops, the layer stays usable).
func applyOne(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) (*CommitConflict, error) {
	switch pe.Op {
	case "upsert-file":
		return upsertFile(ctx, cl, journal, pe)
	case "upsert-symlink":
		target, err := cl.ReadCtx(ctx, treeSourcePath(journal, pe.Path))
		if err != nil {
			return nil, fmt.Errorf("read symlink target %s: %w", pe.Path, err)
		}
		if conflict := ensureAbsentOrReplaceable(ctx, cl, pe); conflict != nil {
			return conflict, nil
		}
		if err := cl.SymlinkCtx(ctx, string(target), pe.Path); err != nil {
			if client.IsNotFound(err) {
				return nil, nil
			}
			if st, serr := cl.StatCtx(ctx, pe.Path); serr == nil && st != nil && !st.IsDir {
				// A same-shape destination is replaced like rename(2).
				if derr := cl.DeleteFileCtx(ctx, pe.Path); derr != nil && !client.IsNotFound(derr) {
					return nil, fmt.Errorf("replace symlink %s: %w", pe.Path, derr)
				}
				if err := cl.SymlinkCtx(ctx, string(target), pe.Path); err != nil {
					return nil, fmt.Errorf("symlink %s: %w", pe.Path, err)
				}
				return nil, nil
			}
			return nil, fmt.Errorf("symlink %s: %w", pe.Path, err)
		}
		return nil, nil
	case "mkdir":
		if st, err := cl.StatCtx(ctx, pe.Path); err == nil && st != nil {
			if !st.IsDir {
				return &CommitConflict{Path: pe.Path, Reason: "base path exists and is not a directory"}, nil
			}
			return nil, nil
		}
		mode := pe.Mode
		if mode == 0 {
			mode = 0o755
		}
		if err := cl.MkdirCtx(ctx, pe.Path, mode); err != nil && !client.IsNotFound(err) && !isConflictAlreadyDone(err) {
			return nil, fmt.Errorf("mkdir %s: %w", pe.Path, err)
		}
		return nil, nil
	case "chmod":
		if pe.Mode == 0 {
			return nil, nil
		}
		if err := cl.ChmodCtx(ctx, pe.Path, pe.Mode); err != nil {
			if client.IsNotFound(err) {
				return &CommitConflict{Path: pe.Path, Reason: "chmod base path missing"}, nil
			}
			return nil, fmt.Errorf("chmod %s: %w", pe.Path, err)
		}
		return nil, nil
	case "whiteout":
		st, err := cl.StatCtx(ctx, pe.Path)
		if err != nil {
			if client.IsNotFound(err) {
				return nil, nil // already gone
			}
			return nil, fmt.Errorf("stat %s: %w", pe.Path, err)
		}
		if st != nil && st.IsDir {
			infos, lerr := cl.ListCtx(ctx, pe.Path)
			if lerr == nil && len(infos) > 0 {
				return &CommitConflict{Path: pe.Path, Reason: "directory whiteout requires empty directory"}, nil
			}
			if err := cl.DeleteDirCtx(ctx, pe.Path); err != nil && !client.IsNotFound(err) {
				return nil, fmt.Errorf("rmdir %s: %w", pe.Path, err)
			}
			return nil, nil
		}
		if err := cl.DeleteFileCtx(ctx, pe.Path); err != nil && !client.IsNotFound(err) {
			return nil, fmt.Errorf("unlink %s: %w", pe.Path, err)
		}
		return nil, nil
	}
	return nil, fmt.Errorf("commit: unknown plan op %q", pe.Op)
}

// upsertFile copies the tree file onto the base path under its CAS anchor:
// a positive anchor must match the live revision, a zero anchor requires
// the base to be absent, and an unanchored (foreign) tree file applies
// unconditionally.
func upsertFile(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) (*CommitConflict, error) {
	src := treeSourcePath(journal, pe.Path)
	expected := int64(-1)
	if pe.HasCas {
		expected = pe.BaseRev
	}
	if pe.Size <= commitInlineMax {
		data, err := cl.ReadCtx(ctx, src)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", pe.Path, err)
		}
		if err := cl.WriteCtxConditional(ctx, pe.Path, data, expected); err != nil {
			return casConflict(ctx, cl, pe, err), nil
		}
	} else {
		if conflict := streamCopyCAS(ctx, cl, src, pe, expected); conflict != nil {
			return conflict, nil
		}
	}
	if pe.Mode != 0 && pe.Mode != 0o644 {
		if err := cl.ChmodCtx(ctx, pe.Path, pe.Mode); err != nil && !client.IsNotFound(err) {
			return nil, fmt.Errorf("chmod %s: %w", pe.Path, err)
		}
	}
	return nil, nil
}

// streamCopyCAS applies a large tree file through the streaming writer
// with the same CAS contract.
func streamCopyCAS(ctx context.Context, cl *client.Client, src string, pe *commitPlanEntry, expected int64) *CommitConflict {
	rc, err := cl.ReadStream(ctx, src)
	if err != nil {
		_ = err
		return &CommitConflict{Path: pe.Path, Reason: "read tree file failed"}
	}
	defer func() { _ = rc.Close() }()
	sw := cl.NewStreamWriterConditional(ctx, pe.Path, pe.Size, expected)
	part := 0
	var last []byte
	buf := make([]byte, commitChunk)
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			part++
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if last != nil {
				if werr := sw.WritePart(ctx, part-1, last); werr != nil {
					abortCtx, cancel := context.WithTimeout(context.Background(), 30e9)
					_ = sw.Abort(abortCtx)
					cancel()
					return &CommitConflict{Path: pe.Path, Reason: werr.Error()}
				}
			}
			last = chunk
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return &CommitConflict{Path: pe.Path, Reason: rerr.Error()}
		}
	}
	if err := sw.Complete(ctx, part, last); err != nil {
		return casConflict(ctx, cl, pe, err)
	}
	return nil
}

// ensureAbsentOrReplaceable checks the destination shape for symlinks.
func ensureAbsentOrReplaceable(ctx context.Context, cl *client.Client, pe *commitPlanEntry) *CommitConflict {
	if pe.HasCas && pe.BaseRev == 0 {
		if st, err := cl.StatCtx(ctx, pe.Path); err == nil && st != nil {
			return &CommitConflict{Path: pe.Path, Reason: "base path exists"}
		}
	}
	return nil
}

// casConflict classifies a failed CAS write: a genuine conflict reports
// live vs expected revisions; anything else surfaces as an error string
// conflict for the operator.
func casConflict(ctx context.Context, cl *client.Client, pe *commitPlanEntry, err error) *CommitConflict {
	if !errors.Is(err, client.ErrConflict) && !isConflictAlreadyDone(err) {
		return &CommitConflict{Path: pe.Path, Reason: err.Error()}
	}
	live := int64(0)
	if st, serr := cl.StatCtx(ctx, pe.Path); serr == nil && st != nil {
		live = st.Revision
	}
	if pe.HasCas && pe.BaseRev == 0 {
		return &CommitConflict{Path: pe.Path, Reason: "base path exists", WantRevision: 0, BaseRevision: live}
	}
	return &CommitConflict{Path: pe.Path, Reason: "base revision changed", WantRevision: pe.BaseRev, BaseRevision: live}
}

func isConflictAlreadyDone(err error) bool {
	return err != nil && strings.Contains(err.Error(), "conflict")
}

// treeSourcePath is the tree location a plan entry copies from.
func treeSourcePath(journal *commitJournal, serverPath string) string {
	return Root + "/" + journal.LayerID + "/" + TreeSubdir + serverPath
}

// writeJournal persists the plan (done=false) or the completion marker
// (done=true).
func writeJournal(ctx context.Context, cl *client.Client, layerID string, journal *commitJournal, done bool) error {
	content, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	base := Root + "/" + layerID + "/" + CommitsSubdir + "/" + journal.CommitID
	if done {
		return cl.WriteCtx(ctx, base+"/done.json", content)
	}
	return cl.WriteCtx(ctx, base+"/journal.json", content)
}

// markApplied records one completed step (idempotent resume cursor).
func markApplied(ctx context.Context, cl *client.Client, journal *commitJournal, seq int) error {
	base := Root + "/" + journal.LayerID + "/" + CommitsSubdir + "/" + journal.CommitID
	return cl.WriteCtx(ctx, base+"/applied/"+fmt.Sprintf("%06d", seq), []byte("1"))
}

// resumeUnfinished looks for journals without done markers and completes
// them (a previous crash mid-commit).
func resumeUnfinished(ctx context.Context, cl *client.Client, layerID string) error {
	commitsRoot := Root + "/" + layerID + "/" + CommitsSubdir
	infos, err := cl.ListCtx(ctx, commitsRoot)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, info := range infos {
		if !info.IsDir {
			continue
		}
		cid := info.Name
		if _, err := cl.StatCtx(ctx, commitsRoot+"/"+cid+"/done.json"); err == nil {
			continue
		}
		content, err := cl.ReadCtx(ctx, commitsRoot+"/"+cid+"/journal.json")
		if err != nil {
			continue // debris, not a journal
		}
		var journal commitJournal
		if err := json.Unmarshal(content, &journal); err != nil || journal.LayerID != layerID {
			continue
		}
		// Rebuild applied flags from the marker set.
		applied, _ := cl.ListCtx(ctx, commitsRoot+"/"+cid+"/applied")
		done := map[string]bool{}
		for _, a := range applied {
			done[a.Name] = true
		}
		for _, pe := range journal.Entries {
			if done[fmt.Sprintf("%06d", pe.Seq)] {
				pe.Applied = true
			}
		}
		res, aerr := applyJournal(ctx, cl, &journal)
		if aerr != nil {
			return fmt.Errorf("resume commit %s: %w", cid, aerr)
		}
		if len(res.Conflicts) > 0 {
			return fmt.Errorf("resume commit %s: conflict at %s: %s", cid, res.Conflicts[0].Path, res.Conflicts[0].Reason)
		}
		if err := writeJournal(ctx, cl, layerID, &journal, true); err != nil {
			return err
		}
		if _, err := cl.CommitFSLayer(ctx, layerID); err != nil {
			return fmt.Errorf("mark layer committed (resume): %w", err)
		}
	}
	return nil
}

// newCommitID mints a commit journal id.
func newCommitID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "c" + hex.EncodeToString(b[:]), nil
}
