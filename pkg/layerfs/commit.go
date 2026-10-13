// Client-side two-phase commit: apply a layer snapshot onto the classic
// base through generic filesystem operations with per-path compare-and-set,
// journalled so a crash anywhere resumes idempotently, then flip the layer
// state server-side. Crash recovery converges: finished journals drive the
// pending state flip, conflicted journals are aborted and skipped, and a
// retried CAS against this journal's own already-applied write is
// recognized by content and treated as applied.
package layerfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

// alreadyAppliedCompareMax bounds the content comparison that recognizes a
// journal's own already-applied write on crash-resumed CAS retries.
const alreadyAppliedCompareMax = 8 << 20

// Commit applies the snapshot of the layer's current tree onto its base
// root. It is journalled and idempotent: previous crashed runs converge
// before a new plan starts.
func Commit(ctx context.Context, cl *client.Client, layer *client.FSLayer) (*CommitResult, error) {
	// Terminal layers must not apply: their trees are settled (an
	// abandoned layer's shadow, in particular, must never reach the base).
	switch layer.State {
	case "", "active", "sealed", "committing":
	default:
		return nil, fmt.Errorf("fs layer commit: layer %s is %s", layer.LayerID, layer.State)
	}
	// A legacy entry log would be replayed by the server-side commit used
	// for the state flip; refuse instead of applying it blindly.
	if legacy, err := cl.DiffFSLayer(ctx, layer.LayerID); err != nil {
		return nil, fmt.Errorf("check legacy entries: %w", err)
	} else if len(legacy) > 0 {
		return nil, fmt.Errorf("fs layer commit: layer %s carries %d legacy entry-log records; migrate the layer before committing", layer.LayerID, len(legacy))
	}
	// Recover prior runs first: finished journals complete their state
	// flip, conflicted journals are aborted and left behind.
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
	if err := markJournal(ctx, cl, journal, journalFile); err != nil {
		return nil, err
	}
	res, err := applyJournal(ctx, cl, journal)
	if err != nil {
		return res, err
	}
	if len(res.Conflicts) > 0 {
		// Abort the conflicted journal: a frozen plan must not block every
		// later commit of this layer.
		if aerr := markJournal(ctx, cl, journal, abortedFile); aerr != nil {
			return res, aerr
		}
		return res, nil
	}
	if err := markJournal(ctx, cl, journal, doneFile); err != nil {
		return nil, err
	}
	// Flip the layer state server-side: the entry log is empty (guarded
	// above), so the server's commit path is a pure committed transition.
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
		// mtime-only patches have no platform verb (the backend derives
		// mtimes from writes); the plan records them, apply skips them.
		plan = append(plan, &commitPlanEntry{Path: p, Op: "chmod", Mode: snap.Attrs[p].Mode & 0o7777, Size: snap.Attrs[p].Mtime})
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
// refusal (the plan stops, the journal aborts, the layer stays usable).
func applyOne(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) (*CommitConflict, error) {
	switch pe.Op {
	case "upsert-file":
		return upsertFile(ctx, cl, journal, pe)
	case "upsert-symlink":
		return upsertSymlink(ctx, cl, journal, pe)
	case "mkdir":
		return applyMkdir(ctx, cl, pe)
	case "chmod":
		return applyChmod(ctx, cl, pe)
	case "whiteout":
		return applyWhiteout(ctx, cl, pe)
	}
	return nil, fmt.Errorf("commit: unknown plan op %q", pe.Op)
}

// upsertSymlink installs the tree's link onto the base under the same CAS
// contract as files.
func upsertSymlink(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) (*CommitConflict, error) {
	target, err := cl.ReadCtx(ctx, treeSourcePath(journal, pe.Path))
	if err != nil {
		return nil, fmt.Errorf("read symlink target %s: %w", pe.Path, err)
	}
	if conflict := ensureAbsentOrReplaceable(ctx, cl, pe); conflict != nil {
		return conflict, nil
	}
	if err := cl.SymlinkCtx(ctx, string(target), pe.Path); err != nil {
		if client.IsNotFound(err) {
			return &CommitConflict{Path: pe.Path, Reason: "symlink parent missing"}, nil
		}
		if isAlreadyAppliedSymlink(ctx, cl, pe, string(target)) {
			return nil, nil
		}
		return nil, fmt.Errorf("symlink %s: %w", pe.Path, err)
	}
	return nil, nil
}

// applyMkdir creates the base directory, tolerating an existing directory.
func applyMkdir(ctx context.Context, cl *client.Client, pe *commitPlanEntry) (*CommitConflict, error) {
	if st, err := cl.StatCtx(ctx, pe.Path); err == nil && st != nil {
		if !st.IsDir {
			return &CommitConflict{Path: pe.Path, Reason: "base path exists and is not a directory"}, nil
		}
		return nil, nil
	} else if !client.IsNotFound(err) {
		return nil, err
	}
	mode := pe.Mode
	if mode == 0 {
		mode = 0o755
	}
	if err := cl.MkdirCtx(ctx, pe.Path, mode); err != nil {
		if isAlreadyAppliedDir(ctx, cl, pe.Path) {
			return nil, nil
		}
		return nil, fmt.Errorf("mkdir %s: %w", pe.Path, err)
	}
	return nil, nil
}

// applyChmod lands a metadata-only patch. mtime-only patches (Mode == 0
// with a nonzero Size carrying the recorded timestamp) have no platform
// verb — the backend derives mtimes from writes — and are skipped.
func applyChmod(ctx context.Context, cl *client.Client, pe *commitPlanEntry) (*CommitConflict, error) {
	if pe.Mode == 0 {
		return nil, nil
	}
	if err := cl.ChmodCtx(ctx, pe.Path, pe.Mode); err != nil {
		if client.IsNotFound(err) {
			return &CommitConflict{Path: pe.Path, Reason: "chmod base path missing"}, nil
		}
		if isConflictAlreadyDone(err) {
			// The chmod raced another writer's identical intent.
			return nil, nil
		}
		return nil, fmt.Errorf("chmod %s: %w", pe.Path, err)
	}
	return nil, nil
}

// applyWhiteout removes the base path under its recorded anchor: a file
// whiteout CASes the live revision, a directory whiteout removes the whole
// subtree the layer view hid (view semantics), guarded by the recorded
// directory revision.
func applyWhiteout(ctx context.Context, cl *client.Client, pe *commitPlanEntry) (*CommitConflict, error) {
	st, err := cl.StatCtx(ctx, pe.Path)
	if err != nil {
		if client.IsNotFound(err) {
			return nil, nil // already gone
		}
		return nil, fmt.Errorf("stat %s: %w", pe.Path, err)
	}
	if st == nil {
		return nil, nil
	}
	if pe.HasBase && st.Revision != pe.BaseRev {
		return &CommitConflict{Path: pe.Path, Reason: "base revision changed", WantRevision: pe.BaseRev, BaseRevision: st.Revision}, nil
	}
	if st.IsDir {
		if err := cl.RemoveAllCtx(ctx, pe.Path); err != nil && !client.IsNotFound(err) {
			return nil, fmt.Errorf("remove dir %s: %w", pe.Path, err)
		}
		return nil, nil
	}
	if err := cl.DeleteFileCtx(ctx, pe.Path); err != nil && !client.IsNotFound(err) {
		return nil, fmt.Errorf("unlink %s: %w", pe.Path, err)
	}
	return nil, nil
}

// upsertFile copies the tree file onto the base path under its CAS anchor:
// a positive anchor must match the live revision, a zero anchor requires
// the base to be absent, and an unanchored (foreign) tree file applies
// unconditionally. Streaming rides the client's negotiated multipart
// contract (adaptive part sizes, part checksums).
func upsertFile(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) (*CommitConflict, error) {
	src := treeSourcePath(journal, pe.Path)
	expected := int64(-1)
	if pe.HasCas {
		expected = pe.BaseRev
	}
	ra, err := newRangedReaderAt(ctx, cl, src, pe.Size)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", pe.Path, err)
	}
	werr := cl.WriteStreamConditional(ctx, pe.Path, ra, pe.Size, nil, expected)
	if werr != nil {
		if conflict := casConflict(ctx, cl, journal, pe, werr); conflict != nil {
			return conflict, nil
		}
		return nil, fmt.Errorf("write %s: %w", pe.Path, werr)
	}
	if pe.Mode != 0 && pe.Mode != 0o644 {
		if err := cl.ChmodCtx(ctx, pe.Path, pe.Mode); err != nil && !client.IsNotFound(err) {
			return nil, fmt.Errorf("chmod %s: %w", pe.Path, err)
		}
	}
	return nil, nil
}

// ensureAbsentOrReplaceable checks the destination shape for symlinks:
// zero anchors require the base to be absent, positive anchors must match
// the live revision.
func ensureAbsentOrReplaceable(ctx context.Context, cl *client.Client, pe *commitPlanEntry) *CommitConflict {
	st, err := cl.StatCtx(ctx, pe.Path)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return &CommitConflict{Path: pe.Path, Reason: err.Error()}
	}
	if st == nil {
		return nil
	}
	if pe.HasCas && pe.BaseRev == 0 {
		return &CommitConflict{Path: pe.Path, Reason: "base path exists", WantRevision: 0, BaseRevision: st.Revision}
	}
	if pe.HasCas && pe.BaseRev > 0 && st.Revision != pe.BaseRev {
		return &CommitConflict{Path: pe.Path, Reason: "base revision changed", WantRevision: pe.BaseRev, BaseRevision: st.Revision}
	}
	return nil
}

// casConflict classifies a failed CAS write. A genuine revision conflict
// reports live vs expected revisions; a conflict that matches this
// journal's own already-applied result (crash between apply and applied
// marker) is recognized by content and treated as applied.
func casConflict(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry, err error) *CommitConflict {
	if errors.Is(err, client.ErrConflict) || isConflictAlreadyDone(err) {
		if isAlreadyAppliedContent(ctx, cl, journal, pe) {
			return nil
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
	return &CommitConflict{Path: pe.Path, Reason: err.Error()}
}

// isAlreadyAppliedContent compares the live base file with the tree source
// (bounded): equal content means this journal entry already landed and the
// CAS failure is its own retry.
func isAlreadyAppliedContent(ctx context.Context, cl *client.Client, journal *commitJournal, pe *commitPlanEntry) bool {
	if pe.Size < 0 || pe.Size > alreadyAppliedCompareMax {
		return false
	}
	if st, err := cl.StatCtx(ctx, pe.Path); err != nil || st == nil || st.Size != pe.Size || st.IsDir {
		return false
	}
	want, err := cl.ReadCtx(ctx, treeSourcePath(journal, pe.Path))
	if err != nil {
		return false
	}
	got, err := cl.ReadCtx(ctx, pe.Path)
	if err != nil {
		return false
	}
	return string(got) == string(want)
}

func isAlreadyAppliedSymlink(ctx context.Context, cl *client.Client, pe *commitPlanEntry, target string) bool {
	got, err := cl.ReadCtx(ctx, pe.Path)
	return err == nil && string(got) == target
}

func isAlreadyAppliedDir(ctx context.Context, cl *client.Client, path string) bool {
	if st, err := cl.StatCtx(ctx, path); err == nil && st != nil && st.IsDir {
		return true
	}
	return false
}

func isConflictAlreadyDone(err error) bool {
	return err != nil && strings.Contains(err.Error(), "conflict")
}

// treeSourcePath is the tree location a plan entry copies from.
func treeSourcePath(journal *commitJournal, serverPath string) string {
	return Root + "/" + journal.LayerID + "/" + TreeSubdir + serverPath
}

// Journal file names inside commits/<cid>/.
const (
	journalFile = "journal.json"
	doneFile    = "done.json"
	abortedFile = "aborted.json"
)

func journalDir(layerID, commitID string) string {
	return Root + "/" + layerID + "/" + CommitsSubdir + "/" + commitID
}

// markJournal persists the plan (journalFile), the completion marker
// (doneFile), or the abort marker (abortedFile).
func markJournal(ctx context.Context, cl *client.Client, journal *commitJournal, name string) error {
	content, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	return cl.WriteCtx(ctx, journalDir(journal.LayerID, journal.CommitID)+"/"+name, content)
}

// markApplied records one completed step (idempotent resume cursor).
func markApplied(ctx context.Context, cl *client.Client, journal *commitJournal, seq int) error {
	return cl.WriteCtx(ctx, journalDir(journal.LayerID, journal.CommitID)+"/applied/"+fmt.Sprintf("%06d", seq), []byte("1"))
}

// resumeUnfinished converges prior runs before a new plan starts:
//   - done.json without the registry flip: flip again (idempotent);
//   - journal.json without done/aborted: replay; a conflict aborts it;
//   - aborted.json: skip (a fresh plan will be built from the live tree).
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
		dir := commitsRoot + "/" + cid
		if _, err := cl.StatCtx(ctx, dir+"/"+abortedFile); err == nil {
			continue
		}
		if _, err := cl.StatCtx(ctx, dir+"/"+doneFile); err == nil {
			// Finished plan: only the state flip may be missing.
			if _, err := cl.CommitFSLayer(ctx, layerID); err != nil {
				return fmt.Errorf("mark layer committed (resume): %w", err)
			}
			continue
		}
		content, err := cl.ReadCtx(ctx, dir+"/"+journalFile)
		if err != nil {
			continue // debris, not a journal
		}
		var journal commitJournal
		if err := json.Unmarshal(content, &journal); err != nil || journal.LayerID != layerID {
			continue
		}
		applied, _ := cl.ListCtx(ctx, dir+"/applied")
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
			// Abort the frozen plan; the next Commit re-plans from the
			// live tree instead of replaying this conflict forever.
			if merr := markJournal(ctx, cl, &journal, abortedFile); merr != nil {
				return merr
			}
			continue
		}
		if err := markJournal(ctx, cl, &journal, doneFile); err != nil {
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
