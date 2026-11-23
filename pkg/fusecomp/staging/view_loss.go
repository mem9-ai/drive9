package staging

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrStagedViewLost marks a recorded loss of a staged image, so callers can
// tell it apart from other terminal failures (a standalone queue transfer
// record, for instance) when a barrier consults the loss record.
var ErrStagedViewLost = errors.New("staged image was lost")

// ViewLoss is a durable record that a staged pair was pruned: its only local
// copy of the image is gone, so a durability barrier for the path must fail
// instead of reading the missing entry as a completed upload.
//
// It lives on disk in the cache directory next to the staged pairs
// (<hash>.lost beside <hash>.dat/<hash>.meta) rather than in a bounded
// in-process map, because a bounded map can evict the FIRST report of a loss and
// turn real data loss into a silent success. A fresh staging or an explicit
// removal clears it; a landing clears it only when it landed a generation at or
// above the lost one, so an older upload finishing cannot erase the record of a
// newer image that was lost.
type ViewLoss struct {
	Path       string `json:"path"`
	Generation uint64 `json:"generation"`
	Error      string `json:"error,omitempty"`
}

// lostFile is the on-disk view-loss marker for a remote path.
func (c *WriteBackCache) lostFile(remotePath string) string {
	return filepath.Join(c.dir, HashPath(remotePath)+".lost")
}

// writeViewLoss persists a loss for the path and reminds this process about it.
// The caller holds the path lock.
func (c *WriteBackCache) writeViewLoss(remotePath string, gen uint64, err error) {
	loss := ViewLoss{Path: remotePath, Generation: gen}
	if err != nil {
		loss.Error = err.Error()
	}
	if raw, merr := json.Marshal(&loss); merr == nil {
		if werr := atomicWrite(c.lostFile(remotePath), raw); werr != nil {
			// The in-process record below still fails this process's barriers;
			// only durability across the next restart is lost.
			safeLogPrintf("writeback cache: view loss marker for %s not durable: %v", remotePath, werr)
		}
	}
	c.mu.Lock()
	if c.lostGens == nil {
		c.lostGens = make(map[string]uint64)
	}
	c.lostGens[remotePath] = gen
	c.mu.Unlock()
}

// ViewLoss reports the recorded loss for a path, if any: from the in-process
// map first, then from the durable marker a previous process may have left.
func (c *WriteBackCache) ViewLoss(remotePath string) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	gen, ok := c.lostGens[remotePath]
	c.mu.Unlock()
	if ok {
		return fmt.Errorf("%w: staged image for %s (generation %d) was pruned", ErrStagedViewLost, remotePath, gen)
	}
	loss, ok := c.readViewLoss(remotePath)
	if !ok {
		return nil
	}
	if loss.Error != "" {
		return fmt.Errorf("%w: %s (%s)", ErrStagedViewLost, loss.Error, remotePath)
	}
	return fmt.Errorf("%w: staged image for %s (generation %d) was pruned", ErrStagedViewLost, remotePath, loss.Generation)
}

// readViewLoss reads a durable marker. Any error other than "not exist" reports
// a loss: a hole in the failure record must not read as "nothing was lost".
func (c *WriteBackCache) readViewLoss(remotePath string) (ViewLoss, bool) {
	raw, err := os.ReadFile(c.lostFile(remotePath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ViewLoss{}, false
		}
		return ViewLoss{Path: remotePath, Generation: ^uint64(0), Error: fmt.Sprintf("unreadable view loss marker: %v", err)}, true
	}
	var loss ViewLoss
	if err := json.Unmarshal(raw, &loss); err != nil || loss.Generation == 0 {
		return ViewLoss{Path: remotePath, Generation: ^uint64(0), Error: "unreadable view loss marker"}, true
	}
	return loss, true
}

// ClearViewLossIfGeneration drops the loss record for a path when the caller
// landed a generation at or above the lost one. A newer loss survives: an older
// upload finishing says nothing about an image that died after it.
func (c *WriteBackCache) ClearViewLossIfGeneration(remotePath string, gen uint64) {
	if c == nil {
		return
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)
	c.clearViewLossLocked(remotePath, gen)
}

// clearViewLossLocked drops the record when the lost generation is at or below
// gen. The caller holds the path lock.
func (c *WriteBackCache) clearViewLossLocked(remotePath string, gen uint64) {
	c.mu.Lock()
	if lost, ok := c.lostGens[remotePath]; ok && lost <= gen {
		delete(c.lostGens, remotePath)
	}
	c.mu.Unlock()
	loss, ok := c.readViewLoss(remotePath)
	if !ok {
		// The overwhelmingly common case: this runs on every Put, and a healthy
		// mount has no markers at all, so it must not pay a directory fsync.
		return
	}
	if loss.Generation > gen {
		return
	}
	if err := os.Remove(c.lostFile(remotePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return
	}
	// The removal must survive a crash: a resurrected marker would fail barriers
	// on a name whose image is fine.
	_ = fsyncDir(c.dir)
}

// clearViewLossForNewStagingLocked drops any record for the path: a fresh
// staging supersedes it. The caller holds the path lock.
func (c *WriteBackCache) clearViewLossForNewStagingLocked(remotePath string) {
	c.clearViewLossLocked(remotePath, ^uint64(0))
}

// ClearViewLoss drops any loss record for a path unconditionally. It is for
// callers that just published the path's image themselves (a queue staging, an
// inline commit, a patched commit): their image is the newest one this mount
// knows, so every earlier loss record for the name is superseded.
func (c *WriteBackCache) ClearViewLoss(remotePath string) {
	if c == nil {
		return
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)
	c.clearViewLossLocked(remotePath, ^uint64(0))
}

// scanViewLosses reads every .lost marker in the cache directory. unreadable
// reports a directory scan or decode failure: callers whose answer decides
// whether data is trusted (HasViewLossPrefix) must fail closed on it.
func (c *WriteBackCache) scanViewLosses() (losses []ViewLoss, unreadable bool) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, true
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".lost") {
			continue
		}
		raw, rerr := os.ReadFile(filepath.Join(c.dir, name))
		if rerr != nil {
			if !errors.Is(rerr, os.ErrNotExist) {
				unreadable = true
			}
			continue
		}
		var loss ViewLoss
		if jerr := json.Unmarshal(raw, &loss); jerr != nil || loss.Path == "" {
			unreadable = true
			continue
		}
		losses = append(losses, loss)
	}
	return losses, unreadable
}

// ClearViewLossPrefix drops the loss records of every path under prefix. It is
// for subtree removals, where the names are gone but their markers would
// otherwise keep failing barriers on re-created paths.
func (c *WriteBackCache) ClearViewLossPrefix(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	var inMemory []string
	for path := range c.lostGens {
		if strings.HasPrefix(path, prefix) {
			inMemory = append(inMemory, path)
		}
	}
	c.mu.Unlock()
	for _, path := range inMemory {
		c.ClearViewLoss(path)
	}
	losses, _ := c.scanViewLosses()
	for _, loss := range losses {
		if strings.HasPrefix(loss.Path, prefix) {
			c.ClearViewLoss(loss.Path)
		}
	}
}

// HasViewLossPrefix reports whether any path under prefix carries a recorded
// loss. A directory rename must consult it: ListByPrefix only sees .meta
// entries, so a pruned descendant would otherwise be moved as if it landed. An
// unreadable marker or a failed scan reports true — those are exactly the cases
// where a descendant's image cannot be proven to have landed.
func (c *WriteBackCache) HasViewLossPrefix(prefix string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	for path := range c.lostGens {
		if strings.HasPrefix(path, prefix) {
			c.mu.Unlock()
			return true
		}
	}
	c.mu.Unlock()
	losses, unreadable := c.scanViewLosses()
	if unreadable {
		return true
	}
	for _, loss := range losses {
		if strings.HasPrefix(loss.Path, prefix) {
			return true
		}
	}
	return false
}

// RekeyViewLoss moves a recorded loss from oldPath to newPath. A rename carries
// the obligation with the name: the destination now holds the unlanded image's
// identity, so dropping the record (or leaving it on the dead name) would let a
// barrier certify bytes that never landed.
//
// Both path locks are taken in a deterministic order, and the new marker is
// written before the old one is unlinked: a crash in between leaves the record
// on one of the two names, never on neither.
func (c *WriteBackCache) RekeyViewLoss(oldPath, newPath string) {
	if c == nil || oldPath == newPath {
		return
	}
	first, second := oldPath, newPath
	if second < first {
		first, second = second, first
	}
	pf := c.AcquirePathLock(first)
	ps := c.AcquirePathLock(second)
	defer c.ReleasePathLock(second, ps)
	defer c.ReleasePathLock(first, pf)

	c.mu.Lock()
	gen, ok := c.lostGens[oldPath]
	delete(c.lostGens, oldPath)
	c.mu.Unlock()
	if !ok {
		loss, has := c.readViewLoss(oldPath)
		if !has {
			return
		}
		gen = loss.Generation
	}
	if raw, merr := json.Marshal(&ViewLoss{Path: newPath, Generation: gen}); merr == nil {
		if werr := atomicWrite(c.lostFile(newPath), raw); werr != nil {
			// Keep the old marker: an unrecorded replacement would read as "no
			// loss" after a restart.
			safeLogPrintf("writeback cache: view loss rekey %s -> %s failed: %v", oldPath, newPath, werr)
			c.mu.Lock()
			c.lostGens[oldPath] = gen
			c.mu.Unlock()
			return
		}
	}
	c.mu.Lock()
	if c.lostGens == nil {
		c.lostGens = make(map[string]uint64)
	}
	c.lostGens[newPath] = gen
	c.mu.Unlock()
	if err := os.Remove(c.lostFile(oldPath)); err == nil {
		_ = fsyncDir(c.dir)
	}
}

// RekeyViewLossPrefix moves every recorded loss under oldPrefix to newPrefix,
// preserving the relative path.
func (c *WriteBackCache) RekeyViewLossPrefix(oldPrefix, newPrefix string) {
	if c == nil || oldPrefix == newPrefix {
		return
	}
	pairs := make(map[string]string)
	c.mu.Lock()
	for path := range c.lostGens {
		if strings.HasPrefix(path, oldPrefix) {
			pairs[path] = newPrefix + strings.TrimPrefix(path, oldPrefix)
		}
	}
	c.mu.Unlock()
	losses, _ := c.scanViewLosses()
	for _, loss := range losses {
		if !strings.HasPrefix(loss.Path, oldPrefix) {
			continue
		}
		if _, ok := pairs[loss.Path]; ok {
			continue
		}
		pairs[loss.Path] = newPrefix + strings.TrimPrefix(loss.Path, oldPrefix)
	}
	for oldPath, newPath := range pairs {
		c.RekeyViewLoss(oldPath, newPath)
	}
}

// loadViewLossFromDisk seeds the in-process mirror (and the generation counter)
// from the markers a previous process left, so a recovered marker is honored and
// a fresh generation can never be mistaken for an older loss.
func (c *WriteBackCache) loadViewLossFromDisk() {
	losses, _ := c.scanViewLosses()
	for _, loss := range losses {
		if loss.Generation == 0 {
			continue
		}
		c.mu.Lock()
		if c.lostGens == nil {
			c.lostGens = make(map[string]uint64)
		}
		c.lostGens[loss.Path] = loss.Generation
		c.mu.Unlock()
		c.advanceNextGen(loss.Generation)
	}
}

// ViewLossCount reports how many unresolved view losses are on record. It is the
// only signal a drain has for a pruned pair whose path no longer has an open
// handle (closed, or a fresh process after a restart).
//
// It fails closed: a scan error reports at least one loss, and an unreadable or
// corrupt marker counts as its own, because a drain must not certify a mount it
// cannot prove clean.
func (c *WriteBackCache) ViewLossCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	seen := make(map[string]struct{}, len(c.lostGens))
	for path := range c.lostGens {
		seen[path] = struct{}{}
	}
	c.mu.Unlock()

	entries, err := os.ReadDir(c.dir)
	if err != nil {
		// Nothing can be proven: at least one unresolved record is possible.
		if len(seen) > 0 {
			return len(seen)
		}
		return 1
	}
	count := len(seen)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".lost") {
			continue
		}
		raw, rerr := os.ReadFile(filepath.Join(c.dir, name))
		if rerr != nil {
			if errors.Is(rerr, os.ErrNotExist) {
				continue
			}
			count++
			continue
		}
		var loss ViewLoss
		if jerr := json.Unmarshal(raw, &loss); jerr != nil || loss.Path == "" || loss.Generation == 0 {
			count++
			continue
		}
		if _, ok := seen[loss.Path]; ok {
			continue
		}
		seen[loss.Path] = struct{}{}
		count++
	}
	return count
}
