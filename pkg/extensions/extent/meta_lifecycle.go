package extent

import (
	"context"
	"path"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
)

func (b *Bridge) Shutdown() error {
	return b.ShutdownCtx(context.Background())
}

func (b *Bridge) ShutdownCtx(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	stopCh := b.retryStop
	if stopCh != nil {
		b.retryOnce.Do(func() { close(stopCh) })
	}
	b.mu.Unlock()
	done := make(chan struct{})
	go func() {
		b.retryWg.Wait()
		b.fetchWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.mu.Lock()
	var spooled int64
	for id, sl := range b.stagedSlices {
		spooled += sl.closeAndSpoolBytes()
		delete(b.stagedSlices, id)
	}
	b.mu.Unlock()
	if spooled > 0 {
		b.blocks.releaseDiskSpool(spooled)
	}
	return nil
}

func (b *Bridge) hasUncommittedLocked(ino meta.Ino) bool {
	if len(b.pending[ino]) > 0 || b.commitErr[ino] != nil {
		return true
	}
	if _, ok := b.truncs[ino]; ok {
		return true
	}
	return b.needsFreshIdentityLocked(ino)
}

func (b *Bridge) isRetainedOrUncommittedLocked(ino meta.Ino) bool {
	if _, ok := b.closedRetained[ino]; ok {
		return true
	}
	return b.hasUncommittedLocked(ino)
}

func (b *Bridge) HasUncommitted(ino meta.Ino) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hasUncommittedLocked(ino)
}

func (b *Bridge) NeedsFreshIdentity(ino meta.Ino) bool {
	if b == nil {
		return false
	}
	return b.needsFreshIdentity(ino)
}

func (b *Bridge) localPathFromFullLocked(full string) (string, bool) {
	if full == "" {
		return "", false
	}
	root := strings.TrimSuffix(b.cfg.RemoteRoot, "/")
	if root == "" || root == "/" {
		return path.Clean("/" + strings.TrimPrefix(full, "/")), true
	}
	if full == root {
		return "/", true
	}
	if strings.HasPrefix(full, root+"/") {
		return full[len(root):], true
	}
	return "", false
}

func (b *Bridge) RetainClosedIfUncommitted(_ string, ino meta.Ino) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	if !b.hasUncommittedLocked(ino) {
		delete(b.closedRetained, ino)
		b.mu.Unlock()
		return false
	}
	b.closedRetained[ino] = struct{}{}
	b.mu.Unlock()
	b.kickPendingFlush()
	return true
}

func (b *Bridge) ClearClosedRetained(ino meta.Ino) {
	if b == nil {
		return
	}
	b.mu.Lock()
	delete(b.closedRetained, ino)
	b.mu.Unlock()
}

func (b *Bridge) isClosedRetained(ino meta.Ino) bool {
	b.mu.Lock()
	_, ok := b.closedRetained[ino]
	b.mu.Unlock()
	return ok
}

func (b *Bridge) RetireClosedIno(localPath string, ino meta.Ino) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	if _, ok := b.closedRetained[ino]; !ok || b.hasUncommittedLocked(ino) {
		b.mu.Unlock()
		return false
	}
	full, mapped := b.paths[ino]
	if !mapped {
		delete(b.closedRetained, ino)
		delete(b.moved, ino)
		b.forgetInoLocked(ino)
		b.mu.Unlock()
		return true
	}
	curLocal, hasLocal := b.localPathFromFullLocked(full)
	if hasLocal && localPath != "" && curLocal != localPath {
		b.mu.Unlock()
		return false
	}
	if hasLocal {
		if heldIno, ours := b.sentinels[curLocal]; ours && heldIno == ino {
			delete(b.sentinels, curLocal)
			if b.replacements[full] == ino {
				delete(b.replacements, full)
			}
			delete(b.moved, ino)
			b.forgetInoLocked(ino)
			b.mu.Unlock()
			_ = b.client().RemoveAllCtx(context.Background(), full)
			return true
		}
	}
	wasReplacementOwner := b.replacements[full] == ino
	if wasReplacementOwner {
		delete(b.replacements, full)
	}
	delete(b.moved, ino)
	b.forgetInoLocked(ino)
	naturalIno := inoFor(full)
	if wasReplacementOwner && naturalIno != ino && b.paths[naturalIno] == full && !b.isRetainedOrUncommittedLocked(naturalIno) {
		if _, held := b.heldInoForLocked(full); !held {
			b.forgetInoLocked(naturalIno)
		}
	}
	b.mu.Unlock()
	return true
}

func (b *Bridge) retireClosed(ino meta.Ino) {
	for i := 0; i < 8; i++ {
		b.mu.Lock()
		_, ok := b.closedRetained[ino]
		if !ok || b.hasUncommittedLocked(ino) {
			b.mu.Unlock()
			return
		}
		full, mapped := b.paths[ino]
		localPath, _ := b.localPathFromFullLocked(full)
		cb := b.onRetireClosed
		b.mu.Unlock()
		if !mapped || cb == nil || localPath == "" {
			b.RetireClosedIno("", ino)
			return
		}
		if cb(localPath, ino) {
			return
		}
	}
}

func (b *Bridge) ForgetInvalidatedPath(local string) {
	full := b.fullPath(local)
	b.mu.Lock()
	defer b.mu.Unlock()
	if rep, ok := b.replacements[full]; ok && !b.isRetainedOrUncommittedLocked(rep) {
		delete(b.replacements, full)
		delete(b.moved, rep)
		b.forgetInoLocked(rep)
	}
	ino := inoFor(full)
	if mapped, ok := b.paths[ino]; !ok || mapped == full || strings.HasPrefix(mapped, full+"/") {
		if !b.isRetainedOrUncommittedLocked(ino) {
			b.forgetInoLocked(ino)
		}
	}
	prefix := full + "/"
	if full == "/" {
		prefix = "/"
	}
	for i, p := range b.paths {
		if i == rootIno {
			continue
		}
		if strings.HasPrefix(p, prefix) && !b.isRetainedOrUncommittedLocked(i) {
			delete(b.paths, i)
		}
	}
}

func (b *Bridge) collectUncommittedLocked() map[meta.Ino]struct{} {
	seen := make(map[meta.Ino]struct{})
	for ino := range b.pending {
		if b.hasUncommittedLocked(ino) {
			seen[ino] = struct{}{}
		}
	}
	for ino := range b.commitErr {
		if b.hasUncommittedLocked(ino) {
			seen[ino] = struct{}{}
		}
	}
	for ino := range b.truncs {
		if b.hasUncommittedLocked(ino) {
			seen[ino] = struct{}{}
		}
	}
	for ino := range b.freshCreate {
		if b.hasUncommittedLocked(ino) {
			seen[ino] = struct{}{}
		}
	}
	for ino := range b.closedRetained {
		if b.hasUncommittedLocked(ino) {
			seen[ino] = struct{}{}
		}
	}
	return seen
}

func (b *Bridge) PendingCommitCount() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.collectUncommittedLocked())
}

func (b *Bridge) Drain(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.flushAnyPending(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	candidates := b.collectUncommittedLocked()
	b.mu.Unlock()

	var firstErr error
	for ino := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b.isClosedRetained(ino) {
			if err := b.FlushFileFinal(ctx, ino); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				b.retireClosed(ino)
			}
			continue
		}
		if err := b.FlushFile(ctx, ino); err != nil && firstErr == nil {
			firstErr = err
		}
		if b.needsFreshIdentity(ino) {
			unlock := b.lockCommit(ino)
			if b.needsFreshIdentity(ino) {
				if err := b.MaterializeEmpty(ctx, ino); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			unlock()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, err := range b.commitErr {
		if err != nil {
			return err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if len(b.collectUncommittedLocked()) > 0 {
		return syscall.EAGAIN
	}
	return nil
}
