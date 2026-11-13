package git

import "context"

type gitBackgroundTask struct {
	cancel context.CancelFunc
}

// runBackground owns discovery and restore work until Stop joins it. A restore
// has its own hydrate budget instead of inheriting the short discovery request's
// deadline, but it must still stop with the runtime that owns its directory.
func (g *Git) runBackground(run func(context.Context)) bool {
	g.backgroundMu.Lock()
	defer g.backgroundMu.Unlock()
	if g.stopping {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	task := &gitBackgroundTask{cancel: cancel}
	if g.background == nil {
		g.background = make(map[*gitBackgroundTask]struct{})
	}
	g.background[task] = struct{}{}
	g.backgroundWG.Add(1)
	go func() {
		defer g.backgroundWG.Done()
		defer func() {
			cancel()
			g.backgroundMu.Lock()
			delete(g.background, task)
			g.backgroundMu.Unlock()
		}()
		run(ctx)
	}()
	return true
}

// Stop closes task admission, cancels and joins all discovery/restore tasks,
// then joins pending checkpoints and makes the final checkpoint. A caller
// timeout does not relinquish directory ownership: retry waits on the same
// cleanup, and the switch coordinator cannot install another runtime yet.
func (g *Git) Stop(ctx context.Context) error {
	g.backgroundMu.Lock()
	if !g.stopping {
		g.stopping = true
		g.stopDone = make(chan struct{})
		for task := range g.background {
			task.cancel()
		}
		go func() {
			g.backgroundWG.Wait()
			if g.checkpoints != nil {
				g.checkpoints.FlushAll()
			}
			g.checkpointAllGitWorkspaces()
			close(g.stopDone)
		}()
	}
	done := g.stopDone
	g.backgroundMu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
