//go:build !windows

package cli

import (
	"context"
	"time"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// Allocate exactly once, before spawning the supervisor. Worker restart and
// supervisor adoption keep this identity in argv and sanitized saved args.
// An explicit recovery identity never enters the journal creation path.
func prepareSupervisedMountView(req mountSuperviseStartRequest) (mountSuperviseStartRequest, error) {
	if req.MountKind == mountstate.MountKindObject || req.LayerRef != "" || req.MountID != "" {
		return req, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id, err := drive9fuse.CreateMountViewJournal(ctx, drive9fuse.MountConfig{
		Server: req.Server, APIKey: req.APIKey, Token: req.Token, RemoteRoot: req.RemoteRoot, MountPoint: req.MountPoint,
	})
	if err != nil {
		return req, err
	}
	req.MountID = id
	req.OriginalArgs = append([]string{"--mount-view-id=" + id}, req.OriginalArgs...)
	return req, nil
}
