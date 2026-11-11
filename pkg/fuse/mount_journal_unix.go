//go:build !windows

package fuse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// CreateMountViewJournal establishes a NEW mount incarnation before a
// supervisor starts its first worker. Every worker receives the returned ID
// and only opens that journal. Do not call this to recover an existing mount.
// No kernel mount or extension is started. The initial selection is main.
func CreateMountViewJournal(ctx context.Context, cfg MountConfig) (string, error) {
	if cfg.MountID != "" || cfg.LayerRef != "" || cfg.CheckpointRef != "" {
		return "", errors.New("new mount journal requires main and no existing incarnation")
	}
	cl, err := mountClient(cfg)
	if err != nil {
		return "", err
	}
	j, err := createMountViewJournal(ctx, cfg, cl)
	if err != nil {
		return "", err
	}
	s, snapshotErr := j.Snapshot()
	if err := errors.Join(snapshotErr, j.Close()); err != nil {
		return "", err
	}
	return s.MountID, nil
}

func mountClient(cfg MountConfig) (*client.Client, error) {
	if (cfg.APIKey == "") == (cfg.Token == "") {
		return nil, errors.New("fuse: exactly one of APIKey or Token is required")
	}
	if cfg.Token != "" {
		return client.NewWithToken(cfg.Server, cfg.Token), nil
	}
	return client.New(cfg.Server, cfg.APIKey), nil
}

func mountJournalScope(ctx context.Context, cfg MountConfig, cl *client.Client) (string, error) {
	if cfg.MountPoint == "" || cl == nil {
		return "", errors.New("fuse: mountpoint and authenticated client required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// This is a startup operation. Never resolve through a mounted endpoint.
	point, err := filepath.Abs(cfg.MountPoint)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(point, 0o755); err != nil {
		return "", err
	}
	point, err = filepath.EvalSymlinks(point)
	if err != nil {
		return "", err
	}
	tenant, err := cl.GetMigrationTenantID(ctx)
	if err != nil {
		return "", err
	}
	return mountstate.SwitchScope(cfg.Server, tenant, cfg.RemoteRoot, point)
}

func createMountViewJournal(ctx context.Context, cfg MountConfig, cl *client.Client) (*mountstate.SwitchJournal, error) {
	if cfg.MountID != "" {
		return nil, errors.New("fuse: cannot create an existing mount incarnation")
	}
	scope, err := mountJournalScope(ctx, cfg, cl)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(nonce[:])
	path, err := mountstate.SwitchJournalPath(id)
	if err != nil {
		return nil, err
	}
	if err := mountstate.EnsureStateDir(); err != nil {
		return nil, err
	}
	return mountstate.CreateSwitchJournal(path, id, scope, viewprotocol.Binding{})
}

func openMountViewJournal(ctx context.Context, cfg MountConfig, cl *client.Client) (*mountstate.SwitchJournal, error) {
	path, err := mountstate.SwitchJournalPath(cfg.MountID)
	if err != nil {
		return nil, err
	}
	scope, err := mountJournalScope(ctx, cfg, cl)
	if err != nil {
		return nil, err
	}
	j, err := mountstate.OpenSwitchJournal(path, cfg.MountID, scope, viewprotocol.Binding{})
	if err != nil {
		return nil, fmt.Errorf("open mount incarnation %s (no fallback): %w", cfg.MountID, err)
	}
	return j, nil
}
