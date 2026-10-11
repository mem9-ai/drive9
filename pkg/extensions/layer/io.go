// Dependency seams: the extension's self-initiated operations. The data
// plane (markers, CoW pump sources, view walks) goes through layerIO; the
// layer registry (metadata) through layerRegistry. Production wires both
// to the HTTP client; tests wire them to a local filesystem and an
// in-memory registry, which keeps the mount-side logic testable over
// [layer → local-fs core] chains without any server.
package layer

import (
	"context"
	"io"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// ioStat is the stat shape the extension needs from its data plane.
type ioStat struct {
	IsDir     bool
	IsSymlink bool
	Mode      uint32 // permission bits
	Size      int64
	Rev       int64 // monotonic revision token (CAS anchors)
}

// ioEntry is one directory-listing row.
type ioEntry struct {
	Name      string
	IsDir     bool
	IsSymlink bool
	Mode      uint32
	Size      int64
}

// layerIO is the data-plane vocabulary of the extension's own operations.
type layerIO interface {
	// Stat resolves a path; nil, nil when absent.
	Stat(ctx context.Context, path string) (*ioStat, error)
	// List reads a directory.
	List(ctx context.Context, dir string) ([]ioEntry, error)
	// Read reads a whole (small) file — markers.
	Read(ctx context.Context, path string) ([]byte, error)
	// ReadStream streams a file from its start — CoW pump sources.
	ReadStream(ctx context.Context, path string) (io.ReadCloser, error)
	// Write creates or replaces a file, creating parent directories as
	// needed (marker bootstrap relies on it).
	Write(ctx context.Context, path string, data []byte) error
	// Delete removes a file; fs.ErrNotFound when already gone is the
	// caller-visible contract (adapters normalize).
	Delete(ctx context.Context, path string) error
	// EnsureParent creates the directory chain of dir (the tree roots
	// materialize lazily; server mkdir/symlink do not auto-create
	// parents, unlike create).
	EnsureParent(ctx context.Context, dir string) error
}

// ioLayer is the registry row of a layer.
type ioLayer struct {
	LayerID string
	State   string
}

// ioCheckpoint pins a checkpoint to its owning layer.
type ioCheckpoint struct {
	CheckpointID string
	LayerID      string
}

// ioEvent is one row of the layer event log.
type ioEvent struct {
	Seq int64
	Op  string
}

// layerRegistry is the metadata plane: the server's layer registry.
type layerRegistry interface {
	GetLayer(ctx context.Context, ref string) (*ioLayer, error)
	GetCheckpoint(ctx context.Context, checkpointID string) (*ioCheckpoint, error)
	ListEvents(ctx context.Context, layerID string, since int64) ([]ioEvent, error)
}

// ClientDeps adapts the HTTP client to both seams (production wiring).
type ClientDeps struct {
	CL *client.Client
}

// NewClientDeps builds the production dependency pair.
func NewClientDeps(cl *client.Client) *ClientDeps { return &ClientDeps{CL: cl} }

// Stat implements layerIO.
func (d *ClientDeps) Stat(ctx context.Context, path string) (*ioStat, error) {
	st, err := d.CL.StatCtx(ctx, path)
	if client.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	mode := uint32(0)
	if st.HasMode {
		mode = st.Mode & 0o777
	}
	return &ioStat{
		IsDir:     st.IsDir,
		IsSymlink: st.HasMode && st.Mode&0o170000 == 0o120000,
		Mode:      mode,
		Size:      st.Size,
		Rev:       st.Revision,
	}, nil
}

// List implements layerIO.
func (d *ClientDeps) List(ctx context.Context, dir string) ([]ioEntry, error) {
	infos, err := d.CL.ListCtx(ctx, dir)
	if err != nil {
		if client.IsNotFound(err) {
			return nil, fs.ErrNotFound
		}
		return nil, err
	}
	out := make([]ioEntry, 0, len(infos))
	for _, info := range infos {
		mode := uint32(0)
		if info.HasMode {
			mode = info.Mode & 0o7777
		}
		out = append(out, ioEntry{
			Name:      info.Name,
			IsDir:     info.IsDir,
			IsSymlink: info.HasMode && info.Mode&0o170000 == 0o120000,
			Mode:      mode,
			Size:      info.Size,
		})
	}
	return out, nil
}

// Read implements layerIO.
func (d *ClientDeps) Read(ctx context.Context, path string) ([]byte, error) {
	return d.CL.ReadCtx(ctx, path)
}

// ReadStream implements layerIO.
func (d *ClientDeps) ReadStream(ctx context.Context, path string) (io.ReadCloser, error) {
	return d.CL.ReadStream(ctx, path)
}

// Write implements layerIO.
func (d *ClientDeps) Write(ctx context.Context, path string, data []byte) error {
	return d.CL.WriteCtx(ctx, path, data)
}

// Delete implements layerIO.
func (d *ClientDeps) Delete(ctx context.Context, path string) error {
	err := d.CL.DeleteFileCtx(ctx, path)
	if err != nil && client.IsNotFound(err) {
		return fs.ErrNotFound
	}
	return err
}

// EnsureParent implements layerIO: it creates each missing ancestor with
// single-level mkdirs (the server's mkdir does not auto-create parents).
func (d *ClientDeps) EnsureParent(ctx context.Context, dir string) error {
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" || dir == "/" {
		return nil
	}
	segments := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	cur := ""
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		cur = cur + "/" + seg
		if _, err := d.CL.StatCtx(ctx, cur); err == nil {
			continue
		} else if !client.IsNotFound(err) {
			return err
		}
		if err := d.CL.MkdirCtx(ctx, cur, 0o755); err != nil {
			// A lost bootstrap race with another mount is fine.
			if st, serr := d.CL.StatCtx(ctx, cur); serr == nil && st != nil && st.IsDir {
				continue
			}
			return err
		}
	}
	return nil
}

// GetLayer implements layerRegistry.
func (d *ClientDeps) GetLayer(ctx context.Context, ref string) (*ioLayer, error) {
	layer, err := d.CL.GetFSLayer(ctx, ref)
	if err != nil {
		return nil, err
	}
	return &ioLayer{LayerID: layer.LayerID, State: layer.State}, nil
}

// GetCheckpoint implements layerRegistry.
func (d *ClientDeps) GetCheckpoint(ctx context.Context, checkpointID string) (*ioCheckpoint, error) {
	cp, err := d.CL.GetFSLayerCheckpoint(ctx, checkpointID)
	if err != nil {
		return nil, err
	}
	return &ioCheckpoint{CheckpointID: cp.CheckpointID, LayerID: cp.LayerID}, nil
}

// ListEvents implements layerRegistry.
func (d *ClientDeps) ListEvents(ctx context.Context, layerID string, since int64) ([]ioEvent, error) {
	events, err := d.CL.ListFSLayerEvents(ctx, layerID, since)
	if err != nil {
		return nil, err
	}
	out := make([]ioEvent, 0, len(events))
	for _, e := range events {
		out = append(out, ioEvent{Seq: e.Seq, Op: e.Op})
	}
	return out, nil
}
