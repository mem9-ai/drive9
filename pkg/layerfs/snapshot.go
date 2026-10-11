// Snapshot walking: fold a layer's (or checkpoint's) marker trees into one
// routing/content snapshot — the fs itself is the index.
package layerfs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// TreeEntry is one content entry of the overlay tree.
type TreeEntry struct {
	ServerPath string
	IsDir      bool
	IsSymlink  bool
	Mode       uint32
	Size       int64
}

// Snapshot is the folded view content of a layer or checkpoint.
type Snapshot struct {
	// Tree maps server-absolute paths to overlay entries (dirs included).
	Tree map[string]TreeEntry
	// Blackouts maps server-absolute paths to whiteout markers.
	Blackouts map[string]Blackout
	// Attrs maps server-absolute paths to metadata-only patches.
	Attrs map[string]AttrsPatch
	// Cas maps server-absolute paths of tree files to commit anchors.
	Cas map[string]CasAnchor
	// Base is the layer (or checkpoint) namespace root the snapshot came
	// from.
	Base string
}

// CasAnchor is the decoded cas marker of a tree file.
type CasAnchor struct {
	BaseRev int64
	HasBase bool
}

// LoadSnapshot walks the marker roots at base and folds them into one
// snapshot.
func LoadSnapshot(ctx context.Context, cl *client.Client, base string) (*Snapshot, error) {
	roots := RootsOf(base)
	snap := &Snapshot{
		Tree:      map[string]TreeEntry{},
		Blackouts: map[string]Blackout{},
		Attrs:     map[string]AttrsPatch{},
		Cas:       map[string]CasAnchor{},
		Base:      base,
	}
	if err := walkRemote(ctx, cl, roots.Tree, func(serverPath string, info client.FileInfo) {
		snap.Tree[serverPath] = TreeEntry{
			ServerPath: serverPath,
			IsDir:      info.IsDir,
			IsSymlink:  !info.IsDir && info.HasMode && info.Mode&0o170000 == 0o120000,
			Mode:       info.Mode & 0o7777,
			Size:       info.Size,
		}
	}); err != nil {
		return nil, err
	}
	if err := readMarkers(ctx, cl, roots.Blackouts, func(serverPath, content string) {
		snap.Blackouts[serverPath] = DecodeBlackout([]byte(content))
	}); err != nil {
		return nil, err
	}
	if err := readMarkers(ctx, cl, roots.BlackoutDirs, func(serverPath, content string) {
		bl := DecodeBlackout([]byte(content))
		bl.IsDir = true
		snap.Blackouts[serverPath] = bl
	}); err != nil {
		return nil, err
	}
	if err := readMarkers(ctx, cl, roots.Attrs, func(serverPath, content string) {
		var patch AttrsPatch
		if err := json.Unmarshal([]byte(content), &patch); err == nil {
			snap.Attrs[serverPath] = patch
		}
	}); err != nil {
		return nil, err
	}
	if err := readMarkers(ctx, cl, roots.Cas, func(serverPath, content string) {
		rev, hasBase := DecodeCas([]byte(content))
		snap.Cas[serverPath] = CasAnchor{BaseRev: rev, HasBase: hasBase}
	}); err != nil {
		return nil, err
	}
	return snap, nil
}

// readMarkers walks a marker tree and hands each marker file's content to
// fn (server-absolute shadowed path, marker body).
func readMarkers(ctx context.Context, cl *client.Client, root string, fn func(serverPath, content string)) error {
	return walkRemote(ctx, cl, root, func(serverPath string, info client.FileInfo) {
		if info.IsDir {
			return
		}
		content, err := cl.ReadCtx(ctx, MarkerPath(root, serverPath))
		if err != nil {
			return // an unreadable marker applies as absent
		}
		fn(serverPath, string(content))
	})
}

// walkRemote lists a server directory tree recursively, invoking fn for
// every entry below root; serverPath is the shadowed server-absolute path.
func walkRemote(ctx context.Context, cl *client.Client, root string, fn func(serverPath string, info client.FileInfo)) error {
	var walk func(dir string) error
	walk = func(dir string) error {
		infos, err := cl.ListCtx(ctx, dir)
		if err != nil {
			if client.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("list %s: %w", dir, err)
		}
		for _, info := range infos {
			child := pathutil.JoinPath(dir, info.Name)
			fn(relToRoot(root, child), info)
			if info.IsDir {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if root == "" {
		return nil
	}
	return walk(root)
}

// relToRoot strips the marker root, leaving the shadowed server-absolute
// path (a path that does not sit below the root is reported as itself).
func relToRoot(root, serverPath string) string {
	if len(serverPath) > len(root) && serverPath[:len(root)] == root {
		return serverPath[len(root):]
	}
	return serverPath
}
