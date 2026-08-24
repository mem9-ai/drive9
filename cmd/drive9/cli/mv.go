package cli

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Mv renames or moves a remote file/directory. Metadata-only, zero S3 cost
// on drive9. Object stores support same-bucket single-file mv only.
//
//	drive9 fs mv /old/path /new/path
//	drive9 fs mv :/old/path :/new/path
func Mv(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs mv [--layer <ref>] [--auth=local|server] <old> <new>")
	}
	oldLoc, err := Parse(args[0])
	if err != nil {
		return err
	}
	newLoc, err := Parse(args[1])
	if err != nil {
		return err
	}
	oldLoc = promoteBareFSArg(oldLoc)
	newLoc = promoteBareFSArg(newLoc)

	if oldLoc.Kind == KindObject || newLoc.Kind == KindObject {
		if layerRef != "" {
			return fmt.Errorf("--layer is drive9-only")
		}
		if oldLoc.Kind != KindObject || newLoc.Kind != KindObject {
			return fmt.Errorf("cross-scheme mv not supported: %q -> %q (use cp + rm)", args[0], args[1])
		}
		if oldLoc.Scheme != newLoc.Scheme || oldLoc.Bucket != newLoc.Bucket {
			return fmt.Errorf("mv: object-store URIs not supported across buckets (use cp + rm)")
		}
		if oldLoc.DirHint || newLoc.DirHint || trailingDir(oldLoc.Path) {
			return fmt.Errorf("mv: directory rename is not supported on object stores (use cp + rm)")
		}
		srcH, err := fsHandleForArgOpts(c, args[0], true)
		if err != nil {
			return err
		}
		dstH, err := fsHandleForArgOpts(c, args[1], true)
		if err != nil {
			return err
		}
		if !SameIdentity(srcH.Backend, dstH.Backend) {
			return fmt.Errorf("mv: object-store URIs must share the same backend identity (use cp + rm)")
		}
		ren, ok := srcH.Backend.(Renamer)
		if !ok {
			return fmt.Errorf("mv: object-store URIs not supported (use cp + rm)")
		}
		ctx, cancel := withObjectOpTimeout(context.Background())
		defer cancel()
		return ren.Rename(ctx, oldLoc, newLoc)
	}

	oldRP, _ := locationAsRemotePath(oldLoc)
	newRP, _ := locationAsRemotePath(newLoc)
	// leftover KindLocal (relative / C:/): send raw to Client
	oldPath := oldLoc.Path
	newPath := newLoc.Path
	if oldLoc.Kind == KindLocal {
		oldPath = oldLoc.Raw
		oldRP = RemotePath{}
	}
	if newLoc.Kind == KindLocal {
		newPath = newLoc.Raw
		newRP = RemotePath{}
	}
	if layerRef != "" {
		if err := requireNoLayerWithRemoteContext(layerRef, oldRP, args[0]); err != nil {
			return err
		}
		if err := requireNoLayerWithRemoteContext(layerRef, newRP, args[1]); err != nil {
			return err
		}
		return renameLayerPath(context.Background(), c, layerRef, oldPath, newPath)
	}

	switch {
	case oldRP.Context == "" && newRP.Context == "":
	case oldRP.Context != "" && newRP.Context != "" && oldRP.Context == newRP.Context:
		var err error
		c, err = newFSClientForContext(oldRP.Context)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cross-context rename not supported: %q -> %q", args[0], args[1])
	}
	return c.Rename(oldPath, newPath)
}

func trailingDir(p string) bool {
	return len(p) > 0 && p[len(p)-1] == '/'
}
