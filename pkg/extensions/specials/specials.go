// Package specials provides metadata-only special nodes (FIFO/char/blk/
// socket): they exist as path→mode records with no remote object and no
// data plane. Mknod with a special mode registers one, lookups synthesize
// the entry, readdir merges the records into listings, hardlinks add
// further aliases, setattr maintains the record locally (size changes are
// refused), and unlink/rmdir/rename drop or move the records with the
// surviving paths.
package specials

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Options configures the extension.
type Options struct {
	// Events, when set, drops a path's special-node records on delete
	// events (remote-origin deletions included).
	Events *fs.EventBus
}

// specialNode is one metadata-only node: the mode (with type bits), the
// creating owner, and the set of hardlink aliases sharing this identity.
// chmod/chown through any alias updates the shared record.
type specialNode struct {
	mode     uint32
	rdev     uint32
	uid, gid uint32
	hasOwner bool
	ctime    time.Time
	aliases  map[string]struct{}
}

// Specials is the fs.Handler owning the special-node table.
type Specials struct {
	mu    sync.RWMutex
	byDir map[string]map[string]*specialNode // parent path → name → node
}

// New creates the extension, subscribing to delete events when Events is
// set.
func New(opts Options) (*Specials, error) {
	s := &Specials{byDir: map[string]map[string]*specialNode{}}
	if opts.Events != nil {
		opts.Events.On(fs.EventDelete, func(e fs.Event) { s.drop(e.Path) })
	}
	return s, nil
}

// Count reports live special nodes (reexec gating).
func (s *Specials) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, m := range s.byDir {
		n += len(m)
	}
	return n
}

// ReexecRefusal reports live special nodes (lost on re-exec).
func (s *Specials) ReexecRefusal() (string, int) {
	if n := s.Count(); n > 0 {
		return "metadata-only special nodes exist", n
	}
	return "", 0
}

// Do intercepts the ops that touch special records; everything else passes
// through.
func (s *Specials) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpMknod:
		if !metadataOnlySpecialMode(op.Mode) {
			return next(ctx, op)
		}
		// EEXIST rows: a special already registered at the path, or any
		// existing entry in the chain below (remote, overlays, pending
		// writes) refuses re-creation.
		if _, ok := s.get(op.Path); ok {
			return fs.Result{}, syscall.EEXIST
		}
		if err := s.refuseIfChainHas(ctx, op.Path, next); err != nil {
			return fs.Result{}, err
		}
		// Special nodes are metadata only: no remote op. The full mode
		// (with type bits) rides on the attr; the facade honors it. The
		// creating caller becomes the record's owner (sticky-dir and chown
		// semantics below the facade need a uid to check against).
		s.register(op.Path, op.Mode, op.Rdev, op.Caller)
		return s.attrResult(op.Path)
	case fs.OpLookup, fs.OpGetAttr:
		if _, ok := s.get(op.Path); ok {
			return s.attrResult(op.Path)
		}
		return next(ctx, op)
	case fs.OpLink:
		if _, ok := s.get(op.Path); !ok {
			return next(ctx, op)
		}
		// Hardlinking a special node adds another alias record; the link
		// never reaches the remote (there is no object to link).
		if _, ok := s.get(op.Path2); ok {
			return fs.Result{}, syscall.EEXIST
		}
		if err := s.refuseIfChainHas(ctx, op.Path2, next); err != nil {
			return fs.Result{}, err
		}
		s.linkAlias(op.Path, op.Path2)
		return s.attrResult(op.Path2)
	case fs.OpSetAttr:
		node, ok := s.get(op.Path)
		if !ok {
			return next(ctx, op)
		}
		// A special node has no bytes: size changes are refused. Mode
		// changes rewrite the record's permission bits (type bits stay);
		// owner changes update the record; timestamp changes are accepted
		// as no-ops (the record carries no clock of its own). Every alias
		// of the node sees the change — hardlinked specials share identity.
		if op.Size >= 0 {
			return fs.Result{}, syscall.EINVAL
		}
		s.mu.Lock()
		if op.HasMode {
			// Keep the full 0o7777 range: sticky/setgid survive chmods on
			// special nodes (chmod/11 expects 01621 on a fifo).
			node.mode = node.mode&syscall.S_IFMT | op.Mode&0o7777
			node.ctime = time.Now()
		}
		if op.HasSetUID || op.HasSetGID {
			node.ctime = time.Now()
		}
		if op.HasSetUID || op.HasSetGID {
			node.hasOwner = true
			if op.HasSetUID {
				node.uid = op.SetUID
			}
			if op.HasSetGID {
				node.gid = op.SetGID
			}
		}
		s.mu.Unlock()
		return s.attrResult(op.Path)
	case fs.OpUnlink:
		if _, ok := s.get(op.Path); ok {
			// The record is the whole node: unlink it locally, no remote
			// call.
			s.drop(op.Path)
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		s.drop(op.Path)
		return res, nil
	case fs.OpRmdir:
		// Records under the directory count as children: the remote sees
		// an empty directory, but replacing it would orphan them.
		if s.hasSubtree(op.Path) {
			return fs.Result{}, syscall.ENOTEMPTY
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		s.dropSubtree(op.Path)
		return res, nil
	case fs.OpRename:
		if op.RegistryOnly {
			// A local namespace move (localize migrating metadata-only
			// nodes): move the records only. The ordinary path below
			// preflights/deletes the TARGET through next() and would wipe a
			// backing remote object the local view never meant to touch.
			if _, ok := s.get(op.Path2); ok {
				s.drop(op.Path2)
			}
			s.renameSubtree(op.Path, op.Path2)
			if _, ok := s.get(op.Path2); ok {
				return s.attrResult(op.Path2)
			}
			return fs.Result{}, nil
		}
		if _, ok := s.get(op.Path); ok {
			// An exact special record has no remote entry to move, so the
			// rename itself stays local. Forwarding it would remotely rename
			// whatever else happens to live at the source path (a stale
			// remote file lands on the special's name) — but a remote entry
			// squatting on the TARGET must still die: POSIX rename replaces
			// the target, and next() here skips this handler, reaching the
			// terminal's delete directly.
			// Preflight and remote-delete the TARGET before committing the
			// local move: a remote directory at the destination cannot be
			// replaced by a special (EISDIR per POSIX), and a failed
			// remote deletion must abort the rename instead of leaving
			// the old target hidden underneath the moved special record.
			if st, serr := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: op.Path2}); serr == nil && st.Attr != nil && st.Attr.IsDir() {
				return fs.Result{}, syscall.EISDIR
			}
			if _, derr := next(ctx, &fs.Op{Kind: fs.OpUnlink, Path: op.Path2}); derr != nil && !fs.IsNotFound(derr) {
				return fs.Result{}, derr
			}
			if _, ok := s.get(op.Path2); ok {
				s.drop(op.Path2)
			}
			s.renameSubtree(op.Path, op.Path2)
			return s.attrResult(op.Path2)
		}
		// The rename replaces whatever the target was: a special record
		// squatting on the target dies with it, or it would shadow the
		// remotely-renamed entry now living at the target (and hijack the
		// rename-back into the local-only path above). The drop waits for
		// the remote rename to SUCCEED — dropping first destroyed the
		// FIFO when a later 403/5xx failed the rename, and the failed
		// namespace op had erased the existing target.
		hadTarget := false
		if _, ok := s.get(op.Path2); ok {
			hadTarget = true
		}
		res, err := next(ctx, op)
		if err != nil {
			// A rename out of (or into) a directory that carries special
			// records below it has no remote entry of its own: a 404 from
			// the passthrough rename still moves the records.
			if !fs.IsNotFound(err) {
				return res, err
			}
		}
		if hadTarget {
			s.drop(op.Path2)
		}
		s.renameSubtree(op.Path, op.Path2)
		return res, nil
	case fs.OpList:
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		s.mergeIntoListing(op.Path, &res)
		return res, nil
	}
	return next(ctx, op)
}

// refuseIfChainHas reports EEXIST when the chain below knows the path;
// ENOENT and unsupported (nothing below answers getattr) both allow
// registration.
func (s *Specials) refuseIfChainHas(ctx context.Context, path string, next fs.Next) error {
	if _, err := next(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path}); err == nil {
		return syscall.EEXIST
	} else if !fs.IsNotFound(err) && fs.StatusOf(err) != fs.StatusOf(fs.ErrUnsupported) {
		return err
	}
	return nil
}

// attrResult synthesizes the record's lookup reply: mode with type bits,
// owner identity, and the live hardlink-alias count.
func (s *Specials) attrResult(p string) (fs.Result, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dir, name := splitPath(p)
	n := s.byDir[dir][name]
	if n == nil {
		return fs.Result{}, fs.ErrNotFound
	}
	return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: n.mode, Nlink: uint32(len(n.aliases)),
		Rdev: n.rdev, Uid: n.uid, Gid: n.gid, HasOwner: n.hasOwner, Mtime: n.ctime}}, nil
}

// mergeIntoListing appends records directly under dir that the listing
// does not already name.
func (s *Specials) mergeIntoListing(dir string, res *fs.Result) {
	s.mu.RLock()
	names := s.byDir[dir]
	if len(names) == 0 {
		s.mu.RUnlock()
		return
	}
	present := make(map[string]struct{}, len(res.Entries))
	for _, e := range res.Entries {
		present[e.Name] = struct{}{}
	}
	extra := make([]fs.Entry, 0, len(names))
	for name, n := range names {
		if _, ok := present[name]; ok {
			continue
		}
		extra = append(extra, fs.Entry{Name: name, Attr: fs.Attr{Kind: fs.KindFile, Mode: n.mode,
			Nlink: uint32(len(n.aliases)), Rdev: n.rdev, Uid: n.uid, Gid: n.gid, HasOwner: n.hasOwner, Mtime: n.ctime}})
	}
	s.mu.RUnlock()
	res.Entries = append(res.Entries, extra...)
}

// renameSubtree moves the exact record and every record under the path's
// subtree to the corresponding new paths (directory rename).
func (s *Specials) renameSubtree(oldP, newP string) {
	if oldP == newP {
		return
	}
	oldPrefix := strings.TrimSuffix(oldP, "/") + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	moves := make(map[string]*specialNode)
	for dir, names := range s.byDir {
		for name, n := range names {
			p := dirJoin(dir, name)
			switch {
			case p == oldP:
				moves[newP] = n
			case strings.HasPrefix(p, oldPrefix):
				moves[newP+p[len(oldP):]] = n
			default:
				continue
			}
			delete(names, name)
		}
		if len(names) == 0 {
			delete(s.byDir, dir)
		}
	}
	for p, n := range moves {
		// Rewire the alias set: renamed paths replace the old spellings.
		for alias := range n.aliases {
			switch {
			case alias == oldP:
				delete(n.aliases, alias)
				n.aliases[p] = struct{}{}
			case strings.HasPrefix(alias, oldPrefix):
				delete(n.aliases, alias)
				n.aliases[p+alias[len(oldP):]] = struct{}{}
			}
		}
		dir2, name2 := splitPath(p)
		if s.byDir[dir2] == nil {
			s.byDir[dir2] = map[string]*specialNode{}
		}
		s.byDir[dir2][name2] = n
	}
}

func (s *Specials) get(p string) (*specialNode, bool) {
	dir, name := splitPath(p)
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.byDir[dir][name]
	return n, ok
}

// register records a fresh special node at p with mode and creator.
func (s *Specials) register(p string, mode, rdev uint32, caller fs.Caller) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir, name := splitPath(p)
	if s.byDir[dir] == nil {
		s.byDir[dir] = map[string]*specialNode{}
	}
	s.byDir[dir][name] = &specialNode{mode: mode, rdev: rdev, uid: caller.Uid, gid: caller.Gid,
		hasOwner: true, ctime: time.Now(), aliases: map[string]struct{}{p: {}}}
}

// linkAlias adds p as another name of src's node.
func (s *Specials) linkAlias(src, p string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir, name := splitPath(p)
	if s.byDir[dir] == nil {
		s.byDir[dir] = map[string]*specialNode{}
	}
	if old, ok := s.byDir[dir][name]; ok {
		delete(old.aliases, p)
	}
	srcDir, srcName := splitPath(src)
	sn := s.byDir[srcDir][srcName]
	if sn == nil {
		return
	}
	sn.aliases[p] = struct{}{}
	s.byDir[dir][name] = sn
}

func (s *Specials) drop(p string) {
	dir, name := splitPath(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.byDir[dir]; m != nil {
		if n, ok := m[name]; ok {
			delete(n.aliases, p)
		}
		delete(m, name)
		if len(m) == 0 {
			delete(s.byDir, dir)
		}
	}
}

// hasSubtree reports whether any record lives under dir's subtree
// (dir itself excluded — specials are never directories).
func (s *Specials) hasSubtree(dir string) bool {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	s.mu.RLock()
	defer s.mu.RUnlock()
	for d, names := range s.byDir {
		for name := range names {
			if strings.HasPrefix(dirJoin(d, name), prefix) {
				return true
			}
		}
	}
	return false
}

// dropSubtree removes every record under dir's subtree.
func (s *Specials) dropSubtree(dir string) {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	for d, names := range s.byDir {
		for name := range names {
			if strings.HasPrefix(dirJoin(d, name), prefix) {
				delete(names, name)
			}
		}
		if len(names) == 0 {
			delete(s.byDir, d)
		}
	}
}

// splitPath splits an absolute path into parent and base name.
func splitPath(p string) (dir, name string) {
	i := len(p) - 1
	for i >= 0 && p[i] != '/' {
		i--
	}
	if i <= 0 {
		return "/", p[1:]
	}
	return p[:i], p[i+1:]
}

func dirJoin(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// metadataOnlySpecialMode reports whether the mode names a metadata-only
// node type (no data plane).
func metadataOnlySpecialMode(mode uint32) bool {
	switch mode & 0170000 {
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return true
	default:
		return false
	}
}
