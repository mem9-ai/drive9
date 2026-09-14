// This file implements the extent engine's namespace operations: the jfs_edge
// tree, the file_nodes projection that mirrors it, attributes, and the
// create/link/unlink/rename/rmdir transitions that keep the two in one
// transaction.
package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

func (s *Store) jfsLookupTx(db execer, parent uint64, name string) (uint64, *ExtentAttr, int, error) {
	return s.jfsLookupWithLockTx(db, parent, name, false)
}

// jfsLookupForUpdateTx is jfsLookupTx with the edge row locked. A caller that
// will move or delete that edge needs it: a concurrent unlink of the same
// dentry otherwise commits in between, the later DELETE matches no row, and the
// caller still inserts its destination edge — leaving an edge that points at a
// deleted inode and reports success.
func (s *Store) jfsLookupForUpdateTx(tx *sql.Tx, parent uint64, name string) (uint64, *ExtentAttr, int, error) {
	return s.jfsLookupWithLockTx(tx, parent, name, true)
}

func (s *Store) jfsLookupWithLockTx(db execer, parent uint64, name string, forUpdate bool) (uint64, *ExtentAttr, int, error) {
	var ino uint64
	q := `SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	err := db.QueryRow(q, parent, []byte(name)).Scan(&ino)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, nil, 0, err
	}
	attr, eno, err := s.jfsGetAttrTx(db, ino)
	return ino, attr, eno, err
}
func (s *Store) jfsGetAttrTx(db execer, ino uint64) (*ExtentAttr, int, error) {
	return s.jfsGetAttrWithLockTx(db, ino, false)
}

// jfsGetAttrForUpdateTx is jfsGetAttrTx with a row lock. Length changes need
// it: a length is a read-modify-write over the node row, so reading it without
// the lock lets a concurrent write commit in between and be overwritten.
func (s *Store) jfsGetAttrForUpdateTx(tx *sql.Tx, ino uint64) (*ExtentAttr, int, error) {
	return s.jfsGetAttrWithLockTx(tx, ino, true)
}

func (s *Store) jfsGetAttrWithLockTx(db execer, ino uint64, forUpdate bool) (*ExtentAttr, int, error) {
	var typ uint8
	var mode uint16
	var uid, gid uint32
	var atime, mtime, ctime int64
	var asec, msec, csec int16
	var nlink uint32
	var length, parent uint64
	var rdev uint32
	q := `SELECT type, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, parent, rdev
		FROM jfs_node WHERE inode = ?`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	err := db.QueryRow(q, ino).Scan(
		&typ, &mode, &uid, &gid, &atime, &mtime, &ctime, &asec, &msec, &csec, &nlink, &length, &parent, &rdev)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	attr := jfsAttrFromNode(typ, mode, uid, gid, atime, mtime, ctime, asec, msec, csec, nlink, length, parent, rdev)
	return &attr, 0, nil
}
func (s *Store) jfsSetAttrTx(tx *sql.Tx, ino uint64, set uint16, patch ExtentAttr, quota *ExtentQuotaLimit, compactOut *[]uint32) (*ExtentAttr, int, error) {
	// FOR UPDATE for the same reason jfsTruncateTx needs it: the statement
	// below writes the whole length column from a value read here, so an
	// unlocked read lets a concurrent write's committed length be clobbered
	// (silently losing the visibility of bytes that are in the chunk map).
	attr, eno, err := s.jfsGetAttrForUpdateTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, eno, err
	}
	const (
		setMode     = 1 << 0
		setUID      = 1 << 1
		setGID      = 1 << 2
		setSize     = 1 << 3
		setAtime    = 1 << 4
		setMtime    = 1 << 5
		setAtimeNow = 1 << 7
		setMtimeNow = 1 << 8
	)
	// A size change must go through the truncate bookkeeping, never through the
	// plain length column: the VFS routes FATTR_SIZE through Truncate first and
	// then calls SetAttr again with the resulting length, so the common case is
	// the no-op below. A direct POST /v1/extent/meta setattr with set=8 is not:
	// writing the column alone would report length X while reads of
	// [oldLen, X) returned nothing, a silent chunk-map corruption.
	if set&setSize != 0 && patch.Length != attr.Length {
		if _, _, _, eno, terr := s.jfsTruncateTx(tx, ino, patch.Length, quota, compactOut); terr != nil || eno != 0 {
			return nil, eno, terr
		}
		attr, eno, err = s.jfsGetAttrTx(tx, ino)
		if err != nil || eno != 0 {
			return attr, eno, err
		}
	}
	if set&setMode != 0 {
		attr.Mode = patch.Mode
	}
	if set&setUID != 0 {
		attr.Uid = patch.Uid
	}
	if set&setGID != 0 {
		attr.Gid = patch.Gid
	}
	if set&setSize != 0 {
		// Equal by construction here: a differing length went through
		// jfsTruncateTx above, which owns the chunk map.
		attr.Length = patch.Length
	}
	nowt := time.Now()
	if set&setAtimeNow != 0 {
		attr.Atime = nowt.Unix()
		attr.Atimensec = uint32(nowt.Nanosecond())
	} else if set&setAtime != 0 {
		attr.Atime = patch.Atime
		attr.Atimensec = patch.Atimensec
	}
	if set&setMtimeNow != 0 {
		attr.Mtime = nowt.Unix()
		attr.Mtimensec = uint32(nowt.Nanosecond())
	} else if set&setMtime != 0 {
		attr.Mtime = patch.Mtime
		attr.Mtimensec = patch.Mtimensec
	}
	now := nowt.UnixNano()
	atime, asec := jfsSplitTime(attr.Atime*1e9 + int64(attr.Atimensec))
	mtime, msec := jfsSplitTime(attr.Mtime*1e9 + int64(attr.Mtimensec))
	ctime, csec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET mode=?, uid=?, gid=?, length=?, atime=?, mtime=?, ctime=?, atimensec=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		attr.Mode, attr.Uid, attr.Gid, attr.Length, atime, mtime, ctime, asec, msec, csec, ino); err != nil {
		return nil, 0, err
	}
	return s.jfsGetAttrTx(tx, ino)
}

// jfsRequireDirTx reports ENOTDIR unless ino is a directory (or ENOENT if it is
// gone), the check every edge insert needs on its parent.
//
// The read takes the row lock. That is what serializes a create against a
// concurrent rmdir of the same directory: both take this lock, so the rmdir's
// emptiness check cannot run between the create's check and its edge insert
// (TiDB has no gap locks, so a plain read would leave exactly that window and a
// child edge could appear just after the directory's node was deleted).
func (s *Store) jfsRequireDirTx(tx *sql.Tx, ino uint64) (int, error) {
	var typ uint8
	err := tx.QueryRow(`SELECT type FROM jfs_node WHERE inode = ? FOR UPDATE`, ino).Scan(&typ)
	if errors.Is(err, sql.ErrNoRows) {
		return int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, err
	}
	if typ != jfsTypeDir {
		return int(syscall.ENOTDIR), nil
	}
	return 0, nil
}

func (s *Store) jfsMknodTx(ctx context.Context, tx *sql.Tx, parent uint64, name string, typ uint8, mode, cumask uint16, inode uint64, attr ExtentAttr, projPath string, uid, gid uint32, symlinkTarget string) (uint64, *ExtentAttr, int, error) {
	if parent == 0 {
		parent = jfsRootIno
	}
	if _, eno, err := s.jfsGetAttrTx(tx, parent); err != nil {
		return 0, nil, 0, err
	} else if eno == int(syscall.ENOENT) && projPath != "" {
		var err error
		parent, err = s.jfsEnsureParentsTx(tx, projPath, uid, gid)
		if err != nil {
			return 0, nil, 0, err
		}
	} else if eno != 0 {
		return 0, nil, eno, nil
	}
	// The parent must be a directory, like upstream doMknod's ENOTDIR. The
	// projection layer never produces a file parent, but this RPC is reachable
	// directly, and a child edge under a file would make readdir of that file
	// return entries and leave the children undeletable through the classic
	// paths.
	if eno, err := s.jfsRequireDirTx(tx, parent); err != nil || eno != 0 {
		return 0, nil, eno, err
	}
	var exist uint64
	err := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&exist)
	if err == nil {
		a, _, gerr := s.jfsGetAttrTx(tx, exist)
		return exist, a, int(syscall.EEXIST), gerr
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, nil, 0, err
	}
	if inode == 0 {
		v, err := s.jfsIncrCounterTx(tx, "nextInode", 1)
		if err != nil {
			return 0, nil, 0, err
		}
		inode = uint64(v)
	}
	mode = mode &^ cumask
	nlink := uint32(1)
	length := uint64(0)
	switch typ {
	case jfsTypeDir:
		nlink = 2
		length = 4096
	case jfsTypeSymlink:
		length = uint64(len(symlinkTarget))
		if attr.Length > 0 {
			length = attr.Length
		}
	}
	now := time.Now().UnixNano()
	atime, asec := jfsSplitTime(now)
	if attr.Uid == 0 {
		attr.Uid = uid
	}
	if attr.Gid == 0 {
		attr.Gid = gid
	}
	if _, err := tx.Exec(`INSERT INTO jfs_node
		(inode, type, flags, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
		VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		inode, typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, attr.Rdev, parent); err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		parent, []byte(name), inode, typ); err != nil {
		return 0, nil, 0, err
	}
	if typ == jfsTypeSymlink {
		if _, err := tx.Exec(`INSERT INTO jfs_symlink (inode, target) VALUES (?, ?)`, inode, []byte(symlinkTarget)); err != nil {
			return 0, nil, 0, err
		}
	}
	if typ != jfsTypeDir && projPath != "" {
		if jfsTestFailBeforeProjection != nil {
			if err := jfsTestFailBeforeProjection(); err != nil {
				return 0, nil, 0, err
			}
		}
		if err := s.insertExtentProjectionTx(tx, projPath, inode, mode); err != nil {
			return 0, nil, 0, err
		}
	}
	if typ == jfsTypeDir && projPath != "" {
		dirPath := projPath
		if !strings.HasSuffix(dirPath, "/") {
			dirPath += "/"
		}
		// Recording the directory's jfs inode is what lets a later recursive
		// delete find and drop its subtree, so a failure here must abort.
		if _, err := tx.Exec(`UPDATE file_nodes SET extent_ino = ? WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			append([]any{inode}, s.scope.Args(fileNodePathHash(dirPath), dirPath)...)...); err != nil {
			return 0, nil, 0, err
		}
	}
	out := jfsAttrFromNode(typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, parent, attr.Rdev)
	return inode, &out, 0, nil
}
func (s *Store) jfsEnsureParentsTx(tx *sql.Tx, filePath string, uid, gid uint32) (uint64, error) {
	// Mirroring a parent writes file_nodes.extent_ino, so the tenant now has
	// extent nodes and every classic-path extent probe must keep running. The
	// root row this guarantees is also what jfs_edge below is relative to.
	if err := s.jfsEnsureInitTx(tx); err != nil {
		return 0, err
	}
	parentPath := pathutil.ParentPath(filePath)
	parent := uint64(jfsRootIno)
	if parentPath == "/" {
		return parent, nil
	}
	rel := strings.Trim(parentPath, "/")
	parts := strings.Split(rel, "/")
	acc := ""
	for _, p := range parts {
		acc += "/" + p
		dirPath := acc + "/"
		var exist uint64
		err := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(p)).Scan(&exist)
		if err == nil {
			// A name that exists but is not a directory cannot be a path
			// component. Without this the walk would adopt a file as the
			// parent of everything below it.
			if eno, derr := s.jfsRequireDirTx(tx, exist); derr != nil || eno != 0 {
				if derr != nil {
					return 0, derr
				}
				return 0, fmt.Errorf("extent path component %s is not a directory: %w", dirPath, syscall.ENOTDIR)
			}
			parent = exist
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		v, err := s.jfsIncrCounterTx(tx, "nextInode", 1)
		if err != nil {
			return 0, err
		}
		ino := uint64(v)
		now := time.Now().UnixNano()
		atime, asec := jfsSplitTime(now)
		// A mirrored parent is created permissively on purpose. drive9's
		// authorization is the projection's (keys, scopes, layers), and the
		// jfs tree is an implementation detail a client reaches only through
		// the projection, so a restrictive mode here buys nothing and can deny
		// work: JuiceFS checks write permission against these attrs, and the
		// server-side rename path has no FUSE caller to take a uid/gid from,
		// which used to leave root-owned 0755 directories that a non-root
		// mount could not create files in.
		if _, err := tx.Exec(`INSERT INTO jfs_node
			(inode, type, flags, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
			VALUES (?, ?, 0, 0777, ?, ?, ?, ?, ?, ?, ?, ?, 2, 4096, 0, ?)`,
			ino, jfsTypeDir, uid, gid, atime, atime, atime, asec, asec, asec, parent); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
			parent, []byte(p), ino, jfsTypeDir); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE file_nodes SET extent_ino = ? WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			append([]any{ino}, s.scope.Args(fileNodePathHash(dirPath), dirPath)...)...); err != nil {
			return 0, err
		}
		parent = ino
	}
	return parent, nil
}
func (s *Store) insertExtentProjectionTx(tx *sql.Tx, path string, extentIno uint64, mode uint16) error {
	now := time.Now().UTC()
	inodeID := strings.ToLower(ulid.Make().String())
	nodeID := strings.ToLower(ulid.Make().String())
	parent := pathutil.ParentPath(path)
	name := pathutil.BaseName(path)
	if _, err := tx.Exec(`INSERT INTO inodes (`+s.scope.InsCols(`inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at`)+`)
		VALUES (`+s.scope.InsVals(`?, 0, 1, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(inodeID, uint32(mode), StatusConfirmed, now, now, now)...); err != nil {
		return err
	}
	// Extent blocks are written by JuiceFS's S3 backend, which cannot set a
	// per-object SSE header, so the data-credential mint refuses to hand out
	// credentials on a deployment whose resolved policy requires one (see
	// pkg/server/extent_meta.go). That is what the mode records: no header was
	// requested. It deliberately does not say "unencrypted", because a
	// bucket-default rule still applies server-side and is not observable from
	// here (see StorageEncryptionBucketDefault).
	if _, err := tx.Exec(`INSERT INTO contents (`+s.scope.InsCols(`inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id, content_layout`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, '', '', ?, '', ?`)+`)`,
		s.scope.Args(inodeID, StorageExtent, string(StorageEncryptionBucketDefault), string(ContentLayoutExtent))...); err != nil {
		return fmt.Errorf("insert extent contents: %w", err)
	}
	// Keep the split-table invariant the standard create path has
	// (insertSplitTablesTx): every file row set has a semantic row. Without it
	// Store.Stat -> GetFile -> GetSemantic returns ErrNotFound, and a later
	// description update silently updates zero rows.
	if err := s.InsertSemanticTx(tx, &Semantic{InodeID: inodeID}); err != nil {
		return fmt.Errorf("insert extent semantic: %w", err)
	}
	_, err := tx.Exec(`INSERT INTO file_nodes (`+s.scope.InsCols(`node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory, file_id, inode_id, created_at, content_layout, extent_ino`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(nodeID, path, fileNodePathHash(path), parent, fileNodePathHash(parent),
			name, inodeID, inodeID, now, string(ContentLayoutExtent), extentIno)...)
	if isUniqueViolation(err) {
		return fmt.Errorf("%w", ErrPathConflict)
	}
	return err
}
func (s *Store) jfsParentInoForPathTx(tx *sql.Tx, filePath string) (uint64, error) {
	parentPath := pathutil.ParentPath(filePath)
	if parentPath == "/" {
		return jfsRootIno, nil
	}
	dirPath := parentPath
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}
	var ino sql.NullInt64
	err := tx.QueryRow(`SELECT extent_ino FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
		s.scope.Args(fileNodePathHash(dirPath), dirPath)...).Scan(&ino)
	if err == nil && ino.Valid && ino.Int64 > 0 {
		// The projection says this directory is mirrored, but the column is not
		// authoritative about its kind: only a directory may be a parent, so a
		// stale value pointing at a file falls through to the edge walk (which
		// then either finds the real directory or reports ENOTDIR).
		if eno, derr := s.jfsRequireDirTx(tx, uint64(ino.Int64)); derr != nil {
			return 0, derr
		} else if eno == 0 {
			return uint64(ino.Int64), nil
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return s.jfsEnsureParentsTx(tx, filePath, 0, 0)
}
func (s *Store) jfsLinkEdgeTx(tx *sql.Tx, dstPath string, ino uint64) error {
	parent, err := s.jfsParentInoForPathTx(tx, dstPath)
	if err != nil {
		return err
	}
	name := pathutil.BaseName(dstPath)
	var exist uint64
	err = tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&exist)
	if err == nil {
		return ErrPathConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var typ uint8
	if err := tx.QueryRow(`SELECT type FROM jfs_node WHERE inode = ?`, ino).Scan(&typ); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		parent, []byte(name), ino, typ); err != nil {
		if isUniqueViolation(err) {
			// Another transaction used this name first; a link onto an
			// existing name is a path conflict, not a raw SQL error.
			return ErrPathConflict
		}
		return err
	}
	now := time.Now().UnixNano()
	ctime, csec := jfsSplitTime(now)
	_, err = tx.Exec(`UPDATE jfs_node SET nlink = nlink + 1, ctime = ?, ctimensec = ? WHERE inode = ?`, ctime, csec, ino)
	return err
}

// jfsUnlinkTx removes one edge, its projection row, and — when this was the
// last reference — the jfs node and its blocks.
//
// It also performs the drive9-side last-reference cleanup the classic delete
// does (mark the inodes row DELETED and drop its tags). Without that, the
// extent file's inode row stays CONFIRMED, and since jfsTruncateTx keeps
// inodes.size_bytes exact for extent files, ConfirmedStorageBytesTx — the number
// both quota planes read — never drops: truncate-then-delete cycles would wedge
// a tenant at EDQUOT with no live data.
func (s *Store) jfsUnlinkTx(ctx context.Context, tx *sql.Tx, parent uint64, name, projPath string, sid uint64, opened bool) (*ExtentAttr, int, error) {
	var ino uint64
	var typ uint8
	// FOR UPDATE, like upstream doUnlink's ForUpdate().Get(&e). The nlink
	// decrement below is a read-modify-write over the node, so two unlinks of
	// the *same* dentry would otherwise both pass this read and then serialize
	// on the node: the loser would re-read the post-commit nlink, decrement it a
	// second time, and — with a hardlink alias present — take the
	// last-reference branch, deleting the node and queueing the blocks of a
	// file whose other name is still live. With the lock the loser sees no row
	// and returns ENOENT, which is what an already-deleted path means.
	err := tx.QueryRow(`SELECT inode, type FROM jfs_edge WHERE parent = ? AND name = ? FOR UPDATE`,
		parent, []byte(name)).Scan(&ino, &typ)
	if errors.Is(err, sql.ErrNoRows) {
		if projPath != "" {
			// No edge means the extent file is already gone; the projection can
			// survive an earlier failed unlink, so clear it. A failed delete
			// must not be reported as a successful unlink.
			// FOR UPDATE: a rename or create can claim this name between the edge
			// read above and this cleanup (TiDB has no gap locks, so the missing
			// edge reserves nothing). Locking the row makes that operation wait
			// for this transaction instead of committing underneath it, and the
			// locking edge re-read below then decides whether the row is still an
			// orphan. A current-mode DELETE on a snapshot read would instead land
			// on whatever the other transaction had just moved here.
			var driveInodeID sql.NullString
			var orphanExtentIno sql.NullInt64
			serr := tx.QueryRow(`SELECT COALESCE(inode_id, file_id), extent_ino FROM file_nodes WHERE `+
				s.scope.And(`path_hash = ? AND path = ?`)+` FOR UPDATE`,
				s.scope.Args(fileNodePathHash(projPath), projPath)...).Scan(&driveInodeID, &orphanExtentIno)
			if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
				return nil, 0, serr
			}
			// The row is an orphan only while no dentry claims the name. If one
			// appeared, this cleanup is stale: the row now belongs to a live
			// file, and deleting it would strip that file's projection (the
			// jfs edge stays, so no block or tag is lost, but the file loses its
			// path row until the next mutation rebuilds it).
			var claimed uint64
			cerr := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ? FOR UPDATE`,
				parent, []byte(name)).Scan(&claimed)
			if cerr != nil && !errors.Is(cerr, sql.ErrNoRows) {
				return nil, 0, cerr
			}
			if cerr == nil {
				return nil, int(syscall.ENOENT), nil
			}
			if _, derr := tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
				s.scope.Args(fileNodePathHash(projPath), projPath)...); derr != nil {
				return nil, 0, derr
			}
			// The drive9 inode goes with its last dentry, exactly as on the
			// normal unlink path: a leftover projection row must not keep the
			// inode CONFIRMED, which would hold its size_bytes against the
			// quota with nothing referencing it.
			if driveInodeID.Valid && driveInodeID.String != "" {
				// Lock the entity before counting its remaining dentries: two
				// concurrent cleanups of two orphaned rows of one inode would
				// otherwise each see the other's row as present and neither
				// would release it.
				if lerr := s.lockFileIDsForDeleteTx(ctx, tx, []string{driveInodeID.String}); lerr != nil {
					return nil, 0, lerr
				}
				// FOR UPDATE, not a plain COUNT: TiDB's REPEATABLE-READ
				// pessimistic mode keeps this transaction's start_ts snapshot
				// for plain reads even after the row lock was granted, so a
				// snapshot count would still see a concurrently deleted sibling
				// row and refuse to release the entity. The locked count is the
				// same shape DeleteFileWithRefCheck uses.
				var remaining int
				if cerr := tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE `+
					s.scope.And(`COALESCE(inode_id, file_id) = ?`)+` FOR UPDATE`,
					s.scope.Args(driveInodeID.String)...).Scan(&remaining); cerr != nil {
					return nil, 0, cerr
				}
				if remaining == 0 {
					if merr := s.markFilesDeletedTx(ctx, tx, []string{driveInodeID.String}); merr != nil {
						return nil, 0, merr
					}
					if terr := s.deleteFileTagsByIDsTx(ctx, tx, []string{driveInodeID.String}); terr != nil {
						return nil, 0, terr
					}
				}
			}
			// The row just deleted was the last thing naming this inode's
			// chunks: the jfs edge is already gone, so nothing else can reach
			// them. Record the inode for the block drain (a no-op unless it is
			// really orphaned).
			if orphanExtentIno.Valid && orphanExtentIno.Int64 != 0 {
				if rerr := s.jfsReclaimOrphanedInoTx(tx, uint64(orphanExtentIno.Int64)); rerr != nil {
					return nil, 0, rerr
				}
			}
		}
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	if typ == jfsTypeDir {
		return nil, int(syscall.EPERM), nil
	}
	// FOR UPDATE: the nlink decrement below is a read-modify-write on the node
	// row. Two concurrent unlinks of two hardlink aliases would otherwise both
	// read nlink=2, both write nlink=1, and leave the inode with no edges,
	// nlink=1 and no jfs_delfile row — a permanent leak of the node, its chunks
	// and its blocks.
	attr, eno, err := s.jfsGetAttrForUpdateTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, eno, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)); err != nil {
		return nil, 0, err
	}
	path := projPath
	if path == "" {
		path, _ = s.jfsProjectionPathTx(tx, parent, name)
	}
	// Capture the drive9 inode id before the projection row goes: the
	// last-reference branch below has to mark it deleted.
	var driveInodeID sql.NullString
	if path != "" {
		if serr := tx.QueryRow(`SELECT COALESCE(inode_id, file_id) FROM file_nodes WHERE `+
			s.scope.And(`path_hash = ? AND path = ?`),
			s.scope.Args(fileNodePathHash(path), path)...).Scan(&driveInodeID); serr != nil && !errors.Is(serr, sql.ErrNoRows) {
			return nil, 0, serr
		}
		if _, err := tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			s.scope.Args(fileNodePathHash(path), path)...); err != nil {
			return nil, 0, err
		}
	}
	attr.Nlink--
	now := time.Now().UnixNano()
	ctime, csec := jfsSplitTime(now)
	if attr.Nlink > 0 {
		_, err = tx.Exec(`UPDATE jfs_node SET nlink = ?, ctime = ?, ctimensec = ? WHERE inode = ?`, attr.Nlink, ctime, csec, ino)
		return attr, 0, err
	}
	// The drive9 entity goes now, on both branches. The projection row is
	// already deleted and this was the last name, so leaving the inode
	// CONFIRMED would hold its size_bytes against the tenant quota for ever
	// (jfsTruncateTx keeps that column exact for extent files and
	// ConfirmedStorageBytesTx sums it); the classic path likewise marks the
	// entity deleted at unlink time even when a handle is still open, and only
	// defers the *bytes*.
	if driveInodeID.Valid && driveInodeID.String != "" {
		if err := s.markFilesDeletedTx(ctx, tx, []string{driveInodeID.String}); err != nil {
			return nil, 0, err
		}
		if err := s.deleteFileTagsByIDsTx(ctx, tx, []string{driveInodeID.String}); err != nil {
			return nil, 0, err
		}
	}
	if opened {
		// The jfs node and its blocks stay past this transaction: jfs_sustained
		// is the record jfsDeleteSustainedTx acts on, and that op only needs
		// jfs_node/jfs_chunk, so the drive9 cleanup above is independent of it.
		// The row goes when the owning session ends (clean unmount, or the
		// stale-session sweep after a crash): the fork's client only sends
		// delete_sustained on fd close for inodes it recorded in removedFiles,
		// which drive9's engine never does. That still satisfies POSIX — the data
		// outlives every handle.
		if _, err := tx.Exec(`INSERT IGNORE INTO jfs_sustained (sid, inode) VALUES (?, ?)`, sid, ino); err != nil {
			return nil, 0, err
		}
		_, err = tx.Exec(`UPDATE jfs_node SET nlink = 0, ctime = ?, ctimensec = ? WHERE inode = ?`, ctime, csec, ino)
		return attr, 0, err
	}
	if err := s.jfsEnqueueFileGCTx(tx, ino, attr.Length); err != nil {
		return nil, 0, err
	}
	if attr.Typ == jfsTypeSymlink {
		if _, err := tx.Exec(`DELETE FROM jfs_symlink WHERE inode = ?`, ino); err != nil {
			return nil, 0, err
		}
	}
	if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
		return nil, 0, err
	}
	return attr, 0, nil
}
func (s *Store) jfsReadlinkTx(db execer, ino uint64) ([]byte, int, error) {
	var target []byte
	err := db.QueryRow(`SELECT target FROM jfs_symlink WHERE inode = ?`, ino).Scan(&target)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	return target, 0, nil
}
func (s *Store) jfsProjectionPathTx(tx *sql.Tx, parent uint64, name string) (string, error) {
	if parent == jfsRootIno {
		return "/" + name, nil
	}
	var p sql.NullString
	err := tx.QueryRow(`SELECT path FROM file_nodes WHERE extent_ino = ? AND is_directory = 1 LIMIT 1`, parent).Scan(&p)
	if err != nil || !p.Valid {
		return "", err
	}
	return strings.TrimSuffix(p.String, "/") + "/" + name, nil
}
func (s *Store) jfsRmdirTx(tx *sql.Tx, parent uint64, name, projPath string) (uint64, *ExtentAttr, int, error) {
	var ino uint64
	var typ uint8
	// Locking read, matching jfsUnlinkTx: one dentry can be addressed by two
	// concurrent removals, and the loser must see that it is already gone.
	err := tx.QueryRow(`SELECT inode, type FROM jfs_edge WHERE parent = ? AND name = ? FOR UPDATE`,
		parent, []byte(name)).Scan(&ino, &typ)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, nil, 0, err
	}
	if typ != jfsTypeDir {
		return 0, nil, int(syscall.ENOTDIR), nil
	}
	// Lock the directory's node before counting, and count with a locking read.
	// A create takes the same node lock (jfsRequireDirTx) before inserting its
	// edge, so this is what makes "no children" true up to the delete below;
	// the locking count is a current read, so an edge committed while we waited
	// for the lock is seen (a plain COUNT would read this transaction's
	// snapshot and miss it).
	if eno, derr := s.jfsRequireDirTx(tx, ino); derr != nil || eno != 0 {
		return 0, nil, eno, derr
	}
	var children int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = ? FOR UPDATE`, ino).Scan(&children); err != nil {
		return 0, nil, 0, err
	}
	if children > 0 {
		return 0, nil, int(syscall.ENOTEMPTY), nil
	}
	attr, _, err := s.jfsGetAttrTx(tx, ino)
	if err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)); err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
		return 0, nil, 0, err
	}
	return ino, attr, 0, nil
}

// jfsRenameTx renames one dentry, replacing whatever sits on the destination.
//
// dstOpened and sid come from the caller that knows the destination's handles: a
// replaced file that a mount still has open keeps its node and blocks — POSIX
// requires the data to survive the replacement — exactly as an unlink of an open
// file does, instead of being reclaimed together with the rename. A replace
// issued without handle knowledge reclaims immediately, which is the classic (and
// cross-mount) behaviour: the HTTP/CLI rename paths never reach this function at
// all — Store.RenameFileReplacingTarget removes a replaced extent destination with
// its own jfsUnlinkTx(..., opened = false).
func (s *Store) jfsRenameTx(ctx context.Context, tx *sql.Tx, srcParent uint64, srcName string, dstParent uint64, dstName, srcPath, dstPath string, flags uint32, dstOpened bool, sid uint64) (uint64, uint64, *ExtentAttr, *ExtentAttr, int, error) {
	ino, attr, eno, err := s.jfsLookupForUpdateTx(tx, srcParent, srcName)
	if err != nil || eno != 0 {
		return 0, 0, nil, nil, eno, err
	}
	if flags&jfsRenameExchange != 0 {
		return 0, 0, attr, nil, int(syscall.ENOTSUP), nil
	}
	// The destination parent must be a directory whether or not anything is
	// already sitting on the destination name: the edge insert below would
	// otherwise hang a child off a file. Upstream doRename checks the same way.
	if deno, derr := s.jfsRequireDirTx(tx, dstParent); derr != nil || deno != 0 {
		return 0, 0, attr, nil, deno, derr
	}
	// Locking read: a concurrent unlink of the destination must be observed as
	// "nothing to replace" (which POSIX and upstream doRename also do) rather
	// than racing this transaction into a spurious ENOENT.
	var exist uint64
	err = tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ? FOR UPDATE`,
		dstParent, []byte(dstName)).Scan(&exist)
	if err == nil {
		if exist == ino {
			return ino, exist, attr, attr, 0, nil
		}
		if flags&jfsRenameNoReplace != 0 {
			return 0, exist, attr, nil, int(syscall.EEXIST), nil
		}
		dstAttr, denos, derr := s.jfsGetAttrTx(tx, exist)
		if derr != nil || denos != 0 {
			return 0, exist, attr, dstAttr, denos, derr
		}
		if attr.Typ == jfsTypeDir && dstAttr.Typ != jfsTypeDir {
			return 0, exist, attr, dstAttr, int(syscall.ENOTDIR), nil
		}
		if attr.Typ != jfsTypeDir && dstAttr.Typ == jfsTypeDir {
			return 0, exist, attr, dstAttr, int(syscall.EISDIR), nil
		}
		// A directory destination is removed by jfsRmdirTx below, which locks
		// the directory's node and counts its children under that lock. Do not
		// pre-check emptiness with a plain COUNT here: a snapshot read decides
		// nothing a locked read does not decide better, and a create landing
		// between the two would look like an empty directory until the locked
		// count saved it. One authority for "may this directory go".
		if _, ueno, uerr := s.jfsUnlinkTx(ctx, tx, dstParent, dstName, dstPath, sid, dstOpened); uerr != nil || ueno != 0 {
			switch {
			case ueno == int(syscall.ENOENT):
				// The destination disappeared between the lookup above and here
				// (another client's unlink): there is nothing to replace, so the
				// rename proceeds.
			case dstAttr.Typ == jfsTypeDir && ueno == int(syscall.EPERM):
				if _, _, reno, rerr := s.jfsRmdirTx(tx, dstParent, dstName, dstPath); rerr != nil || reno != 0 {
					return 0, exist, attr, dstAttr, reno, rerr
				}
			default:
				return 0, exist, attr, dstAttr, ueno, uerr
			}
		}
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil, nil, 0, err
	}
	res, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, srcParent, []byte(srcName))
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	// The source edge is locked above, so this can only be zero if the caller
	// raced a state change this transaction cannot see; report ENOENT rather
	// than inserting a destination edge for a name that no longer exists.
	if n, aerr := res.RowsAffected(); aerr == nil && n == 0 {
		return 0, 0, nil, nil, int(syscall.ENOENT), nil
	}
	// No blind DELETE of whatever now sits on the destination name. Every
	// destination edge this transaction knows about was already removed above
	// (jfsUnlinkTx for a file, jfsRmdirTx for an empty directory), so a DELETE
	// here could only match an edge another client committed after this
	// transaction's locked destination read — TiDB has no gap locks, so the
	// read cannot reserve the name. Deleting that edge would silently discard
	// the other client's create (both callers told "success") and leak its
	// node. Let the unique key catch it instead: the INSERT below fails with a
	// duplicate, RunExtentMetaOp's retry loop re-enters, and this time the
	// locked destination read sees the new edge and replaces it through
	// jfsUnlinkTx with full cleanup.
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		dstParent, []byte(dstName), ino, attr.Typ); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	// Same rule as jfsMoveEdgeTx: a hardlinked inode keeps the parent of an edge
	// that is still present, so only move it when this was its last edge under
	// the source directory. One invariant, one statement.
	if _, err := tx.Exec(`UPDATE jfs_node SET parent = ? WHERE inode = ? AND NOT EXISTS (
		SELECT 1 FROM jfs_edge WHERE inode = ? AND parent = ?)`, dstParent, ino, ino, srcParent); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if dstPath != "" {
		if err := s.jfsMoveProjectionTx(tx, ino, srcPath, srcName, dstPath); err != nil {
			return 0, 0, nil, nil, 0, err
		}
	}
	return ino, 0, attr, nil, 0, nil
}
func (s *Store) jfsMoveProjectionTx(tx *sql.Tx, extentIno uint64, srcPath, srcName, dstPath string) error {
	dstParent := pathutil.ParentPath(dstPath)
	dstName := pathutil.BaseName(dstPath)
	setArgs := []any{dstPath, fileNodePathHash(dstPath), dstParent, fileNodePathHash(dstParent), dstName}
	try := func(where string, whereArgs []any) (int64, error) {
		q := `UPDATE file_nodes SET path = ?, path_hash = ?, parent_path = ?, parent_path_hash = ?, name = ? WHERE ` + s.scope.And(where)
		args := append(append([]any{}, setArgs...), s.scope.Args(whereArgs...)...)
		res, err := tx.Exec(q, args...)
		if isUniqueViolation(err) {
			// An extent file renamed over a single-layout file: the target has
			// no jfs edge, so jfsRenameTx did not unlink it. Drop its dentry
			// and perform the normal last-reference deletion, or the replaced
			// inode stays CONFIRMED with its tags and its object is never
			// reclaimed — repeated mixed-layout replacements would leak
			// storage and permanently consume quota.
			var dstFileID sql.NullString
			var dstExtentIno sql.NullInt64
			_ = tx.QueryRow(`SELECT COALESCE(file_id, inode_id), extent_ino FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`)+` FOR UPDATE`,
				s.scope.Args(fileNodePathHash(dstPath), dstPath)...).Scan(&dstFileID, &dstExtentIno)
			if _, derr := tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
				s.scope.Args(fileNodePathHash(dstPath), dstPath)...); derr != nil {
				return 0, derr
			}
			if dstFileID.Valid && dstFileID.String != "" {
				if _, derr := s.reclaimReplacedTargetTx(tx, dstFileID.String); derr != nil {
					return 0, derr
				}
			}
			// reclaimReplacedTargetTx intentionally enqueues no drive9 file GC
			// for an extent target (the jfs block drain owns those bytes), so
			// the jfs side is recorded here — the replaced row was this inode's
			// last name and no edge is left to find it by.
			if dstExtentIno.Valid && dstExtentIno.Int64 != 0 {
				if derr := s.jfsReclaimOrphanedInoTx(tx, uint64(dstExtentIno.Int64)); derr != nil {
					return 0, derr
				}
			}
			res, err = tx.Exec(q, args...)
		}
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		return n, nil
	}
	if srcPath != "" {
		n, err := try(`path_hash = ? AND path = ?`, []any{fileNodePathHash(srcPath), srcPath})
		if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
	if extentIno == 0 || srcName == "" {
		return nil
	}
	_, err := try(`extent_ino = ? AND name = ?`, []any{extentIno, srcName})
	return err
}

// jfsReaddirTx lists a directory. The attributes come from the same statement
// as the names: reading them one child at a time cost a round trip per entry,
// and readdir is what `ls -l` and every readdirplus walk issue, so it was the
// most expensive op in the data plane. It also dropped the per-child errno, so
// an edge whose node row had gone missing produced a dirent with no attributes
// instead of being skipped.
func (s *Store) jfsReaddirTx(db execer, ino uint64, limit int) ([]map[string]any, error) {
	q := `SELECT e.name, e.inode, n.type, n.mode, n.uid, n.gid,
			n.atime, n.mtime, n.ctime, n.atimensec, n.mtimensec, n.ctimensec,
			n.nlink, n.length, n.parent, n.rdev
		FROM jfs_edge e
		INNER JOIN jfs_node n ON n.inode = e.inode
		WHERE e.parent = ? ORDER BY e.name`
	args := []any{ino}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]any
	for rows.Next() {
		var name []byte
		var child uint64
		var typ uint8
		var mode uint16
		var uid, gid uint32
		var atime, mtime, ctime int64
		var asec, msec, csec int16
		var nlink uint32
		var length, parent uint64
		var rdev uint32
		if err := rows.Scan(&name, &child, &typ, &mode, &uid, &gid,
			&atime, &mtime, &ctime, &asec, &msec, &csec,
			&nlink, &length, &parent, &rdev); err != nil {
			return nil, err
		}
		attr := jfsAttrFromNode(typ, mode, uid, gid, atime, mtime, ctime, asec, msec, csec, nlink, length, parent, rdev)
		out = append(out, map[string]any{"Inode": child, "Name": name, "Attr": &attr})
	}
	return out, rows.Err()
}

// jfsEdgeOfTx returns the (parent, name) of the edge pointing at ino.
func (s *Store) jfsEdgeOfTx(tx *sql.Tx, ino uint64) (uint64, []byte, bool, error) {
	var parent uint64
	var name []byte
	err := tx.QueryRow(`SELECT parent, name FROM jfs_edge WHERE inode = ? LIMIT 1`, ino).Scan(&parent, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	return parent, name, true, nil
}

// jfsMoveEdgeTx moves one edge, identified by its dentry, to newParent/newName.
//
// The edge is addressed by (edgeParent, edgeName) rather than by inode: an
// extent inode can have several edges (hardlinks, created by jfsLinkEdgeTx), and
// an inode-keyed UPDATE would try to put them all on the same dentry, which
// both violates uk_jfs_edge(parent, name) and destroys the other aliases.
//
// `jfs_node.parent` is updated too. JuiceFS keeps it as the node's parent
// directory and the extent unlink resolves the edge from it, so leaving it at
// the old directory made every later delete of the moved file fail ENOENT: the
// projection row was gone before the lookup, and the transaction rolled back.
func (s *Store) jfsMoveEdgeTx(tx *sql.Tx, edgeParent uint64, edgeName string, ino, newParent uint64, newName string) error {
	if ino == 0 || ino == jfsRootIno || edgeName == "" || newName == "" {
		return nil
	}
	// Drop any edge already sitting on the destination name (replacing it),
	// then move this one.
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, newParent, []byte(newName)); err != nil {
		return err
	}
	res, err := tx.Exec(`UPDATE jfs_edge SET parent = ?, name = ? WHERE parent = ? AND name = ?`,
		newParent, []byte(newName), edgeParent, []byte(edgeName))
	if err != nil {
		return err
	}
	if n, aerr := res.RowsAffected(); aerr == nil && n == 0 {
		// The edge was not where the caller said it was. Fall back to the
		// inode, which is exact when the inode has a single edge (every
		// directory, and any file the caller did not reach by dentry).
		var edges int
		// FOR UPDATE: the count decides whether the inode-addressed UPDATE below
		// is exact, so it must be a current read. A plain COUNT would answer
		// from this transaction's snapshot and could still report one edge for
		// an inode that has just been given a second alias.
		if cerr := tx.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE inode = ? FOR UPDATE`, ino).Scan(&edges); cerr != nil {
			return cerr
		}
		if edges == 1 {
			if _, uerr := tx.Exec(`UPDATE jfs_edge SET parent = ?, name = ? WHERE inode = ?`, newParent, []byte(newName), ino); uerr != nil {
				return uerr
			}
		} else if edges > 1 {
			return fmt.Errorf("jfs rename: no edge %q under parent %d for inode %d with %d aliases", edgeName, edgeParent, ino, edges)
		}
	}
	// The node's parent must follow the edge. A hardlinked alias keeps the
	// parent of the edge that is still there, so only move it when this was the
	// inode's last edge on the old parent.
	_, err = tx.Exec(`UPDATE jfs_node SET parent = ? WHERE inode = ? AND NOT EXISTS (
		SELECT 1 FROM jfs_edge WHERE inode = ? AND parent = ?)`, newParent, ino, ino, edgeParent)
	return err
}

// jfsRenameDirEdgeTx moves a directory's edge. A directory always has exactly
// one edge, so the inode identifies it.
func (s *Store) jfsRenameDirEdgeTx(tx *sql.Tx, ino, newParent uint64, newName string) error {
	if ino == 0 || ino == jfsRootIno || newName == "" {
		return nil
	}
	parent, name, ok, err := s.jfsEdgeOfTx(tx, ino)
	if err != nil || !ok {
		return err
	}
	return s.jfsMoveEdgeTx(tx, parent, string(name), ino, newParent, newName)
}
func (s *Store) unlinkExtentPathTx(ctx context.Context, tx *sql.Tx, path string, opened bool) error {
	var ino sql.NullInt64
	var layout sql.NullString
	// FOR UPDATE: the node-gone branch below deletes this row, and a rename that
	// claimed the path in the meantime has its own projection row on it. Reading
	// it under the lock (and re-checking the condition that made it an orphan)
	// keeps that DELETE from landing on the rename's fresh row; same rule as
	// jfsUnlinkTx's orphan branch.
	err := tx.QueryRow(`SELECT content_layout, extent_ino FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ?`)+` FOR UPDATE`,
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&layout, &ino)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if layout.String != string(ContentLayoutExtent) || !ino.Valid || ino.Int64 == 0 {
		return ErrNotFound
	}
	attr, eno, err := s.jfsGetAttrTx(tx, uint64(ino.Int64))
	if err != nil {
		return err
	}
	if eno != 0 || attr == nil {
		// The DELETE names the inode the locked read saw, not just the path: a
		// rename that moved its own row onto this path while this transaction
		// waited for the lock would otherwise have that row deleted under it.
		// Matching extent_ino is exact here — this inode's jfs node is gone, so
		// no rename can be moving a row for it.
		_, err = tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ? AND extent_ino = ?`),
			append(s.scope.Args(fileNodePathHash(path), path), uint64(ino.Int64))...)
		return err
	}
	// Resolve the edge from the path, not from attr.Parent: an extent inode can
	// have several edges (hardlinks) and jfs_node.parent names only one of them,
	// so a delete addressed by an alias path looked for the edge under the wrong
	// directory and came back ENOENT — the file stayed listed and readable but
	// could not be removed through any non-FUSE path.
	parentIno, perr := s.jfsParentInoForPathTx(tx, path)
	if perr != nil {
		return perr
	}
	name := pathutil.BaseName(path)
	_, eno, err = s.jfsUnlinkTx(ctx, tx, parentIno, name, path, 0, opened)
	if err != nil {
		return err
	}
	if eno == int(syscall.ENOENT) {
		return ErrNotFound
	}
	if eno != 0 {
		return fmt.Errorf("extent unlink: %w", syscall.Errno(eno))
	}
	return nil
}
