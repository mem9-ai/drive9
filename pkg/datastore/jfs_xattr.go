package datastore

import (
	"database/sql"
	"errors"
	"strings"
	"syscall"
)

const (
	jfsXattrCreate   = uint32(1)
	jfsXattrReplace  = uint32(2)
	jfsXattrMaxName  = 255
	jfsXattrMaxValue = 64 << 10
)

func jfsValidateXattr(name string, value []byte, flags uint32) int {
	if name == "" || len(name) > jfsXattrMaxName || strings.IndexByte(name, 0) >= 0 || len(value) > jfsXattrMaxValue {
		return int(syscall.EINVAL)
	}
	if flags&^(jfsXattrCreate|jfsXattrReplace) != 0 || flags == (jfsXattrCreate|jfsXattrReplace) {
		return int(syscall.EINVAL)
	}
	return 0
}

func jfsXattrInodeExists(db execer, ino uint64, forUpdate bool) (bool, error) {
	query := `SELECT inode FROM jfs_node WHERE inode = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var got uint64
	err := db.QueryRow(query, ino).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func jfsGetXattrTx(db execer, ino uint64, name string) ([]byte, int, error) {
	if errno := jfsValidateXattr(name, nil, 0); errno != 0 {
		return nil, errno, nil
	}
	var value []byte
	err := db.QueryRow(`SELECT value FROM jfs_xattr WHERE inode = ? AND name = ?`, ino, []byte(name)).Scan(&value)
	if err == nil {
		return value, 0, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.EIO), err
	}
	exists, err := jfsXattrInodeExists(db, ino, false)
	if err != nil {
		return nil, int(syscall.EIO), err
	}
	if !exists {
		return nil, int(syscall.ENOENT), nil
	}
	return nil, int(syscall.ENODATA), nil
}

func jfsListXattrTx(db execer, ino uint64) ([]string, int, error) {
	exists, err := jfsXattrInodeExists(db, ino, false)
	if err != nil {
		return nil, int(syscall.EIO), err
	}
	if !exists {
		return nil, int(syscall.ENOENT), nil
	}
	rows, err := db.Query(`SELECT name FROM jfs_xattr WHERE inode = ? ORDER BY name`, ino)
	if err != nil {
		return nil, int(syscall.EIO), err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name []byte
		if err := rows.Scan(&name); err != nil {
			return nil, int(syscall.EIO), err
		}
		names = append(names, string(name))
	}
	if err := rows.Err(); err != nil {
		return nil, int(syscall.EIO), err
	}
	if names == nil {
		names = []string{}
	}
	return names, 0, nil
}

func jfsSetXattrTx(tx *sql.Tx, ino uint64, name string, value []byte, flags uint32) (int, error) {
	if errno := jfsValidateXattr(name, value, flags); errno != 0 {
		return errno, nil
	}
	exists, err := jfsXattrInodeExists(tx, ino, true)
	if err != nil {
		return int(syscall.EIO), err
	}
	if !exists {
		return int(syscall.ENOENT), nil
	}
	var current []byte
	err = tx.QueryRow(`SELECT value FROM jfs_xattr WHERE inode = ? AND name = ? FOR UPDATE`, ino, []byte(name)).Scan(&current)
	switch {
	case err == nil:
		if flags&jfsXattrCreate != 0 {
			return int(syscall.EEXIST), nil
		}
		if _, err := tx.Exec(`UPDATE jfs_xattr SET value = ? WHERE inode = ? AND name = ?`, value, ino, []byte(name)); err != nil {
			return int(syscall.EIO), err
		}
		return 0, nil
	case !errors.Is(err, sql.ErrNoRows):
		return int(syscall.EIO), err
	case flags&jfsXattrReplace != 0:
		return int(syscall.ENODATA), nil
	default:
		if _, err := tx.Exec(`INSERT INTO jfs_xattr (inode, name, value) VALUES (?, ?, ?)`, ino, []byte(name), value); err != nil {
			return int(syscall.EIO), err
		}
		return 0, nil
	}
}

func jfsRemoveXattrTx(tx *sql.Tx, ino uint64, name string) (int, error) {
	if errno := jfsValidateXattr(name, nil, 0); errno != 0 {
		return errno, nil
	}
	exists, err := jfsXattrInodeExists(tx, ino, true)
	if err != nil {
		return int(syscall.EIO), err
	}
	if !exists {
		return int(syscall.ENOENT), nil
	}
	result, err := tx.Exec(`DELETE FROM jfs_xattr WHERE inode = ? AND name = ?`, ino, []byte(name))
	if err != nil {
		return int(syscall.EIO), err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return int(syscall.EIO), err
	}
	if removed == 0 {
		return int(syscall.ENODATA), nil
	}
	return 0, nil
}

func jfsDeleteXattrsTx(tx *sql.Tx, ino uint64) error {
	_, err := tx.Exec(`DELETE FROM jfs_xattr WHERE inode = ?`, ino)
	return err
}
