package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/pathutil"
)

type FSLayerState string
type FSLayerDurabilityMode string
type FSLayerEntryOp string
type FSLayerEntryKind string

var ErrFSLayerRefAmbiguous = errors.New("fs layer ref is ambiguous")
var ErrFSLayerStateConflict = errors.New("fs layer state conflict")

const (
	FSLayerStateActive     FSLayerState = "active"
	FSLayerStateSealed     FSLayerState = "sealed"
	FSLayerStateCommitting FSLayerState = "committing"
	FSLayerStateCommitted  FSLayerState = "committed"
	FSLayerStateAbandoned  FSLayerState = "abandoned"
	FSLayerStateConflicted FSLayerState = "conflicted"

	FSLayerDurabilityRestoreSafe  FSLayerDurabilityMode = "restore-safe"
	FSLayerDurabilityWriteThrough FSLayerDurabilityMode = "write-through"
	FSLayerDurabilityLocalFast    FSLayerDurabilityMode = "local-fast"

	FSLayerEntryOpUpsert   FSLayerEntryOp = "upsert"
	FSLayerEntryOpWhiteout FSLayerEntryOp = "whiteout"
	FSLayerEntryOpMkdir    FSLayerEntryOp = "mkdir"
	FSLayerEntryOpSymlink  FSLayerEntryOp = "symlink"
	FSLayerEntryOpChmod    FSLayerEntryOp = "chmod"
	FSLayerEntryOpRename   FSLayerEntryOp = "rename"

	FSLayerEntryKindFile    FSLayerEntryKind = "file"
	FSLayerEntryKindDir     FSLayerEntryKind = "dir"
	FSLayerEntryKindSymlink FSLayerEntryKind = "symlink"
)

type FSLayer struct {
	LayerID        string
	BaseRootPath   string
	Name           string
	Tags           map[string]string
	State          FSLayerState
	DurabilityMode FSLayerDurabilityMode
	ActorID        string
	DurableSeq     int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	SealedAt       *time.Time
}

type FSLayerEntry struct {
	LayerID        string
	Path           string
	PathHash       string
	ParentPath     string
	ParentPathHash string
	Name           string
	Op             FSLayerEntryOp
	Kind           FSLayerEntryKind
	BaseInodeID    string
	BaseRevision   int64
	StorageType    string
	StorageRef     string
	StorageRefHash string
	ContentBlob    []byte
	ContentType    string
	ContentText    string
	ChecksumSHA256 string
	SizeBytes      int64
	Mode           uint32
	EntrySeq       int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type FSLayerCheckpoint struct {
	CheckpointID string
	LayerID      string
	DurableSeq   int64
	Label        string
	CreatedAt    time.Time
}

func (s *Store) CreateFSLayer(ctx context.Context, layer *FSLayer) error {
	if layer == nil {
		return fmt.Errorf("fs layer is required")
	}
	layer.LayerID = strings.TrimSpace(layer.LayerID)
	if layer.LayerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	root, err := pathutil.CanonicalizeDir(layer.BaseRootPath)
	if err != nil {
		return fmt.Errorf("invalid fs layer base root: %w", err)
	}
	layer.BaseRootPath = root
	layer.Name = strings.TrimSpace(layer.Name)
	layer.ActorID = strings.TrimSpace(layer.ActorID)
	if layer.State == "" {
		layer.State = FSLayerStateActive
	}
	if err := validateFSLayerState(layer.State); err != nil {
		return err
	}
	if layer.DurabilityMode == "" {
		layer.DurabilityMode = FSLayerDurabilityRestoreSafe
	}
	if err := validateFSLayerDurability(layer.DurabilityMode); err != nil {
		return err
	}
	tags, err := normalizeFSLayerTags(layer.Tags)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create fs layer transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
INSERT INTO fs_layers (
	layer_id, base_root_path, name, state, durability_mode, actor_id, durable_seq, created_at, updated_at, sealed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), ?)`,
		layer.LayerID, layer.BaseRootPath, layer.Name, string(layer.State), string(layer.DurabilityMode), layer.ActorID,
		layer.DurableSeq, nilTime(layer.SealedAt))
	if err != nil {
		return fmt.Errorf("create fs layer %s: %w", layer.LayerID, err)
	}
	if err := s.replaceFSLayerTagsTx(ctx, tx, layer.LayerID, tags); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create fs layer %s: %w", layer.LayerID, err)
	}
	layer.Tags = tags
	return nil
}

func (s *Store) GetFSLayer(ctx context.Context, layerID string) (*FSLayer, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT layer_id, base_root_path, name, state, durability_mode, actor_id, durable_seq, created_at, updated_at, sealed_at
	FROM fs_layers
	WHERE layer_id = ?`, layerID)
	layer, err := scanFSLayer(row)
	if err != nil {
		return nil, err
	}
	if err := s.loadFSLayerTags(ctx, layer); err != nil {
		return nil, err
	}
	return layer, nil
}

func (s *Store) ListFSLayers(ctx context.Context) ([]FSLayer, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT layer_id, base_root_path, name, state, durability_mode, actor_id, durable_seq, created_at, updated_at, sealed_at
FROM fs_layers
ORDER BY updated_at DESC, layer_id`)
	if err != nil {
		return nil, fmt.Errorf("list fs layers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []FSLayer
	for rows.Next() {
		layer, err := scanFSLayer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan fs layer: %w", err)
		}
		if err := s.loadFSLayerTags(ctx, layer); err != nil {
			return nil, err
		}
		out = append(out, *layer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list fs layers: %w", err)
	}
	return out, nil
}

func (s *Store) ResolveFSLayerRef(ctx context.Context, ref string) (*FSLayer, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("fs layer ref is required")
	}
	layer, err := s.GetFSLayer(ctx, ref)
	if err == nil {
		return layer, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if !strings.HasPrefix(ref, "tag:") {
		layers, err := s.listFSLayersByName(ctx, ref)
		if err != nil {
			return nil, err
		}
		if len(layers) == 1 {
			return &layers[0], nil
		}
		if len(layers) > 1 {
			return nil, fmt.Errorf("%w: name %q matched %d layers", ErrFSLayerRefAmbiguous, ref, len(layers))
		}
	}
	key, value, hasValue, ok := parseFSLayerTagRef(ref)
	if !ok {
		return nil, ErrNotFound
	}
	layers, err := s.listFSLayersByTag(ctx, key, value, hasValue)
	if err != nil {
		return nil, err
	}
	if len(layers) == 0 {
		return nil, ErrNotFound
	}
	if len(layers) > 1 {
		return nil, fmt.Errorf("%w: tag %q matched %d layers", ErrFSLayerRefAmbiguous, ref, len(layers))
	}
	return &layers[0], nil
}

func (s *Store) SetFSLayerTags(ctx context.Context, layerID string, tags map[string]string) error {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	normalized, err := normalizeFSLayerTags(tags)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin set fs layer tags transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existing string
	if err := tx.QueryRowContext(ctx, `SELECT layer_id FROM fs_layers WHERE layer_id = ?`, layerID).Scan(&existing); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read fs layer %s: %w", layerID, err)
	}
	if err := s.replaceFSLayerTagsTx(ctx, tx, layerID, normalized); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit set fs layer tags %s: %w", layerID, err)
	}
	return nil
}

func (s *Store) GetFSLayerTags(ctx context.Context, layerID string) (map[string]string, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT tag_key, tag_value
FROM fs_layer_tags
WHERE layer_id = ?
ORDER BY tag_key`, layerID)
	if err != nil {
		return nil, fmt.Errorf("list fs layer tags: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("scan fs layer tag: %w", err)
		}
		out[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list fs layer tags: %w", err)
	}
	return out, nil
}

func (s *Store) SetFSLayerState(ctx context.Context, layerID string, state FSLayerState) error {
	if strings.TrimSpace(layerID) == "" {
		return fmt.Errorf("fs layer id is required")
	}
	if err := validateFSLayerState(state); err != nil {
		return err
	}
	query := `UPDATE fs_layers SET state = ?, updated_at = UTC_TIMESTAMP(3)`
	args := []any{string(state)}
	if state == FSLayerStateSealed || state == FSLayerStateCommitting || state == FSLayerStateCommitted || state == FSLayerStateAbandoned || state == FSLayerStateConflicted {
		query += `, sealed_at = COALESCE(sealed_at, UTC_TIMESTAMP(3))`
	}
	query += ` WHERE layer_id = ?`
	args = append(args, layerID)
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("set fs layer state %s: %w", layerID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		if _, err := s.GetFSLayer(ctx, layerID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) BeginFSLayerCommit(ctx context.Context, layerID string) error {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE fs_layers
SET state = ?, sealed_at = COALESCE(sealed_at, UTC_TIMESTAMP(3)), updated_at = UTC_TIMESTAMP(3)
WHERE layer_id = ? AND state IN (?, ?)`,
		string(FSLayerStateCommitting), layerID, string(FSLayerStateActive), string(FSLayerStateSealed))
	if err != nil {
		return fmt.Errorf("begin fs layer commit %s: %w", layerID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	if _, err := s.GetFSLayer(ctx, layerID); err != nil {
		return err
	}
	return ErrFSLayerStateConflict
}

func (s *Store) RollbackFSLayer(ctx context.Context, layerID string) error {
	return s.SetFSLayerState(ctx, layerID, FSLayerStateAbandoned)
}

func (s *Store) UpsertFSLayerEntry(ctx context.Context, entry *FSLayerEntry) error {
	if entry == nil {
		return fmt.Errorf("fs layer entry is required")
	}
	if err := normalizeFSLayerEntry(entry); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert fs layer entry transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var baseRoot string
	if err := tx.QueryRowContext(ctx, `SELECT base_root_path FROM fs_layers WHERE layer_id = ? FOR UPDATE`, entry.LayerID).Scan(&baseRoot); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read fs layer %s: %w", entry.LayerID, err)
	}
	if err := validateFSLayerEntryWithinBaseRoot(entry, baseRoot); err != nil {
		return err
	}
	if entry.EntrySeq <= 0 {
		var seq sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(entry_seq) FROM fs_layer_entries WHERE layer_id = ?`, entry.LayerID).Scan(&seq); err != nil {
			return fmt.Errorf("read fs layer max seq: %w", err)
		}
		entry.EntrySeq = 1
		if seq.Valid {
			entry.EntrySeq = seq.Int64 + 1
		}
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fs_layer_entries (
	layer_id, path, path_hash, parent_path, parent_path_hash, name, op, kind, base_inode_id, base_revision,
	storage_type, storage_ref, storage_ref_hash, content_blob, content_type, content_text, checksum_sha256,
	size_bytes, mode, entry_seq, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
		entry.LayerID, entry.Path, entry.PathHash, entry.ParentPath, entry.ParentPathHash, entry.Name,
		string(entry.Op), string(entry.Kind), entry.BaseInodeID, entry.BaseRevision, entry.StorageType,
		entry.StorageRef, entry.StorageRefHash, nilBytes(entry.ContentBlob), nullStr(entry.ContentType),
		nullStr(entry.ContentText), entry.ChecksumSHA256, entry.SizeBytes, entry.Mode, entry.EntrySeq)
	if err != nil {
		return fmt.Errorf("upsert fs layer entry %s:%s: %w", entry.LayerID, entry.Path, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert fs layer entry %s:%s: %w", entry.LayerID, entry.Path, err)
	}
	return nil
}

func (s *Store) GetFSLayerEntry(ctx context.Context, layerID, path string) (*FSLayerEntry, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	canonical, err := canonicalFSLayerLookupPath(path)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT layer_id, path, path_hash, parent_path, parent_path_hash, name, op, kind, base_inode_id, base_revision,
	storage_type, storage_ref, storage_ref_hash, content_blob, content_type, content_text, checksum_sha256,
	size_bytes, mode, entry_seq, created_at, updated_at
FROM fs_layer_entries
WHERE layer_id = ? AND path_hash = ? AND path = ?
ORDER BY entry_seq DESC
LIMIT 1`, layerID, fsLayerPathHash(canonical), canonical)
	return scanFSLayerEntry(row)
}

func (s *Store) GetFSLayerEntryAtSeq(ctx context.Context, layerID, path string, maxSeq int64) (*FSLayerEntry, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	if maxSeq < 0 {
		return nil, fmt.Errorf("fs layer max seq must be non-negative")
	}
	canonical, err := canonicalFSLayerLookupPath(path)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT layer_id, path, path_hash, parent_path, parent_path_hash, name, op, kind, base_inode_id, base_revision,
	storage_type, storage_ref, storage_ref_hash, content_blob, content_type, content_text, checksum_sha256,
	size_bytes, mode, entry_seq, created_at, updated_at
FROM fs_layer_entries
WHERE layer_id = ? AND path_hash = ? AND path = ? AND entry_seq <= ?
ORDER BY entry_seq DESC
LIMIT 1`, layerID, fsLayerPathHash(canonical), canonical, maxSeq)
	return scanFSLayerEntry(row)
}

func (s *Store) ListFSLayerEntries(ctx context.Context, layerID string) ([]FSLayerEntry, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	return s.listFSLayerEntriesLatest(ctx, layerID, nil)
}

func (s *Store) ListFSLayerEntriesAtSeq(ctx context.Context, layerID string, maxSeq int64) ([]FSLayerEntry, error) {
	if maxSeq < 0 {
		return nil, fmt.Errorf("fs layer max seq must be non-negative")
	}
	return s.listFSLayerEntriesLatest(ctx, layerID, &maxSeq)
}

func (s *Store) listFSLayerEntriesLatest(ctx context.Context, layerID string, maxSeq *int64) ([]FSLayerEntry, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	where := "WHERE layer_id = ?"
	args := []any{layerID}
	if maxSeq != nil {
		where += " AND entry_seq <= ?"
		args = append(args, *maxSeq)
	}
	query := `
SELECT e.layer_id, e.path, e.path_hash, e.parent_path, e.parent_path_hash, e.name, e.op, e.kind, e.base_inode_id, e.base_revision,
	e.storage_type, e.storage_ref, e.storage_ref_hash, e.content_blob, e.content_type, e.content_text, e.checksum_sha256,
	e.size_bytes, e.mode, e.entry_seq, e.created_at, e.updated_at
FROM fs_layer_entries e
JOIN (
	SELECT path_hash, path, MAX(entry_seq) AS entry_seq
	FROM fs_layer_entries
	` + where + `
	GROUP BY path_hash, path
) latest ON latest.path_hash = e.path_hash AND latest.path = e.path AND latest.entry_seq = e.entry_seq
WHERE e.layer_id = ?
ORDER BY e.entry_seq, e.path`
	args = append(args, layerID)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list fs layer entries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []FSLayerEntry
	for rows.Next() {
		entry, err := scanFSLayerEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("scan fs layer entry: %w", err)
		}
		out = append(out, *entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list fs layer entries: %w", err)
	}
	return out, nil
}

func (s *Store) CreateFSLayerCheckpoint(ctx context.Context, checkpoint *FSLayerCheckpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("fs layer checkpoint is required")
	}
	checkpoint.CheckpointID = strings.TrimSpace(checkpoint.CheckpointID)
	checkpoint.LayerID = strings.TrimSpace(checkpoint.LayerID)
	if checkpoint.CheckpointID == "" {
		return fmt.Errorf("fs layer checkpoint id is required")
	}
	if checkpoint.LayerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fs layer checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existingLayerID string
	if err := tx.QueryRowContext(ctx, `SELECT layer_id FROM fs_layers WHERE layer_id = ? FOR UPDATE`, checkpoint.LayerID).Scan(&existingLayerID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read fs layer %s: %w", checkpoint.LayerID, err)
	}
	var seq sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(entry_seq) FROM fs_layer_entries WHERE layer_id = ?`, checkpoint.LayerID).Scan(&seq); err != nil {
		return fmt.Errorf("read fs layer max seq: %w", err)
	}
	if seq.Valid {
		checkpoint.DurableSeq = seq.Int64
	} else {
		checkpoint.DurableSeq = 0
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fs_layer_checkpoints (checkpoint_id, layer_id, durable_seq, label, created_at)
VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3))`,
		checkpoint.CheckpointID, checkpoint.LayerID, checkpoint.DurableSeq, checkpoint.Label)
	if err != nil {
		return fmt.Errorf("create fs layer checkpoint %s: %w", checkpoint.CheckpointID, err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fs_layers SET durable_seq = GREATEST(durable_seq, ?), updated_at = UTC_TIMESTAMP(3)
WHERE layer_id = ?`, checkpoint.DurableSeq, checkpoint.LayerID); err != nil {
		return fmt.Errorf("update fs layer durable seq %s: %w", checkpoint.LayerID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fs layer checkpoint %s: %w", checkpoint.CheckpointID, err)
	}
	return nil
}

func (s *Store) GetFSLayerCheckpoint(ctx context.Context, checkpointID string) (*FSLayerCheckpoint, error) {
	checkpointID = strings.TrimSpace(checkpointID)
	if checkpointID == "" {
		return nil, fmt.Errorf("fs layer checkpoint id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT checkpoint_id, layer_id, durable_seq, label, created_at
FROM fs_layer_checkpoints
WHERE checkpoint_id = ?`, checkpointID)
	return scanFSLayerCheckpoint(row)
}

func scanFSLayer(row interface{ Scan(dest ...any) error }) (*FSLayer, error) {
	var layer FSLayer
	var state, durability string
	var sealedAt sql.NullTime
	if err := row.Scan(
		&layer.LayerID, &layer.BaseRootPath, &layer.Name, &state, &durability, &layer.ActorID, &layer.DurableSeq,
		&layer.CreatedAt, &layer.UpdatedAt, &sealedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	layer.State = FSLayerState(state)
	layer.DurabilityMode = FSLayerDurabilityMode(durability)
	layer.CreatedAt = layer.CreatedAt.UTC()
	layer.UpdatedAt = layer.UpdatedAt.UTC()
	if sealedAt.Valid {
		t := sealedAt.Time.UTC()
		layer.SealedAt = &t
	}
	return &layer, nil
}

func (s *Store) listFSLayersByName(ctx context.Context, name string) ([]FSLayer, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT layer_id, base_root_path, name, state, durability_mode, actor_id, durable_seq, created_at, updated_at, sealed_at
FROM fs_layers
WHERE name = ?
ORDER BY updated_at DESC, layer_id`, name)
	if err != nil {
		return nil, fmt.Errorf("list fs layers by name: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return s.scanFSLayersWithTags(ctx, rows, "list fs layers by name")
}

func (s *Store) listFSLayersByTag(ctx context.Context, key, value string, hasValue bool) ([]FSLayer, error) {
	query := `
SELECT l.layer_id, l.base_root_path, l.name, l.state, l.durability_mode, l.actor_id, l.durable_seq, l.created_at, l.updated_at, l.sealed_at
FROM fs_layers l
JOIN fs_layer_tags t ON t.layer_id = l.layer_id
WHERE t.tag_key = ?`
	args := []any{key}
	if hasValue {
		query += ` AND t.tag_value = ?`
		args = append(args, value)
	}
	query += `
ORDER BY l.updated_at DESC, l.layer_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list fs layers by tag: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return s.scanFSLayersWithTags(ctx, rows, "list fs layers by tag")
}

func (s *Store) scanFSLayersWithTags(ctx context.Context, rows *sql.Rows, label string) ([]FSLayer, error) {
	var out []FSLayer
	for rows.Next() {
		layer, err := scanFSLayer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan fs layer: %w", err)
		}
		if err := s.loadFSLayerTags(ctx, layer); err != nil {
			return nil, err
		}
		out = append(out, *layer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	return out, nil
}

func (s *Store) loadFSLayerTags(ctx context.Context, layer *FSLayer) error {
	if layer == nil {
		return nil
	}
	tags, err := s.GetFSLayerTags(ctx, layer.LayerID)
	if err != nil {
		return err
	}
	layer.Tags = tags
	return nil
}

func (s *Store) replaceFSLayerTagsTx(ctx context.Context, tx *sql.Tx, layerID string, tags map[string]string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fs_layer_tags WHERE layer_id = ?`, layerID); err != nil {
		return fmt.Errorf("delete fs layer tags %s: %w", layerID, err)
	}
	for key, value := range tags {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fs_layer_tags (layer_id, tag_key, tag_value, created_at)
VALUES (?, ?, ?, UTC_TIMESTAMP(3))`, layerID, key, value); err != nil {
			return fmt.Errorf("insert fs layer tag %s:%s: %w", layerID, key, err)
		}
	}
	return nil
}

func scanFSLayerEntry(row interface{ Scan(dest ...any) error }) (*FSLayerEntry, error) {
	var entry FSLayerEntry
	var op, kind string
	var contentBlob []byte
	var contentType, contentText sql.NullString
	var mode int64
	if err := row.Scan(
		&entry.LayerID, &entry.Path, &entry.PathHash, &entry.ParentPath, &entry.ParentPathHash,
		&entry.Name, &op, &kind, &entry.BaseInodeID, &entry.BaseRevision, &entry.StorageType,
		&entry.StorageRef, &entry.StorageRefHash, &contentBlob, &contentType, &contentText,
		&entry.ChecksumSHA256, &entry.SizeBytes, &mode, &entry.EntrySeq, &entry.CreatedAt, &entry.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	entry.Op = FSLayerEntryOp(op)
	entry.Kind = FSLayerEntryKind(kind)
	entry.ContentBlob = contentBlob
	entry.ContentType = contentType.String
	entry.ContentText = contentText.String
	entry.Mode = uint32(mode)
	entry.CreatedAt = entry.CreatedAt.UTC()
	entry.UpdatedAt = entry.UpdatedAt.UTC()
	return &entry, nil
}

func scanFSLayerCheckpoint(row interface{ Scan(dest ...any) error }) (*FSLayerCheckpoint, error) {
	var checkpoint FSLayerCheckpoint
	if err := row.Scan(
		&checkpoint.CheckpointID, &checkpoint.LayerID, &checkpoint.DurableSeq, &checkpoint.Label, &checkpoint.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	checkpoint.CreatedAt = checkpoint.CreatedAt.UTC()
	return &checkpoint, nil
}

func normalizeFSLayerEntry(entry *FSLayerEntry) error {
	entry.LayerID = strings.TrimSpace(entry.LayerID)
	if entry.LayerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	if entry.Op == "" {
		entry.Op = FSLayerEntryOpUpsert
	}
	if err := validateFSLayerEntryOp(entry.Op); err != nil {
		return err
	}
	if entry.Kind == "" {
		switch entry.Op {
		case FSLayerEntryOpMkdir:
			entry.Kind = FSLayerEntryKindDir
		case FSLayerEntryOpSymlink:
			entry.Kind = FSLayerEntryKindSymlink
		default:
			entry.Kind = FSLayerEntryKindFile
		}
	}
	if err := validateFSLayerEntryKind(entry.Kind); err != nil {
		return err
	}
	var (
		p   string
		err error
	)
	if entry.Kind == FSLayerEntryKindDir || entry.Op == FSLayerEntryOpMkdir {
		p, err = pathutil.CanonicalizeDir(entry.Path)
	} else {
		p, err = pathutil.Canonicalize(entry.Path)
	}
	if err != nil {
		return fmt.Errorf("invalid fs layer entry path: %w", err)
	}
	entry.Path = p
	entry.PathHash = fsLayerPathHash(p)
	entry.ParentPath = pathutil.ParentPath(p)
	entry.ParentPathHash = fsLayerPathHash(entry.ParentPath)
	entry.Name = pathutil.BaseName(p)
	if entry.Mode == 0 {
		entry.Mode = 0o644
		if entry.Kind == FSLayerEntryKindDir {
			entry.Mode = 0o755
		}
	}
	if entry.StorageRefHash == "" && entry.StorageRef != "" {
		entry.StorageRefHash = StorageRefHash(entry.StorageRef)
	}
	return nil
}

func validateFSLayerEntryWithinBaseRoot(entry *FSLayerEntry, baseRoot string) error {
	root, err := pathutil.CanonicalizeDir(baseRoot)
	if err != nil {
		return fmt.Errorf("invalid fs layer base root: %w", err)
	}
	if !fsLayerPathWithinBaseRoot(entry.Path, root) {
		return fmt.Errorf("fs layer entry path %q is outside base root %q", entry.Path, root)
	}
	if entry.Op != FSLayerEntryOpRename {
		return nil
	}
	target := strings.TrimSpace(entry.ContentText)
	if target == "" && len(entry.ContentBlob) > 0 {
		target = strings.TrimSpace(string(entry.ContentBlob))
	}
	if target == "" {
		return nil
	}
	targetPath, err := canonicalFSLayerLookupPath(target)
	if err != nil {
		return fmt.Errorf("invalid fs layer rename target: %w", err)
	}
	if !fsLayerPathWithinBaseRoot(targetPath, root) {
		return fmt.Errorf("fs layer rename target %q is outside base root %q", targetPath, root)
	}
	return nil
}

func fsLayerPathWithinBaseRoot(p, baseRoot string) bool {
	if baseRoot == "/" {
		return strings.HasPrefix(p, "/")
	}
	if p == baseRoot {
		return true
	}
	return strings.HasPrefix(p, baseRoot)
}

func validateFSLayerState(state FSLayerState) error {
	switch state {
	case FSLayerStateActive, FSLayerStateSealed, FSLayerStateCommitting, FSLayerStateCommitted, FSLayerStateAbandoned, FSLayerStateConflicted:
		return nil
	default:
		return fmt.Errorf("invalid fs layer state %q", state)
	}
}

func validateFSLayerDurability(mode FSLayerDurabilityMode) error {
	switch mode {
	case FSLayerDurabilityRestoreSafe, FSLayerDurabilityWriteThrough, FSLayerDurabilityLocalFast:
		return nil
	default:
		return fmt.Errorf("invalid fs layer durability mode %q", mode)
	}
}

func validateFSLayerEntryOp(op FSLayerEntryOp) error {
	switch op {
	case FSLayerEntryOpUpsert, FSLayerEntryOpWhiteout, FSLayerEntryOpMkdir, FSLayerEntryOpSymlink, FSLayerEntryOpChmod, FSLayerEntryOpRename:
		return nil
	default:
		return fmt.Errorf("invalid fs layer entry op %q", op)
	}
}

func validateFSLayerEntryKind(kind FSLayerEntryKind) error {
	switch kind {
	case FSLayerEntryKindFile, FSLayerEntryKindDir, FSLayerEntryKindSymlink:
		return nil
	default:
		return fmt.Errorf("invalid fs layer entry kind %q", kind)
	}
}

func normalizeFSLayerTags(tags map[string]string) (map[string]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(tags))
	for rawKey, rawValue := range tags {
		key := strings.TrimSpace(rawKey)
		value := strings.TrimSpace(rawValue)
		if key == "" {
			return nil, fmt.Errorf("fs layer tag key is required")
		}
		if len(key) > 255 {
			return nil, fmt.Errorf("fs layer tag key %q exceeds 255 bytes", key)
		}
		if len(value) > 255 {
			return nil, fmt.Errorf("fs layer tag %q value exceeds 255 bytes", key)
		}
		if strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("fs layer tag contains NUL")
		}
		if strings.Contains(key, "=") {
			return nil, fmt.Errorf("fs layer tag key %q must not contain '='", key)
		}
		out[key] = value
	}
	return out, nil
}

func parseFSLayerTagRef(ref string) (key string, value string, hasValue bool, ok bool) {
	ref = strings.TrimSpace(ref)
	hasTagPrefix := strings.HasPrefix(ref, "tag:")
	if strings.HasPrefix(ref, "tag:") {
		ref = strings.TrimSpace(strings.TrimPrefix(ref, "tag:"))
	}
	if ref == "" {
		return "", "", false, false
	}
	if k, v, found := strings.Cut(ref, "="); found {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			return "", "", false, false
		}
		return k, v, true, true
	}
	if strings.Contains(ref, " ") {
		return "", "", false, false
	}
	return ref, "", false, hasTagPrefix
}

func canonicalFSLayerLookupPath(p string) (string, error) {
	if strings.HasSuffix(p, "/") {
		canonical, err := pathutil.CanonicalizeDir(p)
		if err != nil {
			return "", fmt.Errorf("invalid fs layer entry path: %w", err)
		}
		return canonical, nil
	}
	canonical, err := pathutil.Canonicalize(p)
	if err != nil {
		return "", fmt.Errorf("invalid fs layer entry path: %w", err)
	}
	return canonical, nil
}

func fsLayerPathHash(path string) string {
	return StorageRefHash(path)
}
