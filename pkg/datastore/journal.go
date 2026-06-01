package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/journal"
)

type JournalWriter struct {
	Type   string
	ID     string
	Source string
}

func (s *Store) CreateJournal(ctx context.Context, tenantID string, req journal.CreateRequest) (*journal.Journal, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "create_journal", start, &opErr)

	if tenantID == "" {
		opErr = ErrJournalValidation
		return nil, fmt.Errorf("%w: tenant_id is required", ErrJournalValidation)
	}
	if req.JournalID == "" {
		req.JournalID = journal.NewID("jrn")
	}
	req, err := journal.NormalizeCreateRequest(req)
	if err != nil {
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	if err := journal.ValidateLabels(req.Labels); err != nil {
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	createHash, err := journal.CreateHash(tenantID, req)
	if err != nil {
		opErr = err
		return nil, err
	}
	now := journal.NormalizeTime(time.Now())
	genesisDoc := journal.GenesisDocument(tenantID, req, now)
	genesisRaw, err := journal.MarshalCanonical(genesisDoc)
	if err != nil {
		opErr = err
		return nil, err
	}
	genesisHash, err := journal.HashCanonical(genesisDoc)
	if err != nil {
		opErr = err
		return nil, err
	}
	metaRaw, err := json.Marshal(req.Meta)
	if err != nil {
		opErr = err
		return nil, err
	}

	var out *journal.Journal
	const maxCreateAttempts = 8
	for attempt := 0; attempt < maxCreateAttempts; attempt++ {
		out = nil
		err = s.InTx(ctx, func(tx *sql.Tx) error {
			existing, existingCreateHash, err := selectJournalTx(ctx, tx, tenantID, req.JournalID, true)
			if err == nil {
				if existingCreateHash != createHash {
					return ErrJournalConflict
				}
				out = existing
				return nil
			}
			if !errors.Is(err, ErrNotFound) {
				return err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO journals
			(tenant_id, journal_id, kind, title, actor_type, actor_id, source, meta, retention,
			 next_seq, genesis, create_hash, genesis_hash, head_hash, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)`,
				tenantID, req.JournalID, req.Kind, nullStr(req.Title), nullStr(req.Actor.Type), nullStr(req.Actor.ID),
				nullStr(req.Source), nullJSON(metaRaw), nullJSON(req.Retention), string(genesisRaw), createHash,
				genesisHash, genesisHash, now, now)
			if isUniqueViolation(err) {
				existing, existingCreateHash, readErr := selectJournalTx(ctx, tx, tenantID, req.JournalID, true)
				if readErr != nil {
					return ErrJournalConflict
				}
				if existingCreateHash != createHash {
					return ErrJournalConflict
				}
				out = existing
				return nil
			}
			if err != nil {
				return err
			}
			for _, label := range req.Labels {
				if _, err := tx.ExecContext(ctx, `INSERT INTO journal_labels
				(tenant_id, label_key, label_hash, label_value, journal_id, created_at, source_seq)
				VALUES (?, ?, ?, ?, ?, ?, NULL)`,
					tenantID, label.Key, journal.LabelHash(label.Key, label.Value), label.Value, req.JournalID, now); err != nil {
					return err
				}
			}
			out = &journal.Journal{
				TenantID:    tenantID,
				JournalID:   req.JournalID,
				Kind:        req.Kind,
				Title:       req.Title,
				Actor:       req.Actor,
				Source:      req.Source,
				Meta:        req.Meta,
				Labels:      req.Labels,
				Retention:   req.Retention,
				NextSeq:     1,
				GenesisHash: genesisHash,
				HeadHash:    genesisHash,
				CreatedAt:   now,
				UpdatedAt:   now,
			}
			return nil
		})
		if !isRetryableJournalCreateConflict(err) {
			break
		}
		time.Sleep(time.Duration(1<<attempt) * 5 * time.Millisecond)
	}
	if err != nil {
		opErr = err
		return nil, err
	}
	return out, nil
}

func (s *Store) AppendJournalEntries(ctx context.Context, tenantID, journalID, appendID string, writer JournalWriter, entries []journal.EntryInput) (*journal.AppendResponse, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "append_journal_entries", start, &opErr)

	if tenantID == "" || journalID == "" {
		opErr = ErrJournalValidation
		return nil, fmt.Errorf("%w: tenant_id and journal_id are required", ErrJournalValidation)
	}
	if err := journal.ValidateJournalID(journalID); err != nil {
		opErr = err
		return nil, fmt.Errorf("%w: %v", ErrJournalValidation, err)
	}
	if appendID == "" {
		opErr = ErrJournalValidation
		return nil, fmt.Errorf("%w: Idempotency-Key is required", ErrJournalValidation)
	}
	if err := journal.ValidateAppendID(appendID); err != nil {
		opErr = err
		return nil, fmt.Errorf("%w: %v", ErrJournalValidation, err)
	}
	if len(entries) == 0 {
		opErr = ErrJournalValidation
		return nil, fmt.Errorf("%w: at least one entry is required", ErrJournalValidation)
	}
	if len(entries) > journal.MaxEntriesPerBatch {
		err := fmt.Errorf("%w: too many entries: %d > %d", journal.ErrPayloadTooLarge, len(entries), journal.MaxEntriesPerBatch)
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	if writer.Type == "" {
		writer.Type = "api_key"
	}
	if writer.ID == "" {
		writer.ID = "unknown"
	}
	effectiveSource := strings.TrimSpace(writer.Source)
	if effectiveSource == "" {
		effectiveSource = journal.SourceSelf
	}
	if err := journal.ValidateSource(effectiveSource); err != nil {
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	normalized := make([]journal.EntryInput, 0, len(entries))
	for _, entry := range entries {
		n, err := journal.NormalizeEntryInput(entry, "", nil)
		if err != nil {
			opErr = err
			return nil, wrapJournalInputError(err)
		}
		if effectiveSource == journal.SourceSelf && n.Source != journal.SourceSelf {
			err := fmt.Errorf("%w: source %q requires a trusted writer", ErrJournalValidation, n.Source)
			opErr = err
			return nil, err
		}
		if n.Source != journal.SourceSelf && n.Source != effectiveSource {
			err := fmt.Errorf("%w: requested source %q does not match writer source %q", ErrJournalValidation, n.Source, effectiveSource)
			opErr = err
			return nil, err
		}
		normalized = append(normalized, n)
	}
	if err := journal.ValidateAppendBatch(normalized); err != nil {
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	requestHash, err := journal.RequestHash(normalized)
	if err != nil {
		opErr = err
		return nil, err
	}
	var out *journal.AppendResponse
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		current, _, err := selectJournalTx(ctx, tx, tenantID, journalID, true)
		if err != nil {
			return err
		}
		if current.ClosedAt != nil {
			return ErrJournalClosed
		}
		if existing, err := selectAppendRequestTx(ctx, tx, tenantID, journalID, appendID); err == nil {
			if existing.requestHash != requestHash || existing.writerType != writer.Type ||
				existing.writerID != writer.ID || existing.effectiveSource != effectiveSource {
				return ErrIdempotencyConflict
			}
			out = existing.response(journalID, appendID)
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}

		observedAt := journal.NormalizeTime(time.Now())
		nextSeq := current.NextSeq
		prevHash := current.HeadHash
		inserted := make([]journal.Entry, 0, len(normalized))
		for i, input := range normalized {
			occurredAt := observedAt
			if input.OccurredAt != nil {
				occurredAt = *input.OccurredAt
			}
			actor := input.Actor
			if actor.Type == "" && actor.ID == "" {
				actor = current.Actor
				if actor.Type == "" && actor.ID == "" {
					actor = journal.Actor{Type: writer.Type, ID: writer.ID}
				}
			}
			entry := journal.Entry{
				TenantID:      tenantID,
				JournalID:     journalID,
				Seq:           nextSeq + int64(i),
				EntryID:       journal.NewID("jre"),
				Type:          input.Type,
				SchemaVersion: input.SchemaVersion,
				Status:        input.Status,
				OccurredAt:    occurredAt,
				ObservedAt:    observedAt,
				Actor:         actor,
				Source:        effectiveSource,
				ParentEntryID: input.ParentEntryID,
				CorrelationID: input.CorrelationID,
				Subjects:      input.Subjects,
				Summary:       input.Summary,
				ArtifactRefs:  nil,
				PrevHash:      prevHash,
			}
			entryHash, err := journal.EntryHash(entry)
			if err != nil {
				return err
			}
			entry.EntryHash = entryHash
			prevHash = entryHash
			inserted = append(inserted, entry)
		}
		firstSeq := inserted[0].Seq
		lastSeq := inserted[len(inserted)-1].Seq
		headHash := inserted[len(inserted)-1].EntryHash
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_append_requests
			(tenant_id, journal_id, append_id, request_hash, writer_type, writer_id, effective_source,
			 first_seq, last_seq, count, head_hash, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tenantID, journalID, appendID, requestHash, writer.Type, writer.ID, effectiveSource,
			firstSeq, lastSeq, len(inserted), headHash, observedAt); err != nil {
			if isUniqueViolation(err) {
				return ErrIdempotencyConflict
			}
			return err
		}
		for _, entry := range inserted {
			if err := insertJournalEntryTx(ctx, tx, entry); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journals
			SET next_seq = ?, head_hash = ?, updated_at = ?
			WHERE tenant_id = ? AND journal_id = ?`,
			lastSeq+1, headHash, observedAt, tenantID, journalID); err != nil {
			return err
		}
		out = &journal.AppendResponse{
			JournalID:  journalID,
			AppendID:   appendID,
			FirstSeq:   firstSeq,
			LastSeq:    lastSeq,
			Count:      len(inserted),
			HeadHash:   headHash,
			Idempotent: false,
		}
		return nil
	})
	if err != nil {
		opErr = err
		return nil, err
	}
	return out, nil
}

func (s *Store) ListJournalEntries(ctx context.Context, tenantID, journalID string, afterSeq int64, limit int) ([]journal.Entry, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "list_journal_entries", start, &opErr)

	if tenantID == "" || journalID == "" {
		opErr = ErrJournalValidation
		return nil, fmt.Errorf("%w: tenant_id and journal_id are required", ErrJournalValidation)
	}
	if err := journal.ValidateJournalID(journalID); err != nil {
		opErr = err
		return nil, fmt.Errorf("%w: %v", ErrJournalValidation, err)
	}
	limit = journal.NormalizeLimit(limit)
	rows, err := s.db.QueryContext(ctx, selectEntryColumns()+`
		FROM journal_entries
		WHERE tenant_id = ? AND journal_id = ? AND seq > ?
		ORDER BY seq
		LIMIT ?`, tenantID, journalID, afterSeq, limit)
	if err != nil {
		opErr = err
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out, err := scanEntryRows(rows)
	if err != nil {
		opErr = err
		return nil, err
	}
	if len(out) == 0 {
		exists, err := s.journalExists(ctx, tenantID, journalID)
		if err != nil {
			opErr = err
			return nil, err
		}
		if !exists {
			opErr = ErrNotFound
			return nil, ErrNotFound
		}
	}
	return out, nil
}

func (s *Store) SearchJournal(ctx context.Context, tenantID string, req journal.SearchRequest) ([]journal.SearchMatch, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "search_journal", start, &opErr)

	req, err := journal.NormalizeSearchRequest(req)
	if err != nil {
		opErr = err
		return nil, wrapJournalInputError(err)
	}
	if req.ResultKind == "" {
		if req.Entries {
			req.ResultKind = journal.SearchResultEntries
		} else if req.Type == "" && req.Status == "" && len(req.Subjects) == 0 && req.ActorType == "" {
			req.ResultKind = journal.SearchResultJournals
		} else {
			req.ResultKind = journal.SearchResultEntries
		}
	}
	if req.ResultKind == journal.SearchResultJournals {
		out, err := s.searchJournalLabels(ctx, tenantID, req)
		opErr = err
		return out, err
	}
	out, err := s.searchJournalEntries(ctx, tenantID, req)
	opErr = err
	return out, err
}

func (s *Store) VerifyJournal(ctx context.Context, tenantID, journalID string) (*journal.VerifyResult, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "verify_journal", start, &opErr)

	var genesisRaw []byte
	var storedGenesisHash, storedHead string
	err := s.db.QueryRowContext(ctx, `SELECT genesis, genesis_hash, head_hash
		FROM journals WHERE tenant_id = ? AND journal_id = ?`, tenantID, journalID).
		Scan(&genesisRaw, &storedGenesisHash, &storedHead)
	if err != nil {
		opErr = err
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var genesisDoc any
	if err := json.Unmarshal(genesisRaw, &genesisDoc); err != nil {
		opErr = err
		return nil, err
	}
	genesisHash, err := journal.HashCanonical(genesisDoc)
	if err != nil {
		opErr = err
		return nil, err
	}
	expectedLabels, err := journal.LabelsFromGenesis(genesisRaw)
	if err != nil {
		opErr = err
		return nil, err
	}
	storedLabels, labelsOK, err := s.loadJournalLabels(ctx, tenantID, journalID)
	if err != nil {
		opErr = err
		return nil, err
	}
	entries, err := s.ListJournalEntries(ctx, tenantID, journalID, 0, journal.MaxLimit)
	if err != nil {
		opErr = err
		return nil, err
	}
	prev := genesisHash
	hashChainOK := genesisHash == storedGenesisHash
	projectionOK := labelsOK && equalLabels(expectedLabels, storedLabels)
	var count int64
	for len(entries) > 0 {
		subjectsBySeq, subjectsOK, err := s.loadJournalEntrySubjects(ctx, tenantID, journalID, entries[0].Seq, entries[len(entries)-1].Seq)
		if err != nil {
			opErr = err
			return nil, err
		}
		if !subjectsOK {
			projectionOK = false
		}
		for _, entry := range entries {
			count++
			storedEntryHash := entry.EntryHash
			if entry.PrevHash != prev {
				hashChainOK = false
			}
			entry.EntryHash = ""
			recomputed, err := journal.EntryHash(entry)
			if err != nil {
				opErr = err
				return nil, err
			}
			if recomputed != storedEntryHash {
				hashChainOK = false
			}
			if !equalStrings(entry.Subjects, subjectsBySeq[entry.Seq]) {
				projectionOK = false
			}
			prev = recomputed
		}
		entries, err = s.ListJournalEntries(ctx, tenantID, journalID, entries[len(entries)-1].Seq, journal.MaxLimit)
		if err != nil {
			opErr = err
			return nil, err
		}
	}
	if prev != storedHead {
		hashChainOK = false
	}
	ok := hashChainOK && projectionOK
	return &journal.VerifyResult{
		OK:           ok,
		JournalID:    journalID,
		Entries:      count,
		HeadHash:     storedHead,
		HashChainOK:  hashChainOK,
		ProjectionOK: &projectionOK,
	}, nil
}

type appendRequestRow struct {
	requestHash     string
	writerType      string
	writerID        string
	effectiveSource string
	firstSeq        int64
	lastSeq         int64
	count           int
	headHash        string
}

func (r appendRequestRow) response(journalID, appendID string) *journal.AppendResponse {
	return &journal.AppendResponse{
		JournalID:  journalID,
		AppendID:   appendID,
		FirstSeq:   r.firstSeq,
		LastSeq:    r.lastSeq,
		Count:      r.count,
		HeadHash:   r.headHash,
		Idempotent: true,
	}
}

func selectJournalTx(ctx context.Context, tx *sql.Tx, tenantID, journalID string, forUpdate bool) (*journal.Journal, string, error) {
	query := `SELECT tenant_id, journal_id, kind, title, actor_type, actor_id, source, meta, retention,
		next_seq, genesis_hash, head_hash, created_at, updated_at, closed_at, create_hash
		FROM journals WHERE tenant_id = ? AND journal_id = ?`
	if forUpdate {
		query += " FOR UPDATE"
	}
	row := tx.QueryRowContext(ctx, query, tenantID, journalID)
	j, createHash, err := scanJournal(row)
	if err != nil {
		return nil, "", err
	}
	labels, err := selectJournalLabelsTx(ctx, tx, tenantID, journalID)
	if err != nil {
		return nil, "", err
	}
	j.Labels = labels
	if len(j.Meta) == 0 {
		j.Meta = journal.LabelsToMap(labels)
	}
	return j, createHash, nil
}

func selectJournalLabelsTx(ctx context.Context, tx *sql.Tx, tenantID, journalID string) ([]journal.Label, error) {
	rows, err := tx.QueryContext(ctx, `SELECT label_key, label_value
		FROM journal_labels
		WHERE tenant_id = ? AND journal_id = ?
		ORDER BY label_key, label_value`, tenantID, journalID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var labels []journal.Label
	for rows.Next() {
		var label journal.Label
		if err := rows.Scan(&label.Key, &label.Value); err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	return labels, rows.Err()
}

func (s *Store) journalExists(ctx context.Context, tenantID, journalID string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM journals WHERE tenant_id = ? AND journal_id = ?`, tenantID, journalID).Scan(&one)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func scanJournal(row interface{ Scan(dest ...any) error }) (*journal.Journal, string, error) {
	var j journal.Journal
	var title, actorType, actorID, source sql.NullString
	var metaRaw, retentionRaw []byte
	var closedAt sql.NullTime
	var createHash string
	if err := row.Scan(&j.TenantID, &j.JournalID, &j.Kind, &title, &actorType, &actorID, &source,
		&metaRaw, &retentionRaw, &j.NextSeq, &j.GenesisHash, &j.HeadHash, &j.CreatedAt,
		&j.UpdatedAt, &closedAt, &createHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	j.Title = title.String
	j.Actor = journal.Actor{Type: actorType.String, ID: actorID.String}
	j.Source = source.String
	j.CreatedAt = journal.NormalizeTime(j.CreatedAt)
	j.UpdatedAt = journal.NormalizeTime(j.UpdatedAt)
	if closedAt.Valid {
		t := journal.NormalizeTime(closedAt.Time)
		j.ClosedAt = &t
	}
	if len(metaRaw) > 0 {
		_ = json.Unmarshal(metaRaw, &j.Meta)
		j.Labels = journal.LabelsFromMap(j.Meta)
	}
	if len(retentionRaw) > 0 {
		j.Retention = append(json.RawMessage(nil), retentionRaw...)
	}
	return &j, createHash, nil
}

func selectAppendRequestTx(ctx context.Context, tx *sql.Tx, tenantID, journalID, appendID string) (*appendRequestRow, error) {
	var r appendRequestRow
	err := tx.QueryRowContext(ctx, `SELECT request_hash, writer_type, writer_id, effective_source,
		first_seq, last_seq, count, head_hash
		FROM journal_append_requests
		WHERE tenant_id = ? AND journal_id = ? AND append_id = ?`,
		tenantID, journalID, appendID).
		Scan(&r.requestHash, &r.writerType, &r.writerID, &r.effectiveSource,
			&r.firstSeq, &r.lastSeq, &r.count, &r.headHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &r, nil
}

func insertJournalEntryTx(ctx context.Context, tx *sql.Tx, entry journal.Entry) error {
	subjectsRaw, err := json.Marshal(entry.Subjects)
	if err != nil {
		return err
	}
	summaryRaw := []byte(entry.Summary)
	artifactRefsRaw, err := json.Marshal(entry.ArtifactRefs)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO journal_entries
		(tenant_id, journal_id, seq, entry_id, type, schema_version, status, occurred_at, observed_at,
		 actor_type, actor_id, source, parent_entry_id, correlation_id, subjects, summary, artifact_refs,
		 prev_hash, entry_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.TenantID, entry.JournalID, entry.Seq, entry.EntryID, entry.Type, entry.SchemaVersion,
		nullStr(entry.Status), entry.OccurredAt, entry.ObservedAt, nullStr(entry.Actor.Type),
		nullStr(entry.Actor.ID), entry.Source, nullStr(entry.ParentEntryID), nullStr(entry.CorrelationID),
		nullJSON(subjectsRaw), nullJSON(summaryRaw), nullJSON(artifactRefsRaw), entry.PrevHash, entry.EntryHash); err != nil {
		return err
	}
	for _, rawSubject := range entry.Subjects {
		subjectType, subjectID, err := journal.ParseSubject(rawSubject)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_entry_subjects
			(tenant_id, subject_type, subject_hash, subject_id, occurred_at, observed_at, journal_id, seq, entry_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.TenantID, subjectType, journal.SubjectHash(subjectType, subjectID), subjectID,
			entry.OccurredAt, entry.ObservedAt, entry.JournalID, entry.Seq, entry.EntryID); err != nil {
			return err
		}
	}
	return nil
}

func selectEntryColumns() string {
	return `SELECT tenant_id, journal_id, seq, entry_id, type, schema_version, status, occurred_at, observed_at,
		actor_type, actor_id, source, parent_entry_id, correlation_id, subjects, summary, artifact_refs,
		prev_hash, entry_hash `
}

func selectEntryColumnsAlias(alias string) string {
	prefix := alias + "."
	return `SELECT ` + prefix + `tenant_id, ` + prefix + `journal_id, ` + prefix + `seq, ` +
		prefix + `entry_id, ` + prefix + `type, ` + prefix + `schema_version, ` + prefix + `status, ` +
		prefix + `occurred_at, ` + prefix + `observed_at, ` + prefix + `actor_type, ` + prefix + `actor_id, ` +
		prefix + `source, ` + prefix + `parent_entry_id, ` + prefix + `correlation_id, ` +
		prefix + `subjects, ` + prefix + `summary, ` + prefix + `artifact_refs, ` +
		prefix + `prev_hash, ` + prefix + `entry_hash `
}

func scanEntryRows(rows *sql.Rows) ([]journal.Entry, error) {
	var out []journal.Entry
	for rows.Next() {
		entry, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *entry)
	}
	return out, rows.Err()
}

func scanEntry(row interface{ Scan(dest ...any) error }) (*journal.Entry, error) {
	var e journal.Entry
	var status, actorType, actorID, parentEntryID, correlationID sql.NullString
	var subjectsRaw, summaryRaw, artifactRefsRaw []byte
	if err := row.Scan(&e.TenantID, &e.JournalID, &e.Seq, &e.EntryID, &e.Type, &e.SchemaVersion,
		&status, &e.OccurredAt, &e.ObservedAt, &actorType, &actorID, &e.Source, &parentEntryID,
		&correlationID, &subjectsRaw, &summaryRaw, &artifactRefsRaw, &e.PrevHash, &e.EntryHash); err != nil {
		return nil, err
	}
	e.Status = status.String
	e.Actor = journal.Actor{Type: actorType.String, ID: actorID.String}
	e.ParentEntryID = parentEntryID.String
	e.CorrelationID = correlationID.String
	e.OccurredAt = journal.NormalizeTime(e.OccurredAt)
	e.ObservedAt = journal.NormalizeTime(e.ObservedAt)
	if len(subjectsRaw) > 0 {
		_ = json.Unmarshal(subjectsRaw, &e.Subjects)
	}
	if len(summaryRaw) > 0 {
		e.Summary = append(json.RawMessage(nil), summaryRaw...)
	}
	if len(artifactRefsRaw) > 0 {
		_ = json.Unmarshal(artifactRefsRaw, &e.ArtifactRefs)
	}
	return &e, nil
}

func (s *Store) loadJournalEntrySubjects(ctx context.Context, tenantID, journalID string, firstSeq, lastSeq int64) (map[int64][]string, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, subject_type, subject_hash, subject_id
		FROM journal_entry_subjects
		WHERE tenant_id = ? AND journal_id = ? AND seq >= ? AND seq <= ?
		ORDER BY seq`, tenantID, journalID, firstSeq, lastSeq)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	out := map[int64][]string{}
	ok := true
	for rows.Next() {
		var seq int64
		var subjectType, subjectHash, subjectID string
		if err := rows.Scan(&seq, &subjectType, &subjectHash, &subjectID); err != nil {
			return nil, false, err
		}
		canonicalType, canonicalID, err := journal.ParseSubject(subjectType + ":" + subjectID)
		if err != nil {
			ok = false
			continue
		}
		if canonicalType != subjectType || canonicalID != subjectID {
			ok = false
		}
		if subjectHash != journal.SubjectHash(canonicalType, canonicalID) {
			ok = false
		}
		out[seq] = append(out[seq], canonicalType+":"+canonicalID)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	for seq, subjects := range out {
		normalized, err := journal.NormalizeSubjects(subjects)
		if err != nil {
			return nil, false, err
		}
		out[seq] = normalized
	}
	return out, ok, nil
}

func (s *Store) loadJournalLabels(ctx context.Context, tenantID, journalID string) ([]journal.Label, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT label_key, label_hash, label_value
		FROM journal_labels
		WHERE tenant_id = ? AND journal_id = ?
		ORDER BY label_key, label_value`, tenantID, journalID)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	var labels []journal.Label
	ok := true
	for rows.Next() {
		var label journal.Label
		var labelHash string
		if err := rows.Scan(&label.Key, &labelHash, &label.Value); err != nil {
			return nil, false, err
		}
		normalized := journal.NormalizeLabels([]journal.Label{label})
		if len(normalized) != 1 {
			ok = false
			continue
		}
		canonical := normalized[0]
		if canonical.Key != label.Key || canonical.Value != label.Value {
			ok = false
		}
		if err := journal.ValidateLabels([]journal.Label{canonical}); err != nil {
			ok = false
			continue
		}
		if labelHash != journal.LabelHash(canonical.Key, canonical.Value) {
			ok = false
		}
		labels = append(labels, canonical)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	labels = journal.NormalizeLabels(labels)
	if err := journal.ValidateLabels(labels); err != nil {
		return nil, false, nil
	}
	return labels, ok, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalLabels(a, b []journal.Label) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Store) searchJournalEntries(ctx context.Context, tenantID string, req journal.SearchRequest) ([]journal.SearchMatch, error) {
	var b strings.Builder
	args := []any{}
	labels := journal.NormalizeLabels(req.Labels)
	labelAnchored := false
	if req.Entries {
		b.WriteString(selectEntryColumnsAlias("e"))
	} else {
		b.WriteString(`SELECT e.journal_id, e.seq, e.type, e.status, e.observed_at `)
	}
	if len(req.Subjects) > 0 {
		subjectType, subjectID, err := journal.ParseSubject(req.Subjects[0])
		if err != nil {
			return nil, err
		}
		b.WriteString(`FROM journal_entry_subjects s JOIN journal_entries e
			ON e.tenant_id = s.tenant_id AND e.journal_id = s.journal_id AND e.seq = s.seq
			JOIN journals j ON j.tenant_id = e.tenant_id AND j.journal_id = e.journal_id
			WHERE s.tenant_id = ? AND s.subject_type = ? AND s.subject_hash = ? AND s.subject_id = ? `)
		args = append(args, tenantID, subjectType, journal.SubjectHash(subjectType, subjectID), subjectID)
	} else if len(labels) > 0 {
		first := labels[0]
		labelAnchored = true
		b.WriteString(`FROM journal_labels l JOIN journals j
			ON j.tenant_id = l.tenant_id AND j.journal_id = l.journal_id
			JOIN journal_entries e ON e.tenant_id = j.tenant_id AND e.journal_id = j.journal_id
			WHERE l.tenant_id = ? AND l.label_key = ? AND l.label_hash = ? AND l.label_value = ? `)
		args = append(args, tenantID, first.Key, journal.LabelHash(first.Key, first.Value), first.Value)
	} else {
		b.WriteString(`FROM journal_entries e
			JOIN journals j ON j.tenant_id = e.tenant_id AND j.journal_id = e.journal_id
			WHERE e.tenant_id = ? `)
		args = append(args, tenantID)
	}
	if err := appendEntryFilters(&b, &args, req, labelAnchored); err != nil {
		return nil, err
	}
	b.WriteString(` ORDER BY e.observed_at DESC, e.journal_id DESC, e.seq DESC LIMIT ?`)
	args = append(args, req.Limit)

	rows, err := s.db.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []journal.SearchMatch
	for rows.Next() {
		if req.Entries {
			entry, err := scanEntry(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, journal.SearchMatch{
				JournalID:       entry.JournalID,
				Seq:             entry.Seq,
				Type:            entry.Type,
				Status:          entry.Status,
				ObservedAt:      entry.ObservedAt,
				MatchedSubjects: append([]string(nil), req.Subjects...),
				MatchedLabels:   cloneLabels(req.Labels),
				Entry:           entry,
			})
			continue
		}
		var m journal.SearchMatch
		var status sql.NullString
		if err := rows.Scan(&m.JournalID, &m.Seq, &m.Type, &status, &m.ObservedAt); err != nil {
			return nil, err
		}
		m.Status = status.String
		m.ObservedAt = journal.NormalizeTime(m.ObservedAt)
		m.MatchedSubjects = append([]string(nil), req.Subjects...)
		m.MatchedLabels = cloneLabels(req.Labels)
		out = append(out, m)
	}
	return out, rows.Err()
}

func appendEntryFilters(b *strings.Builder, args *[]any, req journal.SearchRequest, skipFirstLabel bool) error {
	if req.Type != "" {
		b.WriteString(`AND e.type = ? `)
		*args = append(*args, req.Type)
	}
	if req.Status != "" {
		b.WriteString(`AND e.status = ? `)
		*args = append(*args, req.Status)
	}
	if req.Kind != "" {
		b.WriteString(`AND j.kind = ? `)
		*args = append(*args, req.Kind)
	}
	if req.ActorType != "" {
		b.WriteString(`AND e.actor_type = ? `)
		*args = append(*args, req.ActorType)
		if req.ActorID != "" {
			b.WriteString(`AND e.actor_id = ? `)
			*args = append(*args, req.ActorID)
		}
	}
	if req.Since != nil {
		b.WriteString(`AND e.observed_at >= ? `)
		*args = append(*args, *req.Since)
	}
	if req.Until != nil {
		b.WriteString(`AND e.observed_at <= ? `)
		*args = append(*args, *req.Until)
	}
	if !req.After.ObservedAt.IsZero() {
		b.WriteString(`AND (e.observed_at < ? OR (e.observed_at = ? AND (e.journal_id < ? OR (e.journal_id = ? AND e.seq < ?)))) `)
		*args = append(*args, req.After.ObservedAt, req.After.ObservedAt, req.After.JournalID, req.After.JournalID, req.After.Seq)
	}
	if len(req.Subjects) > 1 {
		for _, rawSubject := range req.Subjects[1:] {
			subjectType, subjectID, err := journal.ParseSubject(rawSubject)
			if err != nil {
				return err
			}
			b.WriteString(`AND EXISTS (SELECT 1 FROM journal_entry_subjects sx
			WHERE sx.tenant_id = e.tenant_id AND sx.journal_id = e.journal_id AND sx.seq = e.seq
			AND sx.subject_type = ? AND sx.subject_hash = ? AND sx.subject_id = ?) `)
			*args = append(*args, subjectType, journal.SubjectHash(subjectType, subjectID), subjectID)
		}
	}
	labels := journal.NormalizeLabels(req.Labels)
	if skipFirstLabel && len(labels) > 0 {
		labels = labels[1:]
	}
	for _, label := range labels {
		b.WriteString(`AND EXISTS (SELECT 1 FROM journal_labels lx
			WHERE lx.tenant_id = j.tenant_id AND lx.journal_id = j.journal_id
			AND lx.label_key = ? AND lx.label_hash = ? AND lx.label_value = ?) `)
		*args = append(*args, label.Key, journal.LabelHash(label.Key, label.Value), label.Value)
	}
	return nil
}

func (s *Store) searchJournalLabels(ctx context.Context, tenantID string, req journal.SearchRequest) ([]journal.SearchMatch, error) {
	labels := journal.NormalizeLabels(req.Labels)
	var b strings.Builder
	args := []any{}
	b.WriteString(`SELECT j.journal_id, j.kind, j.title, j.created_at, `)
	if len(labels) > 0 {
		first := labels[0]
		b.WriteString(`l.created_at FROM `)
		b.WriteString(`journal_labels l JOIN journals j
			ON j.tenant_id = l.tenant_id AND j.journal_id = l.journal_id
			WHERE l.tenant_id = ? AND l.label_key = ? AND l.label_hash = ? AND l.label_value = ? `)
		args = append(args, tenantID, first.Key, journal.LabelHash(first.Key, first.Value), first.Value)
	} else {
		b.WriteString(`j.created_at FROM `)
		b.WriteString(`journals j WHERE j.tenant_id = ? `)
		args = append(args, tenantID)
	}
	if req.Kind != "" {
		b.WriteString(`AND j.kind = ? `)
		args = append(args, req.Kind)
	}
	if req.Since != nil {
		if len(labels) > 0 {
			b.WriteString(`AND l.created_at >= ? `)
		} else {
			b.WriteString(`AND j.created_at >= ? `)
		}
		args = append(args, *req.Since)
	}
	if req.Until != nil {
		if len(labels) > 0 {
			b.WriteString(`AND l.created_at <= ? `)
		} else {
			b.WriteString(`AND j.created_at <= ? `)
		}
		args = append(args, *req.Until)
	}
	if !req.After.CreatedAt.IsZero() {
		if len(labels) > 0 {
			b.WriteString(`AND (l.created_at < ? OR (l.created_at = ? AND l.journal_id < ?)) `)
			args = append(args, req.After.CreatedAt, req.After.CreatedAt, req.After.JournalID)
		} else {
			b.WriteString(`AND (j.created_at < ? OR (j.created_at = ? AND j.journal_id < ?)) `)
			args = append(args, req.After.CreatedAt, req.After.CreatedAt, req.After.JournalID)
		}
	}
	for i, label := range labels {
		if i == 0 {
			continue
		}
		b.WriteString(`AND EXISTS (SELECT 1 FROM journal_labels lx
			WHERE lx.tenant_id = j.tenant_id AND lx.journal_id = j.journal_id
			AND lx.label_key = ? AND lx.label_hash = ? AND lx.label_value = ?) `)
		args = append(args, label.Key, journal.LabelHash(label.Key, label.Value), label.Value)
	}
	if len(labels) > 0 {
		b.WriteString(`ORDER BY l.created_at DESC, l.journal_id DESC LIMIT ?`)
	} else {
		b.WriteString(`ORDER BY j.created_at DESC, j.journal_id DESC LIMIT ?`)
	}
	args = append(args, req.Limit)
	rows, err := s.db.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []journal.SearchMatch
	for rows.Next() {
		var m journal.SearchMatch
		var title sql.NullString
		if err := rows.Scan(&m.JournalID, &m.Kind, &title, &m.CreatedAt, &m.CursorAt); err != nil {
			return nil, err
		}
		m.Title = title.String
		m.CreatedAt = journal.NormalizeTime(m.CreatedAt)
		m.CursorAt = journal.NormalizeTime(m.CursorAt)
		m.MatchedLabels = cloneLabels(req.Labels)
		out = append(out, m)
	}
	return out, rows.Err()
}

func cloneLabels(labels []journal.Label) []journal.Label {
	if len(labels) == 0 {
		return nil
	}
	out := make([]journal.Label, len(labels))
	copy(out, labels)
	return out
}

func nullJSON(raw []byte) interface{} {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return string(raw)
}

func wrapJournalInputError(err error) error {
	if errors.Is(err, journal.ErrPayloadTooLarge) {
		return fmt.Errorf("%w: %v", ErrJournalPayloadTooLarge, err)
	}
	return fmt.Errorf("%w: %v", ErrJournalValidation, err)
}

func isRetryableJournalCreateConflict(err error) bool {
	if err == nil || errors.Is(err, ErrJournalConflict) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadlock found") ||
		strings.Contains(msg, "Error 1213") ||
		strings.Contains(msg, "40001")
}
