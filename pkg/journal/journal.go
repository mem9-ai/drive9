// Package journal defines the shared Drive9 journal envelope, canonicalization,
// and hash helpers used by the server, client, and datastore layers.
package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/oklog/ulid/v2"
	"golang.org/x/text/unicode/norm"
)

const (
	DefaultKind           = "agent"
	DefaultSource         = "self_reported"
	SourceGateway         = "gateway_observed"
	SourceServer          = "server_observed"
	SourceSelf            = "self_reported"
	SourceImported        = "imported"
	DefaultLimit          = 100
	MaxLimit              = 1000
	MaxEntriesPerBatch    = 500
	MaxBatchBytes         = 4 << 20
	MaxInlineSummaryBytes = 64 << 10
	MaxSubjectsPerEntry   = 64
	MaxSubjectValueBytes  = 8 << 10
	MaxLabelsPerJournal   = 64
	MaxLabelValueBytes    = 4 << 10
)

var (
	ErrPayloadTooLarge = errors.New("journal payload too large")

	labelKeyPattern   = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
	subjectTypeRegexp = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
	entryTypePattern  = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
	statusPattern     = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
	journalIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	appendIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
)

type Actor struct {
	Type string `json:"type,omitempty"`
	ID   string `json:"id,omitempty"`
}

type CreateRequest struct {
	JournalID string            `json:"journal_id,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Title     string            `json:"title,omitempty"`
	Actor     Actor             `json:"actor,omitempty"`
	Source    string            `json:"source,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	Labels    []Label           `json:"labels,omitempty"`
	Retention json.RawMessage   `json:"retention,omitempty"`
}

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Journal struct {
	TenantID    string            `json:"tenant_id,omitempty"`
	JournalID   string            `json:"journal_id"`
	Kind        string            `json:"kind"`
	Title       string            `json:"title,omitempty"`
	Actor       Actor             `json:"actor,omitempty"`
	Source      string            `json:"source,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
	Labels      []Label           `json:"labels,omitempty"`
	Retention   json.RawMessage   `json:"retention,omitempty"`
	NextSeq     int64             `json:"next_seq,omitempty"`
	GenesisHash string            `json:"genesis_hash,omitempty"`
	HeadHash    string            `json:"head_hash,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at,omitempty"`
	ClosedAt    *time.Time        `json:"closed_at,omitempty"`
}

type EntryInput struct {
	Type          string          `json:"type,omitempty"`
	SchemaVersion int             `json:"schema_version,omitempty"`
	Status        string          `json:"status,omitempty"`
	OccurredAt    *time.Time      `json:"occurred_at,omitempty"`
	Actor         Actor           `json:"actor,omitempty"`
	Source        string          `json:"source,omitempty"`
	ParentEntryID string          `json:"parent_entry_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Subjects      []string        `json:"subjects,omitempty"`
	Summary       json.RawMessage `json:"summary,omitempty"`
	Artifacts     []ArtifactRef   `json:"artifacts,omitempty"`
	ArtifactRefs  []ArtifactRef   `json:"artifact_refs,omitempty"`
}

type ArtifactRef struct {
	Name        string `json:"name"`
	Hash        string `json:"hash"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes"`
}

type Entry struct {
	TenantID      string          `json:"tenant_id,omitempty"`
	JournalID     string          `json:"journal_id"`
	Seq           int64           `json:"seq"`
	EntryID       string          `json:"entry_id"`
	Type          string          `json:"type"`
	SchemaVersion int             `json:"schema_version"`
	Status        string          `json:"status,omitempty"`
	OccurredAt    time.Time       `json:"occurred_at"`
	ObservedAt    time.Time       `json:"observed_at"`
	Actor         Actor           `json:"actor,omitempty"`
	Source        string          `json:"source"`
	ParentEntryID string          `json:"parent_entry_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Subjects      []string        `json:"subjects,omitempty"`
	Summary       json.RawMessage `json:"summary,omitempty"`
	ArtifactRefs  []ArtifactRef   `json:"artifact_refs,omitempty"`
	PrevHash      string          `json:"prev_hash"`
	EntryHash     string          `json:"entry_hash"`
}

type AppendResponse struct {
	JournalID  string `json:"journal_id"`
	AppendID   string `json:"append_id"`
	FirstSeq   int64  `json:"first_seq"`
	LastSeq    int64  `json:"last_seq"`
	Count      int    `json:"count"`
	HeadHash   string `json:"head_hash"`
	Idempotent bool   `json:"idempotent"`
}

type SearchRequest struct {
	Type       string
	Status     string
	Kind       string
	ActorType  string
	ActorID    string
	Subjects   []string
	Labels     []Label
	Since      *time.Time
	Until      *time.Time
	SinceRaw   string
	UntilRaw   string
	Limit      int
	Entries    bool
	Cursor     string
	After      SearchAfter
	ResultKind SearchResultKind
}

type SearchAfter struct {
	ObservedAt time.Time
	CreatedAt  time.Time
	JournalID  string
	Seq        int64
}

type SearchResultKind string

const (
	SearchResultEntries  SearchResultKind = "entries"
	SearchResultJournals SearchResultKind = "journals"
)

type SearchMatch struct {
	JournalID       string    `json:"journal_id"`
	Seq             int64     `json:"seq,omitempty"`
	Type            string    `json:"type,omitempty"`
	Status          string    `json:"status,omitempty"`
	Kind            string    `json:"kind,omitempty"`
	Title           string    `json:"title,omitempty"`
	ObservedAt      time.Time `json:"observed_at,omitempty"`
	CreatedAt       time.Time `json:"created_at,omitempty"`
	CursorAt        time.Time `json:"-"`
	MatchedSubjects []string  `json:"matched_subjects,omitempty"`
	MatchedLabels   []Label   `json:"matched_labels,omitempty"`
	Cursor          string    `json:"cursor,omitempty"`
	Entry           *Entry    `json:"entry,omitempty"`
}

type VerifyResult struct {
	OK                     bool   `json:"ok"`
	JournalID              string `json:"journal_id"`
	Entries                int64  `json:"entries"`
	HeadHash               string `json:"head_hash"`
	HashChainOK            bool   `json:"hash_chain_ok"`
	SealOK                 *bool  `json:"seal_ok,omitempty"`
	ProjectionOK           *bool  `json:"projection_ok,omitempty"`
	ArtifactBytesAvailable *bool  `json:"artifact_bytes_available,omitempty"`
	HeadSealed             *bool  `json:"head_sealed,omitempty"`
	LatestSealSeq          *int64 `json:"latest_seal_seq,omitempty"`
}

func (m SearchMatch) MarshalJSON() ([]byte, error) {
	type searchMatchJSON struct {
		JournalID       string     `json:"journal_id"`
		Seq             int64      `json:"seq,omitempty"`
		Type            string     `json:"type,omitempty"`
		Status          string     `json:"status,omitempty"`
		Kind            string     `json:"kind,omitempty"`
		Title           string     `json:"title,omitempty"`
		ObservedAt      *time.Time `json:"observed_at,omitempty"`
		CreatedAt       *time.Time `json:"created_at,omitempty"`
		MatchedSubjects []string   `json:"matched_subjects,omitempty"`
		MatchedLabels   []Label    `json:"matched_labels,omitempty"`
		Cursor          string     `json:"cursor,omitempty"`
		Entry           *Entry     `json:"entry,omitempty"`
	}
	out := searchMatchJSON{
		JournalID:       m.JournalID,
		Seq:             m.Seq,
		Type:            m.Type,
		Status:          m.Status,
		Kind:            m.Kind,
		Title:           m.Title,
		MatchedSubjects: m.MatchedSubjects,
		MatchedLabels:   m.MatchedLabels,
		Cursor:          m.Cursor,
		Entry:           m.Entry,
	}
	if !m.ObservedAt.IsZero() {
		t := NormalizeTime(m.ObservedAt)
		out.ObservedAt = &t
	}
	if !m.CreatedAt.IsZero() {
		t := NormalizeTime(m.CreatedAt)
		out.CreatedAt = &t
	}
	return json.Marshal(out)
}

func NewID(prefix string) string {
	return prefix + "_" + strings.ToLower(ulid.Make().String())
}

func NormalizeTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

func NormalizeOptionalTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	nt := NormalizeTime(*t)
	return &nt
}

func NormalizeCreateRequest(req CreateRequest) (CreateRequest, error) {
	req.JournalID = strings.TrimSpace(req.JournalID)
	if req.JournalID != "" {
		if err := ValidateJournalID(req.JournalID); err != nil {
			return CreateRequest{}, err
		}
	}
	req.Kind = normalizeDefault(req.Kind, DefaultKind)
	req.Source = normalizeDefault(req.Source, DefaultSource)
	if req.Source != SourceSelf {
		return CreateRequest{}, fmt.Errorf("source %q is not allowed for ordinary journal create", req.Source)
	}
	if err := ValidateKind(req.Kind); err != nil {
		return CreateRequest{}, err
	}
	if err := ValidateSource(req.Source); err != nil {
		return CreateRequest{}, err
	}
	req.Actor.Type = strings.TrimSpace(req.Actor.Type)
	req.Actor.ID = strings.TrimSpace(req.Actor.ID)
	req.Title = strings.TrimSpace(req.Title)
	req.Labels = NormalizeLabels(append(LabelsFromMap(req.Meta), req.Labels...))
	req.Meta = LabelsToMap(req.Labels)
	if len(req.Retention) > 0 {
		retention, err := CanonicalJSONRaw(req.Retention)
		if err != nil {
			return CreateRequest{}, fmt.Errorf("retention: %w", err)
		}
		req.Retention = retention
	}
	return req, nil
}

func NormalizeSearchRequest(req SearchRequest) (SearchRequest, error) {
	req.Type = normalizeOptional(req.Type)
	if req.Type != "" {
		if err := ValidateEntryType(req.Type); err != nil {
			return SearchRequest{}, err
		}
	}
	req.Status = normalizeOptional(req.Status)
	if req.Status != "" {
		if err := ValidateStatus(req.Status); err != nil {
			return SearchRequest{}, err
		}
	}
	req.Kind = normalizeOptional(req.Kind)
	if req.Kind != "" {
		if err := ValidateKind(req.Kind); err != nil {
			return SearchRequest{}, err
		}
	}
	req.ActorType = normalizeOptional(req.ActorType)
	req.ActorID = strings.TrimSpace(req.ActorID)
	if len(req.Subjects) > 0 {
		subjects, err := NormalizeSubjects(req.Subjects)
		if err != nil {
			return SearchRequest{}, err
		}
		req.Subjects = subjects
	}
	req.Labels = NormalizeLabels(req.Labels)
	if err := ValidateLabels(req.Labels); err != nil {
		return SearchRequest{}, err
	}
	if req.Since != nil {
		req.Since = NormalizeOptionalTime(req.Since)
	}
	if req.Until != nil {
		req.Until = NormalizeOptionalTime(req.Until)
	}
	req.SinceRaw = strings.TrimSpace(req.SinceRaw)
	req.UntilRaw = strings.TrimSpace(req.UntilRaw)
	req.Limit = NormalizeLimit(req.Limit)
	return req, nil
}

func NormalizeEntryInput(in EntryInput, defaultType string, defaultSubjects []string) (EntryInput, error) {
	in.Type = normalizeDefault(in.Type, defaultType)
	if in.Type == "" {
		return EntryInput{}, fmt.Errorf("entry type is required")
	}
	if err := ValidateEntryType(in.Type); err != nil {
		return EntryInput{}, err
	}
	if in.SchemaVersion <= 0 {
		in.SchemaVersion = 1
	}
	if in.Status != "" {
		in.Status = strings.TrimSpace(strings.ToLower(in.Status))
		if err := ValidateStatus(in.Status); err != nil {
			return EntryInput{}, err
		}
	}
	in.Actor.Type = strings.TrimSpace(in.Actor.Type)
	in.Actor.ID = strings.TrimSpace(in.Actor.ID)
	in.Source = normalizeDefault(in.Source, SourceSelf)
	if err := ValidateSource(in.Source); err != nil {
		return EntryInput{}, err
	}
	in.OccurredAt = NormalizeOptionalTime(in.OccurredAt)
	if len(in.Artifacts) > 0 || len(in.ArtifactRefs) > 0 {
		return EntryInput{}, fmt.Errorf("artifact references are not supported in this phase")
	}
	in.Subjects = append(append([]string{}, defaultSubjects...), in.Subjects...)
	subjects, err := NormalizeSubjects(in.Subjects)
	if err != nil {
		return EntryInput{}, err
	}
	in.Subjects = subjects
	if len(in.Subjects) > MaxSubjectsPerEntry {
		return EntryInput{}, payloadTooLargef("too many subjects: %d > %d", len(in.Subjects), MaxSubjectsPerEntry)
	}
	if len(in.Summary) > 0 {
		summary, err := CanonicalJSONRaw(in.Summary)
		if err != nil {
			return EntryInput{}, fmt.Errorf("summary: %w", err)
		}
		if len(summary) > MaxInlineSummaryBytes {
			return EntryInput{}, payloadTooLargef("summary too large: %d > %d", len(summary), MaxInlineSummaryBytes)
		}
		in.Summary = summary
	}
	in.ParentEntryID = strings.TrimSpace(in.ParentEntryID)
	in.CorrelationID = strings.TrimSpace(in.CorrelationID)
	return in, nil
}

func NormalizeSubjects(subjects []string) ([]string, error) {
	seen := make(map[string]struct{}, len(subjects))
	out := make([]string, 0, len(subjects))
	for _, raw := range subjects {
		subjectType, subjectID, err := ParseSubject(raw)
		if err != nil {
			return nil, err
		}
		normalized := subjectType + ":" + subjectID
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out, nil
}

func ParseSubject(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	subjectType, subjectID, ok := strings.Cut(raw, ":")
	if !ok || subjectType == "" || subjectID == "" {
		return "", "", fmt.Errorf("invalid subject %q", raw)
	}
	subjectType = strings.ToLower(strings.TrimSpace(subjectType))
	subjectID = norm.NFC.String(strings.TrimSpace(subjectID))
	if !subjectTypeRegexp.MatchString(subjectType) {
		return "", "", fmt.Errorf("invalid subject type %q", subjectType)
	}
	if !utf8.ValidString(subjectID) {
		return "", "", fmt.Errorf("subject id is not valid UTF-8")
	}
	if len([]byte(subjectID)) > MaxSubjectValueBytes {
		return "", "", payloadTooLargef("subject id too large: %d > %d", len([]byte(subjectID)), MaxSubjectValueBytes)
	}
	return subjectType, subjectID, nil
}

func SubjectHash(subjectType, subjectID string) string {
	return hashString(subjectType + "\x00" + subjectID)
}

func LabelsFromMap(labels map[string]string) []Label {
	if len(labels) == 0 {
		return nil
	}
	out := make([]Label, 0, len(labels))
	for k, v := range labels {
		out = append(out, Label{Key: k, Value: v})
	}
	return out
}

func LabelsToMap(labels []Label) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for _, label := range labels {
		out[label.Key] = label.Value
	}
	return out
}

func NormalizeLabelMap(labels map[string]string) map[string]string {
	normalized := NormalizeLabels(LabelsFromMap(labels))
	return LabelsToMap(normalized)
}

func NormalizeLabels(labels []Label) []Label {
	if len(labels) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(labels))
	out := make([]Label, 0, len(labels))
	for _, label := range labels {
		key := strings.ToLower(strings.TrimSpace(label.Key))
		value := norm.NFC.String(strings.TrimSpace(label.Value))
		fingerprint := key + "\x00" + value
		if _, ok := seen[fingerprint]; ok {
			continue
		}
		seen[fingerprint] = struct{}{}
		out = append(out, Label{Key: key, Value: value})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key == out[j].Key {
			return out[i].Value < out[j].Value
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func ValidateLabels(labels []Label) error {
	if len(labels) > MaxLabelsPerJournal {
		return payloadTooLargef("too many labels: %d > %d", len(labels), MaxLabelsPerJournal)
	}
	for _, label := range labels {
		if !labelKeyPattern.MatchString(label.Key) {
			return fmt.Errorf("invalid label key %q", label.Key)
		}
		if !utf8.ValidString(label.Value) {
			return fmt.Errorf("label value for %q is not valid UTF-8", label.Key)
		}
		if len([]byte(label.Value)) > MaxLabelValueBytes {
			return payloadTooLargef("label value for %q too large: %d > %d", label.Key, len([]byte(label.Value)), MaxLabelValueBytes)
		}
	}
	return nil
}

func ValidateAppendBatch(entries []EntryInput) error {
	if len(entries) > MaxEntriesPerBatch {
		return payloadTooLargef("too many entries: %d > %d", len(entries), MaxEntriesPerBatch)
	}
	raw, err := MarshalCanonical(entries)
	if err != nil {
		return err
	}
	if len(raw) > MaxBatchBytes {
		return payloadTooLargef("batch too large: %d > %d", len(raw), MaxBatchBytes)
	}
	return nil
}

func LabelHash(key, value string) string {
	return hashString(key + "\x00" + value)
}

func ValidateKind(kind string) error {
	if !statusPattern.MatchString(kind) {
		return fmt.Errorf("invalid journal kind %q", kind)
	}
	return nil
}

func ValidateJournalID(id string) error {
	if !journalIDPattern.MatchString(id) || strings.Contains(id, "/") {
		return fmt.Errorf("invalid journal id %q", id)
	}
	return nil
}

func ValidateAppendID(id string) error {
	if !appendIDPattern.MatchString(id) || strings.Contains(id, "/") {
		return fmt.Errorf("invalid idempotency key %q", id)
	}
	return nil
}

func ValidateEntryType(entryType string) error {
	if !entryTypePattern.MatchString(entryType) {
		return fmt.Errorf("invalid entry type %q", entryType)
	}
	return nil
}

func ValidateStatus(status string) error {
	if !statusPattern.MatchString(status) {
		return fmt.Errorf("invalid status %q", status)
	}
	return nil
}

func ValidateSource(source string) error {
	switch source {
	case SourceSelf, SourceGateway, SourceServer, SourceImported:
		return nil
	default:
		return fmt.Errorf("invalid source %q", source)
	}
}

func CreateHash(tenantID string, req CreateRequest) (string, error) {
	payload := map[string]any{
		"tenant_id":  tenantID,
		"journal_id": req.JournalID,
		"kind":       req.Kind,
		"title":      req.Title,
		"actor":      req.Actor,
		"source":     req.Source,
		"meta":       req.Meta,
		"labels":     req.Labels,
		"retention":  json.RawMessage(nullIfEmpty(req.Retention)),
	}
	return HashCanonical(payload)
}

func GenesisDocument(tenantID string, req CreateRequest, createdAt time.Time) map[string]any {
	return map[string]any{
		"tenant_id":  tenantID,
		"journal_id": req.JournalID,
		"kind":       req.Kind,
		"title":      req.Title,
		"actor":      req.Actor,
		"source":     req.Source,
		"meta":       req.Meta,
		"labels":     req.Labels,
		"retention":  json.RawMessage(nullIfEmpty(req.Retention)),
		"created_at": FormatTime(createdAt),
	}
}

func LabelsFromGenesis(raw json.RawMessage) ([]Label, error) {
	var doc struct {
		Meta   map[string]string `json:"meta"`
		Labels []Label           `json:"labels"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	labels := NormalizeLabels(append(LabelsFromMap(doc.Meta), doc.Labels...))
	if err := ValidateLabels(labels); err != nil {
		return nil, err
	}
	return labels, nil
}

func RequestHash(entries []EntryInput) (string, error) {
	return HashCanonical(entries)
}

func EntryHash(entry Entry) (string, error) {
	payload := map[string]any{
		"tenant_id":       entry.TenantID,
		"journal_id":      entry.JournalID,
		"seq":             entry.Seq,
		"entry_id":        entry.EntryID,
		"type":            entry.Type,
		"schema_version":  entry.SchemaVersion,
		"status":          entry.Status,
		"occurred_at":     FormatTime(entry.OccurredAt),
		"observed_at":     FormatTime(entry.ObservedAt),
		"actor_type":      entry.Actor.Type,
		"actor_id":        entry.Actor.ID,
		"source":          entry.Source,
		"parent_entry_id": entry.ParentEntryID,
		"correlation_id":  entry.CorrelationID,
		"subjects":        entry.Subjects,
		"summary":         json.RawMessage(nullIfEmpty(entry.Summary)),
		"artifact_refs":   entry.ArtifactRefs,
		"prev_hash":       entry.PrevHash,
	}
	return HashCanonical(payload)
}

func HashCanonical(v any) (string, error) {
	b, err := MarshalCanonical(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func MarshalCanonical(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalJSONRaw(b)
}

func CanonicalJSONRaw(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("multiple JSON values")
	}
	var out bytes.Buffer
	if err := writeCanonicalJSON(&out, v); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeCanonicalJSON(out *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if x {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		out.Write(b)
	case json.Number:
		if !json.Valid([]byte(x.String())) {
			return fmt.Errorf("invalid JSON number %q", x.String())
		}
		out.WriteString(x.String())
	case []any:
		out.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeCanonicalJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			keyRaw, err := json.Marshal(key)
			if err != nil {
				return err
			}
			out.Write(keyRaw)
			out.WriteByte(':')
			if err := writeCanonicalJSON(out, x[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical JSON type %T", v)
	}
	return nil
}

func FormatTime(t time.Time) string {
	return NormalizeTime(t).Format("2006-01-02T15:04:05.000Z")
}

func NormalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultLimit
	}
	if limit > MaxLimit {
		return MaxLimit
	}
	return limit
}

func SplitActor(actor string) (string, string, error) {
	if actor == "" {
		return "", "", nil
	}
	actorType, actorID, ok := strings.Cut(actor, ":")
	if !ok || actorType == "" || actorID == "" {
		return "", "", fmt.Errorf("invalid actor %q", actor)
	}
	return strings.ToLower(strings.TrimSpace(actorType)), strings.TrimSpace(actorID), nil
}

func normalizeDefault(value, def string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return def
	}
	return strings.ToLower(value)
}

func normalizeOptional(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToLower(value)
}

func nullIfEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func payloadTooLargef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrPayloadTooLarge, fmt.Sprintf(format, args...))
}
