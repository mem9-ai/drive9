package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const (
	// DefaultManifestPageEntries is the Server default raw-row page limit.
	DefaultManifestPageEntries = 1000
	// MaxManifestPageEntries is the maximum raw-row page limit accepted by the Server.
	MaxManifestPageEntries = 10000
	// MaxBatchMkdirItems is the maximum number of items accepted by BatchMkdirCtx.
	MaxBatchMkdirItems = 128
	// MaxBatchChmodItems is the maximum number of items accepted by BatchChmodCtx.
	MaxBatchChmodItems = 128

	maxManifestResponseBytes      int64 = 64 << 20
	maxBatchMutationBodyBytes           = 1 << 20
	maxBatchMutationResponseBytes       = 4 << 20
)

// ManifestEntryType is the independently encoded Target object type.
type ManifestEntryType string

const (
	ManifestEntryRegular   ManifestEntryType = "regular"
	ManifestEntryDirectory ManifestEntryType = "directory"
	ManifestEntrySymlink   ManifestEntryType = "symlink"
)

// ManifestIdentityKind describes whether resource identity is inode-backed or legacy.
type ManifestIdentityKind string

const (
	ManifestIdentityInode        ManifestIdentityKind = "inode"
	ManifestIdentityLegacyDentry ManifestIdentityKind = "legacy_dentry"
)

// ManifestEntry is one strict Target Flat Manifest record.
type ManifestEntry struct {
	Path             string
	Type             ManifestEntryType
	MetadataComplete bool
	IdentityKind     ManifestIdentityKind
	Mode             *uint32
	Size             int64
	ChecksumSHA256   *string
	Revision         *int64
	ResourceID       string
	Nlink            uint32
}

// ManifestPage is one opaque-cursor Target Flat Manifest page.
type ManifestPage struct {
	Entries    []ManifestEntry
	NextCursor string
	Done       bool
	// ResponseBytes is the exact decoded HTTP body size for bounded progress accounting.
	ResponseBytes int64
}

// BatchMkdirItem is one explicit non-recursive directory creation request.
type BatchMkdirItem struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode"`
}

// BatchMkdirResult is one ordered per-path BatchMkdir result.
type BatchMkdirResult struct {
	Path       string
	Status     int
	Error      *string
	Created    *bool
	ResourceID *string
}

// OK reports whether the per-path BatchMkdir result is successful.
func (r BatchMkdirResult) OK() bool {
	return r.Status >= http.StatusOK && r.Status < http.StatusMultipleChoices && r.Error == nil
}

// BatchChmodItem is one identity-fenced chmod request.
type BatchChmodItem struct {
	Path               string `json:"path"`
	Mode               uint32 `json:"mode"`
	ExpectedResourceID string `json:"expected_resource_id"`
	ExpectedRevision   *int64 `json:"expected_revision,omitempty"`
}

// BatchChmodResult is one ordered per-path BatchChmod result.
type BatchChmodResult struct {
	Path       string
	Status     int
	Error      *string
	ResourceID *string
	Revision   *int64
	Mode       *uint32
}

// OK reports whether the per-path BatchChmod result is successful.
func (r BatchChmodResult) OK() bool {
	return r.Status >= http.StatusOK && r.Status < http.StatusMultipleChoices && r.Error == nil
}

// ManifestPageCtx returns one strictly validated Target Flat Manifest page.
func (c *Client) ManifestPageCtx(ctx context.Context, prefix, cursor string, limit int) (ManifestPage, error) {
	if prefix == "" {
		prefix = "/"
	}
	canonical, err := pathutil.CanonicalizeDir(prefix)
	if err != nil || canonical != prefix {
		return ManifestPage{}, fmt.Errorf("manifest: prefix must be a canonical directory path")
	}
	if limit == 0 {
		limit = DefaultManifestPageEntries
	}
	if limit < 1 || limit > MaxManifestPageEntries {
		return ManifestPage{}, fmt.Errorf("manifest: limit %d is outside 1..%d", limit, MaxManifestPageEntries)
	}
	query := url.Values{}
	query.Set("prefix", prefix)
	query.Set("limit", fmt.Sprintf("%d", limit))
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/migration/manifest?"+query.Encode(), nil)
	if err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: create request: %w", err)
	}
	resp, err := c.do(req)
	if err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return ManifestPage{}, fmt.Errorf("manifest: %w", readError(resp))
	}
	body, err := readBoundedResponse(resp.Body, maxManifestResponseBytes)
	if err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: %w", err)
	}
	page, err := decodeManifestPage(body)
	if err != nil {
		return ManifestPage{}, err
	}
	page.ResponseBytes = int64(len(body))
	if len(page.Entries) > limit {
		return ManifestPage{}, fmt.Errorf("manifest: got %d entries for limit %d", len(page.Entries), limit)
	}
	if !page.Done && (page.NextCursor == "" || page.NextCursor == cursor) {
		return ManifestPage{}, fmt.Errorf("manifest: non-final cursor did not advance")
	}
	if page.Done && page.NextCursor != "" {
		return ManifestPage{}, fmt.Errorf("manifest: final page has a next cursor")
	}
	seen := make(map[string]struct{}, len(page.Entries))
	for i := range page.Entries {
		if err := validateManifestEntry(page.Entries[i]); err != nil {
			return ManifestPage{}, fmt.Errorf("manifest: entry[%d]: %w", i, err)
		}
		if _, exists := seen[page.Entries[i].Path]; exists {
			return ManifestPage{}, fmt.Errorf("manifest: duplicate page path %q", page.Entries[i].Path)
		}
		seen[page.Entries[i].Path] = struct{}{}
	}
	return page, nil
}

// BatchMkdirCtx creates or adopts a bounded ordered set of directories.
func (c *Client) BatchMkdirCtx(ctx context.Context, items []BatchMkdirItem) ([]BatchMkdirResult, error) {
	if err := validateBatchMkdirItems(items); err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		Items []BatchMkdirItem `json:"items"`
	}{Items: items})
	if err != nil {
		return nil, fmt.Errorf("batch mkdir: encode request: %w", err)
	}
	if len(body) > maxBatchMutationBodyBytes {
		return nil, fmt.Errorf("batch mkdir: request body exceeds %d bytes", maxBatchMutationBodyBytes)
	}
	response, err := c.doBatchMutation(ctx, "/v1/fs:batch-mkdir", body)
	if err != nil {
		return nil, fmt.Errorf("batch mkdir: %w", err)
	}
	rawResults, err := decodeResultEnvelope(response, MaxBatchMkdirItems)
	if err != nil {
		return nil, fmt.Errorf("batch mkdir: %w", err)
	}
	if len(rawResults) != len(items) {
		return nil, fmt.Errorf("batch mkdir: got %d results for %d items", len(rawResults), len(items))
	}
	results := make([]BatchMkdirResult, len(rawResults))
	for i := range rawResults {
		result, err := decodeBatchMkdirResult(rawResults[i])
		if err != nil {
			return nil, fmt.Errorf("batch mkdir: result[%d]: %w", i, err)
		}
		if result.Path != items[i].Path {
			return nil, fmt.Errorf("batch mkdir: result[%d] path = %q, want %q", i, result.Path, items[i].Path)
		}
		results[i] = result
	}
	return results, nil
}

// BatchChmodCtx conditionally updates a bounded ordered set of modes.
func (c *Client) BatchChmodCtx(ctx context.Context, items []BatchChmodItem) ([]BatchChmodResult, error) {
	if err := validateBatchChmodItems(items); err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		Items []BatchChmodItem `json:"items"`
	}{Items: items})
	if err != nil {
		return nil, fmt.Errorf("batch chmod: encode request: %w", err)
	}
	if len(body) > maxBatchMutationBodyBytes {
		return nil, fmt.Errorf("batch chmod: request body exceeds %d bytes", maxBatchMutationBodyBytes)
	}
	response, err := c.doBatchMutation(ctx, "/v1/fs:batch-chmod", body)
	if err != nil {
		return nil, fmt.Errorf("batch chmod: %w", err)
	}
	rawResults, err := decodeResultEnvelope(response, MaxBatchChmodItems)
	if err != nil {
		return nil, fmt.Errorf("batch chmod: %w", err)
	}
	if len(rawResults) != len(items) {
		return nil, fmt.Errorf("batch chmod: got %d results for %d items", len(rawResults), len(items))
	}
	results := make([]BatchChmodResult, len(rawResults))
	for i := range rawResults {
		result, err := decodeBatchChmodResult(rawResults[i])
		if err != nil {
			return nil, fmt.Errorf("batch chmod: result[%d]: %w", i, err)
		}
		if result.Path != items[i].Path {
			return nil, fmt.Errorf("batch chmod: result[%d] path = %q, want %q", i, result.Path, items[i].Path)
		}
		results[i] = result
	}
	return results, nil
}

func (c *Client) doBatchMutation(ctx context.Context, endpoint string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, readError(resp)
	}
	return readBoundedResponse(resp.Body, maxBatchMutationResponseBytes)
}

func validateBatchMkdirItems(items []BatchMkdirItem) error {
	if len(items) == 0 || len(items) > MaxBatchMkdirItems {
		return fmt.Errorf("batch mkdir: item count %d is outside 1..%d", len(items), MaxBatchMkdirItems)
	}
	seen := make(map[string]struct{}, len(items))
	for i := range items {
		canonical, err := pathutil.CanonicalizeDir(items[i].Path)
		if err != nil || canonical != items[i].Path {
			return fmt.Errorf("batch mkdir: item[%d] path must be canonical directory path", i)
		}
		if items[i].Mode > 0o7777 {
			return fmt.Errorf("batch mkdir: item[%d] mode exceeds 07777", i)
		}
		if _, exists := seen[items[i].Path]; exists {
			return fmt.Errorf("batch mkdir: duplicate path %q", items[i].Path)
		}
		seen[items[i].Path] = struct{}{}
	}
	return nil
}

func validateBatchChmodItems(items []BatchChmodItem) error {
	if len(items) == 0 || len(items) > MaxBatchChmodItems {
		return fmt.Errorf("batch chmod: item count %d is outside 1..%d", len(items), MaxBatchChmodItems)
	}
	seen := make(map[string]struct{}, len(items))
	for i := range items {
		if err := validateCanonicalObjectPath(items[i].Path); err != nil {
			return fmt.Errorf("batch chmod: item[%d]: %w", i, err)
		}
		if items[i].Mode > 0o7777 {
			return fmt.Errorf("batch chmod: item[%d] mode exceeds 07777", i)
		}
		if items[i].ExpectedResourceID == "" {
			return fmt.Errorf("batch chmod: item[%d] requires expected resource ID", i)
		}
		if pathutil.IsDir(items[i].Path) {
			if items[i].ExpectedRevision != nil && *items[i].ExpectedRevision <= 0 {
				return fmt.Errorf("batch chmod: item[%d] directory revision must be positive", i)
			}
		} else if items[i].ExpectedRevision == nil || *items[i].ExpectedRevision <= 0 {
			return fmt.Errorf("batch chmod: item[%d] non-directory requires positive revision", i)
		}
		if _, exists := seen[items[i].Path]; exists {
			return fmt.Errorf("batch chmod: duplicate path %q", items[i].Path)
		}
		seen[items[i].Path] = struct{}{}
	}
	return nil
}

func decodeManifestPage(body []byte) (ManifestPage, error) {
	fields, err := decodeJSONObject(body)
	if err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: decode response: %w", err)
	}
	entriesRaw, err := requiredJSONField(fields, "entries")
	if err != nil || isJSONNull(entriesRaw) {
		return ManifestPage{}, fmt.Errorf("manifest: entries must be a non-null array")
	}
	var entryValues []json.RawMessage
	if err := json.Unmarshal(entriesRaw, &entryValues); err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: decode entries: %w", err)
	}
	page := ManifestPage{Entries: make([]ManifestEntry, len(entryValues))}
	for i := range entryValues {
		entry, err := decodeManifestEntry(entryValues[i])
		if err != nil {
			return ManifestPage{}, fmt.Errorf("manifest: entry[%d]: %w", i, err)
		}
		page.Entries[i] = entry
	}
	nextCursor, err := decodeNullableField[string](fields, "next_cursor")
	if err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: %w", err)
	}
	if nextCursor != nil {
		page.NextCursor = *nextCursor
	}
	if err := decodeRequiredField(fields, "done", &page.Done); err != nil {
		return ManifestPage{}, fmt.Errorf("manifest: %w", err)
	}
	return page, nil
}

func decodeManifestEntry(body []byte) (ManifestEntry, error) {
	fields, err := decodeJSONObject(body)
	if err != nil {
		return ManifestEntry{}, err
	}
	var entry ManifestEntry
	if err := decodeRequiredField(fields, "path", &entry.Path); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "type", &entry.Type); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "metadata_complete", &entry.MetadataComplete); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "identity_kind", &entry.IdentityKind); err != nil {
		return ManifestEntry{}, err
	}
	if entry.Mode, err = decodeNullableField[uint32](fields, "mode"); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "size", &entry.Size); err != nil {
		return ManifestEntry{}, err
	}
	if entry.ChecksumSHA256, err = decodeNullableField[string](fields, "checksum_sha256"); err != nil {
		return ManifestEntry{}, err
	}
	if entry.Revision, err = decodeNullableField[int64](fields, "revision"); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "resource_id", &entry.ResourceID); err != nil {
		return ManifestEntry{}, err
	}
	if err := decodeRequiredField(fields, "nlink", &entry.Nlink); err != nil {
		return ManifestEntry{}, err
	}
	return entry, nil
}

func validateManifestEntry(entry ManifestEntry) error {
	switch entry.Type {
	case ManifestEntryRegular, ManifestEntrySymlink:
		canonical, err := pathutil.Canonicalize(entry.Path)
		if err != nil || canonical != entry.Path || entry.Path == "/" {
			return fmt.Errorf("path %q is not canonical for %s", entry.Path, entry.Type)
		}
	case ManifestEntryDirectory:
		canonical, err := pathutil.CanonicalizeDir(entry.Path)
		if err != nil || canonical != entry.Path || entry.Path == "/" {
			return fmt.Errorf("path %q is not canonical for directory", entry.Path)
		}
	default:
		return fmt.Errorf("unsupported type %q", entry.Type)
	}
	if entry.IdentityKind != ManifestIdentityInode && entry.IdentityKind != ManifestIdentityLegacyDentry {
		return fmt.Errorf("unsupported identity kind %q", entry.IdentityKind)
	}
	if entry.Mode != nil && *entry.Mode > 0o7777 {
		return fmt.Errorf("mode exceeds 07777")
	}
	if entry.Size < 0 {
		return fmt.Errorf("negative size")
	}
	if entry.ChecksumSHA256 != nil {
		if err := validateWholeChecksumSHA256(*entry.ChecksumSHA256); err != nil {
			return err
		}
	}
	if entry.Revision != nil && *entry.Revision <= 0 {
		return fmt.Errorf("revision must be positive when present")
	}
	if entry.ResourceID == "" || entry.Nlink == 0 {
		return fmt.Errorf("resource identity and nlink are required")
	}
	if entry.Type == ManifestEntryDirectory && (entry.Size != 0 || entry.ChecksumSHA256 != nil || entry.Nlink != 2) {
		return fmt.Errorf("directory size/checksum/nlink contract mismatch")
	}
	if entry.IdentityKind == ManifestIdentityLegacyDentry {
		if entry.Type != ManifestEntryDirectory || entry.MetadataComplete || entry.Mode != nil || entry.Revision != nil {
			return fmt.Errorf("legacy identity shape mismatch")
		}
		return nil
	}
	if entry.MetadataComplete {
		if entry.Mode == nil || entry.Revision == nil {
			return fmt.Errorf("complete inode metadata is missing mode or revision")
		}
		if entry.Type != ManifestEntryDirectory && entry.ChecksumSHA256 == nil {
			return fmt.Errorf("complete file metadata is missing checksum")
		}
	}
	return nil
}

func decodeResultEnvelope(body []byte, limit int) ([]json.RawMessage, error) {
	fields, err := decodeJSONObject(body)
	if err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	raw, err := requiredJSONField(fields, "results")
	if err != nil || isJSONNull(raw) {
		return nil, fmt.Errorf("results must be a non-null array")
	}
	var results []json.RawMessage
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("decode results: %w", err)
	}
	if len(results) > limit {
		return nil, fmt.Errorf("got %d results, limit %d", len(results), limit)
	}
	return results, nil
}

func decodeBatchMkdirResult(body []byte) (BatchMkdirResult, error) {
	fields, err := decodeJSONObject(body)
	if err != nil {
		return BatchMkdirResult{}, err
	}
	var result BatchMkdirResult
	if err := decodeRequiredField(fields, "path", &result.Path); err != nil {
		return BatchMkdirResult{}, err
	}
	if err := decodeRequiredField(fields, "status", &result.Status); err != nil {
		return BatchMkdirResult{}, err
	}
	if result.Error, err = decodeNullableField[string](fields, "error"); err != nil {
		return BatchMkdirResult{}, err
	}
	if result.Created, err = decodeNullableField[bool](fields, "created"); err != nil {
		return BatchMkdirResult{}, err
	}
	if result.ResourceID, err = decodeNullableField[string](fields, "resource_id"); err != nil {
		return BatchMkdirResult{}, err
	}
	if err := validateBatchResultShape(result.Status, result.Error); err != nil {
		return BatchMkdirResult{}, err
	}
	if result.OK() && result.Created == nil {
		return BatchMkdirResult{}, fmt.Errorf("successful result is missing created")
	}
	return result, nil
}

func decodeBatchChmodResult(body []byte) (BatchChmodResult, error) {
	fields, err := decodeJSONObject(body)
	if err != nil {
		return BatchChmodResult{}, err
	}
	var result BatchChmodResult
	if err := decodeRequiredField(fields, "path", &result.Path); err != nil {
		return BatchChmodResult{}, err
	}
	if err := decodeRequiredField(fields, "status", &result.Status); err != nil {
		return BatchChmodResult{}, err
	}
	if result.Error, err = decodeNullableField[string](fields, "error"); err != nil {
		return BatchChmodResult{}, err
	}
	if result.ResourceID, err = decodeNullableField[string](fields, "resource_id"); err != nil {
		return BatchChmodResult{}, err
	}
	if result.Revision, err = decodeNullableField[int64](fields, "revision"); err != nil {
		return BatchChmodResult{}, err
	}
	if result.Mode, err = decodeNullableField[uint32](fields, "mode"); err != nil {
		return BatchChmodResult{}, err
	}
	if err := validateBatchResultShape(result.Status, result.Error); err != nil {
		return BatchChmodResult{}, err
	}
	if result.OK() && (result.ResourceID == nil || *result.ResourceID == "" || result.Revision == nil || *result.Revision <= 0 || result.Mode == nil || *result.Mode > 0o7777) {
		return BatchChmodResult{}, fmt.Errorf("successful result has incomplete identity, revision, or mode")
	}
	return result, nil
}

func validateBatchResultShape(status int, message *string) error {
	if status < 100 || status > 599 {
		return fmt.Errorf("invalid status %d", status)
	}
	success := status >= http.StatusOK && status < http.StatusMultipleChoices
	if success && message != nil {
		return fmt.Errorf("successful result has error")
	}
	if !success && (message == nil || *message == "") {
		return fmt.Errorf("failed result lacks error")
	}
	return nil
}

func validateCanonicalObjectPath(value string) error {
	var canonical string
	var err error
	if pathutil.IsDir(value) {
		canonical, err = pathutil.CanonicalizeDir(value)
	} else {
		canonical, err = pathutil.Canonicalize(value)
	}
	if err != nil || canonical != value {
		return fmt.Errorf("path %q is not canonical", value)
	}
	return nil
}

func decodeJSONObject(body []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	return fields, nil
}

func requiredJSONField(fields map[string]json.RawMessage, name string) (json.RawMessage, error) {
	raw, exists := fields[name]
	if !exists {
		return nil, fmt.Errorf("missing required field %q", name)
	}
	return raw, nil
}

func decodeRequiredField(fields map[string]json.RawMessage, name string, target any) error {
	raw, err := requiredJSONField(fields, name)
	if err != nil {
		return err
	}
	if isJSONNull(raw) {
		return fmt.Errorf("required field %q is null", name)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode field %q: %w", name, err)
	}
	return nil
}

func decodeNullableField[T any](fields map[string]json.RawMessage, name string) (*T, error) {
	raw, err := requiredJSONField(fields, name)
	if err != nil {
		return nil, err
	}
	if isJSONNull(raw) {
		return nil, nil
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("decode field %q: %w", name, err)
	}
	return &value, nil
}

func isJSONNull(raw []byte) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func readBoundedResponse(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return body, nil
}
