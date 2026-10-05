package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const RuntimeProtocolVersion = "runtime.v1"

const (
	runtimeDurability           = "durable_per_operation"
	runtimeWorkspaceConsistency = "per_file_capture"
	runtimeExecutionEffects     = "workspace_and_external_no_automatic_replay"
)

const (
	maxRuntimeArtifactBytes      = 1 << 30
	maxRuntimeJSONResponseBytes  = 16 << 20
	maxRuntimeErrorResponseBytes = 1 << 20
	maxRuntimeEventCursor        = int64(1<<53 - 1)
)

var ErrRuntimeUnsupported = errors.New("drive9 Runtime is unavailable")

type RuntimeCapabilities struct {
	Enabled              bool          `json:"enabled"`
	ProtocolVersion      string        `json:"protocol_version"`
	Durability           string        `json:"durability"`
	WorkspaceConsistency string        `json:"workspace_consistency"`
	ExecutionEffects     string        `json:"execution_effects"`
	FileActions          []string      `json:"file_actions"`
	Profiles             []string      `json:"profiles"`
	DefaultProfile       string        `json:"default_profile"`
	SupportsReplay       bool          `json:"supports_arbitrary_command_replay"`
	SupportsSandboxCRUD  bool          `json:"supports_sandbox_crud"`
	SupportsMountedMode  bool          `json:"supports_mounted_mode"`
	Limits               RuntimeLimits `json:"limits"`
}

type RuntimeLimits struct {
	MaxTenantQueuedOperations    int   `json:"max_tenant_queued_operations"`
	MaxPrincipalQueuedOperations int   `json:"max_principal_queued_operations"`
	MaxTenantActiveOperations    int   `json:"max_tenant_active_operations"`
	MaxPrincipalActiveOperations int   `json:"max_principal_active_operations"`
	MaxWorkspaceEntries          int   `json:"max_workspace_entries"`
	MaxWorkspaceBytes            int64 `json:"max_workspace_bytes"`
	MaxSingleFileBytes           int64 `json:"max_single_file_bytes"`
	MaxPathBytes                 int   `json:"max_path_bytes"`
	MaxPathDepth                 int   `json:"max_path_depth"`
	MaxManifestBytes             int   `json:"max_manifest_bytes"`
	MaxReadResultBytes           int64 `json:"max_read_result_bytes"`
	MaxListEntries               int   `json:"max_list_entries"`
	MaxSearchFileBytes           int64 `json:"max_search_file_bytes"`
	MaxSearchResultBytes         int64 `json:"max_search_result_bytes"`
	MaxOperationResultBytes      int64 `json:"max_operation_result_bytes"`
	MaxLogBytes                  int64 `json:"max_log_bytes"`
	MaxChangedBytes              int64 `json:"max_changed_bytes"`
	MaxExecutionSeconds          int   `json:"max_execution_seconds"`
	CaptureDeadlineSeconds       int   `json:"capture_deadline_seconds"`
	CheckpointDeadlineSeconds    int   `json:"checkpoint_deadline_seconds"`
}

func (c RuntimeCapabilities) validate() error {
	if !c.Enabled || c.ProtocolVersion != RuntimeProtocolVersion {
		return fmt.Errorf("%w: incompatible protocol %q", ErrRuntimeUnsupported, c.ProtocolVersion)
	}
	if c.Durability != runtimeDurability || c.WorkspaceConsistency != runtimeWorkspaceConsistency ||
		c.ExecutionEffects != runtimeExecutionEffects || c.SupportsReplay || c.SupportsSandboxCRUD || c.SupportsMountedMode {
		return fmt.Errorf("%w: incompatible effect boundaries", ErrRuntimeUnsupported)
	}
	profiles := make(map[string]struct{}, len(c.Profiles))
	for _, profile := range c.Profiles {
		if strings.TrimSpace(profile) == "" {
			return fmt.Errorf("%w: missing Runtime profile", ErrRuntimeUnsupported)
		}
		profiles[profile] = struct{}{}
	}
	if strings.TrimSpace(c.DefaultProfile) == "" {
		return fmt.Errorf("%w: missing Runtime default profile", ErrRuntimeUnsupported)
	}
	if _, ok := profiles[c.DefaultProfile]; !ok {
		return fmt.Errorf("%w: Runtime default profile is not advertised", ErrRuntimeUnsupported)
	}
	if !c.Limits.valid() {
		return fmt.Errorf("%w: missing or invalid limits", ErrRuntimeUnsupported)
	}
	return nil
}

func (l RuntimeLimits) valid() bool {
	return l.MaxTenantQueuedOperations > 0 &&
		l.MaxPrincipalQueuedOperations > 0 &&
		l.MaxTenantActiveOperations > 0 &&
		l.MaxPrincipalActiveOperations > 0 &&
		l.MaxWorkspaceEntries > 0 &&
		l.MaxWorkspaceBytes > 0 &&
		l.MaxSingleFileBytes > 0 &&
		l.MaxPathBytes > 0 &&
		l.MaxPathDepth > 0 &&
		l.MaxManifestBytes > 0 &&
		l.MaxReadResultBytes > 0 &&
		l.MaxListEntries > 0 &&
		l.MaxSearchFileBytes > 0 &&
		l.MaxSearchResultBytes > 0 &&
		l.MaxOperationResultBytes > 0 &&
		l.MaxLogBytes > 0 &&
		l.MaxChangedBytes > 0 &&
		l.MaxExecutionSeconds > 0 &&
		l.CaptureDeadlineSeconds > 0 &&
		l.CheckpointDeadlineSeconds > 0 &&
		l.protocolSafe()
}

func (l RuntimeLimits) protocolSafe() bool {
	integerLimits := []int{
		l.MaxTenantQueuedOperations,
		l.MaxPrincipalQueuedOperations,
		l.MaxTenantActiveOperations,
		l.MaxPrincipalActiveOperations,
		l.MaxWorkspaceEntries,
		l.MaxPathBytes,
		l.MaxPathDepth,
		l.MaxManifestBytes,
		l.MaxListEntries,
		l.MaxExecutionSeconds,
		l.CaptureDeadlineSeconds,
		l.CheckpointDeadlineSeconds,
	}
	for _, value := range integerLimits {
		if int64(value) > maxRuntimeEventCursor {
			return false
		}
	}
	byteLimits := []int64{
		l.MaxWorkspaceBytes,
		l.MaxSingleFileBytes,
		l.MaxReadResultBytes,
		l.MaxSearchFileBytes,
		l.MaxSearchResultBytes,
		l.MaxOperationResultBytes,
		l.MaxChangedBytes,
	}
	for _, value := range byteLimits {
		if value > maxRuntimeEventCursor {
			return false
		}
	}
	return l.MaxLogBytes <= maxRuntimeArtifactBytes
}

func (c RuntimeCapabilities) supportsFileAction(action string) bool {
	for _, supported := range c.FileActions {
		if supported == action {
			return true
		}
	}
	return false
}

type RuntimeWorkspaceSource struct {
	Root string `json:"root"`
}

type RuntimeWorkspaceInput struct {
	ClientScopeKey string                 `json:"client_scope_key"`
	Source         RuntimeWorkspaceSource `json:"source"`
}

type RuntimeExecutionSpec struct {
	Argv             []string          `json:"argv,omitempty"`
	Shell            string            `json:"shell,omitempty"`
	WorkingDirectory string            `json:"working_directory,omitempty"`
	Environment      map[string]string `json:"environment,omitempty"`
	TimeoutSeconds   int               `json:"timeout_seconds,omitempty"`
}

type RuntimeExecutionRequest struct {
	WorkspaceRef string                 `json:"workspace_ref,omitempty"`
	Workspace    *RuntimeWorkspaceInput `json:"workspace,omitempty"`
	Profile      string                 `json:"profile,omitempty"`
	Execution    RuntimeExecutionSpec   `json:"execution"`
}

type RuntimeFileOperationSpec struct {
	Action       string `json:"action"`
	Path         string `json:"path"`
	DataBase64   string `json:"data_base64,omitempty"`
	ExpectedHash string `json:"expected_sha256,omitempty"`
	Pattern      string `json:"pattern,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

type RuntimeFileOperationRequest struct {
	WorkspaceRef string                   `json:"workspace_ref,omitempty"`
	Workspace    *RuntimeWorkspaceInput   `json:"workspace,omitempty"`
	Profile      string                   `json:"profile,omitempty"`
	Operation    RuntimeFileOperationSpec `json:"operation"`
}

type RuntimeOperation struct {
	ID               string          `json:"id"`
	Kind             string          `json:"kind"`
	WorkspaceRef     string          `json:"workspace_ref"`
	Profile          string          `json:"profile"`
	WorkspaceSeq     int64           `json:"workspace_seq"`
	State            string          `json:"state"`
	Attempt          int             `json:"attempt"`
	CancelRequested  bool            `json:"cancel_requested"`
	RecoveryRequired bool            `json:"recovery_required"`
	CleanupPending   bool            `json:"cleanup_pending"`
	Result           json.RawMessage `json:"result,omitempty"`
	Usage            RuntimeUsage    `json:"usage"`
	ErrorCode        string          `json:"error_code,omitempty"`
	ErrorMessage     string          `json:"error_message,omitempty"`
	CheckpointID     string          `json:"checkpoint_id,omitempty"`
	LogArtifactID    string          `json:"log_artifact_id,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
}

type RuntimeUsage struct {
	RuntimeMillis  int64 `json:"runtime_ms"`
	BytesScanned   int64 `json:"bytes_scanned"`
	EntriesScanned int64 `json:"entries_scanned"`
	ChangedBytes   int64 `json:"changed_bytes"`
	LogBytes       int64 `json:"log_bytes"`
}

func (o RuntimeOperation) Terminal() bool {
	switch o.State {
	case "succeeded", "failed", "canceled", "timed_out", "outcome_unknown":
		return true
	default:
		return false
	}
}

type RuntimeEvent struct {
	OperationID string          `json:"operation_id"`
	Cursor      int64           `json:"cursor"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type RuntimeRecovery struct {
	RecoveryID   string    `json:"recovery_id"`
	OperationID  string    `json:"operation_id"`
	WorkspaceRef string    `json:"workspace_ref"`
	State        string    `json:"state"`
	Action       string    `json:"action"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (c *Client) RuntimeCapabilities(ctx context.Context) (*RuntimeCapabilities, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/runtime/capabilities", nil)
	if err != nil {
		return nil, err
	}
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusServiceUnavailable {
		return nil, ErrRuntimeUnsupported
	}
	if response.StatusCode >= 300 {
		return nil, readRuntimeError(response)
	}
	var capabilities RuntimeCapabilities
	if err := decodeRuntimeJSONBody(response, &capabilities); err != nil {
		return nil, fmt.Errorf("decode Runtime capabilities: %w", err)
	}
	if err := capabilities.validate(); err != nil {
		return nil, err
	}
	return &capabilities, nil
}

func (c *Client) ensureRuntime(ctx context.Context) error {
	_, err := c.RuntimeCapabilities(ctx)
	return err
}

func (c *Client) SubmitRuntimeExecution(ctx context.Context, idempotencyKey string, input RuntimeExecutionRequest) (*RuntimeOperation, error) {
	return c.submitRuntimeOperation(ctx, "/v1/runtime/executions", idempotencyKey, input, "")
}

func (c *Client) SubmitRuntimeFileOperation(ctx context.Context, idempotencyKey string, input RuntimeFileOperationRequest) (*RuntimeOperation, error) {
	return c.submitRuntimeOperation(ctx, "/v1/runtime/file-operations", idempotencyKey, input, input.Operation.Action)
}

func (c *Client) submitRuntimeOperation(ctx context.Context, endpoint, idempotencyKey string, input any, requiredFileAction string) (*RuntimeOperation, error) {
	capabilities, err := c.RuntimeCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if requiredFileAction != "" && !capabilities.supportsFileAction(requiredFileAction) {
		return nil, fmt.Errorf("%w: file action %q is unavailable", ErrRuntimeUnsupported, requiredFileAction)
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("runtime Idempotency-Key is required")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	return decodeRuntimeOperation(response)
}

func (c *Client) GetRuntimeExecution(ctx context.Context, id string) (*RuntimeOperation, error) {
	return c.getRuntimeOperation(ctx, "executions", id)
}

func (c *Client) GetRuntimeFileOperation(ctx context.Context, id string) (*RuntimeOperation, error) {
	return c.getRuntimeOperation(ctx, "file-operations", id)
}

func (c *Client) getRuntimeOperation(ctx context.Context, collection, id string) (*RuntimeOperation, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/runtime/"+collection+"/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	return decodeRuntimeOperation(response)
}

func decodeRuntimeOperation(response *http.Response) (*RuntimeOperation, error) {
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return nil, readRuntimeError(response)
	}
	var operation RuntimeOperation
	if err := decodeRuntimeJSONBody(response, &operation); err != nil {
		return nil, fmt.Errorf("decode Runtime operation: %w", err)
	}
	return &operation, nil
}

func (c *Client) CancelRuntimeExecution(ctx context.Context, id string) (*RuntimeOperation, error) {
	return c.runtimeOperationAction(ctx, "executions", id, "cancel", "", nil)
}

func (c *Client) CancelRuntimeFileOperation(ctx context.Context, id string) (*RuntimeOperation, error) {
	return c.runtimeOperationAction(ctx, "file-operations", id, "cancel", "", nil)
}

func (c *Client) RecoverRuntimeExecution(ctx context.Context, id, idempotencyKey, action string) (*RuntimeRecovery, error) {
	return c.recoverRuntimeOperation(ctx, "executions", id, idempotencyKey, action)
}

func (c *Client) RecoverRuntimeFileOperation(ctx context.Context, id, idempotencyKey, action string) (*RuntimeRecovery, error) {
	return c.recoverRuntimeOperation(ctx, "file-operations", id, idempotencyKey, action)
}

func (c *Client) runtimeOperationAction(ctx context.Context, collection, id, action, idempotencyKey string, body any) (*RuntimeOperation, error) {
	raw, err := json.Marshal(body)
	if body == nil {
		raw = []byte("{}")
	}
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/runtime/"+collection+"/"+url.PathEscape(id)+"/"+action, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	return decodeRuntimeOperation(response)
}

func (c *Client) recoverRuntimeOperation(ctx context.Context, collection, id, idempotencyKey, action string) (*RuntimeRecovery, error) {
	if err := c.ensureRuntime(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("runtime recovery Idempotency-Key is required")
	}
	body, _ := json.Marshal(map[string]string{"action": action})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/runtime/"+collection+"/"+url.PathEscape(id)+"/recover", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	return decodeRuntimeRecovery(response)
}

func (c *Client) GetRuntimeRecovery(ctx context.Context, id string) (*RuntimeRecovery, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/runtime/recoveries/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	return decodeRuntimeRecovery(response)
}

func decodeRuntimeRecovery(response *http.Response) (*RuntimeRecovery, error) {
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return nil, readRuntimeError(response)
	}
	var recovery RuntimeRecovery
	if err := decodeRuntimeJSONBody(response, &recovery); err != nil {
		return nil, fmt.Errorf("decode Runtime recovery: %w", err)
	}
	return &recovery, nil
}

func (c *Client) WatchRuntimeEvents(ctx context.Context, collection, id string, after int64, onEvent func(RuntimeEvent) error) error {
	if collection != "executions" && collection != "file-operations" {
		return fmt.Errorf("unsupported Runtime operation collection %q", collection)
	}
	if after < 0 || after > maxRuntimeEventCursor {
		return fmt.Errorf("runtime event cursor must be between 0 and %d", maxRuntimeEventCursor)
	}
	for {
		last, err := c.watchRuntimeEventWindow(ctx, collection, id, after, onEvent)
		if last > after {
			after = last
		}
		if err != nil {
			if !retryableRuntimeWatchError(err) || ctx.Err() != nil {
				return err
			}
			if err := runtimeWatchDelay(ctx); err != nil {
				return err
			}
			continue
		}
		operation, err := c.getRuntimeOperation(ctx, collection, id)
		if err != nil {
			if !retryableRuntimeWatchError(err) || ctx.Err() != nil {
				return err
			}
			if err := runtimeWatchDelay(ctx); err != nil {
				return err
			}
			continue
		}
		if operation.Terminal() {
			return nil
		}
		if err := runtimeWatchDelay(ctx); err != nil {
			return err
		}
	}
}

type runtimeEventTransportError struct{ error }

func (e *runtimeEventTransportError) Unwrap() error { return e.error }

func retryableRuntimeWatchError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var transport *runtimeEventTransportError
	if errors.As(err, &transport) {
		return true
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= 500
	}
	var urlError *url.Error
	return errors.As(err, &urlError)
}

func runtimeWatchDelay(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) watchRuntimeEventWindow(ctx context.Context, collection, id string, after int64, onEvent func(RuntimeEvent) error) (int64, error) {
	values := url.Values{}
	if after > 0 {
		values.Set("after", strconv.FormatInt(after, 10))
	}
	endpoint := c.baseURL + "/v1/runtime/" + collection + "/" + url.PathEscape(id) + "/events"
	if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return after, err
	}
	response, err := c.do(request)
	if err != nil {
		return after, &runtimeEventTransportError{error: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return after, readRuntimeError(response)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event RuntimeEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			return after, fmt.Errorf("decode Runtime event: %w", err)
		}
		if event.OperationID != id {
			return after, fmt.Errorf("runtime event operation_id %q does not match %q", event.OperationID, id)
		}
		if event.Cursor < 1 || event.Cursor > maxRuntimeEventCursor {
			return after, fmt.Errorf("invalid Runtime event cursor %d", event.Cursor)
		}
		if event.Cursor <= after {
			continue
		}
		if onEvent != nil {
			if err := onEvent(event); err != nil {
				return after, err
			}
		}
		after = event.Cursor
	}
	if err := scanner.Err(); err != nil {
		return after, &runtimeEventTransportError{error: err}
	}
	return after, nil
}

func (c *Client) DownloadRuntimeArtifact(ctx context.Context, id string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/runtime/artifacts/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return nil, readRuntimeError(response)
	}
	if response.ContentLength < 0 {
		return nil, fmt.Errorf("runtime artifact response omitted Content-Length")
	}
	if response.ContentLength > maxRuntimeArtifactBytes {
		return nil, fmt.Errorf("runtime artifact exceeds %d bytes", maxRuntimeArtifactBytes)
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, maxRuntimeArtifactBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) > maxRuntimeArtifactBytes {
		return nil, fmt.Errorf("runtime artifact exceeds %d bytes", maxRuntimeArtifactBytes)
	}
	if int64(len(content)) != response.ContentLength {
		return nil, fmt.Errorf("runtime artifact Content-Length mismatch: declared %d, read %d", response.ContentLength, len(content))
	}
	expected := strings.ToLower(response.Header.Get("X-Content-SHA256"))
	if expected == "" {
		return nil, fmt.Errorf("runtime artifact response omitted X-Content-SHA256")
	}
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != expected {
		return nil, fmt.Errorf("runtime artifact checksum mismatch")
	}
	return content, nil
}

func decodeRuntimeJSONBody(response *http.Response, target any) error {
	if response.ContentLength > maxRuntimeJSONResponseBytes {
		return fmt.Errorf("response exceeds %d bytes", maxRuntimeJSONResponseBytes)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRuntimeJSONResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxRuntimeJSONResponseBytes {
		return fmt.Errorf("response exceeds %d bytes", maxRuntimeJSONResponseBytes)
	}
	if err := json.Unmarshal(body, target); err != nil {
		return err
	}
	return nil
}

func readRuntimeError(response *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRuntimeErrorResponseBytes+1))
	if err != nil {
		return &StatusError{StatusCode: response.StatusCode, Message: fmt.Sprintf("read Runtime error response: %v", err)}
	}
	if len(body) > maxRuntimeErrorResponseBytes {
		return &StatusError{StatusCode: response.StatusCode, Message: fmt.Sprintf("Runtime error response exceeds %d bytes", maxRuntimeErrorResponseBytes)}
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return readError(response)
}
