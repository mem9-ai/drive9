package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	maxRuntimeFrameBytes = 1 << 20
	runtimeCancelTimeout = 20 * time.Second

	RuntimePersistenceFullRoot  = "full_root"
	RuntimePersistenceWorkspace = "workspace"
	RuntimeCapabilityWorkspace  = "workspace_persistence"
	RuntimeCapabilityFullRoot   = "full_root_persistence"
	runtimeRootFSUserUnion      = "user-union"
	runtimeRootFSWorkspace      = "workspace-mount"
	runtimeRootFSCapabilityV6   = "drive9_rootfs.user_union.extent.v6"
	runtimeWorkspaceCapability  = "drive9_workspace.mount.v1"
)

type RuntimeWorkspace struct {
	Root string `json:"root"`
	// LayerID selects one exact LayerFS view and requires workspace persistence.
	// The server rejects full_root persistence, including its omitted default,
	// when LayerID is set.
	LayerID     string `json:"layer_id,omitempty"`
	ReadOnly    bool   `json:"read_only,omitempty"`
	Persistence string `json:"persistence,omitempty"`
}

// ExecRequest describes one process invocation. Argv is not a shell string.
// Drive9 never retries the request automatically.
type ExecRequest struct {
	Argv      []string          `json:"argv"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Workspace RuntimeWorkspace  `json:"workspace"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
}

type ExecStarted struct {
	ExecutionID string
}

type ExecOptions struct {
	Stdout    io.Writer
	Stderr    io.Writer
	OnStarted func(ExecStarted) error
}

type ExecResult struct {
	ExecutionID string
	ExitCode    int
	DurationMS  int64
}

type RuntimeCapabilities struct {
	Version     int                         `json:"version"`
	Streaming   bool                        `json:"streaming"`
	SeparateIO  bool                        `json:"separate_stdout_stderr"`
	Cancel      bool                        `json:"cancel"`
	CancelScope string                      `json:"cancel_scope"`
	Detached    bool                        `json:"detached"`
	Replay      bool                        `json:"replay"`
	Providers   []RuntimeProviderCapability `json:"providers"`
}

type RuntimeProviderCapability struct {
	Provider   string                       `json:"provider"`
	Profiles   []string                     `json:"profiles"`
	Candidates []RuntimeCandidateCapability `json:"candidates,omitempty"`
}

type RuntimeCandidateCapability struct {
	Profile                  string                `json:"profile"`
	ExecutionClass           string                `json:"execution_class"`
	Persistence              string                `json:"persistence"`
	RootFS                   RuntimeRootFSIdentity `json:"rootfs"`
	Capabilities             map[string]bool       `json:"capabilities"`
	ProductionEligible       bool                  `json:"production_eligible"`
	BoundedSelectionEligible bool                  `json:"bounded_selection_eligible"`
}

type RuntimeRootFSIdentity struct {
	Driver            string `json:"driver"`
	CapabilityVersion string `json:"capability_version"`
	ConfigHash        string `json:"config_hash"`
	LowerImageDigest  string `json:"lower_image_digest"`
}

// RuntimeExecutionError is a terminal error frame returned by the server.
// A non-zero process exit is represented by ExecResult, not by this type.
type RuntimeExecutionError struct {
	ExecutionID string
	Code        string
	Message     string
}

func (e *RuntimeExecutionError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "runtime execution failed: " + e.Code
}

type RuntimeStartError struct{ *RuntimeExecutionError }

func (e *RuntimeStartError) Unwrap() error { return e.RuntimeExecutionError }

type RuntimeTimeoutError struct{ *RuntimeExecutionError }

func (e *RuntimeTimeoutError) Unwrap() error { return e.RuntimeExecutionError }

type RuntimeCanceledError struct{ *RuntimeExecutionError }

func (e *RuntimeCanceledError) Unwrap() error { return e.RuntimeExecutionError }

// RuntimeOutcomeUnknownError means the client did not receive and validate a
// terminal frame. The process may have run; callers must never infer failure
// or automatically resubmit from this error.
type RuntimeOutcomeUnknownError struct {
	ExecutionID string
	Cause       error
}

func (e *RuntimeOutcomeUnknownError) Error() string {
	if e.ExecutionID == "" {
		return fmt.Sprintf("runtime outcome is unknown: %v", e.Cause)
	}
	return fmt.Sprintf("runtime execution %s outcome is unknown: %v", e.ExecutionID, e.Cause)
}

func (e *RuntimeOutcomeUnknownError) Unwrap() error { return e.Cause }

func IsRuntimeOutcomeUnknown(err error) bool {
	var unknown *RuntimeOutcomeUnknownError
	return errors.As(err, &unknown)
}

type runtimeWireFrame struct {
	Type        string `json:"type"`
	ExecutionID string `json:"execution_id,omitempty"`
	DataBase64  string `json:"data_base64,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	DurationMS  *int64 `json:"duration_ms,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	Message     string `json:"message,omitempty"`
}

// Exec streams one execution. Context cancellation requests remote
// cancellation after a started frame has supplied the execution ID. It returns
// RuntimeCanceledError only when the server confirms that the process stopped
// and bounded workspace cleanup completed.
func (c *Client) Exec(ctx context.Context, input ExecRequest, options ExecOptions) (*ExecResult, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, newRuntimeStartError("", "invalid_request", fmt.Errorf("marshal runtime exec request: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/runtime/exec", bytes.NewReader(body))
	if err != nil {
		return nil, newRuntimeStartError("", "invalid_request", fmt.Errorf("create runtime exec request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return nil, runtimeUnknown("", fmt.Errorf("send runtime exec request: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, newRuntimeStartError("", "http_error", readRuntimeHTTPError(resp))
	}
	if mediaType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0]); mediaType != "application/x-ndjson" {
		return nil, runtimeUnknown("", fmt.Errorf("unexpected runtime content type %q", mediaType))
	}

	reader := bufio.NewReaderSize(resp.Body, 32<<10)
	executionID := ""
	started := false
	for {
		line, atEOF, readErr := readBoundedRuntimeLine(reader)
		if readErr != nil {
			return nil, c.runtimeObservationEnded(ctx, executionID, readErr)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			if atEOF {
				return nil, c.runtimeObservationEnded(ctx, executionID, io.ErrUnexpectedEOF)
			}
			continue
		}
		wire, decodeErr := decodeRuntimeWireFrame(line)
		if decodeErr != nil {
			return nil, c.runtimeObservationEnded(ctx, executionID, decodeErr)
		}
		if frameErr := validateRuntimeFrame(wire, started, executionID); frameErr != nil {
			return nil, c.runtimeObservationEnded(ctx, executionID, frameErr)
		}
		switch wire.Type {
		case "started":
			started = true
			executionID = wire.ExecutionID
			if options.OnStarted != nil {
				if err := options.OnStarted(ExecStarted{ExecutionID: executionID}); err != nil {
					c.cancelAfterLostObservation(executionID)
					return nil, runtimeUnknown(executionID, fmt.Errorf("runtime started callback: %w", err))
				}
			}
		case "stdout", "stderr":
			data, decodeErr := base64.StdEncoding.Strict().DecodeString(wire.DataBase64)
			if decodeErr != nil || len(data) == 0 {
				return nil, c.runtimeObservationEnded(ctx, executionID, errors.New("invalid runtime output encoding"))
			}
			writer := options.Stdout
			if wire.Type == "stderr" {
				writer = options.Stderr
			}
			if writer != nil {
				written, err := writer.Write(data)
				if err == nil && written != len(data) {
					err = io.ErrShortWrite
				}
				if err != nil {
					c.cancelAfterLostObservation(executionID)
					return nil, runtimeUnknown(executionID, fmt.Errorf("write runtime %s: %w", wire.Type, err))
				}
			}
		case "exit":
			return &ExecResult{ExecutionID: executionID, ExitCode: *wire.ExitCode, DurationMS: *wire.DurationMS}, nil
		case "error":
			return nil, runtimeTerminalError(wire)
		}
		if atEOF {
			return nil, c.runtimeObservationEnded(ctx, executionID, io.ErrUnexpectedEOF)
		}
	}
}

func (c *Client) runtimeObservationEnded(ctx context.Context, executionID string, cause error) error {
	if ctx.Err() != nil && executionID != "" {
		cancelCtx, cancel := context.WithTimeout(context.Background(), runtimeCancelTimeout)
		err := c.CancelRuntimeExecution(cancelCtx, executionID)
		cancel()
		if err == nil {
			return &RuntimeCanceledError{RuntimeExecutionError: &RuntimeExecutionError{
				ExecutionID: executionID, Code: "canceled", Message: "runtime execution was canceled and stopped",
			}}
		}
		return runtimeUnknown(executionID, errors.Join(cause, fmt.Errorf("confirm remote cancellation: %w", err)))
	}
	return runtimeUnknown(executionID, cause)
}

func (c *Client) cancelAfterLostObservation(executionID string) {
	if executionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeCancelTimeout)
	defer cancel()
	_ = c.CancelRuntimeExecution(ctx, executionID)
}

func (c *Client) GetRuntimeCapabilities(ctx context.Context) (*RuntimeCapabilities, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/runtime/capabilities", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readRuntimeHTTPError(resp)
	}
	var result RuntimeCapabilities
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode runtime capabilities: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode runtime capabilities: %w", err)
	}
	if err := validateRuntimeCapabilities(result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CancelRuntimeExecution asks the server to stop one active execution. A nil
// error means the provider confirmed that the process stopped and bounded
// workspace cleanup completed.
func (c *Client) CancelRuntimeExecution(ctx context.Context, executionID string) error {
	if !validRuntimeExecutionID(executionID) {
		return errors.New("invalid runtime execution id")
	}
	endpoint := c.baseURL + "/v1/runtime/executions/" + url.PathEscape(executionID) + "/cancel"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return runtimeUnknown(executionID, fmt.Errorf("send runtime cancel request: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		err := readRuntimeHTTPError(resp)
		var status *StatusError
		if errors.As(err, &status) && status.Code == "outcome_unknown" {
			return runtimeUnknown(executionID, err)
		}
		return err
	}
	var result struct {
		Canceled bool `json:"canceled"`
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || !result.Canceled {
		if err == nil {
			err = errors.New("server did not confirm cancellation")
		}
		return runtimeUnknown(executionID, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return runtimeUnknown(executionID, err)
	}
	return nil
}

func runtimeTerminalError(wire runtimeWireFrame) error {
	execution := &RuntimeExecutionError{ExecutionID: wire.ExecutionID, Code: wire.ErrorCode, Message: wire.Message}
	switch wire.ErrorCode {
	case "invalid_request", "unavailable", "start_failed":
		return &RuntimeStartError{RuntimeExecutionError: execution}
	case "timed_out":
		return &RuntimeTimeoutError{RuntimeExecutionError: execution}
	case "canceled":
		return &RuntimeCanceledError{RuntimeExecutionError: execution}
	case "outcome_unknown":
		return runtimeUnknown(wire.ExecutionID, execution)
	default:
		return execution
	}
}

func newRuntimeStartError(executionID, code string, cause error) error {
	return &RuntimeStartError{RuntimeExecutionError: &RuntimeExecutionError{
		ExecutionID: executionID, Code: code, Message: cause.Error(),
	}}
}

func decodeRuntimeWireFrame(line []byte) (runtimeWireFrame, error) {
	var frame runtimeWireFrame
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return runtimeWireFrame{}, fmt.Errorf("decode runtime frame: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return runtimeWireFrame{}, fmt.Errorf("decode runtime frame: %w", err)
	}
	return frame, nil
}

func validateRuntimeFrame(wire runtimeWireFrame, started bool, executionID string) error {
	switch wire.Type {
	case "started":
		if started || !validRuntimeExecutionID(wire.ExecutionID) || wire.DataBase64 != "" || wire.ExitCode != nil ||
			wire.DurationMS != nil || wire.ErrorCode != "" || wire.Message != "" {
			return errors.New("invalid runtime started frame")
		}
	case "stdout", "stderr":
		if !started || wire.ExecutionID != executionID || wire.DataBase64 == "" || wire.ExitCode != nil ||
			wire.DurationMS != nil || wire.ErrorCode != "" || wire.Message != "" {
			return errors.New("invalid runtime output frame")
		}
	case "exit":
		if !started || wire.ExecutionID != executionID || wire.ExitCode == nil || *wire.ExitCode < 0 || *wire.ExitCode > 255 || wire.DurationMS == nil || *wire.DurationMS < 0 ||
			wire.DataBase64 != "" || wire.ErrorCode != "" || wire.Message != "" {
			return errors.New("invalid runtime exit frame")
		}
	case "error":
		if started && wire.ExecutionID != executionID || !started && wire.ExecutionID != "" && !validRuntimeExecutionID(wire.ExecutionID) ||
			wire.ErrorCode == "" || wire.Message == "" || wire.ExitCode != nil || wire.DurationMS != nil || wire.DataBase64 != "" {
			return errors.New("invalid runtime error frame")
		}
	default:
		return fmt.Errorf("unknown runtime frame type %q", wire.Type)
	}
	return nil
}

func readBoundedRuntimeLine(reader *bufio.Reader) ([]byte, bool, error) {
	line := make([]byte, 0, 1024)
	for {
		fragment, err := reader.ReadSlice('\n')
		payloadBytes := len(fragment)
		if err == nil && payloadBytes > 0 {
			payloadBytes--
		}
		if len(line)+payloadBytes > maxRuntimeFrameBytes {
			return nil, false, fmt.Errorf("runtime frame exceeds %d bytes", maxRuntimeFrameBytes)
		}
		line = append(line, fragment...)
		switch err {
		case nil:
			return bytes.TrimSuffix(line, []byte{'\n'}), false, nil
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			return line, true, nil
		default:
			return nil, false, fmt.Errorf("read runtime frame: %w", err)
		}
	}
}

func validateRuntimeCapabilities(capabilities RuntimeCapabilities) error {
	if capabilities.Version != 1 || !capabilities.Streaming || !capabilities.SeparateIO {
		return errors.New("server does not support Runtime exec protocol version 1 with separate streaming output")
	}
	if capabilities.Cancel && capabilities.CancelScope != "active_execution" {
		return errors.New("server advertises an unsupported Runtime cancel scope")
	}
	if capabilities.Detached || capabilities.Replay {
		return errors.New("server advertises unsupported durable Runtime capabilities")
	}
	providers := make(map[string]struct{}, len(capabilities.Providers))
	for _, provider := range capabilities.Providers {
		if strings.TrimSpace(provider.Provider) == "" {
			return errors.New("server returned an invalid Runtime provider capability")
		}
		if _, exists := providers[provider.Provider]; exists {
			return errors.New("server returned a duplicate Runtime provider capability")
		}
		providers[provider.Provider] = struct{}{}
		profiles := make(map[string]struct{}, len(provider.Profiles))
		for _, profile := range provider.Profiles {
			if strings.TrimSpace(profile) == "" {
				return errors.New("server returned an invalid Runtime provider profile")
			}
			if _, exists := profiles[profile]; exists {
				return errors.New("server returned a duplicate Runtime provider profile")
			}
			profiles[profile] = struct{}{}
		}
		candidateProfiles := make(map[string]struct{}, len(provider.Candidates))
		for _, candidate := range provider.Candidates {
			if strings.TrimSpace(candidate.Profile) == "" || candidate.ExecutionClass != "linux-full" ||
				!validRuntimeSHA256Hex(candidate.RootFS.ConfigHash) ||
				!strings.HasPrefix(candidate.RootFS.LowerImageDigest, "sha256:") || !validRuntimeSHA256Hex(strings.TrimPrefix(candidate.RootFS.LowerImageDigest, "sha256:")) ||
				candidate.BoundedSelectionEligible && !candidate.ProductionEligible ||
				!validRuntimePersistenceCapability(candidate) {
				return errors.New("server returned an invalid Runtime candidate capability")
			}
			if _, exists := profiles[candidate.Profile]; !exists {
				return errors.New("server returned a Runtime candidate outside its provider profiles")
			}
			if _, exists := candidateProfiles[candidate.Profile]; exists {
				return errors.New("server returned a duplicate Runtime candidate capability")
			}
			candidateProfiles[candidate.Profile] = struct{}{}
		}
	}
	return nil
}

func validRuntimePersistenceCapability(candidate RuntimeCandidateCapability) bool {
	if !candidate.Capabilities[RuntimeCapabilityWorkspace] {
		return false
	}
	switch candidate.Persistence {
	case RuntimePersistenceFullRoot:
		return candidate.RootFS.Driver == runtimeRootFSUserUnion &&
			candidate.RootFS.CapabilityVersion == runtimeRootFSCapabilityV6 &&
			candidate.Capabilities[RuntimeCapabilityFullRoot]
	case RuntimePersistenceWorkspace:
		return candidate.RootFS.Driver == runtimeRootFSWorkspace &&
			candidate.RootFS.CapabilityVersion == runtimeWorkspaceCapability &&
			!candidate.Capabilities[RuntimeCapabilityFullRoot]
	default:
		return false
	}
}

func validRuntimeSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			if char < 'a' || char > 'f' {
				return false
			}
		}
	}
	return true
}

func readRuntimeHTTPError(resp *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1))
	if readErr != nil {
		return readErr
	}
	if len(body) > maxRuntimeFrameBytes {
		return &StatusError{StatusCode: resp.StatusCode, Message: "runtime error response exceeds size limit"}
	}
	var result struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if json.Unmarshal(body, &result) == nil && result.Error != "" {
		return &StatusError{StatusCode: resp.StatusCode, Message: result.Error, Code: result.Code}
	}
	return &StatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("HTTP %d", resp.StatusCode)}
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func runtimeUnknown(executionID string, cause error) error {
	return &RuntimeOutcomeUnknownError{ExecutionID: executionID, Cause: cause}
}

func validRuntimeExecutionID(id string) bool {
	if len(id) < 8 || len(id) > 80 {
		return false
	}
	for _, char := range id {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}
