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
	"strconv"
	"strings"
	"time"
)

const (
	maxRuntimeFrameBytes = 1 << 20
	runtimeCancelTimeout = 20 * time.Second

	// RuntimeSandboxNew and RuntimeSandboxWarm describe whether a command got
	// a fresh sandbox or reused the previous one for the same workspace.
	RuntimeSandboxNew  = "new"
	RuntimeSandboxWarm = "warm"
)

// ErrRuntimeNotEnabled means the server does not offer remote exec.
var ErrRuntimeNotEnabled = errors.New("remote exec is not enabled on this server")

// RuntimeWorkspace selects where the command runs. The whole drive is at
// /workspace; the command starts in /workspace/<Root>.
type RuntimeWorkspace struct {
	// Root is the Drive9 directory, for example "/repo". Empty means "/".
	Root string `json:"root,omitempty"`
	// LayerID selects one LayerFS view of the workspace.
	LayerID  string `json:"layer_id,omitempty"`
	ReadOnly bool   `json:"read_only,omitempty"`
}

// ExecRequest describes one command. Set either Argv or Command: Argv is run
// as-is, Command is a shell command line run with /bin/sh -c. Drive9 never
// retries a request automatically.
type ExecRequest struct {
	Argv    []string `json:"argv"`
	Command string   `json:"-"`
	// Cwd is relative to the Root directory unless absolute.
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Workspace RuntimeWorkspace  `json:"workspace"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
	// Fresh starts in a new sandbox instead of reusing the warm one.
	Fresh bool `json:"fresh,omitempty"`
}

// ExecStarted is reported once the command is running.
type ExecStarted struct {
	ExecutionID string
	// Cwd is the command's working directory inside the sandbox.
	Cwd string
	// Sandbox is RuntimeSandboxNew or RuntimeSandboxWarm.
	Sandbox string
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
	Sandbox     string
}

// RuntimeCapabilities describes the server's remote exec support.
type RuntimeCapabilities struct {
	Version            int    `json:"version"`
	Provider           string `json:"provider"`
	Persistence        string `json:"persistence"`
	WorkspacePath      string `json:"workspace_path"`
	User               string `json:"user"`
	Streaming          bool   `json:"streaming"`
	Cancel             bool   `json:"cancel"`
	HeartbeatMS        int64  `json:"heartbeat_ms"`
	DefaultTimeoutMS   int64  `json:"default_timeout_ms"`
	MaxTimeoutMS       int64  `json:"max_timeout_ms"`
	MaxOutputBytes     int64  `json:"max_output_bytes"`
	MaxActivePerTenant int    `json:"max_active_per_tenant"`
	WarmSandboxIdleMS  int64  `json:"warm_sandbox_idle_ms"`
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
	return "remote exec failed: " + e.Code
}

type RuntimeStartError struct{ *RuntimeExecutionError }

func (e *RuntimeStartError) Unwrap() error { return e.RuntimeExecutionError }

type RuntimeTimeoutError struct{ *RuntimeExecutionError }

func (e *RuntimeTimeoutError) Unwrap() error { return e.RuntimeExecutionError }

type RuntimeCanceledError struct{ *RuntimeExecutionError }

func (e *RuntimeCanceledError) Unwrap() error { return e.RuntimeExecutionError }

// RuntimeBusyError means the owner or server is at its concurrency limit. No
// command was started; it is safe to retry after RetryAfter.
type RuntimeBusyError struct {
	Message    string
	RetryAfter time.Duration
}

func (e *RuntimeBusyError) Error() string { return e.Message }

func IsRuntimeBusy(err error) bool {
	var busy *RuntimeBusyError
	return errors.As(err, &busy)
}

// RuntimeOutcomeUnknownError means the client did not receive and validate a
// terminal frame. The command may have run; callers must never infer failure
// or automatically resubmit from this error.
type RuntimeOutcomeUnknownError struct {
	ExecutionID string
	Cause       error
}

func (e *RuntimeOutcomeUnknownError) Error() string {
	if e.ExecutionID == "" {
		return fmt.Sprintf("remote exec outcome is unknown: %v", e.Cause)
	}
	return fmt.Sprintf("remote exec %s outcome is unknown: %v", e.ExecutionID, e.Cause)
}

func (e *RuntimeOutcomeUnknownError) Unwrap() error { return e.Cause }

func IsRuntimeOutcomeUnknown(err error) bool {
	var unknown *RuntimeOutcomeUnknownError
	return errors.As(err, &unknown)
}

type runtimeWireFrame struct {
	Type        string `json:"type"`
	ExecutionID string `json:"execution_id,omitempty"`
	Cwd         string `json:"cwd,omitempty"`
	Sandbox     string `json:"sandbox,omitempty"`
	DataBase64  string `json:"data_base64,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"`
	DurationMS  *int64 `json:"duration_ms,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	Message     string `json:"message,omitempty"`
}

// Exec runs one command and streams its output. Context cancellation requests
// remote cancellation once the command has started; it returns
// RuntimeCanceledError only when the server confirms the command stopped and
// the workspace drained.
func (c *Client) Exec(ctx context.Context, input ExecRequest, options ExecOptions) (*ExecResult, error) {
	if input.Command != "" {
		if len(input.Argv) > 0 {
			return nil, newRuntimeStartError("", "invalid_request", errors.New("set either Argv or Command, not both"))
		}
		input.Argv = []string{"/bin/sh", "-c", input.Command}
	}
	if len(input.Argv) == 0 {
		return nil, newRuntimeStartError("", "invalid_request", errors.New("a command is required"))
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, newRuntimeStartError("", "invalid_request", fmt.Errorf("marshal exec request: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/runtime/exec", bytes.NewReader(body))
	if err != nil {
		return nil, newRuntimeStartError("", "invalid_request", fmt.Errorf("create exec request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return nil, runtimeUnknown("", fmt.Errorf("send exec request: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, runtimeHTTPStartError(resp)
	}
	if mediaType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0]); mediaType != "application/x-ndjson" {
		return nil, runtimeUnknown("", fmt.Errorf("unexpected exec content type %q", mediaType))
	}

	reader := bufio.NewReaderSize(resp.Body, 32<<10)
	executionID, sandbox := "", ""
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
			executionID, sandbox = wire.ExecutionID, wire.Sandbox
			if options.OnStarted != nil {
				if err := options.OnStarted(ExecStarted{ExecutionID: executionID, Cwd: wire.Cwd, Sandbox: wire.Sandbox}); err != nil {
					c.cancelAfterLostObservation(executionID)
					return nil, runtimeUnknown(executionID, fmt.Errorf("exec started callback: %w", err))
				}
			}
		case "stdout", "stderr":
			data, decodeErr := base64.StdEncoding.Strict().DecodeString(wire.DataBase64)
			if decodeErr != nil || len(data) == 0 {
				return nil, c.runtimeObservationEnded(ctx, executionID, errors.New("invalid exec output encoding"))
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
					return nil, runtimeUnknown(executionID, fmt.Errorf("write %s: %w", wire.Type, err))
				}
			}
		case "exit":
			return &ExecResult{ExecutionID: executionID, ExitCode: *wire.ExitCode, DurationMS: *wire.DurationMS, Sandbox: sandbox}, nil
		case "error":
			return nil, runtimeTerminalError(wire, started)
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
				ExecutionID: executionID, Code: "canceled", Message: "the command was canceled",
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

// GetRuntimeCapabilities reports the server's remote exec support. It returns
// ErrRuntimeNotEnabled when the server does not offer it.
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
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrRuntimeNotEnabled
	}
	if resp.StatusCode >= 300 {
		return nil, readRuntimeHTTPError(resp)
	}
	var result RuntimeCapabilities
	// Capabilities may grow; unknown fields are ignored.
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode runtime capabilities: %w", err)
	}
	if result.Version < 2 || !result.Streaming {
		return nil, errors.New("server runtime is too old for this client; upgrade the server")
	}
	return &result, nil
}

// CancelRuntimeExecution asks the server to stop one active execution. A nil
// error means the server confirmed the command stopped and the workspace
// drained.
func (c *Client) CancelRuntimeExecution(ctx context.Context, executionID string) error {
	if !validRuntimeExecutionID(executionID) {
		return errors.New("invalid execution id")
	}
	endpoint := c.baseURL + "/v1/runtime/executions/" + url.PathEscape(executionID) + "/cancel"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.doNoRedirect(req)
	if err != nil {
		return runtimeUnknown(executionID, fmt.Errorf("send cancel request: %w", err))
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
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1)).Decode(&result); err != nil || !result.Canceled {
		if err == nil {
			err = errors.New("server did not confirm cancellation")
		}
		return runtimeUnknown(executionID, err)
	}
	return nil
}

// runtimeHTTPStartError maps a non-streaming response. Only a 4xx rejection
// from the server proves the command never ran; a 5xx (including a gateway
// 502/504 after the server may have accepted the request) is unknown.
func runtimeHTTPStartError(resp *http.Response) error {
	if resp.StatusCode == http.StatusNotFound {
		return ErrRuntimeNotEnabled
	}
	err := readRuntimeHTTPError(resp)
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout {
		return runtimeUnknown("", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		retry := 5 * time.Second
		if seconds, parseErr := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); parseErr == nil && seconds > 0 {
			retry = time.Duration(seconds) * time.Second
		}
		return &RuntimeBusyError{Message: err.Error(), RetryAfter: retry}
	}
	code := "http_error"
	var status *StatusError
	if errors.As(err, &status) && status.Code != "" {
		code = status.Code
	}
	return &RuntimeStartError{RuntimeExecutionError: &RuntimeExecutionError{Code: code, Message: err.Error()}}
}

// runtimeTerminalError maps a terminal error frame. A start-type code after
// the command started is contradictory; it may have run, so it is reported
// as an unknown outcome rather than a safe-to-retry start failure.
func runtimeTerminalError(wire runtimeWireFrame, started bool) error {
	execution := &RuntimeExecutionError{ExecutionID: wire.ExecutionID, Code: wire.ErrorCode, Message: wire.Message}
	switch wire.ErrorCode {
	case "invalid_request", "unavailable", "start_failed":
		if started {
			return runtimeUnknown(wire.ExecutionID, execution)
		}
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
		return runtimeWireFrame{}, fmt.Errorf("decode exec frame: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return runtimeWireFrame{}, fmt.Errorf("decode exec frame: %w", err)
	}
	return frame, nil
}

func validateRuntimeFrame(wire runtimeWireFrame, started bool, executionID string) error {
	noPayload := wire.DataBase64 == "" && wire.ExitCode == nil && wire.DurationMS == nil && wire.ErrorCode == "" && wire.Message == ""
	switch wire.Type {
	case "heartbeat":
		if !noPayload || wire.Cwd != "" || wire.Sandbox != "" {
			return errors.New("invalid exec heartbeat frame")
		}
	case "started":
		if started || !validRuntimeExecutionID(wire.ExecutionID) || !noPayload ||
			wire.Sandbox != "" && wire.Sandbox != RuntimeSandboxNew && wire.Sandbox != RuntimeSandboxWarm {
			return errors.New("invalid exec started frame")
		}
	case "stdout", "stderr":
		if !started || wire.ExecutionID != executionID || wire.DataBase64 == "" || wire.ExitCode != nil ||
			wire.DurationMS != nil || wire.ErrorCode != "" || wire.Message != "" {
			return errors.New("invalid exec output frame")
		}
	case "exit":
		if !started || wire.ExecutionID != executionID || wire.ExitCode == nil || *wire.ExitCode < 0 || *wire.ExitCode > 255 || wire.DurationMS == nil || *wire.DurationMS < 0 ||
			wire.DataBase64 != "" || wire.ErrorCode != "" || wire.Message != "" {
			return errors.New("invalid exec exit frame")
		}
	case "error":
		if started && wire.ExecutionID != executionID || !started && wire.ExecutionID != "" && !validRuntimeExecutionID(wire.ExecutionID) ||
			wire.ErrorCode == "" || wire.Message == "" || wire.ExitCode != nil || wire.DurationMS != nil || wire.DataBase64 != "" {
			return errors.New("invalid exec error frame")
		}
	default:
		return fmt.Errorf("unknown exec frame type %q", wire.Type)
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
			return nil, false, fmt.Errorf("exec frame exceeds %d bytes", maxRuntimeFrameBytes)
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
			return nil, false, fmt.Errorf("read exec frame: %w", err)
		}
	}
}

func readRuntimeHTTPError(resp *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxRuntimeFrameBytes+1))
	if readErr != nil {
		return readErr
	}
	if len(body) > maxRuntimeFrameBytes {
		return &StatusError{StatusCode: resp.StatusCode, Message: "error response exceeds size limit"}
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
