package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type runtimeAPI interface {
	RuntimeCapabilities(context.Context) (*client.RuntimeCapabilities, error)
	SubmitRuntimeExecution(context.Context, string, client.RuntimeExecutionRequest) (*client.RuntimeOperation, error)
	SubmitRuntimeFileOperation(context.Context, string, client.RuntimeFileOperationRequest) (*client.RuntimeOperation, error)
	GetRuntimeExecution(context.Context, string) (*client.RuntimeOperation, error)
	GetRuntimeFileOperation(context.Context, string) (*client.RuntimeOperation, error)
	CancelRuntimeExecution(context.Context, string) (*client.RuntimeOperation, error)
	CancelRuntimeFileOperation(context.Context, string) (*client.RuntimeOperation, error)
	RecoverRuntimeExecution(context.Context, string, string, string) (*client.RuntimeRecovery, error)
	RecoverRuntimeFileOperation(context.Context, string, string, string) (*client.RuntimeRecovery, error)
	GetRuntimeRecovery(context.Context, string) (*client.RuntimeRecovery, error)
	WatchRuntimeEvents(context.Context, string, string, int64, func(client.RuntimeEvent) error) error
	DownloadRuntimeArtifact(context.Context, string) ([]byte, error)
}

var runtimeClientFactory = func() runtimeAPI { return NewFromEnv() }

var runtimeInterruptContext = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func Runtime(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", runtimeUsage())
	}
	switch args[0] {
	case "capabilities":
		if len(args) != 1 {
			return fmt.Errorf("usage: drive9 runtime capabilities")
		}
		value, err := runtimeClientFactory().RuntimeCapabilities(context.Background())
		return printRuntimeJSON(value, err)
	case "fs":
		return RuntimeFS(args[1:])
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, runtimeUsage())
		return nil
	default:
		return fmt.Errorf("unknown runtime command %q", args[0])
	}
}

func RuntimeExec(args []string) error {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspaceRef, root, scopeKey := runtimeWorkspaceFlags(fs)
	workspacePath := fs.String("workspace", "", "create a one-shot Runtime workspace from this Drive9 root")
	profile := fs.String("profile", "", "approved Runtime toolchain profile")
	shell := fs.String("shell-script", "", "shell script")
	cwd := fs.String("cwd", "", "workspace-relative working directory")
	timeout := fs.Duration("timeout", 0, "execution timeout")
	var environment runtimeEnvironment
	fs.Var(&environment, "env", "explicit environment variable KEY=VALUE (repeatable)")
	idempotencyKey := fs.String("idempotency-key", "", "stable submission key")
	idempotencyReceipt := fs.String("idempotency-receipt", "", "protected receipt to create or reuse after an ambiguous submit")
	wait := fs.Bool("wait", true, "wait for terminal state")
	detach := fs.Bool("detach", false, "return after durable submission")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printRuntimeHelp(fs, "usage: drive9 exec [flags] [-- argv...]")
		}
		return err
	}
	argv := fs.Args()
	if (*shell == "") == (len(argv) == 0) {
		return fmt.Errorf("exactly one of --shell-script or argv is required")
	}
	if *timeout < 0 || *timeout%time.Second != 0 {
		return fmt.Errorf("--timeout must be a non-negative whole number of seconds")
	}
	if *detach {
		*wait = false
	}
	receipt, err := prepareRuntimeReceiptKey(*idempotencyKey, *idempotencyReceipt)
	if err != nil {
		return err
	}
	workspace, err := resolveRuntimeWorkspace(*workspaceRef, *root, *workspacePath, *scopeKey, receipt.receipt.IdempotencyKey)
	if err != nil {
		return err
	}
	request := client.RuntimeExecutionRequest{WorkspaceRef: workspace.ref, Workspace: workspace.input,
		Profile:   *profile,
		Execution: client.RuntimeExecutionSpec{Argv: argv, Shell: *shell, WorkingDirectory: *cwd, Environment: environment, TimeoutSeconds: int(timeout.Seconds())}}
	if err := receipt.persist("runtime-execution", request); err != nil {
		return err
	}
	api := runtimeClientFactory()
	operation, err := api.SubmitRuntimeExecution(context.Background(), receipt.receipt.IdempotencyKey, request)
	if err != nil {
		return receipt.submitError(err)
	}
	if err := receipt.complete(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "warning:", err)
	}
	if *wait {
		waitCtx, stop := runtimeInterruptContext()
		operation, err = waitRuntimeOperation(waitCtx, operation, api.GetRuntimeExecution)
		stop()
		if errors.Is(err, context.Canceled) {
			cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			operation, err = api.CancelRuntimeExecution(cancelCtx, operation.ID)
			if err == nil {
				operation, err = waitRuntimeOperation(cancelCtx, operation, api.GetRuntimeExecution)
			}
		}
	}
	return printRuntimeJSON(operation, err)
}

func RuntimeExecution(args []string) error {
	return runtimeResource(args, "execution")
}

func RuntimeFileOperation(args []string) error {
	return runtimeResource(args, "file-operation")
}

func RuntimeRecovery(args []string) error {
	if len(args) != 2 || args[0] != "get" && args[0] != "wait" {
		return fmt.Errorf("usage: drive9 recovery <get|wait> ID")
	}
	api := runtimeClientFactory()
	value, err := api.GetRuntimeRecovery(context.Background(), args[1])
	if err == nil && args[0] == "wait" {
		value, err = waitRuntimeRecovery(context.Background(), value, api.GetRuntimeRecovery)
	}
	return printRuntimeJSON(value, err)
}

func RuntimeFS(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: drive9 runtime fs <read|write|edit|delete|list|find|grep> [flags] PATH")
	}
	action := args[0]
	fs := flag.NewFlagSet("runtime fs "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspaceRef, root, scopeKey := runtimeWorkspaceFlags(fs)
	workspacePath := fs.String("workspace", "", "create a one-shot Runtime workspace from this Drive9 root")
	profile := fs.String("profile", "", "approved Runtime toolchain profile")
	dataBase64 := fs.String("data-base64", "", "base64 file content")
	dataFile := fs.String("data-file", "", "read file content from path")
	expected := fs.String("expected-sha256", "", "existing content precondition")
	pattern := fs.String("pattern", "", "find/grep pattern")
	limit := fs.Int("limit", 0, "result limit")
	idempotencyKey := fs.String("idempotency-key", "", "stable submission key")
	idempotencyReceipt := fs.String("idempotency-receipt", "", "protected receipt to create or reuse after an ambiguous submit")
	wait := fs.Bool("wait", true, "wait for terminal state")
	if err := fs.Parse(normalizeHelpFlags(args[1:], fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printRuntimeHelp(fs, "usage: drive9 runtime fs "+action+" [flags] PATH")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 runtime fs %s [flags] PATH", action)
	}
	if *dataBase64 != "" && *dataFile != "" {
		return fmt.Errorf("--data-base64 and --data-file are mutually exclusive")
	}
	if *dataFile != "" {
		content, err := os.ReadFile(*dataFile)
		if err != nil {
			return err
		}
		*dataBase64 = base64.StdEncoding.EncodeToString(content)
	}
	receipt, err := prepareRuntimeReceiptKey(*idempotencyKey, *idempotencyReceipt)
	if err != nil {
		return err
	}
	workspace, err := resolveRuntimeWorkspace(*workspaceRef, *root, *workspacePath, *scopeKey, receipt.receipt.IdempotencyKey)
	if err != nil {
		return err
	}
	request := client.RuntimeFileOperationRequest{WorkspaceRef: workspace.ref, Workspace: workspace.input,
		Profile: *profile,
		Operation: client.RuntimeFileOperationSpec{Action: action, Path: fs.Arg(0), DataBase64: *dataBase64,
			ExpectedHash: *expected, Pattern: *pattern, Limit: *limit}}
	if err := receipt.persist("runtime-file-operation", request); err != nil {
		return err
	}
	api := runtimeClientFactory()
	operation, err := api.SubmitRuntimeFileOperation(context.Background(), receipt.receipt.IdempotencyKey, request)
	if err != nil {
		return receipt.submitError(err)
	}
	if err := receipt.complete(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "warning:", err)
	}
	if *wait {
		operation, err = waitRuntimeOperation(context.Background(), operation, api.GetRuntimeFileOperation)
	}
	return printRuntimeJSON(operation, err)
}

func runtimeResource(args []string, kind string) error {
	if len(args) < 2 {
		if kind == "execution" {
			return fmt.Errorf("usage: drive9 execution <get|wait|logs|cancel|recover> ID [flags]")
		}
		return fmt.Errorf("usage: drive9 %s <get|wait|cancel|recover> ID [flags]", kind)
	}
	action, id := args[0], args[1]
	api := runtimeClientFactory()
	ctx := context.Background()
	isExecution := kind == "execution"
	switch action {
	case "get", "wait":
		if len(args) != 2 {
			return fmt.Errorf("usage: drive9 %s %s ID", kind, action)
		}
		var value *client.RuntimeOperation
		var err error
		if isExecution {
			value, err = api.GetRuntimeExecution(ctx, id)
			if err == nil && action == "wait" {
				value, err = waitRuntimeOperation(ctx, value, api.GetRuntimeExecution)
			}
		} else {
			value, err = api.GetRuntimeFileOperation(ctx, id)
			if err == nil && action == "wait" {
				value, err = waitRuntimeOperation(ctx, value, api.GetRuntimeFileOperation)
			}
		}
		return printRuntimeJSON(value, err)
	case "logs":
		if !isExecution {
			return fmt.Errorf("unknown %s command %q", kind, action)
		}
		fs := flag.NewFlagSet("execution logs", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		follow := fs.Bool("follow", false, "follow output until the execution reaches a terminal state")
		if err := fs.Parse(args[2:]); err != nil || fs.NArg() != 0 {
			return fmt.Errorf("usage: drive9 execution logs ID [--follow]")
		}
		if !*follow {
			return runtimeExecutionLogs(ctx, api, id, false, os.Stdout)
		}
		logCtx, stop := runtimeInterruptContext()
		defer stop()
		err := runtimeExecutionLogs(logCtx, api, id, true, os.Stdout)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case "cancel":
		fs := flag.NewFlagSet(kind+" cancel", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		wait := fs.Bool("wait", false, "wait for terminal state")
		if err := fs.Parse(args[2:]); err != nil || fs.NArg() != 0 {
			return fmt.Errorf("usage: drive9 %s cancel ID [--wait]", kind)
		}
		var value *client.RuntimeOperation
		var err error
		if isExecution {
			value, err = api.CancelRuntimeExecution(ctx, id)
			if err == nil && *wait {
				value, err = waitRuntimeOperation(ctx, value, api.GetRuntimeExecution)
			}
		} else {
			value, err = api.CancelRuntimeFileOperation(ctx, id)
			if err == nil && *wait {
				value, err = waitRuntimeOperation(ctx, value, api.GetRuntimeFileOperation)
			}
		}
		return printRuntimeJSON(value, err)
	case "recover":
		fs := flag.NewFlagSet(kind+" recover", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		actionName := fs.String("action", "", "reconcile or discard_uncommitted")
		reconcile := fs.Bool("reconcile", false, "reconcile only when retained evidence proves the original result")
		discard := fs.Bool("discard-uncommitted", false, "discard unpublished state and restore the published checkpoint")
		keyFlag := fs.String("idempotency-key", "", "stable recovery key")
		wait := fs.Bool("wait", false, "wait for terminal recovery state")
		if err := fs.Parse(args[2:]); err != nil || fs.NArg() != 0 {
			return fmt.Errorf("usage: drive9 %s recover ID (--reconcile|--discard-uncommitted|--action ACTION) [--wait]", kind)
		}
		selected := 0
		if *actionName != "" {
			selected++
		}
		if *reconcile {
			selected++
			*actionName = "reconcile"
		}
		if *discard {
			selected++
			*actionName = "discard_uncommitted"
		}
		if selected != 1 || *actionName != "reconcile" && *actionName != "discard_uncommitted" {
			return fmt.Errorf("exactly one recovery action is required: --reconcile or --discard-uncommitted")
		}
		key, err := runtimeKey(*keyFlag)
		if err != nil {
			return err
		}
		if isExecution {
			value, err := api.RecoverRuntimeExecution(ctx, id, key, *actionName)
			if err == nil && *wait {
				value, err = waitRuntimeRecovery(ctx, value, api.GetRuntimeRecovery)
			}
			return printRuntimeJSON(value, err)
		}
		value, err := api.RecoverRuntimeFileOperation(ctx, id, key, *actionName)
		if err == nil && *wait {
			value, err = waitRuntimeRecovery(ctx, value, api.GetRuntimeRecovery)
		}
		return printRuntimeJSON(value, err)
	default:
		return fmt.Errorf("unknown %s command %q", kind, action)
	}
}

type runtimeOutputEvent struct {
	Stream     string `json:"stream"`
	DataBase64 string `json:"data_base64"`
	Truncated  bool   `json:"truncated"`
}

func runtimeExecutionLogs(ctx context.Context, api runtimeAPI, id string, follow bool, output io.Writer) error {
	operation, err := api.GetRuntimeExecution(ctx, id)
	if err != nil {
		return err
	}
	if !follow && !operation.Terminal() {
		return fmt.Errorf("Runtime execution %s is %s; use --follow to wait for durable logs", id, operation.State)
	}
	if !follow {
		return writeRuntimeLogArtifact(ctx, api, operation, nil, output)
	}

	var replayed bytes.Buffer
	truncated := false
	err = api.WatchRuntimeEvents(ctx, "executions", id, 0, func(event client.RuntimeEvent) error {
		if event.Kind != "output" {
			return nil
		}
		var payload runtimeOutputEvent
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode Runtime output event: %w", err)
		}
		if payload.Stream != "combined" {
			return fmt.Errorf("unsupported Runtime output stream %q", payload.Stream)
		}
		chunk, err := base64.StdEncoding.DecodeString(payload.DataBase64)
		if err != nil {
			return fmt.Errorf("decode Runtime output event data: %w", err)
		}
		if _, err := output.Write(chunk); err != nil {
			return err
		}
		_, _ = replayed.Write(chunk)
		truncated = truncated || payload.Truncated
		return nil
	})
	if err != nil {
		return err
	}
	operation, err = api.GetRuntimeExecution(ctx, id)
	if err != nil {
		return err
	}
	if !operation.Terminal() {
		return fmt.Errorf("Runtime event stream ended before execution %s reached a terminal state", id)
	}
	if operation.LogArtifactID == "" {
		if replayed.Len() > 0 || truncated {
			return fmt.Errorf("Runtime execution %s has output events but no durable log artifact", id)
		}
		return nil
	}
	return writeRuntimeLogArtifact(ctx, api, operation, replayed.Bytes(), output)
}

func writeRuntimeLogArtifact(ctx context.Context, api runtimeAPI, operation *client.RuntimeOperation, replayed []byte, output io.Writer) error {
	if operation.LogArtifactID == "" {
		return nil
	}
	content, err := api.DownloadRuntimeArtifact(ctx, operation.LogArtifactID)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(content, replayed) {
		return fmt.Errorf("Runtime log artifact does not match replayed output")
	}
	_, err = output.Write(content[len(replayed):])
	return err
}

type runtimeWorkspaceSelection struct {
	ref   string
	input *client.RuntimeWorkspaceInput
}

func runtimeWorkspaceFlags(fs *flag.FlagSet) (*string, *string, *string) {
	return fs.String("workspace-ref", "", "existing Runtime workspace"),
		fs.String("root", "", "initial Drive9 root"), fs.String("scope-key", "", "stable client workspace key")
}

func runtimeWorkspace(ref, root, scopeKey string) (runtimeWorkspaceSelection, error) {
	hasRef := strings.TrimSpace(ref) != ""
	hasInput := strings.TrimSpace(root) != "" || strings.TrimSpace(scopeKey) != ""
	if hasRef == hasInput {
		return runtimeWorkspaceSelection{}, fmt.Errorf("exactly one of --workspace-ref or both --root and --scope-key is required")
	}
	if hasRef {
		return runtimeWorkspaceSelection{ref: ref}, nil
	}
	if strings.TrimSpace(root) == "" || strings.TrimSpace(scopeKey) == "" {
		return runtimeWorkspaceSelection{}, fmt.Errorf("--root and --scope-key must be provided together")
	}
	return runtimeWorkspaceSelection{input: &client.RuntimeWorkspaceInput{ClientScopeKey: scopeKey,
		Source: client.RuntimeWorkspaceSource{Root: root}}}, nil
}

func resolveRuntimeWorkspace(ref, root, workspacePath, scopeKey, idempotencyKey string) (runtimeWorkspaceSelection, error) {
	if strings.TrimSpace(workspacePath) != "" {
		if strings.TrimSpace(root) != "" || strings.TrimSpace(ref) != "" {
			return runtimeWorkspaceSelection{}, fmt.Errorf("--workspace is mutually exclusive with --root and --workspace-ref")
		}
		root = workspacePath
		if strings.TrimSpace(scopeKey) == "" {
			scopeKey = "drive9-cli-workspace-" + idempotencyKey
		}
	}
	if strings.TrimSpace(root) != "" {
		normalized, err := normalizeRuntimeRoot(root)
		if err != nil {
			return runtimeWorkspaceSelection{}, err
		}
		root = normalized
	}
	return runtimeWorkspace(ref, root, scopeKey)
}

func normalizeRuntimeRoot(raw string) (string, error) {
	location, err := Parse(raw)
	if err != nil {
		return "", err
	}
	location = promoteBareFSArg(location)
	if location.Kind != KindDrive9 {
		return "", fmt.Errorf("Runtime workspace root must be a Drive9 path")
	}
	if location.Context != "" {
		return "", fmt.Errorf("Runtime workspace root cannot select named context %q; use the active context", location.Context)
	}
	if !strings.HasPrefix(location.Path, "/") {
		return "", fmt.Errorf("Runtime workspace root must be absolute")
	}
	return location.Path, nil
}

type runtimeEnvironment map[string]string

var runtimeEnvironmentNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (e *runtimeEnvironment) String() string {
	if e == nil || len(*e) == 0 {
		return ""
	}
	values := make([]string, 0, len(*e))
	for key, value := range *e {
		values = append(values, key+"="+value)
	}
	return strings.Join(values, ",")
}

func (e *runtimeEnvironment) Set(value string) error {
	key, item, ok := strings.Cut(value, "=")
	if !ok || !runtimeEnvironmentNamePattern.MatchString(key) || strings.ContainsRune(item, '\x00') {
		return fmt.Errorf("--env requires a portable NAME=VALUE entry without NUL bytes")
	}
	if *e == nil {
		*e = make(runtimeEnvironment)
	}
	(*e)[key] = item
	return nil
}

func runtimeKey(value string) (string, error) {
	if strings.TrimSpace(value) != "" {
		return value, nil
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "drive9-cli-" + hex.EncodeToString(random[:]), nil
}

func waitRuntimeOperation(ctx context.Context, initial *client.RuntimeOperation, get func(context.Context, string) (*client.RuntimeOperation, error)) (*client.RuntimeOperation, error) {
	operation := initial
	for !operation.Terminal() {
		select {
		case <-ctx.Done():
			return operation, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		var err error
		operation, err = get(ctx, operation.ID)
		if err != nil {
			return nil, err
		}
	}
	return operation, nil
}

func waitRuntimeRecovery(ctx context.Context, initial *client.RuntimeRecovery, get func(context.Context, string) (*client.RuntimeRecovery, error)) (*client.RuntimeRecovery, error) {
	recovery := initial
	for recovery.State != "succeeded" && recovery.State != "failed" {
		select {
		case <-ctx.Done():
			return recovery, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		var err error
		recovery, err = get(ctx, recovery.RecoveryID)
		if err != nil {
			return nil, err
		}
	}
	return recovery, nil
}

func printRuntimeJSON(value any, err error) error {
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(value)
}

func printRuntimeHelp(fs *flag.FlagSet, usage string) error {
	_, _ = fmt.Fprintln(os.Stdout, usage)
	fs.SetOutput(os.Stdout)
	fs.PrintDefaults()
	return nil
}

func runtimeUsage() string {
	return "usage: drive9 runtime <capabilities|fs>"
}
