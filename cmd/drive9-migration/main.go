package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mem9-ai/drive9/internal/migration"
)

const (
	exitSuccess            = 0
	exitFailure            = 1
	exitIllegalOperation   = 2
	exitControlUnavailable = 3
)

type controlRequest = migration.ControlRequest

type dependencies struct {
	ctx            context.Context
	getenv         func(string) string
	credentialRoot string
	load           func(string, string, string, string, migration.Phase) (*migration.Startup, error)
	preflight      func(context.Context, *migration.Startup) (migration.PreflightResult, error)
	start          func(context.Context, *migration.Startup) error
	control        func(context.Context, controlRequest) error
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	deps := dependencies{
		ctx:            ctx,
		getenv:         os.Getenv,
		credentialRoot: migration.DefaultCredentialRoot,
		load:           migration.LoadStartup,
		preflight:      migration.Preflight,
		start:          migration.RunWorker,
		control: func(ctx context.Context, request controlRequest) error {
			return migration.Control(ctx, migration.DefaultControlSocket, request, os.Stdout)
		},
	}
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr, deps))
}

func execute(args []string, stdout, stderr io.Writer, deps dependencies) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		writeUsage(stdout)
		if len(args) == 0 {
			return exitFailure
		}
		return exitSuccess
	}
	ctx := deps.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	switch args[0] {
	case "plan", "run":
		err = executeStartupCommand(ctx, args[0], args[1:], stdout, stderr, deps)
	case "status":
		var request controlRequest
		request, err = parseStatus(args[1:], stderr)
		if err == nil {
			err = deps.control(ctx, request)
		}
	case "diff":
		var request controlRequest
		request, err = parseDiff(args[1:], stderr)
		if err == nil {
			err = deps.control(ctx, request)
		}
	case "verify-full", "prepare-drive9-cutover":
		if len(args) != 1 {
			err = fmt.Errorf("%s accepts no arguments", args[0])
		} else {
			err = deps.control(ctx, controlRequest{Command: args[0]})
		}
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err == nil {
		return exitSuccess
	}
	if errors.Is(err, flag.ErrHelp) {
		return exitSuccess
	}
	_, _ = fmt.Fprintln(stderr, err)
	switch {
	case errors.Is(err, migration.ErrControlUnavailable):
		return exitControlUnavailable
	case errors.Is(err, migration.ErrInvalidPhase), errors.Is(err, migration.ErrIllegalAction):
		return exitIllegalOperation
	default:
		return exitFailure
	}
}

func executeStartupCommand(ctx context.Context, command string, args []string, stdout, stderr io.Writer, deps dependencies) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("f", "", "path to config.yaml")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" || flags.NArg() != 0 {
		return fmt.Errorf("%s requires exactly -f <config.yaml>", command)
	}
	startup, err := deps.load(
		*configPath,
		deps.getenv(migration.MigrationNodeNameEnv),
		deps.getenv(migration.MigrationPhaseEnv),
		deps.credentialRoot,
		"",
	)
	if err != nil {
		return err
	}
	result, err := deps.preflight(ctx, startup)
	if err != nil {
		return err
	}
	if command == "run" {
		return deps.start(ctx, startup)
	}
	return json.NewEncoder(stdout).Encode(result)
}

func parseStatus(args []string, stderr io.Writer) (controlRequest, error) {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("output", "", "output format")
	if err := flags.Parse(args); err != nil {
		return controlRequest{}, err
	}
	if flags.NArg() != 0 || *output != "json" {
		return controlRequest{}, errors.New("status requires --output json")
	}
	return controlRequest{Command: "status", Output: *output}, nil
}

func parseDiff(args []string, stderr io.Writer) (controlRequest, error) {
	flags := flag.NewFlagSet("diff", flag.ContinueOnError)
	flags.SetOutput(stderr)
	output := flags.String("output", "", "output format")
	typeFilter := flags.String("type", "", "finding type")
	limit := flags.Int("limit", 0, "maximum findings; zero is unlimited")
	if err := flags.Parse(args); err != nil {
		return controlRequest{}, err
	}
	if flags.NArg() != 0 || *output != "jsonl" || *limit < 0 {
		return controlRequest{}, errors.New("diff requires --output jsonl and a non-negative --limit")
	}
	return controlRequest{Command: "diff", Output: *output, Type: *typeFilter, Limit: *limit}, nil
}

func writeUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "usage: drive9-migration <plan|run|status|diff|verify-full|prepare-drive9-cutover> [options]")
	_, _ = fmt.Fprintln(w, "  plan -f <config.yaml>                 validate one local Job and print its non-sensitive mapping")
	_, _ = fmt.Fprintln(w, "  run -f <config.yaml>                  run one foreground Worker")
	_, _ = fmt.Fprintln(w, "  status --output json                  print process-local status")
	_, _ = fmt.Fprintln(w, "  diff --output jsonl [--type T] [--limit N]")
	_, _ = fmt.Fprintln(w, "  verify-full                           run serialized in-memory full verification")
	_, _ = fmt.Fprintln(w, "  prepare-drive9-cutover                write the irreversible cutover fence")
}
