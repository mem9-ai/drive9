package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"syscall"
	"time"
)

const (
	exitSuccess = 0
	exitFailure = 1
	exitUsage   = 2
)

var batchLabelPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$`)

type options struct {
	namespace     string
	allNamespaces bool
	batch         string
	contextName   string
	kubeconfig    string
	output        string
}

type commandResult struct {
	stdout []byte
	stderr []byte
	err    error
}

type dependencies struct {
	ctx         context.Context
	now         func() time.Time
	run         func(context.Context, string, ...string) commandResult
	execTimeout time.Duration
	concurrency int
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr, dependencies{
		ctx:         ctx,
		now:         time.Now,
		run:         runCommand,
		execTimeout: defaultExecTimeout,
		concurrency: maxConcurrentExec,
	}))
}

func execute(args []string, stdout, stderr io.Writer, deps dependencies) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		writeUsage(stdout)
		return exitSuccess
	}
	if len(args) == 0 {
		writeUsage(stderr)
		return exitUsage
	}
	if args[0] != "status" {
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		writeUsage(stderr)
		return exitUsage
	}

	opts, err := parseStatusOptions(args[1:])
	if errors.Is(err, flag.ErrHelp) {
		writeUsage(stdout)
		return exitSuccess
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	deps = normalizeDependencies(deps)
	jobs, batches, partial, err := collectStatus(deps.ctx, opts, deps)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitFailure
	}
	if err := renderStatus(stdout, opts, deps.now().UTC(), jobs, batches); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitFailure
	}
	if partial {
		return exitFailure
	}
	return exitSuccess
}

func normalizeDependencies(deps dependencies) dependencies {
	if deps.ctx == nil {
		deps.ctx = context.Background()
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.run == nil {
		deps.run = runCommand
	}
	if deps.execTimeout <= 0 {
		deps.execTimeout = defaultExecTimeout
	}
	if deps.concurrency <= 0 {
		deps.concurrency = maxConcurrentExec
	} else if deps.concurrency > maxConcurrentExec {
		deps.concurrency = maxConcurrentExec
	}
	return deps
}

func parseStatusOptions(args []string) (options, error) {
	opts := options{output: "table"}
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	flags.StringVar(&opts.namespace, "namespace", "", "Kubernetes namespace")
	flags.StringVar(&opts.namespace, "n", "", "Kubernetes namespace")
	flags.BoolVar(&opts.allNamespaces, "all-namespaces", false, "query all namespaces")
	flags.BoolVar(&opts.allNamespaces, "A", false, "query all namespaces")
	flags.StringVar(&opts.batch, "batch", "", "migration batch label")
	flags.StringVar(&opts.contextName, "context", "", "kubectl context")
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "path to kubeconfig")
	flags.StringVar(&opts.output, "output", "table", "output format: table, wide, or json")
	flags.StringVar(&opts.output, "o", "table", "output format: table, wide, or json")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, errors.New("status accepts no positional arguments")
	}
	if opts.namespace != "" && opts.allNamespaces {
		return options{}, errors.New("--namespace and --all-namespaces are mutually exclusive")
	}
	switch opts.output {
	case "table", "wide", "json":
	default:
		return options{}, fmt.Errorf("unsupported output format %q", opts.output)
	}
	if opts.batch != "" && !batchLabelPattern.MatchString(opts.batch) {
		return options{}, errors.New("--batch must be a valid non-empty Kubernetes label value")
	}
	return opts, nil
}

func runCommand(ctx context.Context, name string, args ...string) commandResult {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return commandResult{stdout: stdout.Bytes(), stderr: stderr.Bytes(), err: err}
}

func writeUsage(output io.Writer) {
	_, _ = fmt.Fprintln(output, "usage: kubectl drive9 migration status [options]")
	_, _ = fmt.Fprintln(output, "  -n, --namespace NAME       query one namespace")
	_, _ = fmt.Fprintln(output, "  -A, --all-namespaces       query all namespaces")
	_, _ = fmt.Fprintln(output, "      --batch NAME           select one migration batch")
	_, _ = fmt.Fprintln(output, "      --context NAME         use a kubectl context")
	_, _ = fmt.Fprintln(output, "      --kubeconfig PATH      use a kubeconfig file")
	_, _ = fmt.Fprintln(output, "  -o, --output FORMAT        table (default), wide, or json")
}
