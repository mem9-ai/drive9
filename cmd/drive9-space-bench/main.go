// Command drive9-space-bench provisions or reuses Drive9 spaces and applies
// continuous verified read/write pressure through the public HTTP API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	code := runCLI(ctx, os.Args[1:], os.Getenv, os.UserHomeDir, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func runCLI(
	ctx context.Context,
	args []string,
	getenv func(string) string,
	userHomeDir func() (string, error),
	stdout, stderr io.Writer,
) int {
	home, err := userHomeDir()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-space-bench: determine home directory: %v\n", err)
		return 2
	}
	cfg, err := parseConfig(args, getenv, home, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "drive9-space-bench: %v\n", err)
		return 2
	}

	client := newBenchHTTPClient(cfg)
	defer client.CloseIdleConnections()
	report, runErr := runBenchmark(ctx, cfg, client, stdout, stderr)
	reportErr := writeReport(cfg.Out, report)
	printFinalSummary(stdout, report)
	if reportErr == nil {
		_, _ = fmt.Fprintf(stdout, "report: %s\n", cfg.Out)
	}
	if runErr != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-space-bench: %v\n", runErr)
	}
	if reportErr != nil {
		_, _ = fmt.Fprintf(stderr, "drive9-space-bench: write report: %v\n", reportErr)
	}
	if runErr != nil || reportErr != nil {
		return 1
	}
	return 0
}

func newBenchHTTPClient(cfg benchConfig) *http.Client {
	var transport *http.Transport
	if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = defaultTransport.Clone()
	} else {
		transport = &http.Transport{Proxy: http.ProxyFromEnvironment}
	}
	workerConnections := int64(cfg.SpaceCount)*int64(cfg.WorkersPerSpace) +
		int64(cfg.ProvisionConcurrency)
	connectionLimit := int(min(int64(10_000), max(int64(100), workerConnections*2)))
	transport.MaxIdleConns = connectionLimit
	transport.MaxIdleConnsPerHost = connectionLimit
	transport.MaxConnsPerHost = connectionLimit
	return &http.Client{Transport: transport}
}
