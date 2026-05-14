// Command drive9-agent-harness runs reusable Drive9 agent workloads.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/runner"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "preflight":
		cfg, err := parseConfig(os.Args[2:], "preflight")
		exitOnErr(err)
		exitOnErr(runner.Preflight(ctx, cfg))
		fmt.Println("preflight ok")
	case "run":
		cfg, err := parseConfig(os.Args[2:], "run")
		exitOnErr(err)
		runDir, err := runner.Run(ctx, cfg)
		exitOnErr(err)
		fmt.Println(runDir)
	case "report":
		fs := flag.NewFlagSet("report", flag.ExitOnError)
		runDir := fs.String("run-dir", "", "existing run directory")
		exitOnErr(fs.Parse(os.Args[2:]))
		if *runDir == "" {
			exitOnErr(fmt.Errorf("report requires --run-dir"))
		}
		exitOnErr(runner.Regenerate(*runDir))
		fmt.Println(*runDir)
	case "gc":
		cfg, err := parseGC(os.Args[2:])
		exitOnErr(err)
		exitOnErr(runner.GC(ctx, cfg))
		fmt.Println("gc ok")
	case "collect-server-evidence":
		cfg, err := parseEvidence(os.Args[2:])
		exitOnErr(err)
		exitOnErr(runner.CollectServerEvidence(ctx, cfg))
		fmt.Println("server evidence ok")
	default:
		usage()
		os.Exit(2)
	}
}

func parseConfig(args []string, name string) (runner.Config, error) {
	cfg := runner.DefaultConfig()
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	suites := strings.Join(cfg.Suites, ",")
	provisionTimeout := cfg.ProvisionTimeout
	fs.StringVar(&cfg.ArtifactRoot, "artifact-root", cfg.ArtifactRoot, "parent for run artifacts")
	fs.StringVar(&cfg.MountRoot, "mount-root", cfg.MountRoot, "parent for generated mountpoints")
	fs.StringVar(&cfg.RemoteRootBase, "remote-root-base", cfg.RemoteRootBase, "remote generated root base")
	fs.StringVar(&cfg.Drive9Bin, "drive9-bin", cfg.Drive9Bin, "drive9 binary under test")
	fs.StringVar(&cfg.Server, "server", cfg.Server, "Drive9 server URL")
	fs.StringVar(&cfg.APIKey, "api-key", cfg.APIKey, "Drive9 API key")
	fs.BoolVar(&cfg.Provision, "provision", cfg.Provision, "provision a fresh tenant")
	fs.BoolVar(&cfg.AllowFault, "allow-fault", cfg.AllowFault, "allow fault-injection suite cases")
	fs.DurationVar(&provisionTimeout, "provision-timeout", provisionTimeout, "fresh tenant provision timeout")
	fs.StringVar(&cfg.SuiteDir, "suite-dir", cfg.SuiteDir, "suite YAML directory")
	fs.StringVar(&suites, "suite", suites, "comma-separated suites")
	fs.StringVar(&cfg.CaseFilter, "case", cfg.CaseFilter, "comma-separated case ids")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.ProvisionTimeout = provisionTimeout
	cfg.Suites = splitCSV(suites)
	if len(cfg.Suites) == 0 {
		return cfg, fmt.Errorf("--suite must select at least one suite")
	}
	return cfg, nil
}

func parseGC(args []string) (runner.GCConfig, error) {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	cfg := runner.GCConfig{}
	fs.StringVar(&cfg.RunDir, "run-dir", "", "run directory")
	fs.StringVar(&cfg.Drive9Bin, "drive9-bin", "drive9", "drive9 binary")
	fs.StringVar(&cfg.Server, "server", os.Getenv("DRIVE9_BASE"), "Drive9 server URL")
	fs.StringVar(&cfg.APIKey, "api-key", os.Getenv("DRIVE9_API_KEY"), "Drive9 API key")
	fs.BoolVar(&cfg.SuccessfulOnly, "successful-only", true, "refuse GC for failed runs")
	fs.BoolVar(&cfg.ConfirmDelete, "confirm-delete", false, "delete generated local mountpoints and remote roots")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.RunDir == "" {
		return cfg, fmt.Errorf("gc requires --run-dir")
	}
	return cfg, nil
}

func parseEvidence(args []string) (runner.EvidenceConfig, error) {
	fs := flag.NewFlagSet("collect-server-evidence", flag.ExitOnError)
	cfg := runner.EvidenceConfig{}
	fs.StringVar(&cfg.RunDir, "run-dir", "", "run directory")
	fs.StringVar(&cfg.KubeContext, "kube-context", "", "Kubernetes context")
	fs.StringVar(&cfg.Namespace, "namespace", "dat9", "Kubernetes namespace")
	fs.StringVar(&cfg.Selector, "selector", "app=dat9-server", "pod label selector")
	fs.StringVar(&cfg.Since, "since", "10m", "kubectl logs since duration")
	fs.IntVar(&cfg.Tail, "tail", 500, "kubectl logs tail lines")
	fs.StringVar(&cfg.MetricsRawPath, "metrics-raw", "", "existing Prometheus text file to attach")
	fs.BoolVar(&cfg.ApproveExternal, "approve-external", false, "allow external Kubernetes/API reads")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.RunDir == "" {
		return cfg, fmt.Errorf("collect-server-evidence requires --run-dir")
	}
	return cfg, nil
}

func splitCSV(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func exitOnErr(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: drive9-agent-harness <preflight|run|report|gc|collect-server-evidence> [flags]")
}
