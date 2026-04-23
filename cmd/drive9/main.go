// Command drive9 provides a CLI for drive9 file and data operations.
//
// Usage:
//
//	drive9 <command> [arguments]
//
// Commands:
//
//	create  provision a new database
//	ctx     switch or list contexts
//	fs      filesystem operations (cp, cat, ls, stat, mv, rm, sh, grep, find)
//	vault   vault operations (set, get, put, with, ls, rm, grant, revoke, audit)
//	mount   mount drive9 as a local FUSE filesystem
//	umount  unmount a drive9 FUSE mount
package main

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"sync"

	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/cmd/drive9/cli"
	"github.com/mem9-ai/dat9/pkg/buildinfo"
	"github.com/mem9-ai/dat9/pkg/logger"
)

var cliLogger *zap.Logger
var cpuProfileStop = func() {}
var exitFunc = os.Exit

// vaultHandler is the `drive9 vault` command entry point, indirected through
// a var so dispatch tests can swap in a spy and assert "handler reached" vs
// "handler not reached". Production callers see no change: the default value
// is the real cli.Secret and nothing else reassigns it outside tests.
var vaultHandler = cli.Secret

func main() {
	if logger.CLIEnabled() {
		if l, err := logger.NewCLILogger(); err == nil {
			cliLogger = l
			logger.Set(l)
			defer func() { _ = cliLogger.Sync() }()
		} else {
			fmt.Fprintf(os.Stderr, "drive9: failed to initialize CLI logger: %v\n", err)
		}
	}

	stopCPUProfile, err := startCPUProfileFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "drive9: %v\n", err)
		os.Exit(1)
	}
	cpuProfileStop = stopCPUProfile
	defer cpuProfileStop()

	if len(os.Args) < 2 {
		usage()
	}

	dispatch(os.Args[1], os.Args[2:])
}

// dispatch routes a parsed (verb, args) pair to the matching command handler.
// Extracted from main() so the verb table is testable without spawning a
// subprocess. The `secret` verb was removed in V2b (hard-cut); callers
// that still type `drive9 secret ...` fall into the default branch and hit
// the generic `unknown command` path — there is no alias, no legacy shim.
func dispatch(cmd string, args []string) {
	switch cmd {
	case "--version", "-v", "version":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "version"))
		}
		fmt.Print(versionString())
	case "-h", "-help", "help":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "help"))
		}
		usage()
	case "create":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "create"))
		}
		if err := cli.Create(args); err != nil {
			fatal("create", err)
		}
	case "ctx":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "ctx"))
		}
		if err := cli.Ctx(args); err != nil {
			fatal("ctx", err)
		}
	case "fs":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "fs"), zap.String("subcommand", sub))
		}
		runFS(args)
	case "vault":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "vault"), zap.String("subcommand", sub))
		}
		if err := vaultHandler(args); err != nil {
			sub := ""
			if len(args) > 0 {
				sub = " " + args[0]
			}
			fatal("vault"+sub, err)
		}
	case "mount":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "mount"))
		}
		if err := cli.MountCmd(args); err != nil {
			fatal("mount", err)
		}
	case "umount":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "umount"))
		}
		if err := cli.UmountCmd(args); err != nil {
			fatal("umount", err)
		}
	default:
		if cliLogger != nil {
			logger.Warn(context.Background(), "cli_unknown_command", zap.String("command", cmd))
		}
		fmt.Fprintf(os.Stderr, "drive9: unknown command %q\n", cmd)
		usage()
	}
}

func versionString() string {
	return buildinfo.String("drive9")
}

func startCPUProfileFromEnv() (func(), error) {
	profilePath := os.Getenv("DRIVE9_PROF_CPU_PROFILE")
	if profilePath == "" {
		return func() {}, nil
	}

	f, err := os.Create(profilePath)
	if err != nil {
		return nil, fmt.Errorf("create cpu profile %s: %w", profilePath, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start cpu profile %s: %w", profilePath, err)
	}

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			pprof.StopCPUProfile()
			_ = f.Close()
		})
	}, nil
}

func runFS(args []string) {
	if len(args) < 1 {
		fsUsage()
	}
	sub := args[0]
	rest := args[1:]
	c := cli.NewFromEnv()

	var err error
	switch sub {
	case "cp":
		err = cli.Cp(c, rest)
	case "cat":
		err = cli.Cat(c, rest)
	case "ls":
		err = cli.Ls(c, rest)
	case "stat":
		err = cli.Stat(c, rest)
	case "mv":
		err = cli.Mv(c, rest)
	case "rm":
		err = cli.Rm(c, rest)
	case "sh":
		err = cli.Sh(c, rest)
	case "grep":
		err = cli.Grep(c, rest)
	case "find":
		err = cli.Find(c, rest)
	case "-h", "-help", "help":
		fsUsage()
	default:
		fmt.Fprintf(os.Stderr, "drive9 fs: unknown command %q\n", sub)
		fsUsage()
	}
	if err != nil {
		fatal("fs "+sub, err)
	}
}

func fatal(cmd string, err error) {
	if cliLogger != nil {
		logger.Error(context.Background(), "cli_command_failed", zap.String("command", cmd), zap.Error(err))
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", cmd, err)
	type exitCoder interface{ ExitCode() int }
	if ec, ok := err.(exitCoder); ok && ec.ExitCode() > 0 {
		exitWithCode(ec.ExitCode())
	}
	exitWithCode(1)
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: drive9 <command> [arguments]

commands:
  create           provision a new database
  ctx [name]       switch context (or show current)
  ctx list         list all contexts
  fs               filesystem operations
  vault            vault operations
  mount <dir>      mount drive9 as a local FUSE filesystem
  umount <dir>     unmount a drive9 FUSE mount
`)
	exitWithCode(2)
}

func fsUsage() {
	// Keep this usage block visually aligned. When editing wrapped help text,
	// preserve the column layout so subcommand descriptions remain easy to scan.
	fmt.Fprintf(os.Stderr, `usage: drive9 fs <command> [arguments]

commands:
  cp <src> <dst>       copy files (local↔remote)
    --tag <key=value>  set file tag (repeatable, upload only; not with --append;
                       any --tag on re-upload replaces the existing tag set;
                       omit --tag to preserve existing tags)
  cat <path>           read file to stdout
  ls [path]            list directory
  stat [-o text|json] <path>
                       file metadata
  mv <old> <new>       rename/move
  rm [-r|--recursive] <path>
                       remove file or directory tree
  sh                   interactive shell
  grep <pattern> [dir] search file contents
  find [dir] [flags]   find files by attributes
    -name <glob>         match filename
    -tag <key=value>     exact match tag key/value
    -tag <key>           match files containing tag key
    -newer <YYYY-MM-DD>  modified after date
    -older <YYYY-MM-DD>  modified before date
    -size <+N|-N>        size filter in bytes
`)
	exitWithCode(2)
}

func exitWithCode(code int) {
	cpuProfileStop()
	exitFunc(code)
}
