// Command drive9 provides a CLI for drive9 file and data operations.
//
// Usage:
//
//	drive9 <command> [arguments]
//
// Commands:
//
//	create  provision a new database and owner context
//	ctx     manage contexts (show, add, import, fork, ls, use, rm)
//	fs      filesystem operations (cp, cat, ls, stat, mv, rm, mkdir, chmod,
//	        symlink, hardlink, sh, grep, find, layer)
//	token  issue and revoke workspace-zone scoped filesystem tokens
//	vault   vault operations (set, get, put, with, ls, rm, grant, revoke, audit)
//	journal append-only agent/workflow journal operations
//	git     git-aware drive9 workflows
//	profile show mount profile configuration
//	mount   mount drive9 as a local filesystem, or mount vault secrets
//	umount  unmount a drive9 local mount
//	doctor  diagnose local drive9 runtime prerequisites
//	update  update the drive9 CLI binary in place
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
	"github.com/mem9-ai/dat9/pkg/client"
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
var tokenHandler = cli.Token
var doctorHandler = cli.Doctor
var journalHandler = cli.Journal
var gitHandler = cli.Git
var packHandler = cli.PackCommand
var unpackHandler = cli.UnpackCommand
var profileHandler = cli.Profile
var umountHandler = cli.UmountCmd
var updateHandler = cli.Update

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
		usage(2)
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
	case "-h", "-help", "--help", "help":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "help"))
		}
		usage(0)
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
	case "token":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "token"), zap.String("subcommand", sub))
		}
		if err := tokenHandler(args); err != nil {
			sub := ""
			if len(args) > 0 {
				sub = " " + args[0]
			}
			fatal("token"+sub, err)
		}
	case "journal":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "journal"), zap.String("subcommand", sub))
		}
		if err := journalHandler(args); err != nil {
			sub := ""
			if len(args) > 0 {
				sub = " " + args[0]
			}
			fatal("journal"+sub, err)
		}
	case "git":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "git"), zap.String("subcommand", sub))
		}
		if err := gitHandler(args); err != nil {
			sub := ""
			if len(args) > 0 {
				sub = " " + args[0]
			}
			fatal("git"+sub, err)
		}
	case "pack":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "pack"))
		}
		if err := packHandler(args); err != nil {
			fatal("pack", err)
		}
	case "unpack":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "unpack"))
		}
		if err := unpackHandler(args); err != nil {
			fatal("unpack", err)
		}
	case "profile":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "profile"), zap.String("subcommand", sub))
		}
		if err := profileHandler(args); err != nil {
			fatal("profile", err)
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
		if err := umountHandler(args); err != nil {
			fatal("umount", err)
		}
	case "doctor":
		if cliLogger != nil {
			sub := ""
			if len(args) > 0 {
				sub = args[0]
			}
			logger.Info(context.Background(), "cli_command", zap.String("command", "doctor"), zap.String("subcommand", sub))
		}
		if err := doctorHandler(args); err != nil {
			fatal("doctor", err)
		}
	case "update":
		if cliLogger != nil {
			logger.Info(context.Background(), "cli_command", zap.String("command", "update"))
		}
		if err := updateHandler(args); err != nil {
			fatal("update", err)
		}
	default:
		if cliLogger != nil {
			logger.Warn(context.Background(), "cli_unknown_command", zap.String("command", cmd))
		}
		fmt.Fprintf(os.Stderr, "drive9: unknown command %q\n", cmd)
		usage(2)
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
		fsUsage(2)
	}
	sub := args[0]
	rest := args[1:]

	// Only commands that may upload pay the /v1/status warm RTT. Read-only
	// commands (cat/ls/stat/rm/grep/find) and namespace-only writes (mv,
	// mkdir) do not consult the upload threshold and can skip the warm —
	// keeps cold-start latency unchanged for the common case and avoids
	// hanging an `ls` behind a slow status endpoint.
	var c *client.Client
	switch sub {
	case "cp", "sh":
		c = cli.NewFromEnvWithWarm()
	default:
		c = cli.NewFromEnv()
	}

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
	case "mkdir":
		err = cli.Mkdir(c, rest)
	case "chmod":
		err = cli.Chmod(c, rest)
	case "symlink":
		err = cli.Symlink(c, rest)
	case "hardlink":
		err = cli.Hardlink(c, rest)
	case "sh":
		err = cli.Sh(c, rest)
	case "grep":
		err = cli.Grep(c, rest)
	case "find":
		err = cli.Find(c, rest)
	case "layer":
		err = cli.Layer(c, rest)
	case "-h", "-help", "--help", "help":
		fsUsage(0)
	default:
		fmt.Fprintf(os.Stderr, "drive9 fs: unknown command %q\n", sub)
		fsUsage(2)
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

func usage(code int) {
	fmt.Fprint(os.Stderr,
		"usage: drive9 <command> [arguments]\n\n"+
			"commands:\n"+
			"  create [--name NAME] [--server URL]\n"+
			"                         provision a new database and owner context\n"+
			"  ctx show [--json] [--reveal]\n"+
			"                         show current context\n"+
			"  ctx add --api-key <key> [--name NAME] [--server URL]\n"+
			"                         add owner context\n"+
			"  ctx import [--from-file <path|->] [--name NAME]\n"+
			"                         add delegated context\n"+
			"  ctx fork [<new>] [--from <ctx>] [--json]\n"+
			"                         create a copy-on-write fork context\n"+
			"  ctx ls [-l|--json]     list contexts\n"+
			"  ctx use <name>         activate context\n"+
			"  ctx rm <name>          delete context\n"+
			"  fs <command>           filesystem operations\n"+
			"  token <issue|revoke>   issue and revoke workspace-zone scoped tokens\n"+
			"  vault <set|get|put|with|ls|rm|grant|revoke|audit>\n"+
			"                         vault operations\n"+
			"  journal <new|append|cat|find|verify>\n"+
			"                         append-only agent/workflow journal operations\n"+
			"  git clone --fast <repo-url> <mounted-path>\n"+
			"                         git-aware fast clone workflow\n"+
			"  pack [flags] [archive] [path...]\n"+
			"                         archive coding-agent local overlay paths to drive9/S3\n"+
			"  unpack [flags] [archive]\n"+
			"                         restore a drive9 pack archive to a local overlay\n"+
			"  profile show [profile]\n"+
			"                         print mount profile configuration\n"+
			"  mount [flags] [:/remote] <mountpoint>\n"+
			"                         mount drive9 filesystem\n"+
			"  mount vault [flags] <mountpoint>\n"+
			"                         mount vault secrets read-only\n"+
			"  umount <mountpoint>    unmount a drive9 mount\n"+
			"  doctor fuse            diagnose local FUSE prerequisites\n"+
			"  update [--check]       update drive9 CLI in place\n\n"+
			"global:\n"+
			"  -h, --help, help       show this help\n"+
			"  -v, --version, version print version information\n",
	)
	exitWithCode(code)
}

func fsUsage(code int) {
	// Keep this usage block visually aligned. When editing wrapped help text,
	// preserve the column layout so subcommand descriptions remain easy to scan.
	fmt.Fprintf(os.Stderr, `usage: drive9 fs <command> [arguments]

commands:
  cp [flags] <src> <dst>
                       copy files between local, remote, stdin, and stdout
    --resume          resume an incomplete local-to-remote upload
    --append          append a local file to a remote file
    --tag <key=value> set file tag (repeatable, upload only; not with --append;
                       any --tag on re-upload replaces the existing tag set;
                       omit --tag to preserve existing tags)
    --description <text>
                       set file description (local/stdin upload only)
  cat <path>          read file to stdout
  ls [-l] [path]      list directory
  stat [-o text|json] <path>
                       file metadata
  mv <old> <new>      rename/move
  mkdir <path>        create directory (parents auto-created)
  chmod <mode> <path>  change file permissions (octal, e.g. 644)
  symlink <target> <link>
                       create symbolic link
  hardlink <target> <link>
                       create hard link
  rm [-r|--recursive] <path>
                       remove file or directory tree
  sh                  interactive shell
  grep <pattern> [dir] search file contents
  find [dir] [flags]   find files by attributes
    -name <glob>         match filename
    -tag <key=value>     exact match tag key/value
    -tag <key>           match files containing tag key
    -newer <YYYY-MM-DD>  modified after date
    -older <YYYY-MM-DD>  modified before date
    -size <+N|-N>        size filter in bytes
  layer <command>      manage filesystem layers
    create [flags] <base-root>
    list [--json]
    status [--json] <layer>
    diff [--json] <layer>
    checkpoint [flags] <layer>
    rollback <layer>
    commit <layer>

global:
  -h, --help, help       show this help
`)
	exitWithCode(code)
}

func exitWithCode(code int) {
	cpuProfileStop()
	exitFunc(code)
}
