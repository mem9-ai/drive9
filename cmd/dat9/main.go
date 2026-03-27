// Command dat9 provides a CLI for dat9 file and data operations.
//
// Usage:
//
//	dat9 <command> [arguments]
//
// Commands:
//
//	create  provision a new database
//	ctx     switch or list contexts
//	fs      filesystem operations (cp, cat, ls, stat, mv, rm, sh, grep, find)
package main

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/cmd/dat9/cli"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "--version", "-v", "version":
		fmt.Printf("dat9 %s\n", version)
	case "-h", "-help", "help":
		usage()
	case "create":
		if err := cli.Create(args); err != nil {
			fatal("create", err)
		}
	case "ctx":
		if err := cli.Ctx(args); err != nil {
			fatal("ctx", err)
		}
	case "fs":
		runFS(args)
	default:
		fmt.Fprintf(os.Stderr, "dat9: unknown command %q\n", cmd)
		usage()
	}
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
		fmt.Fprintf(os.Stderr, "dat9 fs: unknown command %q\n", sub)
		fsUsage()
	}
	if err != nil {
		fatal("fs "+sub, err)
	}
}

func fatal(cmd string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", cmd, err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 <command> [arguments]

commands:
  create           provision a new database
  ctx [name]       switch context (or show current)
  ctx list         list all contexts
  fs               filesystem operations

environment:
  DAT9_SERVER      server URL (default: http://localhost:9009)
  DAT9_API_KEY     API key (overrides config)
`)
	os.Exit(2)
}

func fsUsage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 fs <command> [arguments]

commands:
  cp <src> <dst>       copy files (local↔remote)
  cat <path>           read file to stdout
  ls [path]            list directory
  stat <path>          file metadata
  mv <old> <new>       rename/move
  rm <path>            remove
  sh                   interactive shell
  grep <pattern> [dir] search file contents
  find [dir] [flags]   find files by attributes
    -name <glob>         match filename
    -tag <key=value>     match tag
    -newer <YYYY-MM-DD>  modified after date
    -older <YYYY-MM-DD>  modified before date
    -size <+N|-N>        size filter in bytes
`)
	os.Exit(2)
}
