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
//	fs      filesystem operations (cp, cat, ls, stat, mv, rm, sh)
//	db      database operations (sql)
package main

import (
	"fmt"
	"os"
	"strings"

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
	case "db":
		runDB(args)
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

	switch sub {
	case "cp":
		c := cli.NewFromEnv()
		if err := cli.Cp(c, rest); err != nil {
			fatal("fs cp", err)
		}
	case "cat", "ls", "stat", "mv", "rm", "sh":
		ctxName, rest := extractContext(rest)
		c := cli.NewClientForContext(ctxName)
		var err error
		switch sub {
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
		}
		if err != nil {
			fatal("fs "+sub, err)
		}
	case "-h", "-help", "help":
		fsUsage()
	default:
		fmt.Fprintf(os.Stderr, "dat9 fs: unknown command %q\n", sub)
		fsUsage()
	}
}

func runDB(args []string) {
	if len(args) < 1 {
		dbUsage()
	}
	sub := args[0]
	rest := args[1:]

	switch sub {
	case "sql":
		c := cli.NewFromEnv()
		if err := cli.SQL(c, rest); err != nil {
			fatal("db sql", err)
		}
	case "-h", "-help", "help":
		dbUsage()
	default:
		fmt.Fprintf(os.Stderr, "dat9 db: unknown command %q\n", sub)
		dbUsage()
	}
}

func extractContext(args []string) (string, []string) {
	ctxName := ""
	for i, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		idx := strings.Index(arg, ":/")
		if idx >= 0 {
			name := ""
			if idx > 0 {
				name = arg[:idx]
			}
			if name != "" && ctxName != "" && ctxName != name {
				fmt.Fprintf(os.Stderr, "error: conflicting contexts: %s vs %s\n", ctxName, name)
				os.Exit(1)
			}
			if name != "" {
				ctxName = name
			}
			args[i] = arg[idx+1:]
		}
	}
	return ctxName, args
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
  db               database operations

environment:
  DAT9_SERVER      server URL (default: http://localhost:9009)
  DAT9_API_KEY     API key (overrides config)
`)
	os.Exit(2)
}

func fsUsage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 fs <command> [arguments]

commands:
  cp <src> <dst>   copy files (local↔remote)
  cat <path>       read file to stdout
  ls [path]        list directory
  stat <path>      file metadata
  mv <old> <new>   rename/move
  rm <path>        remove
  sh               interactive shell
`)
	os.Exit(2)
}

func dbUsage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 db <command> [arguments]

commands:
  sql -q "query"   execute SQL query
  sql -f file.sql  execute SQL from file
`)
	os.Exit(2)
}
