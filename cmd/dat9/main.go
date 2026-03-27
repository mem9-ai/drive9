// Command dat9 provides a CLI for dat9 file and data operations.
//
// Usage:
//
//	dat9 <command> <subcommand> [arguments]
//
// Commands:
//
//	fs    filesystem operations (cp, cat, ls, stat, mv, rm, sh)
//	db    database operations (sql)
//	auth  save or show API key
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
	case "auth":
		if err := cli.Auth(nil, args); err != nil {
			fatal("auth", err)
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

func runDB(args []string) {
	if len(args) < 1 {
		dbUsage()
	}
	sub := args[0]
	rest := args[1:]
	c := cli.NewFromEnv()

	var err error
	switch sub {
	case "sql":
		err = cli.SQL(c, rest)
	case "-h", "-help", "help":
		dbUsage()
	default:
		fmt.Fprintf(os.Stderr, "dat9 db: unknown command %q\n", sub)
		dbUsage()
	}
	if err != nil {
		fatal("db "+sub, err)
	}
}

func fatal(cmd string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", cmd, err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 <command> [arguments]

commands:
  fs               filesystem operations
  db               database operations
  auth [api-key]   save or show API key (~/.dat9/credentials)
  version          show version

environment:
  DAT9_SERVER      server URL (default: http://localhost:9009)
  DAT9_API_KEY     API key (or use: dat9 auth <key>)
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
