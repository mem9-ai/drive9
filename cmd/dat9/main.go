// Command dat9 provides a CLI for dat9 file and data operations.
//
// Usage:
//
//	dat9 <command> <subcommand> [arguments]
//
// Commands:
//
//	fs      filesystem operations (cp, cat, ls, stat, mv, rm, sh)
//	db      database operations (create, list, status, sql)
//	use     set default database
//	version show version
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
	case "use":
		if err := cli.Use(args); err != nil {
			fatal("use", err)
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

func parseDBAndArgs(args []string) (dbName string, rest []string) {
	if len(args) == 0 {
		return "", args
	}
	for i, arg := range args {
		if idx := strings.Index(arg, ":/"); idx > 0 {
			args[i] = arg[idx:]
			return arg[:idx], args
		}
	}
	return "", args
}

func runFS(args []string) {
	if len(args) < 1 {
		fsUsage()
	}
	sub := args[0]
	rest := args[1:]
	dbName, rest := parseDBAndArgs(rest)
	c := cli.NewClient(dbName)

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

	switch sub {
	case "create":
		if err := cli.DBCreate(rest); err != nil {
			fatal("db create", err)
		}
	case "list", "ls":
		if err := cli.DBList(); err != nil {
			fatal("db list", err)
		}
	case "status":
		if err := cli.DBStatus(rest); err != nil {
			fatal("db status", err)
		}
	case "sql":
		dbName, sqlArgs := parseDBFromSQLArgs(rest)
		c := cli.NewClient(dbName)
		if err := cli.SQL(c, sqlArgs); err != nil {
			fatal("db sql", err)
		}
	case "-h", "-help", "help":
		dbUsage()
	default:
		fmt.Fprintf(os.Stderr, "dat9 db: unknown command %q\n", sub)
		dbUsage()
	}
}

func parseDBFromSQLArgs(args []string) (dbName string, rest []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
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
  use [name]       set or show default database

environment:
  DAT9_SERVER      server URL (default: http://localhost:9009)
  DAT9_API_KEY     API key
`)
	os.Exit(2)
}

func fsUsage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 fs <command> [arguments]

Remote paths use <db>:/path format (e.g. mydb:/data/file.txt).
Omit <db> to use the default database.

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
  create <name>           create a new database
  list                    list databases
  status <name>           show database status
  sql [db] -q "query"     execute SQL query
  sql [db] -f file.sql    execute SQL from file
`)
	os.Exit(2)
}
