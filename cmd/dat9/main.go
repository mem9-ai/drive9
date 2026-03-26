// Command dat9 provides a Plan 9-inspired CLI for dat9 file operations.
//
// Usage:
//
//	dat9 <command> [arguments]
//
// Commands:
//
//	cp    copy files (local→remote, remote→local, remote→remote)
//	cat   read file content to stdout
//	ls    list directory contents
//	stat  show file metadata
//	mv    rename/move a file or directory
//	rm    remove a file or directory
//	sh    interactive shell
package main

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/cmd/dat9/cli"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	c := cli.NewFromEnv()

	var err error
	switch cmd {
	case "cp":
		err = cli.Cp(c, args)
	case "cat":
		err = cli.Cat(c, args)
	case "ls":
		err = cli.Ls(c, args)
	case "stat":
		err = cli.Stat(c, args)
	case "mv":
		err = cli.Mv(c, args)
	case "rm":
		err = cli.Rm(c, args)
	case "sh":
		err = cli.Sh(c, args)
	case "-h", "-help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "dat9: unknown command %q\n", cmd)
		usage()
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: dat9 <command> [arguments]

commands:
  cp <src> <dst>   copy files
  cat <path>       read file to stdout
  ls [path]        list directory
  stat <path>      file metadata
  mv <old> <new>   rename/move
  rm <path>        remove
  sh               interactive shell

environment:
  DAT9_SERVER      server URL (default: http://localhost:9009)
  DAT9_API_KEY     API key
`)
	os.Exit(2)
}
