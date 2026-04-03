package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

// Sh runs an interactive shell.
//
//	drive9 sh
//
// The shell supports: cd, pwd, ls, cat, cp, mv, rm, stat, help, exit.
// Paths are resolved relative to the current working directory.
func Sh(c *client.Client, _ []string) error {
	cwd := "/"
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("drive9:%s> ", cwd)
		if !scanner.Scan() {
			fmt.Println()
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		cmd := fields[0]
		args := fields[1:]

		switch cmd {
		case "exit", "quit":
			return nil

		case "help":
			shHelp()

		case "pwd":
			fmt.Println(cwd)

		case "cd":
			dir := "/"
			if len(args) > 0 {
				dir = resolve(cwd, args[0])
			}
			// Verify the path exists and is a directory
			s, err := c.Stat(dir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "cd: %v\n", err)
				continue
			}
			if !s.IsDir {
				fmt.Fprintf(os.Stderr, "cd: %s: not a directory\n", dir)
				continue
			}
			cwd = dir
			if !strings.HasSuffix(cwd, "/") {
				cwd += "/"
			}

		case "ls":
			path := cwd
			var lsArgs []string
			for _, a := range args {
				if strings.HasPrefix(a, "-") {
					lsArgs = append(lsArgs, a)
				} else {
					path = resolve(cwd, a)
				}
			}
			lsArgs = append(lsArgs, path)
			if err := Ls(c, lsArgs); err != nil {
				fmt.Fprintf(os.Stderr, "ls: %v\n", err)
			}

		case "cat":
			if len(args) != 1 {
				fmt.Fprintln(os.Stderr, "usage: cat <path>")
				continue
			}
			if err := Cat(c, []string{resolve(cwd, args[0])}); err != nil {
				fmt.Fprintf(os.Stderr, "cat: %v\n", err)
			}

		case "cp":
			if len(args) != 2 {
				fmt.Fprintln(os.Stderr, "usage: cp <src> <dst>")
				continue
			}
			// In shell mode, all paths are remote — add ":" prefix for Cp
			if err := Cp(c, []string{":" + resolve(cwd, args[0]), ":" + resolve(cwd, args[1])}); err != nil {
				fmt.Fprintf(os.Stderr, "cp: %v\n", err)
			}

		case "mv":
			if len(args) != 2 {
				fmt.Fprintln(os.Stderr, "usage: mv <old> <new>")
				continue
			}
			if err := Mv(c, []string{resolve(cwd, args[0]), resolve(cwd, args[1])}); err != nil {
				fmt.Fprintf(os.Stderr, "mv: %v\n", err)
			}

		case "rm":
			if len(args) != 1 {
				fmt.Fprintln(os.Stderr, "usage: rm <path>")
				continue
			}
			if err := Rm(c, []string{resolve(cwd, args[0])}); err != nil {
				fmt.Fprintf(os.Stderr, "rm: %v\n", err)
			}

		case "stat":
			if len(args) != 1 {
				fmt.Fprintln(os.Stderr, "usage: stat <path>")
				continue
			}
			if err := Stat(c, []string{resolve(cwd, args[0])}); err != nil {
				fmt.Fprintf(os.Stderr, "stat: %v\n", err)
			}

		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s (type help)\n", cmd)
		}
	}
}

// resolve resolves a path relative to the current working directory.
// Handles ":" prefixed remote paths like cp command (fixes issue #19).
func resolve(cwd, path string) string {
	// Handle ":" prefixed remote paths
	if rp, isRemote := ParseRemote(path); isRemote {
		return rp.Path
	}
	if strings.HasPrefix(path, "/") {
		return path
	}
	joined := strings.TrimSuffix(cwd, "/") + "/" + path
	// Canonicalize to clean up any double slashes
	canon, err := pathutil.Canonicalize(joined)
	if err != nil {
		return joined
	}
	return canon
}

func shHelp() {
	fmt.Println(`commands:
  cd [path]       change directory
  pwd             print working directory
  ls [-l] [path]  list directory
  cat <path>      read file
  cp <src> <dst>  copy files
  mv <old> <new>  rename/move
  rm <path>       remove
  stat <path>     file metadata
  help            this help
  exit            quit shell`)
}
