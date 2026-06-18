package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"unicode/utf8"

	"golang.org/x/term"
)

type visualHelpOptions struct {
	showUsage bool
	plain     bool
	noPager   bool
	colorMode string
}

type visualHelpCommand struct {
	Name     string
	Args     string
	Summary  string
	Badge    string
	Details  []string
	Flags    []visualHelpFlag
	Sections []visualHelpSection
	Examples []visualHelpExample
}

type visualHelpSection struct {
	Title string
	Flags []visualHelpFlag
}

type visualHelpFlag struct {
	Name string
	Desc string
}

type visualHelpExample struct {
	Command string
	Desc    string
}

type helpStyle struct {
	reset   string
	bold    string
	dim     string
	cmd     string
	arg     string
	flag    string
	meta    string
	accent  string
	badge   string
	tree    string
	comment string
}

func runHelp(args []string) error {
	opts, err := parseVisualHelpOptions(args)
	if err != nil {
		return err
	}
	if opts.showUsage {
		_, _ = fmt.Fprintln(os.Stdout, visualHelpUsage())
		exitWithCode(0)
		return nil
	}
	if opts.plain {
		usage(0)
		return nil
	}

	stdoutIsTerminal := term.IsTerminal(int(os.Stdout.Fd()))
	useColor := shouldColorVisualHelp(opts.colorMode, stdoutIsTerminal)
	rendered := renderDrive9VisualHelp(useColor)
	if !opts.noPager && shouldPageVisualHelp(rendered, stdoutIsTerminal) {
		if err := pageVisualHelp(rendered); err == nil {
			exitWithCode(0)
			return nil
		}
	}

	_, _ = fmt.Fprint(os.Stdout, rendered)
	exitWithCode(0)
	return nil
}

func parseVisualHelpOptions(args []string) (visualHelpOptions, error) {
	opts := visualHelpOptions{colorMode: "auto"}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "-help" || arg == "--help":
			opts.showUsage = true
		case arg == "-t" || arg == "--tree":
			// Visual tree help is the default for `drive9 help`.
		case arg == "--plain":
			opts.plain = true
		case arg == "--no-pager":
			opts.noPager = true
		case arg == "--color":
			if i+1 >= len(args) {
				return opts, fmt.Errorf("--color requires auto, always, or never\n%s", visualHelpUsage())
			}
			i++
			opts.colorMode = args[i]
		case strings.HasPrefix(arg, "--color="):
			opts.colorMode = strings.TrimPrefix(arg, "--color=")
		default:
			return opts, fmt.Errorf("unknown help option %q\n%s", arg, visualHelpUsage())
		}
	}
	switch opts.colorMode {
	case "auto", "always", "never":
	default:
		return opts, fmt.Errorf("invalid --color value %q; use auto, always, or never\n%s", opts.colorMode, visualHelpUsage())
	}
	return opts, nil
}

func visualHelpUsage() string {
	return `usage: drive9 help [--plain] [--no-pager] [--color=auto|always|never]

flags:
  --plain          show the classic plain usage block
  --no-pager       print directly instead of opening less
  --color MODE     color mode: auto, always, or never`
}

func shouldColorVisualHelp(mode string, isTerminal bool) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	default:
		if !isTerminal {
			return false
		}
		if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
			return false
		}
		return true
	}
}

func shouldPageVisualHelp(rendered string, isTerminal bool) bool {
	if !isTerminal {
		return false
	}
	if _, err := exec.LookPath("less"); err != nil {
		return false
	}
	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || width <= 0 || height <= 0 {
		return false
	}
	lines := strings.Split(strings.TrimSuffix(rendered, "\n"), "\n")
	if len(lines) > height-1 {
		return true
	}
	for _, line := range lines {
		if printableLen(line) > width {
			return true
		}
	}
	return false
}

func pageVisualHelp(rendered string) error {
	cmd := exec.Command("less", "-R", "-S", "-F", "-X")
	in, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = in.Close()
		return err
	}
	_, writeErr := io.WriteString(in, rendered)
	closeErr := in.Close()
	waitErr := cmd.Wait()
	if writeErr != nil && !isPagerClosedPipe(writeErr) {
		return writeErr
	}
	if closeErr != nil && !isPagerClosedPipe(closeErr) {
		return closeErr
	}
	return waitErr
}

func isPagerClosedPipe(err error) bool {
	return errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE)
}

func printableLen(s string) int {
	n := 0
	for i := 0; i < len(s); {
		if s[i] == '\x1b' && i+1 < len(s) && s[i+1] == '[' {
			i += 2
			for i < len(s) {
				b := s[i]
				i++
				if b >= '@' && b <= '~' {
					break
				}
			}
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		if size == 0 {
			break
		}
		n++
		i += size
	}
	return n
}

func renderDrive9VisualHelp(color bool) string {
	style := newHelpStyle(color)
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s %s\n", style.command("drive9"), style.argText("<command>"), style.argText("[args] [flags]"))
	fmt.Fprintf(&b, "%s\n", style.treeText("│"))

	commands := drive9VisualHelpCommands()
	for i, cmd := range commands {
		renderVisualHelpCommand(&b, style, cmd, i == len(commands)-1)
	}

	fmt.Fprintf(&b, "\n%s\n", style.section("Help controls:"))
	fmt.Fprintf(&b, "  %s %s %s\n", padStyled(style.exampleCommand("drive9 help --no-pager"), 36), style.commentText("#"), style.commentText("print without less"))
	fmt.Fprintf(&b, "  %s %s %s\n", padStyled(style.exampleCommand("drive9 help --color=always"), 36), style.commentText("#"), style.commentText("force ANSI color"))
	fmt.Fprintf(&b, "  %s %s %s\n", padStyled(style.exampleCommand("drive9 help --plain"), 36), style.commentText("#"), style.commentText("classic script-friendly usage"))
	fmt.Fprintf(&b, "  %s %s %s\n", padStyled(style.flagText("less -R -S"), 36), style.commentText("#"), style.commentText("color-preserving vertical and horizontal scrolling"))
	return b.String()
}

func renderVisualHelpCommand(b *strings.Builder, style helpStyle, cmd visualHelpCommand, last bool) {
	const flagWidth = 42
	const descWidth = 72
	const exampleWidth = 52

	rootBranch := "├─"
	childPrefix := "│  "
	afterPrefix := "│"
	if last {
		rootBranch = "└─"
		childPrefix = "   "
		afterPrefix = " "
	}

	line := style.treeText(rootBranch) + " " + style.command("drive9 "+cmd.Name)
	if cmd.Args != "" {
		line += " " + style.argText(cmd.Args)
	}
	line += " " + style.arrow() + " " + style.commentText(cmd.Summary)
	if cmd.Badge != "" {
		line += " " + style.badgeText(cmd.Badge)
	}
	fmt.Fprintln(b, line)

	rows := make([]func(bool), 0, len(cmd.Details)+len(cmd.Flags)+len(cmd.Sections)+1)
	for _, detail := range cmd.Details {
		detail := detail
		rows = append(rows, func(lastChild bool) {
			renderWrappedTextRow(b, style, childPrefix, detail, lastChild, descWidth+flagWidth)
		})
	}
	for _, flag := range cmd.Flags {
		flag := flag
		rows = append(rows, func(lastChild bool) {
			renderFlagRow(b, style, childPrefix, flag, lastChild, flagWidth, descWidth)
		})
	}
	for _, section := range cmd.Sections {
		section := section
		rows = append(rows, func(lastChild bool) {
			sectionPrefix := childPrefix + style.treeText(childBranch(lastChild)) + " "
			fmt.Fprintf(b, "%s%s\n", sectionPrefix, style.section(section.Title+":"))
			nestedPrefix := childPrefix
			if lastChild {
				nestedPrefix += "   "
			} else {
				nestedPrefix += "│  "
			}
			for i, flag := range section.Flags {
				renderFlagRow(b, style, nestedPrefix, flag, i == len(section.Flags)-1, flagWidth, descWidth)
			}
		})
	}
	if len(cmd.Examples) > 0 {
		examples := cmd.Examples
		rows = append(rows, func(lastChild bool) {
			examplePrefix := childPrefix + style.treeText(childBranch(lastChild)) + " "
			fmt.Fprintf(b, "%s%s\n", examplePrefix, style.section("Examples:"))
			nestedPrefix := childPrefix
			if lastChild {
				nestedPrefix += "   "
			} else {
				nestedPrefix += "│  "
			}
			for i, ex := range examples {
				branch := childBranch(i == len(examples)-1)
				renderExampleRow(b, style, nestedPrefix, branch, ex, i == len(examples)-1, exampleWidth, descWidth)
			}
		})
	}
	for i, row := range rows {
		row(i == len(rows)-1)
	}
	if !last {
		fmt.Fprintln(b, style.treeText(afterPrefix))
	}
}

func childBranch(last bool) string {
	if last {
		return "└─"
	}
	return "├─"
}

func padStyled(v string, width int) string {
	padding := width - printableLen(v)
	if padding <= 0 {
		return v
	}
	return v + strings.Repeat(" ", padding)
}

func renderWrappedTextRow(b *strings.Builder, style helpStyle, prefix, text string, last bool, width int) {
	branch := childBranch(last)
	cont := "│ "
	if last {
		cont = "  "
	}
	lines := wrapPlainText(text, width)
	for i, line := range lines {
		if i == 0 {
			fmt.Fprintf(b, "%s%s %s\n", prefix, style.treeText(branch), style.commentText(line))
			continue
		}
		fmt.Fprintf(b, "%s%s %s\n", prefix, style.treeText(cont), style.commentText(line))
	}
}

func renderFlagRow(b *strings.Builder, style helpStyle, prefix string, flag visualHelpFlag, last bool, flagWidth, descWidth int) {
	branch := childBranch(last)
	cont := "│ "
	if last {
		cont = "  "
	}
	lines := wrapPlainText(flag.Desc, descWidth)
	for i, line := range lines {
		if i == 0 {
			fmt.Fprintf(b, "%s%s %s %s\n", prefix, style.treeText(branch), padStyled(style.flagText(flag.Name), flagWidth), style.commentText(line))
			continue
		}
		fmt.Fprintf(b, "%s%s %s %s\n", prefix, style.treeText(cont), strings.Repeat(" ", flagWidth), style.commentText(line))
	}
}

func renderExampleRow(b *strings.Builder, style helpStyle, prefix, branch string, ex visualHelpExample, last bool, commandWidth, descWidth int) {
	cont := "│ "
	if last {
		cont = "  "
	}
	cmd := style.exampleCommand(ex.Command)
	descLines := wrapPlainText(ex.Desc, descWidth)
	if printableLen(cmd) > commandWidth {
		fmt.Fprintf(b, "%s%s %s\n", prefix, style.treeText(branch), cmd)
		for i, line := range descLines {
			marker := "#"
			if i > 0 {
				marker = " "
			}
			fmt.Fprintf(b, "%s%s %s %s\n", prefix, style.treeText(cont), style.commentText(marker), style.commentText(line))
		}
		return
	}
	for i, line := range descLines {
		if i == 0 {
			fmt.Fprintf(b, "%s%s %s %s %s\n", prefix, style.treeText(branch), padStyled(cmd, commandWidth), style.commentText("#"), style.commentText(line))
			continue
		}
		fmt.Fprintf(b, "%s%s %s %s\n", prefix, style.treeText(cont), strings.Repeat(" ", commandWidth), style.commentText(line))
	}
}

func wrapPlainText(text string, width int) []string {
	if width <= 0 || printableLen(text) <= width {
		return []string{text}
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}
	var lines []string
	var line string
	for _, word := range words {
		if line == "" {
			line = word
			continue
		}
		if printableLen(line)+1+printableLen(word) <= width {
			line += " " + word
			continue
		}
		lines = append(lines, line)
		line = word
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

func drive9VisualHelpCommands() []visualHelpCommand {
	return []visualHelpCommand{
		{
			Name:    "create",
			Args:    "[flags]",
			Summary: "provision a new tenant and save an owner context",
			Details: []string{
				"no TiDB Cloud keys -> Anonymous mode; TiDB Cloud keys -> TiDBCloud mode",
				"Anonymous mode transfers data management rights to PingCAP",
			},
			Flags: []visualHelpFlag{
				{Name: "--name NAME", Desc: "context name; defaults to an auto-generated 7-character name"},
				{Name: "--region-code CODE", Desc: "provisioning region code; use drive9 region list to see available regions"},
				{Name: "--server URL", Desc: "override server URL and bypass region manifest lookup"},
				{Name: "--tidbcloud-public-key KEY", Desc: "TiDB Cloud public key; required for TiDBCloud mode"},
				{Name: "--tidbcloud-private-key KEY", Desc: "TiDB Cloud private key; required for TiDBCloud mode"},
				{Name: "--json", Desc: "output result as JSON"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 create", Desc: "provision an Anonymous tenant using the default region"},
				{Command: "drive9 create --region-code aws-ap-southeast-1 --tidbcloud-public-key <public-key> --tidbcloud-private-key <private-key>", Desc: "provision a TiDBCloud tenant"},
				{Command: "drive9 create --server http://127.0.0.1:9009", Desc: "provision directly against a known server"},
				{Command: "drive9 region list", Desc: "list available regions"},
			},
		},
		{
			Name:    "delete",
			Args:    "[flags]",
			Summary: "delete the current tenant, cluster, database, and API keys",
			Badge:   "DANGER",
			Details: []string{
				"TiDBCloud mode requires TiDB Cloud credentials for tenant deletion",
			},
			Flags: []visualHelpFlag{
				{Name: "--server URL", Desc: "server URL; defaults to active context server"},
				{Name: "--api-key KEY", Desc: "owner API key; defaults to active context API key"},
				{Name: "--tidbcloud-public-key KEY", Desc: "TiDB Cloud public key; required for TiDBCloud mode"},
				{Name: "--tidbcloud-private-key KEY", Desc: "TiDB Cloud private key; required for TiDBCloud mode"},
				{Name: "--json", Desc: "output result as JSON"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 delete", Desc: "delete the active context's tenant"},
				{Command: "drive9 delete --server https://api.drive9.ai --api-key drive9_xxx --tidbcloud-public-key <public-key> --tidbcloud-private-key <private-key>", Desc: "delete a TiDBCloud tenant with explicit credentials"},
			},
		},
		{
			Name:    "ctx",
			Args:    "<show|add|import|fork|ls|use|rm>",
			Summary: "manage local drive9 contexts",
			Details: []string{
				"show -> inspect active context; ls -> scan saved contexts",
				"add/import -> save owner or delegated keys",
				"fork -> create a copy-on-write context; rm -> remove local credentials",
			},
			Flags: []visualHelpFlag{
				{Name: "--json", Desc: "available on show, fork, and ls"},
				{Name: "--reveal", Desc: "show sensitive fields on ctx show"},
				{Name: "--type <kind>|--scoped", Desc: "filter ctx ls results"},
				{Name: "--tidbcloud-public-key KEY", Desc: "TiDB Cloud public key for ctx fork in TiDBCloud mode"},
				{Name: "--tidbcloud-private-key KEY", Desc: "TiDB Cloud private key; prefer DRIVE9_PRIVATE_KEY in shared shells"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 ctx show", Desc: "show active context"},
				{Command: "drive9 ctx add --api-key drive9_xxx --name dev", Desc: "save a context"},
				{Command: "drive9 ctx use dev", Desc: "activate context"},
			},
		},
		{
			Name:    "fs",
			Args:    "<command> [arguments]",
			Summary: "filesystem operations over drive9 paths",
			Details: []string{
				"paths are absolute remote paths; directories end with /",
			},
			Sections: []visualHelpSection{
				{Title: "Read/write", Flags: []visualHelpFlag{
					{Name: "cp [-r] [--resume] <src> <dst>", Desc: "copy local, remote, stdin, and stdout paths"},
					{Name: "cp --append <src> <dst>", Desc: "append local or stdin input to a remote file"},
					{Name: "cp --tag key=value", Desc: "set upload tags; repeatable; re-upload with tags replaces existing tag set"},
					{Name: "cp --description TEXT", Desc: "set file description on local/stdin upload"},
					{Name: "cat <path>", Desc: "read a remote file to stdout"},
					{Name: "ls [-l] [path]", Desc: "list a remote directory"},
					{Name: "stat [-o text|json] <path>", Desc: "show file metadata"},
				}},
				{Title: "Namespace", Flags: []visualHelpFlag{
					{Name: "mv <old> <new>", Desc: "rename or move a path"},
					{Name: "mkdir <path>", Desc: "create a directory; parents are auto-created"},
					{Name: "chmod <mode> <path>", Desc: "change permissions with octal mode, e.g. 644"},
					{Name: "symlink <target> <link>", Desc: "create a symbolic link"},
					{Name: "hardlink <target> <link>", Desc: "create a hard link"},
					{Name: "rm [-r|--recursive] <path>", Desc: "remove a file or directory tree"},
				}},
				{Title: "Search", Flags: []visualHelpFlag{
					{Name: "grep [--layer REF] <pattern> [dir]", Desc: "search file contents"},
					{Name: "find [dir] -name GLOB", Desc: "match filename glob"},
					{Name: "find [dir] -tag key=value", Desc: "exact tag key/value match"},
					{Name: "find [dir] -tag key", Desc: "tag-key existence match"},
					{Name: "find [dir] -newer YYYY-MM-DD", Desc: "modified after date"},
					{Name: "find [dir] -older YYYY-MM-DD", Desc: "modified before date"},
					{Name: "find [dir] -size +N|-N", Desc: "size filter in bytes"},
				}},
				{Title: "Layers", Flags: []visualHelpFlag{
					{Name: "layer create [flags] <base-root>", Desc: "create writable fs layer"},
					{Name: "layer list [--json]", Desc: "list layers"},
					{Name: "layer status|diff [--json] <layer>", Desc: "inspect layer state or diff"},
					{Name: "layer checkpoint [flags] <layer>", Desc: "create a layer checkpoint"},
					{Name: "layer rollback <layer>", Desc: "rollback a layer"},
					{Name: "layer commit <layer>", Desc: "commit a layer into its base"},
				}},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 fs ls -l :/", Desc: "list root"},
				{Command: "drive9 fs cp ./notes.md :/team/notes.md", Desc: "upload a file"},
				{Command: "drive9 fs find :/team -tag owner=agent", Desc: "find by exact tag"},
			},
		},
		{
			Name:    "token",
			Args:    "<issue|revoke>",
			Summary: "issue and revoke workspace-zone scoped filesystem tokens",
			Badge:   "SCOPED",
			Flags: []visualHelpFlag{
				{Name: "issue --subject NAME", Desc: "token subject"},
				{Name: "issue --zone PATH", Desc: "workspace zone root"},
				{Name: "revoke <name|->", Desc: "revoke by name or stdin"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 token issue --subject vm0 --zone :/work", Desc: "create scoped token"},
				{Command: "drive9 token revoke vm0", Desc: "revoke token"},
			},
		},
		{
			Name:    "vault",
			Args:    "<set|get|put|with|ls|rm|grant|revoke|audit>",
			Summary: "manage encrypted vault secrets",
			Details: []string{
				"put imports local secret directories; with runs a command with mounted secrets",
				"grant/revoke/audit manage secret access and audit trails",
			},
			Examples: []visualHelpExample{
				{Command: "drive9 vault ls", Desc: "list secrets"},
				{Command: "drive9 vault put /n/vault/app --from ./secrets", Desc: "import a secret directory"},
				{Command: "drive9 mount vault ./vault", Desc: "read-only vault mount"},
			},
		},
		{
			Name:    "journal",
			Args:    "<new|append|cat|find|verify>",
			Summary: "append-only agent/workflow journal operations",
			Flags: []visualHelpFlag{
				{Name: "--type TYPE", Desc: "journal or entry type"},
				{Name: "--label key=value", Desc: "repeatable metadata label"},
				{Name: "--json", Desc: "machine-readable output"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 journal new --type run", Desc: "create journal"},
				{Command: "drive9 journal append :/journal/run.md --label phase=plan", Desc: "append entry"},
			},
		},
		{
			Name:    "git",
			Args:    "<clone|worktree|hydrate>",
			Summary: "git-aware workflows for mounted drive9 workspaces",
			Badge:   "FAST",
			Sections: []visualHelpSection{
				{Title: "clone", Flags: []visualHelpFlag{
					{Name: "clone --fast <repo-url> <path>", Desc: "clone and register a git workspace"},
					{Name: "--blobless", Desc: "use a partial local .git; clean blobs lazy-fetch from remote"},
					{Name: "--hydrate=auto|background|sync|off", Desc: "clean-tree hydration strategy for blobless clones"},
				}},
				{Title: "worktree", Flags: []visualHelpFlag{
					{Name: "worktree add --fast <base> <path>", Desc: "create and register a linked worktree"},
					{Name: "-b <branch>", Desc: "create a new branch for the worktree"},
					{Name: "--detach", Desc: "detach HEAD in the new worktree"},
					{Name: "--blobless", Desc: "require a blobless base workspace"},
					{Name: "--hydrate=auto|background|sync|off", Desc: "clean-tree hydration strategy"},
					{Name: "worktree remove --fast [--force]", Desc: "remove a registered worktree"},
				}},
				{Title: "hydrate", Flags: []visualHelpFlag{
					{Name: "hydrate [--timeout=30m] <path>", Desc: "materialize a blobless clean tree into the local cache"},
				}},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 git clone --fast https://repo.git ./repo", Desc: "fast clone into mount"},
				{Command: "drive9 git clone --fast --blobless --hydrate=sync https://repo.git ./repo", Desc: "blobless fast clone"},
				{Command: "drive9 git hydrate ./repo", Desc: "materialize clean blobs"},
			},
		},
		{
			Name:    "region",
			Args:    "list [--json] [--manifest-url URL]",
			Summary: "list provisioning regions from the drive9 manifest",
			Flags: []visualHelpFlag{
				{Name: "--json", Desc: "output region manifest entries as JSON"},
				{Name: "--manifest-url URL", Desc: "override the region manifest URL"},
			},
			Details: []string{
				"text output columns: REGION CODE, CLOUD PROVIDER, REGION, MODE, SERVER",
				"Anonymous regions print a data-management rights note on stderr",
			},
			Examples: []visualHelpExample{
				{Command: "drive9 region list", Desc: "human-readable regions"},
				{Command: "drive9 region list --json", Desc: "scriptable regions"},
				{Command: "drive9 region list --manifest-url http://127.0.0.1:9009/regions.json", Desc: "use a custom manifest during local testing"},
			},
		},
		{
			Name:    "pack",
			Args:    "[flags] [archive] [path...]",
			Summary: "archive local overlay paths to drive9/S3",
			Flags: []visualHelpFlag{
				{Name: "--local-root DIR", Desc: "profile local overlay root"},
				{Name: "--remote-root PATH", Desc: "remote root used to resolve relative pack paths"},
				{Name: "--profile NAME", Desc: "profile defaults when no paths are provided"},
				{Name: "--mount PATH", Desc: "mounted path whose metadata provides local-root, remote-root, and profile"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 pack --profile coding-agent", Desc: "pack configured overlay paths"},
				{Command: "drive9 pack :/packs/work.tar.gz .git dist", Desc: "explicit archive and paths"},
				{Command: "drive9 pack --mount ./mnt", Desc: "pack using mount metadata"},
			},
		},
		{
			Name:    "unpack",
			Args:    "[flags] [archive]",
			Summary: "restore a drive9 pack archive to a local overlay",
			Flags: []visualHelpFlag{
				{Name: "--local-root DIR", Desc: "profile local overlay root"},
				{Name: "--remote-root PATH", Desc: "remote root used to resolve default profile pack archive"},
				{Name: "--profile NAME", Desc: "profile defaults when no archive is provided"},
				{Name: "--mount PATH", Desc: "mounted path whose metadata provides local-root"},
				{Name: "--no-replace", Desc: "do not remove archived root paths before extracting"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 unpack --profile coding-agent", Desc: "restore default pack"},
				{Command: "drive9 unpack :/packs/work.tar.gz", Desc: "restore explicit archive"},
				{Command: "drive9 unpack --mount ./mnt --no-replace", Desc: "restore into mount local-root without deleting existing roots"},
			},
		},
		{
			Name:    "profile",
			Args:    "show [profile]",
			Summary: "print mount profile configuration",
			Examples: []visualHelpExample{
				{Command: "drive9 profile show coding-agent", Desc: "inspect a profile"},
			},
		},
		{
			Name:    "mount",
			Args:    "[flags] [:/remote] <mountpoint>",
			Summary: "mount drive9 as a local filesystem",
			Sections: []visualHelpSection{
				{Title: "Connection", Flags: []visualHelpFlag{
					{Name: "--server URL", Desc: "drive9 server URL; overrides DRIVE9_SERVER and config"},
					{Name: "--api-key KEY", Desc: "owner API key; overrides DRIVE9_API_KEY and config"},
					{Name: "--mode auto|fuse|webdav", Desc: "mount mode; auto selects the best supported mode"},
					{Name: "--foreground", Desc: "run in foreground and block until unmounted"},
				}},
				{Title: "Read cache", Flags: []visualHelpFlag{
					{Name: "--cache-dir DIR", Desc: "write-back cache directory; default ~/.cache/drive9"},
					{Name: "--cache-size MB", Desc: "memory read cache size"},
					{Name: "--read-cache-max-file-mb MB", Desc: "maximum single file admitted to read cache"},
					{Name: "--read-cache-ttl DURATION", Desc: "read cache TTL; 0 disables time-based expiry"},
					{Name: "--disk-read-cache-size-mb MB", Desc: "disk-backed read cache size"},
					{Name: "--disk-read-cache-free-ratio FLOAT", Desc: "minimum filesystem free-space ratio before disk cache evicts"},
					{Name: "--dir-ttl DURATION", Desc: "directory cache TTL"},
					{Name: "--attr-ttl DURATION", Desc: "kernel attr cache TTL"},
					{Name: "--entry-ttl DURATION", Desc: "kernel entry cache TTL"},
				}},
				{Title: "FUSE read path", Flags: []visualHelpFlag{
					{Name: "--lookup-retry-count N", Desc: "detached retries after transient Lookup/GetAttr stat failures; 0 disables"},
					{Name: "--lookup-retry-timeout DURATION", Desc: "timeout per detached Lookup/GetAttr stat retry"},
					{Name: "--read-concurrency N", Desc: "maximum concurrent backend reads issued by FUSE"},
					{Name: "--parallel-read-concurrency N", Desc: "maximum concurrent block reads for one large FUSE read"},
					{Name: "--parallel-read-block-size-mb MB", Desc: "block size for parallel large-file reads"},
					{Name: "--fuse-sync-read", Desc: "disable kernel async read dispatch"},
					{Name: "--legacy-dir-stat-fallback", Desc: "list parent on Lookup stat 404 for legacy servers without directory stat"},
					{Name: "--trust-process-local-events", Desc: "allow revision-bound dir-cache hits using process-local SSE freshness"},
				}},
				{Title: "Prefetch", Flags: []visualHelpFlag{
					{Name: "--readdir-prefetch", Desc: "prefetch small files after directory reads into read cache"},
					{Name: "--readdir-prefetch-max-files N", Desc: "maximum small files prefetched per directory read"},
					{Name: "--readdir-prefetch-max-file-bytes N", Desc: "maximum individual file size for readdir prefetch"},
					{Name: "--readdir-prefetch-max-bytes N", Desc: "maximum aggregate bytes prefetched per directory read"},
					{Name: "--readdir-prefetch-timeout DURATION", Desc: "timeout for one readdir prefetch batch"},
				}},
				{Title: "Overlay profiles", Flags: []visualHelpFlag{
					{Name: "--profile NAME", Desc: "mount profile: coding-agent, portable, none, interactive, or a custom profile file"},
					{Name: "--local-root DIR", Desc: "local-only overlay storage root; auto-generated for overlay profiles"},
					{Name: "--local-only PATTERN", Desc: "additional local-only path pattern; repeatable"},
					{Name: "--remote-only PATTERN", Desc: "remote-persistent override path pattern; repeatable"},
					{Name: "--unpack :/archive.tar.gz", Desc: "restore a drive9 pack archive into local-root before mounting; repeatable"},
					{Name: "--no-auto-unpack", Desc: "disable automatic profile pack restore before mounting"},
				}},
				{Title: "Layers and writes", Flags: []visualHelpFlag{
					{Name: "--layer REF", Desc: "mount through writable fs layer by id, name, or tag ref"},
					{Name: "--checkpoint REF", Desc: "restore fs layer checkpoint before mounting"},
					{Name: "--durability MODE", Desc: "write durability: auto, interactive, fsync, close-sync, or write-sync"},
					{Name: "--flush-debounce DURATION", Desc: "debounce window for small-file flush coalescing; 0 disables"},
					{Name: "--upload-concurrency N", Desc: "maximum concurrent background uploads issued by FUSE"},
				}},
				{Title: "Access and debug", Flags: []visualHelpFlag{
					{Name: "--allow-other", Desc: "allow other users to access mount"},
					{Name: "--read-only", Desc: "mount as read-only"},
					{Name: "--debug", Desc: "enable FUSE debug logging"},
				}},
				{Title: "Profiling", Flags: []visualHelpFlag{
					{Name: "--perf-dir DIR", Desc: "enable standard FUSE profiling outputs in this directory"},
					{Name: "--perf-interval DURATION", Desc: "continuous performance sample interval"},
					{Name: "--perf-max-samples N", Desc: "maximum samples per continuous perf JSONL segment"},
					{Name: "--perf-max-sample-files N", Desc: "maximum retained continuous perf sample files"},
					{Name: "--perf-max-profile-files N", Desc: "maximum retained CPU and heap profile files per type"},
					{Name: "--perf-cpu-duration DURATION", Desc: "CPU profile capture window duration"},
					{Name: "--perf-cpu-interval DURATION", Desc: "periodically capture CPU profiles at this interval"},
					{Name: "--perf-heap-interval DURATION", Desc: "periodically write heap profiles at this interval"},
					{Name: "--perf-addr ADDR", Desc: "serve live pprof, e.g. 127.0.0.1:6060"},
				}},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 mount :/ ./mnt", Desc: "mount root"},
				{Command: "drive9 mount --profile coding-agent :/workspace ./mnt", Desc: "profile mount"},
				{Command: "drive9 mount --layer layer_123 --checkpoint cp_456 :/team ./mnt", Desc: "restore a layer checkpoint before mounting"},
				{Command: "drive9 mount --perf-dir ./perf --foreground :/ ./mnt", Desc: "foreground mount with profiling outputs"},
			},
		},
		{
			Name:    "mount vault",
			Args:    "[flags] <mountpoint>",
			Summary: "mount vault secrets read-only",
			Badge:   "READONLY",
			Flags: []visualHelpFlag{
				{Name: "--server URL", Desc: "drive9 server URL; overrides DRIVE9_SERVER and config"},
				{Name: "--api-key KEY", Desc: "owner API key; overrides DRIVE9_API_KEY and config"},
				{Name: "--foreground", Desc: "run in foreground and block until unmounted"},
				{Name: "--dir-ttl DURATION", Desc: "vault list / field cache TTL"},
				{Name: "--allow-other", Desc: "allow other users to access mount"},
				{Name: "--debug", Desc: "enable FUSE debug logging"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 mount vault ./vault", Desc: "browse vault secrets locally"},
			},
		},
		{
			Name:    "umount",
			Args:    "[flags] <mountpoint>",
			Summary: "unmount a drive9 mount and optionally pack overlay state",
			Flags: []visualHelpFlag{
				{Name: "--timeout DURATION", Desc: "wait for flush/unmount"},
				{Name: "--pack :/archive.tar.gz", Desc: "write pack archive before unmount"},
				{Name: "--pack-path PATH", Desc: "include a local overlay path in the automatic/default pack; repeatable"},
				{Name: "--no-auto-pack", Desc: "skip configured auto-pack"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 umount ./mnt", Desc: "clean unmount"},
				{Command: "drive9 umount --pack :/packs/work.tar.gz ./mnt", Desc: "pack on unmount"},
			},
		},
		{
			Name:    "doctor",
			Args:    "fuse",
			Summary: "diagnose local runtime prerequisites",
			Flags: []visualHelpFlag{
				{Name: "--mountpoint PATH", Desc: "mountpoint path to inspect"},
				{Name: "--cache-dir DIR", Desc: "cache directory to inspect"},
				{Name: "--server URL", Desc: "drive9 server URL to check"},
				{Name: "--timeout DURATION", Desc: "server connectivity timeout"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 doctor fuse", Desc: "check FUSE support"},
				{Command: "drive9 doctor fuse --mountpoint ./mnt --server http://127.0.0.1:9009", Desc: "check a local dev mount target and server"},
			},
		},
		{
			Name:    "update",
			Args:    "[--check] [--force] [--base-url URL]",
			Summary: "update the drive9 CLI binary in place",
			Badge:   "HTTPS",
			Flags: []visualHelpFlag{
				{Name: "--check", Desc: "report available update without installing"},
				{Name: "--force", Desc: "reinstall current or older release"},
				{Name: "--base-url URL", Desc: "release root; HTTPS required by default"},
			},
			Examples: []visualHelpExample{
				{Command: "drive9 update --check", Desc: "check latest release"},
				{Command: "drive9 update", Desc: "install newer release"},
			},
		},
	}
}

func newHelpStyle(color bool) helpStyle {
	if !color {
		return helpStyle{}
	}
	return helpStyle{
		reset:   "\x1b[0m",
		bold:    "\x1b[1m",
		dim:     "\x1b[2m",
		cmd:     "\x1b[1;36m",
		arg:     "\x1b[3;32m",
		flag:    "\x1b[33m",
		meta:    "\x1b[35m",
		accent:  "\x1b[36m",
		badge:   "\x1b[1;38;5;213m",
		tree:    "\x1b[90m",
		comment: "\x1b[2;37m",
	}
}

func (s helpStyle) command(v string) string {
	return s.wrap(s.cmd, v)
}

func (s helpStyle) argText(v string) string {
	return s.wrap(s.arg, v)
}

func (s helpStyle) flagText(v string) string {
	return colorFlagMetavars(s, v)
}

func (s helpStyle) commentText(v string) string {
	return s.wrap(s.comment, v)
}

func (s helpStyle) treeText(v string) string {
	return s.wrap(s.tree, v)
}

func (s helpStyle) section(v string) string {
	return s.wrap(s.bold, v)
}

func (s helpStyle) badgeText(v string) string {
	return s.wrap(s.badge, "["+v+"]")
}

func (s helpStyle) arrow() string {
	return s.wrap(s.accent, "→")
}

func (s helpStyle) exampleCommand(v string) string {
	fields := strings.Fields(v)
	for i, field := range fields {
		switch {
		case i == 0 || (i == 1 && field != "help"):
			fields[i] = s.command(field)
		case strings.HasPrefix(field, "-"):
			fields[i] = s.flagText(field)
		case strings.HasPrefix(field, ":/") || strings.HasPrefix(field, "./") || strings.HasPrefix(field, "https://"):
			fields[i] = s.argText(field)
		}
	}
	return strings.Join(fields, " ")
}

func (s helpStyle) wrap(code, v string) string {
	if code == "" {
		return v
	}
	return code + v + s.reset
}

func colorFlagMetavars(s helpStyle, v string) string {
	if s.flag == "" {
		return v
	}
	fields := strings.Fields(v)
	for i, field := range fields {
		if strings.HasPrefix(field, "-") {
			fields[i] = s.wrap(s.flag, field)
			continue
		}
		allMeta := true
		for _, r := range field {
			if r < 'A' || r > 'Z' {
				allMeta = false
				break
			}
		}
		if allMeta || strings.HasPrefix(field, "<") || strings.HasPrefix(field, "[") {
			fields[i] = s.wrap(s.meta, field)
		}
	}
	return strings.Join(fields, " ")
}
