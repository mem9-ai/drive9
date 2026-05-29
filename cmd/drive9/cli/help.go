package cli

// IsHelpArg reports whether arg is one of the accepted CLI help spellings.
func IsHelpArg(arg string) bool {
	switch arg {
	case "-h", "-help", "--help", "help":
		return true
	default:
		return false
	}
}

// IsHelpArgs reports whether argv contains an explicit dash-prefixed drive9
// help token.
// It stops at "--" because arguments after that separator belong to a nested
// command or data payload rather than drive9's own parser.
func IsHelpArgs(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "--":
			return false
		case "-h", "-help", "--help":
			return true
		}
	}
	return false
}

func IsHelpArgsWithValueFlags(args []string, valueFlags ...string) bool {
	valueFlag := map[string]struct{}{}
	for _, flag := range valueFlags {
		valueFlag[flag] = struct{}{}
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--":
			return false
		case "-h", "-help", "--help":
			return true
		}
		if _, ok := valueFlag[arg]; ok && i+1 < len(args) {
			i++
		}
	}
	return false
}

func stripLeadingDashDash(args []string) ([]string, bool) {
	if len(args) > 0 && args[0] == "--" {
		return args[1:], true
	}
	return args, false
}

// FSSubcommandUsage returns the focused usage line for a drive9 fs leaf
// command. The bool is false when sub is not a known fs subcommand.
func FSSubcommandUsage(sub string) (string, bool) {
	switch sub {
	case "cp":
		return fsCpUsage(), true
	case "cat":
		return fsCatUsage(), true
	case "ls":
		return fsLsUsage(), true
	case "stat":
		return fsStatUsage(), true
	case "mv":
		return fsMvUsage(), true
	case "rm":
		return fsRmUsage(), true
	case "mkdir":
		return fsMkdirUsage(), true
	case "chmod":
		return fsChmodUsage(), true
	case "symlink":
		return fsSymlinkUsage(), true
	case "sh":
		return fsShUsage(), true
	case "grep":
		return fsGrepUsage(), true
	case "find":
		return fsFindUsage(), true
	default:
		return "", false
	}
}

func fsCpUsage() string {
	return "usage: drive9 fs cp [-r|--recursive] [--resume] [--append] [--tag key=value]... [--description <text>] <src> <dst>"
}

func fsCatUsage() string { return "usage: drive9 fs cat [--offset N --length N] <path>" }

func fsLsUsage() string { return "usage: drive9 fs ls [-l] [path]" }

func fsStatUsage() string { return "usage: drive9 fs stat [-o text|json] <path>" }

func fsMvUsage() string { return "usage: drive9 fs mv <old> <new>" }

func fsRmUsage() string { return "usage: drive9 fs rm [-r|--recursive] <path>" }

func fsMkdirUsage() string { return "usage: drive9 fs mkdir <path>" }

func fsChmodUsage() string { return "usage: drive9 fs chmod <mode> <path>" }

func fsSymlinkUsage() string { return "usage: drive9 fs symlink <target> <link>" }

func fsShUsage() string { return "usage: drive9 fs sh" }

func fsGrepUsage() string { return "usage: drive9 fs grep <pattern> [path]" }

func fsFindUsage() string {
	return "usage: drive9 fs find [dir] [-name <glob>] [-tag <key[=value]>] [-newer <YYYY-MM-DD>] [-older <YYYY-MM-DD>] [-size <+N|-N>]"
}
