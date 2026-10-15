package main

import (
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/cmd/drive9/cli"
	"github.com/mem9-ai/drive9/pkg/buildinfo"
	"github.com/mem9-ai/drive9/pkg/telemetry"
)

const (
	maxTelemetryCommandSegments = 4 // "drive9" + up to 3 subcommands
	testEndpointsEnv            = "DRIVE9_ALLOW_TEST_ENDPOINTS"
	testEndpointEnv             = "DRIVE9_TEST_TELEMETRY_ENDPOINT"
	telemetryDebugEnv           = "DRIVE9_TELEMETRY_DEBUG"
)

var telemetryFlagNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

type telemetryNode struct {
	aliases     map[string]string
	defaultName string
	excluded    bool
	leaf        bool
	// bareHelp marks command trees whose leaf parsers treat a bare "help"
	// operand as a help request (`drive9 admin tenant create help` is a
	// successful help invocation). It is inherited by descendants. Commands like
	// `fs grep` do not set it, because there "help" is a legitimate operand.
	bareHelp bool
	children map[string]*telemetryNode
}

var telemetryLeaf = &telemetryNode{}

var telemetryCommands = &telemetryNode{
	children: map[string]*telemetryNode{
		"create":  telemetryLeaf,
		"delete":  telemetryLeaf,
		"pack":    telemetryLeaf,
		"unpack":  telemetryLeaf,
		"umount":  telemetryLeaf,
		"update":  {excluded: true},
		"version": {excluded: true},
		"help":    {excluded: true},
		"admin": {
			aliases:  map[string]string{"tenants": "tenant"},
			bareHelp: true,
			children: map[string]*telemetryNode{
				"tenant": {
					children: map[string]*telemetryNode{
						"create":    telemetryLeaf,
						"list":      telemetryLeaf,
						"get":       telemetryLeaf,
						"delete":    telemetryLeaf,
						"set-quota": telemetryLeaf,
						"extract-config": {
							children: map[string]*telemetryNode{
								"get": telemetryLeaf,
								"set": telemetryLeaf,
							},
						},
						"embedding-config": {
							children: map[string]*telemetryNode{
								"get": telemetryLeaf,
								"set": telemetryLeaf,
							},
						},
						"object-namespace": {
							children: map[string]*telemetryNode{
								"get":   telemetryLeaf,
								"set":   telemetryLeaf,
								"clear": telemetryLeaf,
							},
						},
						"pool": {
							children: map[string]*telemetryNode{
								"create": telemetryLeaf,
								"get":    telemetryLeaf,
								"update": telemetryLeaf,
								"delete": telemetryLeaf,
							},
						},
					},
				},
				"pool": {
					children: map[string]*telemetryNode{
						"create": telemetryLeaf,
						"get":    telemetryLeaf,
						"update": telemetryLeaf,
						"delete": telemetryLeaf,
					},
				},
				"object-backend": {
					aliases: map[string]string{"list": "ls", "delete": "rm"},
					children: map[string]*telemetryNode{
						"add":    telemetryLeaf,
						"get":    telemetryLeaf,
						"ls":     telemetryLeaf,
						"update": telemetryLeaf,
						"rm":     telemetryLeaf,
					},
				},
			},
		},
		"ctx": {
			defaultName: "show",
			aliases:     map[string]string{"list": "ls"},
			children: map[string]*telemetryNode{
				"show":   telemetryLeaf,
				"add":    telemetryLeaf,
				"import": telemetryLeaf,
				"fork":   telemetryLeaf,
				"ls":     telemetryLeaf,
				"use":    telemetryLeaf,
				"rm":     telemetryLeaf,
			},
		},
		"fs": {
			children: map[string]*telemetryNode{
				"cp":       telemetryLeaf,
				"cat":      telemetryLeaf,
				"ls":       telemetryLeaf,
				"stat":     telemetryLeaf,
				"mv":       telemetryLeaf,
				"rm":       telemetryLeaf,
				"mkdir":    telemetryLeaf,
				"chmod":    telemetryLeaf,
				"setmeta":  telemetryLeaf,
				"symlink":  telemetryLeaf,
				"hardlink": telemetryLeaf,
				"sh":       telemetryLeaf,
				"grep":     telemetryLeaf,
				"find":     telemetryLeaf,
				"archive":  telemetryLeaf,
				"layer": {
					aliases: map[string]string{"ls": "list", "get": "status", "rm": "delete"},
					children: map[string]*telemetryNode{
						"create":     telemetryLeaf,
						"list":       telemetryLeaf,
						"status":     telemetryLeaf,
						"diff":       telemetryLeaf,
						"checkpoint": telemetryLeaf,
						"rollback":   telemetryLeaf,
						"commit":     telemetryLeaf,
						"fork":       telemetryLeaf,
						"chain":      telemetryLeaf,
						"delete":     telemetryLeaf,
					},
				},
			},
		},
		"token": {
			aliases: map[string]string{"ls": "list"},
			children: map[string]*telemetryNode{
				"issue":  telemetryLeaf,
				"revoke": telemetryLeaf,
				"list":   telemetryLeaf,
				"forget": telemetryLeaf,
			},
		},
		"vault": {
			children: map[string]*telemetryNode{
				"set":    telemetryLeaf,
				"get":    telemetryLeaf,
				"put":    telemetryLeaf,
				"with":   telemetryLeaf,
				"ls":     telemetryLeaf,
				"rm":     telemetryLeaf,
				"grant":  telemetryLeaf,
				"revoke": telemetryLeaf,
				"audit":  telemetryLeaf,
			},
		},
		"journal": {
			children: map[string]*telemetryNode{
				"new":    telemetryLeaf,
				"append": telemetryLeaf,
				"cat":    telemetryLeaf,
				"find":   telemetryLeaf,
				"verify": telemetryLeaf,
				"seal":   telemetryLeaf,
			},
		},
		"git": {
			children: map[string]*telemetryNode{
				"clone":   telemetryLeaf,
				"hydrate": telemetryLeaf,
				"worktree": {
					children: map[string]*telemetryNode{
						"add":    telemetryLeaf,
						"remove": telemetryLeaf,
					},
				},
			},
		},
		"region": {
			aliases: map[string]string{"ls": "list"},
			children: map[string]*telemetryNode{
				"list": telemetryLeaf,
			},
		},
		"profile": {
			children: map[string]*telemetryNode{
				"show": telemetryLeaf,
			},
		},
		"mount": {
			leaf: true,
			children: map[string]*telemetryNode{
				"vault":        telemetryLeaf,
				"drain":        telemetryLeaf,
				"status":       telemetryLeaf,
				"health":       telemetryLeaf,
				"ensure":       telemetryLeaf,
				"systemd-unit": telemetryLeaf,
				"supervise":    {excluded: true},
			},
		},
		"doctor": {
			children: map[string]*telemetryNode{
				"fuse": telemetryLeaf,
			},
		},
	},
}

type telemetryInvocation struct {
	commandPath string
	flagArgs    []string
	eligible    bool
}

func resolveTelemetryCommand(args []string) telemetryInvocation {
	if len(args) == 0 || hasTelemetryExclusionFlag(args) || hasInternalProcessFlag(args) {
		return telemetryInvocation{}
	}
	path, rest, ok := telemetryCommands.resolve(args, []string{"drive9"}, false)
	if !ok || len(path) == 0 {
		return telemetryInvocation{}
	}
	return telemetryInvocation{
		commandPath: canonicalTelemetryCommandPath(path),
		flagArgs:    rest,
		eligible:    true,
	}
}

func (n *telemetryNode) resolve(args []string, path []string, bareHelp bool) ([]string, []string, bool) {
	if n == nil || n.excluded {
		return nil, nil, false
	}
	bareHelp = bareHelp || n.bareHelp
	if len(args) == 0 {
		if n.defaultName != "" {
			child := n.lookupChild(n.defaultName)
			if child == nil {
				return nil, nil, false
			}
			return child.resolve(nil, append(path, n.defaultName), bareHelp)
		}
		if len(n.children) > 0 && !n.leaf {
			return nil, nil, false
		}
		return path, nil, true
	}
	tok := args[0]
	// A known subcommand always wins: `mount supervise` is a subcommand even
	// though `mount` also accepts positional arguments.
	if childName, ok := n.childName(tok); ok {
		child := n.lookupChild(childName)
		if child == nil {
			return nil, nil, false
		}
		return child.resolve(args[1:], append(path, childName), bareHelp)
	}
	// Everything left belongs to a leaf command, so it is normally an argument —
	// even when it spells "version" (`drive9 fs grep help`). Trees that do treat
	// a bare "help" as a help request opt out through bareHelp.
	if n.leaf || len(n.children) == 0 {
		if bareHelp && hasBareHelpOperand(args) {
			return nil, nil, false
		}
		return path, args, true
	}
	// Unknown token on a namespace command: help/version or a command we do not
	// know about. Either way there is nothing to report.
	return nil, nil, false
}

func (n *telemetryNode) childName(tok string) (string, bool) {
	if n.aliases != nil {
		if canonical, ok := n.aliases[tok]; ok {
			tok = canonical
		}
	}
	if _, ok := n.children[tok]; ok {
		return tok, true
	}
	return "", false
}

func (n *telemetryNode) lookupChild(name string) *telemetryNode {
	if n.children == nil {
		return nil
	}
	return n.children[name]
}

func canonicalTelemetryCommandPath(path []string) string {
	if len(path) <= maxTelemetryCommandSegments {
		return strings.Join(path, " ")
	}
	head := append([]string(nil), path[:maxTelemetryCommandSegments-1]...)
	tail := strings.Join(path[maxTelemetryCommandSegments-1:], "-")
	return strings.Join(append(head, tail), " ")
}

// hasTelemetryExclusionFlag reports whether argv asks for help or version
// before any "--" terminator. Bare "help"/"version" words are command tokens,
// not flag values: they are recognized while the command path is resolved, so
// `drive9 fs find -name version` still reports normally.
func hasTelemetryExclusionFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		switch strings.ToLower(name) {
		case "h", "help", "v", "version":
		default:
			continue
		}
		if !hasValue || strings.EqualFold(value, "true") || value == "1" {
			return true
		}
	}
	return false
}

// hasBareHelpOperand reports whether a leaf command's arguments contain a bare
// "help" that its parser would treat as a help request. A "help" consumed as a
// flag's value (`--server help`) does not count.
func hasBareHelpOperand(args []string) bool {
	previousWasFlag := false
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if !strings.HasPrefix(arg, "-") {
			if arg == "help" && !previousWasFlag {
				return true
			}
			previousWasFlag = false
			continue
		}
		// An inline "--flag=value" token consumes nothing after it.
		previousWasFlag = !strings.Contains(arg, "=")
	}
	return false
}

// hasContextScopedOperand reports whether argv addresses a named context
// (name:/path). Such a command is served by that context's client, so the
// active context's placement must not be reported as if it were its own.
func hasContextScopedOperand(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			continue
		}
		if strings.HasPrefix(arg, "-") || strings.Contains(arg, "://") {
			continue
		}
		if index := strings.Index(arg, ":/"); index > 0 {
			return true
		}
	}
	return false
}

// hasInternalProcessFlag reports whether argv belongs to a drive9 process that
// drive9 itself spawned instead of a command the user typed. The mount
// supervisor, its workers, and the generated systemd unit all re-exec this
// binary, and the invocation that started them has already been counted.
func hasInternalProcessFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		switch name {
		case "supervised", "supervise-foreground":
			return true
		}
	}
	return false
}

// telemetryFlagNames extracts flag names from argv without any knowledge of the
// command's flag schema. A dash-prefixed token that directly follows another
// dash-prefixed token is treated as that flag's value and never recorded: a
// value-taking flag consumes the next argv entry unconditionally, and that
// value may itself start with a dash. The cost is under-reporting flags written
// back to back, which is the safe direction — guessing arity instead would let
// a flag value be recorded as a flag name.
func telemetryFlagNames(args []string) []string {
	names := make([]string, 0)
	seen := map[string]struct{}{}
	previousWasFlag := false
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			previousWasFlag = false
			continue
		}
		if previousWasFlag {
			// Ambiguous token: the previous flag may have consumed it as a
			// value. Only an inline "--flag=value" form proves it did not.
			previousWasFlag = strings.Contains(arg, "=")
			continue
		}
		// An inline "--flag=value" token consumes nothing after it.
		previousWasFlag = !strings.Contains(arg, "=")
		name := strings.TrimLeft(arg, "-")
		name, _, _ = strings.Cut(name, "=")
		name = strings.ToLower(strings.TrimSpace(name))
		if !telemetryFlagNamePattern.MatchString(name) {
			continue
		}
		// Closed allowlist: a dash-prefixed token that is not a flag this CLI
		// defines is user input — an unknown or mistyped flag, or a value the
		// command rejects — and must never be reported.
		if _, known := telemetryKnownFlagNames[name]; !known {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func effectiveTelemetryEndpoint() string {
	if endpoint := strings.TrimSpace(buildinfo.TelemetryEndpoint); endpoint != "" {
		return endpoint
	}
	if isReleaseInstallSource(buildinfo.InstallSource) {
		return ""
	}
	if os.Getenv(testEndpointsEnv) != "1" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(testEndpointEnv))
}

func isReleaseInstallSource(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "archive", "github-release", "homebrew", "scoop":
		return true
	default:
		return false
	}
}

func telemetryDebugEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(telemetryDebugEnv))) {
	case "1", "true", "on":
		return true
	default:
		return false
	}
}

func telemetryProfileSource(args []string, fallback string) string {
	for _, name := range telemetryFlagNames(args) {
		if name == "api-key" {
			return "explicit"
		}
	}
	return fallback
}

func startCLITelemetry(args []string) (*telemetry.Session, telemetryInvocation) {
	inv := resolveTelemetryCommand(args)
	if !inv.eligible {
		return nil, inv
	}
	session := telemetry.Start(telemetry.Config{
		Eligible:      true,
		Endpoint:      effectiveTelemetryEndpoint(),
		Version:       buildinfo.Version,
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		InstallSource: buildinfo.InstallSource,
		Preference:    cli.TelemetryPreference,
		Debug:         telemetryDebugEnabled(),
		DebugWriter:   os.Stderr,
	})
	return session, inv
}

func finishCLITelemetry(session *telemetry.Session, inv telemetryInvocation, args []string, exitCode int, duration time.Duration) {
	if session == nil || !inv.eligible {
		return
	}
	provider, region, source := cli.TelemetryContextMetadata()
	if hasContextScopedOperand(args) {
		// The command ran against another context; the active context's
		// placement does not describe it, and reporting it would be wrong.
		provider, region, source = "", "", "unknown"
	}
	session.Finish(telemetry.EventInput{
		CommandPath:   inv.commandPath,
		FlagNames:     telemetryFlagNames(inv.flagArgs),
		ExitCode:      exitCode,
		Duration:      duration,
		CloudProvider: provider,
		RegionCode:    region,
		ProfileSource: telemetryProfileSource(args, source),
	})
}

func withCLITelemetry(args []string, run func()) {
	started := time.Now()
	session, inv := startCLITelemetry(args)
	var once sync.Once
	finish := func(code int) {
		once.Do(func() {
			finishCLITelemetry(session, inv, args, code, time.Since(started))
		})
	}
	origExit := exitFunc
	exitFunc = func(code int) {
		finish(code)
		origExit(code)
	}
	defer func() { exitFunc = origExit }()
	run()
	finish(0)
}
