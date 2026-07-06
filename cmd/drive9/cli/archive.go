package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathfilter"
)

// archive.go implements `drive9 fs archive <remote:/dir> [<out>]`: download a
// remote directory tree as a single compressed archive (tar.gz by default,
// zip via --format). The tree walk + bounded download + archive writing all
// live in pkg/client.ArchiveDir; this file is the CLI surface — flag parsing,
// profile translation, and output-target resolution.
//
// Filtering mirrors the mount profile rule language but is exposed with the
// archive-domain vocabulary --exclude/--include rather than --local-only/
// --remote-only (the latter's "local layer vs remote layer" routing semantics
// read awkwardly in a bulk-download context). --profile loads a profile's
// [local] rules as excludes and [remote] rules as include-overrides, so a
// single `--profile coding-agent` reproduces the same skip set that mount
// applies to node_modules/.git/dist/etc.

// splitArchiveArgs separates positional args from flag tokens so users can
// place flags before OR after the positional source/output. A token starting
// with "-" is a flag; the next token after a value-taking flag is its value.
var archiveValueFlags = map[string]bool{
	"--format": true, "-format": true,
	"--exclude": true, "-exclude": true,
	"--include": true, "-include": true,
	"--profile": true, "-profile": true,
	"--jobs": true, "-jobs": true,
	"--output": true, "-output": true,
}

func splitArchiveArgs(args []string) (positionals, flagArgs []string) {
	i := 0
	for i < len(args) {
		a := args[i]
		if a == "--" {
			positionals = append(positionals, args[i+1:]...)
			return positionals, flagArgs
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			flagArgs = append(flagArgs, a)
			if strings.Contains(a, "=") {
				i++
				continue
			}
			if archiveValueFlags[a] && i+1 < len(args) {
				flagArgs = append(flagArgs, args[i+1])
				i += 2
				continue
			}
			i++
			continue
		}
		positionals = append(positionals, a)
		i++
	}
	return positionals, flagArgs
}

// Archive is the entry point for `drive9 fs archive`.
func Archive(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs archive", flag.ContinueOnError)
	fs.Usage = archiveUsage

	var (
		formatFlag  = fs.String("format", "tar.gz", "archive format: tar.gz or zip")
		excludes    stringListFlag
		includes    stringListFlag
		profileName = fs.String("profile", "", "apply profile [local]/[remote] rules (default none)")
		jobs        = fs.Int("jobs", 16, "concurrent file downloads")
		stdoutFlag  = fs.Bool("stdout", false, "write archive to stdout (for piping)")
		flatFlag    = fs.Bool("flat", false, "strip directory hierarchy; archive file basenames only")
		output      = fs.String("output", "", "output file path (or '-' for stdout)")
	)
	fs.Var(&excludes, "exclude", "skip paths matching pattern (repeatable); patterns use **/x/**, prefix/**, or exact/glob forms")
	fs.Var(&includes, "include", "include only paths matching pattern (repeatable); override > exclude > include")

	positionals, flagArgs := splitArchiveArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positionals) < 1 || len(positionals) > 2 {
		fs.Usage()
		return fmt.Errorf("usage: drive9 fs archive <remote:/dir> [<out>] [flags]")
	}

	// Resolve source remote dir.
	srcArg := positionals[0]
	srcRP, isRemote := ParseRemote(srcArg)
	if !isRemote {
		return fmt.Errorf("archive source must be a remote path (got %q); use drive9 pack for local→remote archiving", srcArg)
	}
	if srcRP.Path == "" {
		srcRP.Path = "/"
	}
	srcRP.Path = strings.TrimSuffix(srcRP.Path, "/")
	if srcRP.Path == "" {
		srcRP.Path = "/"
	}
	srcBasename := strings.TrimPrefix(srcRP.Path, "/")
	if idx := strings.LastIndex(srcBasename, "/"); idx >= 0 {
		srcBasename = srcBasename[idx+1:]
	}
	if srcBasename == "" {
		srcBasename = "root"
	}

	// Validate format.
	format := strings.TrimSpace(*formatFlag)
	if format == "" {
		format = "tar.gz"
	}
	if format != "tar.gz" && format != "zip" {
		return fmt.Errorf("invalid --format %q: must be tar.gz or zip", format)
	}

	// Determine output target.
	useStdout := *stdoutFlag
	outPath := strings.TrimSpace(*output)
	if outPath == "-" {
		useStdout = true
		outPath = ""
	}
	if len(positionals) == 2 {
		positionalOut := positionals[1]
		if positionalOut == "-" {
			useStdout = true
		} else {
			outPath = positionalOut
		}
	}
	if useStdout && outPath != "" {
		return fmt.Errorf("--stdout and an output path are mutually exclusive")
	}
	if !useStdout && outPath == "" {
		ext := "tar.gz"
		if format == "zip" {
			ext = "zip"
		}
		outPath = srcBasename + "." + ext
	}

	// Build the archive options: profile rules + explicit flags.
	opts, err := buildArchiveOptions(*profileName, includes, excludes, format, *flatFlag, *jobs)
	if err != nil {
		return err
	}

	// Honor a context-qualified source (e.g. `drive9 fs archive prod:/proj`)
	// by switching to that context's client, mirroring `fs cp` behavior.
	sourceClient := c
	if srcRP.Context != "" {
		sourceClient, err = newFSClientForContext(srcRP.Context)
		if err != nil {
			return fmt.Errorf("resolve context %q: %w", srcRP.Context, err)
		}
	}

	ctx := context.Background()
	var out io.Writer
	var closeOut func() error
	if useStdout {
		out = os.Stdout
		closeOut = func() error { return nil }
	} else {
		f, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("create %q: %w", outPath, err)
		}
		out = f
		closeOut = f.Close
	}
	defer func() { _ = closeOut() }()

	return sourceClient.ArchiveDir(ctx, srcRP.Path, out, opts)
}

// buildArchiveOptions merges profile rules with explicit flags into
// client.ArchiveOptions.
//
//	exclude           = profile.[local]  + --exclude
//	include-override  = profile.[remote]                (override, restores an excluded path)
//	include-whitelist = --include                        (only these are kept when non-empty)
func buildArchiveOptions(profileName string, includes, excludes stringListFlag, format string, flat bool, jobs int) (client.ArchiveOptions, error) {
	var profileLocalOnly, profileRemoteOnly []string
	if strings.TrimSpace(profileName) != "" {
		cfg, err := loadProfileConfig(profileName)
		if err != nil {
			return client.ArchiveOptions{}, fmt.Errorf("load profile %q: %w", profileName, err)
		}
		profileLocalOnly = cfg.LocalOnlyPatterns
		profileRemoteOnly = cfg.RemoteOnlyPatterns
	}
	excludePatterns := mergeProfileValues(profileLocalOnly, []string(excludes))
	includePatterns := mergeProfileValues(nil, []string(includes))
	overridePatterns := mergeProfileValues(profileRemoteOnly)

	if err := pathfilter.Validate(excludePatterns, includePatterns, overridePatterns); err != nil {
		return client.ArchiveOptions{}, err
	}
	archiveFormat := client.ArchiveFormatTarGz
	if format == "zip" {
		archiveFormat = client.ArchiveFormatZip
	}
	return client.ArchiveOptions{
		Format:   archiveFormat,
		Exclude:  excludePatterns,
		Include:  includePatterns,
		Override: overridePatterns,
		Flat:     flat,
		Jobs:     jobs,
	}, nil
}

func archiveUsage() {
	fmt.Fprint(os.Stderr, `usage: drive9 fs archive <remote:/dir> [<out>] [flags]

download a remote directory tree as a compressed archive (tar.gz by default)

flags:
  --format tar.gz|zip     archive format (default tar.gz)
  --exclude <pattern>     skip paths matching pattern (repeatable)
  --include <pattern>     keep only paths matching pattern (repeatable)
  --profile <name>        apply profile [local]/[remote] rules
  --jobs <n>              concurrent file downloads (default 16)
  --stdout                write archive to stdout (pipe-friendly)
  --flat                  strip directory hierarchy; archive basenames only
  --output <path>         output file path

patterns support three forms:
  **/x/**   matches any path containing the x subpath (e.g. **/node_modules/**)
  prefix/** matches everything under a prefix (e.g. dist/**)
  name      exact name or glob (e.g. *.log, go.mod)
`)
}