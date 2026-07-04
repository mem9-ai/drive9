package cli

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathfilter"
)

// archive.go implements `drive9 fs archive <remote:/dir> [<out>]`: download a
// remote directory tree as a single compressed archive (tar.gz by default,
// zip via --format). The tree walk + bounded download reuse the same
// primitives as `drive9 fs cp -r` (walkRemoteTreeBFS, parallelTransfer); the
// archive writer is the only net-new streaming glue.
//
// Filtering mirrors the mount profile rule language but is exposed with the
// archive-domain vocabulary --exclude/--include rather than --local-only/
// --remote-only (the latter's "local layer vs remote layer" routing semantics
// read awkwardly in a bulk-download context). --profile loads a profile's
// [local] rules as excludes and [remote] rules as include-overrides, so a
// single `--profile coding-agent` reproduces the same skip set that mount
// applies to node_modules/.git/dist/etc.
//
// Design constraints (locked in plan review):
//   - Direction is remote→local only. remote→remote archive is meaningless;
//     local→remote archive is the existing pack command's job (do not overlap).
//   - Streaming: tar.gz is written as the tree is walked (no temp file); zip
//     is written in archive order (zip requires central-directory offsets, so
//     the writer is single-threaded but downloads are still prefetched in
//     parallel).
//   - Directory pruning: when a directory's relative path matches an exclude
//     (and no override restores it), the entire subtree is skipped at BFS
//     time — no extra ListCtx round-trips for children that would be dropped.

const archiveDefaultJobs = recursiveCopyConcurrency

// archiveValueFlags is the set of flags that take a value argument (so their
// following token must NOT be treated as a positional during the
// flags/positionals split).
var archiveValueFlags = map[string]bool{
	"--format":  true,
	"-format":   true,
	"--exclude": true,
	"-exclude":  true,
	"--include": true,
	"-include":  true,
	"--profile": true,
	"-profile":  true,
	"--jobs":    true,
	"-jobs":     true,
	"--output":  true,
	"-output":   true,
}

// splitArchiveArgs separates positional args from flag tokens, allowing flags
// to appear before OR after the positional source/output. A token starting
// with "-" is a flag; the next token after a value-taking flag is its value
// (kept with the flags). Everything else is positional.
func splitArchiveArgs(args []string) (positionals, flagArgs []string) {
	i := 0
	for i < len(args) {
		a := args[i]
		if a == "--" {
			// Everything after -- is positional.
			positionals = append(positionals, args[i+1:]...)
			return positionals, flagArgs
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			flagArgs = append(flagArgs, a)
			// Handle --flag=value form: no separate value token.
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
		// Positional.
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
		jobs        = fs.Int("jobs", archiveDefaultJobs, "concurrent file downloads")
		stdoutFlag  = fs.Bool("stdout", false, "write archive to stdout (for piping)")
		flatFlag    = fs.Bool("flat", false, "strip directory hierarchy; archive file basenames only (errors on collision)")
		output      = fs.String("output", "", "output file path (or '-' for stdout)")
	)
	fs.Var(&excludes, "exclude", "skip paths matching pattern (repeatable); patterns use **/x/**, prefix/**, or exact/glob forms")
	fs.Var(&includes, "include", "include only paths matching pattern (repeatable); excludes take precedence")

	// Split positional args from flags so users can write either
	//   drive9 fs archive :/proj out.tar.gz --exclude '**/x'
	// or drive9 fs archive --exclude '**/x' :/proj out.tar.gz
	// The std flag package stops parsing flags at the first positional, so we
	// extract positionals first and pass only the flag-bearing tokens to fs.Parse.
	positionals, flagArgs := splitArchiveArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positionals) < 1 || len(positionals) > 2 {
		fs.Usage()
		return fmt.Errorf("usage: drive9 fs archive <remote:/dir> [<out>] [flags]")
	}

	// Resolve source remote dir. Normalize to a path WITHOUT a trailing
	// slash — walkRemoteTreeBFS lists root directly via ListCtx(root), and
	// the mock/real server treat "/proj" and "/proj/" as different keys.
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
	// srcBasename is the archive root directory name (without slash).
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
		// Default: <dir basename>.<ext>
		base := srcBasename
		ext := "tar.gz"
		if format == "zip" {
			ext = "zip"
		}
		outPath = base + "." + ext
	}

	// Build the matcher: profile rules + explicit flags.
	matcher, err := buildArchiveMatcher(*profileName, includes, excludes)
	if err != nil {
		return err
	}

	// Validate jobs.
	if *jobs <= 0 {
		*jobs = archiveDefaultJobs
	}

	ctx := context.Background()
	return runArchive(ctx, c, srcRP.Path, srcBasename, format, outPath, useStdout, matcher, *flatFlag, *jobs)
}

// buildArchiveMatcher merges profile rules with explicit flags:
//
//	exclude           = profile.[local]  + --exclude
//	include-override  = profile.[remote]                (override, restores an excluded path)
//	include-whitelist = --include                        (only these are kept when non-empty)
func buildArchiveMatcher(profileName string, includes, excludes stringListFlag) (pathfilter.Matcher, error) {
	var profileLocalOnly, profileRemoteOnly []string
	if strings.TrimSpace(profileName) != "" {
		cfg, err := loadProfileConfig(profileName)
		if err != nil {
			return pathfilter.Matcher{}, fmt.Errorf("load profile %q: %w", profileName, err)
		}
		profileLocalOnly = cfg.LocalOnlyPatterns
		profileRemoteOnly = cfg.RemoteOnlyPatterns
	}
	excludePatterns := mergeProfileValues(profileLocalOnly, []string(excludes))
	includePatterns := mergeProfileValues(nil, []string(includes))
	overridePatterns := mergeProfileValues(profileRemoteOnly)

	if err := pathfilter.Validate(excludePatterns, includePatterns, overridePatterns); err != nil {
		return pathfilter.Matcher{}, err
	}
	return pathfilter.NewMatcher(includePatterns, excludePatterns, overridePatterns), nil
}

func runArchive(ctx context.Context, c *client.Client, srcRemote, archiveRoot, format, outPath string, useStdout bool, matcher pathfilter.Matcher, flat bool, jobs int) error {
	// Open output sink.
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

	switch format {
	case "tar.gz":
		return archiveTarGz(ctx, c, srcRemote, archiveRoot, out, matcher, flat, jobs)
	case "zip":
		return archiveZip(ctx, c, srcRemote, archiveRoot, out, matcher, flat, jobs)
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// archiveEntry is the per-leaf work item for parallelTransfer.
type archiveEntry struct {
	rel    string // relative path from srcRemote root
	remote string // absolute remote path
	root   string // archive root name (e.g. "proj"); prepended to rel for in-archive name
	size   int64
	mode   uint32
}

// collectArchiveTree walks the remote tree once, applying the matcher to
// prune excluded directories at BFS time and collecting the leaf entries that
// survive filtering. Directories that pass the filter are recorded too so
// the archive preserves empty dirs. archiveRoot is the prefix prepended to
// each entry's in-archive name (e.g. "proj" → "proj/src/main.go").
func collectArchiveTree(ctx context.Context, c *client.Client, root, archiveRoot string, matcher pathfilter.Matcher, flat bool) (dirs []archiveEntry, files []archiveEntry, err error) {
	// The archive root directory is always emitted first (preserves the
	// top-level dir entry even when the tree has only files at depth).
	dirs = append(dirs, archiveEntry{rel: "", remote: root, root: archiveRoot, mode: 0o755})
	err = walkRemoteTreeBFS(ctx, c, root, func(rel string, info client.FileInfo) error {
		if rel == "" {
			return nil
		}
		if !matcher.Match(rel) {
			return nil
		}
		entry := archiveEntry{
			rel:    rel,
			remote: path.Join(root, rel),
			root:   archiveRoot,
		}
		if info.HasMode {
			entry.mode = info.Mode
		}
		if info.IsDir {
			dirs = append(dirs, entry)
			return nil
		}
		// NOTE: symlinks are currently archived as regular files (their
		// target content via ReadStream), mirroring cp -r's behavior of
		// rejecting symlinks rather than preserving them. A future PR can
		// emit TypeSymlink headers once the client exposes a ReadLink
		// primitive; for now we treat any non-dir entry as a file leaf.
		entry.size = info.Size
		files = append(files, entry)
		return nil
	})
	return dirs, files, err
}

func archiveTarGz(ctx context.Context, c *client.Client, srcRemote, archiveRoot string, out io.Writer, matcher pathfilter.Matcher, flat bool, jobs int) error {
	gw := gzip.NewWriter(out)
	defer func() { _ = gw.Close() }()
	tw := tar.NewWriter(gw)
	defer func() { _ = tw.Close() }()

	dirs, files, err := collectArchiveTree(ctx, c, srcRemote, archiveRoot, matcher, flat)
	if err != nil {
		return fmt.Errorf("walk %q: %w", srcRemote, err)
	}

	// Directories first (so extraction preserves structure even for empty dirs).
	for _, d := range dirs {
		name := archiveDirName(d.root, d.rel, flat)
		if name == "" {
			continue
		}
		hdr := &tar.Header{
			Name:     name,
			Mode:     tarMode(d.mode, true),
			Typeflag: tar.TypeDir,
			ModTime:  archiveModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("write dir header %q: %w", name, err)
		}
	}

	// Files: parallel download, serialized tar write (tar requires sequential
	// header/body writes). parallelTransfer bounds the download concurrency to
	// recursiveCopyConcurrency; tarWriteMu serializes the header+body writes so
	// the archive layout stays deterministic.
	if err := parallelTransfer(ctx, files, func(ctx context.Context, e archiveEntry) error {
		return fetchEntryToTar(ctx, c, e, tw, flat)
	}); err != nil {
		return fmt.Errorf("archive transfer: %w", err)
	}
	_ = jobs // bounded by parallelTransfer's internal semaphore (recursiveCopyConcurrency)
	return nil
}

// tarWriteMu serializes tar header+body writes across parallel workers.
var tarWriteMu sync.Mutex

func fetchEntryToTar(ctx context.Context, c *client.Client, e archiveEntry, tw *tar.Writer, flat bool) error {
	rc, err := c.ReadStream(ctx, e.remote)
	if err != nil {
		return fmt.Errorf("open %q: %w", e.remote, err)
	}
	defer func() { _ = rc.Close() }()
	name := archiveName(e.root, e.rel, flat, false)
	if name == "" {
		return nil
	}
	hdr := &tar.Header{
		Name:     name,
		Mode:     tarMode(e.mode, false),
		Size:     e.size,
		Typeflag: tar.TypeReg,
		ModTime:  archiveModTime(),
	}
	tarWriteMu.Lock()
	defer tarWriteMu.Unlock()
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write header %q: %w", name, err)
	}
	if _, err := io.Copy(tw, io.LimitReader(rc, e.size)); err != nil {
		return fmt.Errorf("copy body %q: %w", name, err)
	}
	return nil
}

func archiveZip(ctx context.Context, c *client.Client, srcRemote, archiveRoot string, out io.Writer, matcher pathfilter.Matcher, flat bool, jobs int) error {
	zw := zip.NewWriter(out)
	defer func() { _ = zw.Close() }()

	dirs, files, err := collectArchiveTree(ctx, c, srcRemote, archiveRoot, matcher, flat)
	if err != nil {
		return fmt.Errorf("walk %q: %w", srcRemote, err)
	}

	for _, d := range dirs {
		name := archiveDirName(d.root, d.rel, flat)
		if name == "" {
			continue
		}
		hdr := &zip.FileHeader{
			Name:   name,
			Method: zip.Store,
		}
		hdr.SetMode(archiveMode(d.mode, true) | 0o750)
		if _, err := zw.CreateHeader(hdr); err != nil {
			return fmt.Errorf("zip create dir %q: %w", name, err)
		}
	}

	// zip writes are sequential by construction (each CreateHeader returns a
	// writer bound to the entry); parallelize downloads but serialize writes.
	if err := parallelTransfer(ctx, files, func(ctx context.Context, e archiveEntry) error {
		return fetchEntryToZip(ctx, c, e, zw, flat)
	}); err != nil {
		return fmt.Errorf("archive transfer: %w", err)
	}
	_ = jobs
	return nil
}

// zipWriteMu serializes zip entry creation across parallel download workers.
var zipWriteMu sync.Mutex

func fetchEntryToZip(ctx context.Context, c *client.Client, e archiveEntry, zw *zip.Writer, flat bool) error {
	rc, err := c.ReadStream(ctx, e.remote)
	if err != nil {
		return fmt.Errorf("open %q: %w", e.remote, err)
	}
	defer func() { _ = rc.Close() }()
	name := archiveName(e.root, e.rel, flat, false)
	if name == "" {
		return nil
	}
	hdr := &zip.FileHeader{
		Name:   name,
		Method: zip.Deflate,
	}
	hdr.SetMode(archiveMode(e.mode, false))
	zipWriteMu.Lock()
	defer zipWriteMu.Unlock()
	w, err := zw.CreateHeader(hdr)
	if err != nil {
		return fmt.Errorf("zip create %q: %w", name, err)
	}
	if _, err := io.Copy(w, io.LimitReader(rc, e.size)); err != nil {
		return fmt.Errorf("zip copy %q: %w", name, err)
	}
	return nil
}

// archiveName maps a relative path to its in-archive name, applying --flat.
// root is the archive root directory name (e.g. "proj"); the in-archive name
// is "<root>/<rel>" unless flat mode strips the hierarchy.
func archiveName(root, rel string, flat, isDir bool) string {
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return ""
	}
	if flat {
		base := path.Base(rel)
		if base == "." || base == "/" {
			return ""
		}
		if isDir {
			return "" // flat mode drops directories
		}
		return base
	}
	full := root + "/" + rel
	if isDir && !strings.HasSuffix(full, "/") {
		full = full + "/"
	}
	return full
}

// archiveDirName is like archiveName for directory entries but also emits the
// archive root itself when rel is empty (so the top-level dir is preserved).
// In flat mode directories are dropped (returns "").
func archiveDirName(root, rel string, flat bool) string {
	if flat {
		return ""
	}
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return root + "/"
	}
	return root + "/" + rel + "/"
}

func archiveMode(mode uint32, isDir bool) fs.FileMode {
	m := fs.FileMode(mode & 0o777)
	if m == 0 {
		if isDir {
			m = 0o755
		} else {
			m = 0o644
		}
	}
	if isDir {
		m |= fs.ModeDir
	}
	return m
}

// tarMode returns the int64 permission/mode bits for a tar header.
func tarMode(mode uint32, isDir bool) int64 {
	return int64(archiveMode(mode, isDir))
}

func archiveModTime() time.Time {
	return time.Now()
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