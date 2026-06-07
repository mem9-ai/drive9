package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/mountstate"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

const (
	packArchiveFormat      = "drive9.pack.v1"
	packManifestEntryName  = ".drive9-pack-manifest.json"
	packArchiveEntryPrefix = "entries/"
	codingAgentPackRoot    = "/.drive9/packs/coding-agent"
)

var defaultCodingAgentPackNames = []string{".git", "dist", "build", "target"}

type packManifest struct {
	Format     string              `json:"format"`
	Version    int                 `json:"version"`
	CreatedAt  time.Time           `json:"created_at"`
	Profile    string              `json:"profile,omitempty"`
	RemoteRoot string              `json:"remote_root,omitempty"`
	Paths      []string            `json:"paths"`
	Entries    []packManifestEntry `json:"entries"`
}

type packManifestEntry struct {
	Path       string `json:"path"`
	RemotePath string `json:"remote_path,omitempty"`
	Type       string `json:"type"`
	Mode       uint32 `json:"mode,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Linkname   string `json:"linkname,omitempty"`
	ModTime    int64  `json:"mtime,omitempty"`
}

type packOptions struct {
	LocalRoot   string
	RemoteRoot  string
	LocalPrefix string
	Profile     string
	Paths       []string
	SkipMissing bool
}

type unpackOptions struct {
	LocalRoot string
	Replace   bool
}

type packSource struct {
	ArchivePath string
	RemotePath  string
	LocalPath   string
}

type packItem struct {
	ArchivePath string
	RemotePath  string
	LocalPath   string
	Info        fs.FileInfo
	Linkname    string
	Type        string
}

type mountedPackTarget struct {
	MountPoint string
	MountRel   string
	RemoteRoot string
	Profile    string
	LocalRoot  string
}

// PackCommand is the top-level `drive9 pack` entry point.
func PackCommand(args []string) error {
	return Pack(NewFromEnvWithWarm(), args)
}

// UnpackCommand is the top-level `drive9 unpack` entry point.
func UnpackCommand(args []string) error {
	return Unpack(NewFromEnv(), args)
}

// Pack writes a tar.gz snapshot of selected local coding-agent overlay paths
// to a Drive9 remote file.
func Pack(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("pack", flag.ContinueOnError)
	localRoot := fs.String("local-root", "", "coding-agent local overlay root")
	remoteRoot := fs.String("remote-root", "/", "remote root used to resolve relative pack paths")
	profile := fs.String("profile", "", "profile defaults to use when no paths are provided (coding-agent)")
	mountTarget := fs.String("mount", "", "mounted path whose drive9 mount metadata provides local-root, remote-root, and profile")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 pack [flags] [remote-archive] [path...]\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	archiveArg := ""
	paths := append([]string(nil), fs.Args()...)
	if fs.NArg() > 0 {
		if _, ok := ParseRemote(fs.Arg(0)); ok {
			archiveArg = fs.Arg(0)
			paths = append([]string(nil), fs.Args()[1:]...)
		}
	}
	localPrefix := ""

	if strings.TrimSpace(*mountTarget) != "" {
		resolved, err := resolveMountedPackTarget(*mountTarget)
		if err != nil {
			return err
		}
		if strings.TrimSpace(*localRoot) == "" {
			*localRoot = resolved.LocalRoot
		}
		if !flagProvided(fs, "remote-root") {
			*remoteRoot = resolved.RemoteRoot
		}
		if strings.TrimSpace(*profile) == "" {
			*profile = resolved.Profile
		}
		localPrefix = filepath.ToSlash(resolved.MountRel)
		if localPrefix == "." {
			localPrefix = ""
		}
	}

	archiveClient, archivePath, err := clientForPackArchiveArg(c, archiveArg, *remoteRoot, *profile)
	if err != nil {
		return err
	}
	return packRemoteArchive(context.Background(), archiveClient, archivePath, packOptions{
		LocalRoot:   *localRoot,
		RemoteRoot:  *remoteRoot,
		LocalPrefix: localPrefix,
		Profile:     *profile,
		Paths:       paths,
	})
}

// Unpack restores a Drive9 pack archive into a coding-agent local overlay.
func Unpack(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("unpack", flag.ContinueOnError)
	localRoot := fs.String("local-root", "", "coding-agent local overlay root")
	remoteRoot := fs.String("remote-root", "/", "remote root used to resolve the default coding-agent pack archive")
	profile := fs.String("profile", "", "profile defaults to use when no archive is provided (coding-agent)")
	mountTarget := fs.String("mount", "", "mounted path whose drive9 mount metadata provides local-root")
	noReplace := fs.Bool("no-replace", false, "do not remove archived root paths before extracting")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 unpack [flags] [remote-archive]\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		fs.Usage()
		return fmt.Errorf("usage: drive9 unpack [flags] [remote-archive]")
	}
	if strings.TrimSpace(*mountTarget) != "" {
		resolved, err := resolveMountedPackTarget(*mountTarget)
		if err != nil {
			return err
		}
		if strings.TrimSpace(*localRoot) == "" {
			*localRoot = resolved.LocalRoot
		}
		if !flagProvided(fs, "remote-root") {
			*remoteRoot = resolved.RemoteRoot
		}
		if strings.TrimSpace(*profile) == "" {
			*profile = resolved.Profile
		}
	}

	archiveArg := ""
	if fs.NArg() == 1 {
		archiveArg = fs.Arg(0)
	}
	archiveClient, archivePath, err := clientForPackArchiveArg(c, archiveArg, *remoteRoot, *profile)
	if err != nil {
		return err
	}
	return unpackRemoteArchive(context.Background(), archiveClient, archivePath, unpackOptions{
		LocalRoot: *localRoot,
		Replace:   !*noReplace,
	})
}

func packRemoteArchive(ctx context.Context, c *client.Client, archivePath string, opts packOptions) error {
	if c == nil {
		return fmt.Errorf("drive9 pack: client is required")
	}
	if strings.HasSuffix(archivePath, "/") {
		return fmt.Errorf("drive9 pack: remote archive path must be a file, got %q", archivePath)
	}
	tmp, err := os.CreateTemp("", "drive9-pack-*.tar.gz")
	if err != nil {
		return fmt.Errorf("create temporary pack archive: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	manifest, err := writePackArchive(ctx, tmp, opts)
	if closeErr := tmp.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("open temporary pack archive: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat temporary pack archive: %w", err)
	}
	summary, err := c.WriteStreamWithSummary(ctx, archivePath, f, info.Size(), printProgress, client.WithTags(packArchiveTags(opts.Profile)))
	if err != nil {
		return err
	}
	emitUploadSummary(ctx, summary, tmpPath)
	fmt.Fprintf(os.Stderr, "drive9: packed %d paths (%d entries, %s) to :%s\n",
		len(manifest.Paths), len(manifest.Entries), humanizePackBytes(info.Size()), archivePath)
	return nil
}

func unpackRemoteArchiveIfExists(ctx context.Context, c *client.Client, archivePath string, opts unpackOptions) (bool, error) {
	err := unpackRemoteArchive(ctx, c, archivePath, opts)
	if err != nil {
		if client.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func unpackRemoteArchive(ctx context.Context, c *client.Client, archivePath string, opts unpackOptions) error {
	if c == nil {
		return fmt.Errorf("drive9 unpack: client is required")
	}
	rc, err := c.ReadStream(ctx, archivePath)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	manifest, err := extractPackArchive(ctx, rc, opts)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "drive9: unpacked %d paths (%d entries) from :%s\n",
		len(manifest.Paths), len(manifest.Entries), archivePath)
	return nil
}

func packArchiveTags(profile string) map[string]string {
	tags := map[string]string{"drive9.pack.format": packArchiveFormat}
	if profile = strings.TrimSpace(profile); profile != "" {
		tags["drive9.pack.profile"] = profile
	}
	return tags
}

func writePackArchive(ctx context.Context, w io.Writer, opts packOptions) (*packManifest, error) {
	if strings.TrimSpace(opts.LocalRoot) == "" {
		return nil, fmt.Errorf("drive9 pack: --local-root is required")
	}
	if !filepath.IsAbs(opts.LocalRoot) {
		return nil, fmt.Errorf("drive9 pack: --local-root must be an absolute path")
	}
	remoteRoot, err := mountpath.NormalizeRoot(strings.TrimSpace(opts.RemoteRoot))
	if err != nil {
		return nil, fmt.Errorf("drive9 pack: %w", err)
	}
	opts.RemoteRoot = remoteRoot
	sources, err := resolvePackSources(opts)
	if err != nil {
		return nil, err
	}
	items, err := collectPackItems(ctx, sources)
	if err != nil {
		return nil, err
	}
	manifest := packManifest{
		Format:     packArchiveFormat,
		Version:    1,
		CreatedAt:  time.Now().UTC(),
		Profile:    strings.TrimSpace(opts.Profile),
		RemoteRoot: remoteRoot,
		Paths:      packSourcePaths(sources),
		Entries:    packManifestEntries(items),
	}

	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	if err := writePackManifest(tw, manifest); err != nil {
		_ = tw.Close()
		_ = gz.Close()
		return nil, err
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}
		if err := writePackItem(tw, item); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func extractPackArchive(ctx context.Context, r io.Reader, opts unpackOptions) (*packManifest, error) {
	if strings.TrimSpace(opts.LocalRoot) == "" {
		return nil, fmt.Errorf("drive9 unpack: --local-root is required")
	}
	if !filepath.IsAbs(opts.LocalRoot) {
		return nil, fmt.Errorf("drive9 unpack: --local-root must be an absolute path")
	}
	overlayRoot := filepath.Join(opts.LocalRoot, "overlay")
	if err := os.MkdirAll(overlayRoot, 0o755); err != nil {
		return nil, fmt.Errorf("prepare local overlay root: %w", err)
	}

	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open pack gzip stream: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	manifest := packManifest{Format: packArchiveFormat, Version: 1}
	replaced := false
	sawManifest := false
	var dirTimes []packDirTime
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read pack entry: %w", err)
		}
		if hdr.Name == packManifestEntryName {
			sawManifest = true
			if err := json.NewDecoder(tr).Decode(&manifest); err != nil {
				return nil, fmt.Errorf("decode pack manifest: %w", err)
			}
			if manifest.Format != packArchiveFormat {
				return nil, fmt.Errorf("unsupported pack format %q", manifest.Format)
			}
			if opts.Replace && !replaced {
				if err := removeManifestPaths(opts.LocalRoot, manifest.Paths); err != nil {
					return nil, err
				}
				replaced = true
			}
			continue
		}
		if !sawManifest {
			return nil, fmt.Errorf("invalid pack archive: missing leading %s", packManifestEntryName)
		}
		if err := extractPackEntry(opts.LocalRoot, hdr, tr, &dirTimes); err != nil {
			return nil, err
		}
	}
	if !sawManifest {
		return nil, fmt.Errorf("invalid pack archive: missing %s", packManifestEntryName)
	}
	for i := len(dirTimes) - 1; i >= 0; i-- {
		_ = os.Chtimes(dirTimes[i].Path, dirTimes[i].ModTime, dirTimes[i].ModTime)
	}
	return &manifest, nil
}

func writePackManifest(tw *tar.Writer, manifest packManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	hdr := &tar.Header{
		Name:    packManifestEntryName,
		Mode:    0o644,
		Size:    int64(len(data)),
		ModTime: manifest.CreatedAt,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = tw.Write(data)
	return err
}

func writePackItem(tw *tar.Writer, item packItem) error {
	hdr, err := tar.FileInfoHeader(item.Info, item.Linkname)
	if err != nil {
		return fmt.Errorf("pack header %s: %w", item.ArchivePath, err)
	}
	hdr.Name = packArchiveEntryPrefix + strings.TrimPrefix(item.ArchivePath, "/")
	if item.Type == "file" {
		hdr.Size = item.Info.Size()
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write pack header %s: %w", item.ArchivePath, err)
	}
	if item.Type != "file" {
		return nil
	}
	f, err := os.Open(item.LocalPath)
	if err != nil {
		return fmt.Errorf("open pack source %s: %w", item.LocalPath, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("write pack content %s: %w", item.ArchivePath, err)
	}
	return nil
}

func resolvePackSources(opts packOptions) ([]packSource, error) {
	if len(opts.Paths) == 0 {
		if opts.Profile != "coding-agent" {
			return nil, fmt.Errorf("drive9 pack: path arguments are required unless --profile=coding-agent is set")
		}
		return discoverCodingAgentPackSources(opts.LocalRoot, opts.RemoteRoot, opts.LocalPrefix)
	}
	out := make([]packSource, 0, len(opts.Paths))
	for _, raw := range opts.Paths {
		archivePath, remotePath, err := resolvePackPath(opts.RemoteRoot, opts.LocalPrefix, raw)
		if err != nil {
			return nil, err
		}
		localPath, err := overlayPathForArchivePath(opts.LocalRoot, archivePath)
		if err != nil {
			return nil, err
		}
		if _, err := os.Lstat(localPath); err != nil {
			if opts.SkipMissing && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("stat pack path %s (%s): %w", remotePath, localPath, err)
		}
		out = append(out, packSource{ArchivePath: archivePath, RemotePath: remotePath, LocalPath: localPath})
	}
	return normalizePackSources(out), nil
}

func discoverCodingAgentPackSources(localRoot, remoteRoot string, localPrefix string) ([]packSource, error) {
	prefixPath, err := canonicalArchivePath(localPrefix)
	if err != nil {
		return nil, err
	}
	root, err := overlayPathForArchivePath(localRoot, prefixPath)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat profile pack root %s: %w", root, err)
	}
	names := map[string]struct{}{}
	for _, name := range defaultCodingAgentPackNames {
		names[name] = struct{}{}
	}
	var out []packSource
	err = filepath.WalkDir(root, func(localPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if localPath == root {
			return nil
		}
		if _, ok := names[d.Name()]; !ok {
			return nil
		}
		archivePath, err := archivePathForOverlay(localRoot, localPath)
		if err != nil {
			return err
		}
		remotePath := mountpath.ToRemote(remoteRoot, archivePath)
		out = append(out, packSource{ArchivePath: archivePath, RemotePath: remotePath, LocalPath: localPath})
		if d.IsDir() {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("discover coding-agent pack paths: %w", err)
	}
	return normalizePackSources(out), nil
}

func normalizePackSources(in []packSource) []packSource {
	sort.Slice(in, func(i, j int) bool {
		if len(in[i].ArchivePath) == len(in[j].ArchivePath) {
			return in[i].ArchivePath < in[j].ArchivePath
		}
		return len(in[i].ArchivePath) < len(in[j].ArchivePath)
	})
	out := make([]packSource, 0, len(in))
	seen := map[string]struct{}{}
	for _, source := range in {
		if _, ok := seen[source.ArchivePath]; ok {
			continue
		}
		skip := false
		for _, existing := range out {
			if source.ArchivePath != existing.ArchivePath && strings.HasPrefix(source.ArchivePath, strings.TrimSuffix(existing.ArchivePath, "/")+"/") {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		seen[source.ArchivePath] = struct{}{}
		out = append(out, source)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ArchivePath < out[j].ArchivePath })
	return out
}

func collectPackItems(ctx context.Context, sources []packSource) ([]packItem, error) {
	var items []packItem
	seen := map[string]struct{}{}
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := os.Lstat(source.LocalPath)
		if err != nil {
			return nil, fmt.Errorf("stat pack source %s: %w", source.LocalPath, err)
		}
		if !info.IsDir() {
			item, err := newPackItem(source.ArchivePath, source.RemotePath, source.LocalPath, info)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[item.ArchivePath]; !ok {
				items = append(items, item)
				seen[item.ArchivePath] = struct{}{}
			}
			continue
		}
		err = filepath.WalkDir(source.LocalPath, func(localPath string, d fs.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(source.LocalPath, localPath)
			if err != nil {
				return err
			}
			archivePath := source.ArchivePath
			if rel != "." {
				archivePath = path.Join(source.ArchivePath, filepath.ToSlash(rel))
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			remotePath := mountpath.ToRemote(source.RemotePath, strings.TrimPrefix(strings.TrimPrefix(archivePath, source.ArchivePath), "/"))
			if archivePath == source.ArchivePath {
				remotePath = source.RemotePath
			}
			item, err := newPackItem(archivePath, remotePath, localPath, info)
			if err != nil {
				return err
			}
			if _, ok := seen[item.ArchivePath]; ok {
				return nil
			}
			items = append(items, item)
			seen[item.ArchivePath] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk pack source %s: %w", source.LocalPath, err)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if strings.Count(items[i].ArchivePath, "/") == strings.Count(items[j].ArchivePath, "/") {
			return items[i].ArchivePath < items[j].ArchivePath
		}
		return strings.Count(items[i].ArchivePath, "/") < strings.Count(items[j].ArchivePath, "/")
	})
	return items, nil
}

func newPackItem(archivePath, remotePath, localPath string, info fs.FileInfo) (packItem, error) {
	item := packItem{ArchivePath: archivePath, RemotePath: remotePath, LocalPath: localPath, Info: info}
	switch {
	case info.IsDir():
		item.Type = "dir"
	case info.Mode()&os.ModeSymlink != 0:
		linkname, err := os.Readlink(localPath)
		if err != nil {
			return packItem{}, fmt.Errorf("read symlink %s: %w", localPath, err)
		}
		item.Type = "symlink"
		item.Linkname = linkname
	case info.Mode().IsRegular():
		item.Type = "file"
	default:
		return packItem{}, fmt.Errorf("unsupported pack source type %s (%s)", localPath, info.Mode())
	}
	return item, nil
}

func packSourcePaths(sources []packSource) []string {
	out := make([]string, len(sources))
	for i, source := range sources {
		out[i] = source.ArchivePath
	}
	return out
}

func packManifestEntries(items []packItem) []packManifestEntry {
	out := make([]packManifestEntry, len(items))
	for i, item := range items {
		mode := uint32(item.Info.Mode().Perm())
		if item.Type == "symlink" {
			mode = 0o777
		}
		out[i] = packManifestEntry{
			Path:       item.ArchivePath,
			RemotePath: item.RemotePath,
			Type:       item.Type,
			Mode:       mode,
			Size:       item.Info.Size(),
			Linkname:   item.Linkname,
			ModTime:    item.Info.ModTime().Unix(),
		}
	}
	return out
}

func resolvePackPath(remoteRoot, localPrefix, raw string) (archivePath, remotePath string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("drive9 pack: empty pack path")
	}
	prefix, err := canonicalArchivePath(localPrefix)
	if err != nil {
		return "", "", err
	}
	if strings.HasPrefix(raw, "/") {
		// Absolute paths that fall under the mounted remote root are treated
		// as Drive9 remote paths. Other absolute slash paths are treated as
		// mount-local paths, which is useful when the remote root is non-root.
		if local, ok := mountpath.ToLocal(remoteRoot, raw); ok {
			archivePath, err = canonicalArchivePath(local)
			if err != nil {
				return "", "", err
			}
			remotePath, err = pathutil.Canonicalize(raw)
			if err != nil {
				return "", "", err
			}
			return archivePath, remotePath, nil
		}
		archivePath, err = canonicalArchivePath(raw)
		if err != nil {
			return "", "", err
		}
		remotePath, err = pathutil.Canonicalize(mountpath.ToRemote(remoteRoot, archivePath))
		if err != nil {
			return "", "", err
		}
		return archivePath, remotePath, nil
	}
	archivePath, err = joinArchivePath(prefix, raw)
	if err != nil {
		return "", "", err
	}
	remotePath, err = pathutil.Canonicalize(mountpath.ToRemote(remoteRoot, archivePath))
	if err != nil {
		return "", "", err
	}
	return archivePath, remotePath, nil
}

func overlayPathForArchivePath(localRoot, archivePath string) (string, error) {
	archivePath, err := canonicalArchivePath(archivePath)
	if err != nil {
		return "", err
	}
	rel := strings.TrimPrefix(archivePath, "/")
	root := filepath.Join(localRoot, "overlay")
	if rel == "" {
		return root, nil
	}
	return filepath.Join(root, filepath.FromSlash(rel)), nil
}

func archivePathForOverlay(localRoot, localPath string) (string, error) {
	root := filepath.Join(localRoot, "overlay")
	rel, err := filepath.Rel(root, localPath)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return "/", nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("local path %s is outside overlay root %s", localPath, root)
	}
	return pathutil.Canonicalize("/" + filepath.ToSlash(rel))
}

func canonicalArchivePath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "." {
		return "/", nil
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	return pathutil.Canonicalize(value)
}

func joinArchivePath(base, rel string) (string, error) {
	base, err := canonicalArchivePath(base)
	if err != nil {
		return "", err
	}
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", fmt.Errorf("drive9 pack: empty pack path")
	}
	if strings.Contains(rel, "\\") {
		return "", fmt.Errorf("drive9 pack: pack path contains backslash: %q", rel)
	}
	if strings.HasPrefix(rel, "/") {
		return canonicalArchivePath(rel)
	}
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("drive9 pack: invalid relative pack path %q", rel)
		}
	}
	if base == "/" {
		return canonicalArchivePath("/" + filepath.ToSlash(rel))
	}
	return canonicalArchivePath(strings.TrimSuffix(base, "/") + "/" + filepath.ToSlash(rel))
}

type packDirTime struct {
	Path    string
	ModTime time.Time
}

func extractPackEntry(localRoot string, hdr *tar.Header, r io.Reader, dirTimes *[]packDirTime) error {
	rel, err := packEntryRel(hdr.Name)
	if err != nil {
		return err
	}
	target, err := safeOverlayTarget(localRoot, rel)
	if err != nil {
		return err
	}
	mode := fs.FileMode(hdr.Mode & 0o777)
	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := os.MkdirAll(target, mode); err != nil {
			return fmt.Errorf("mkdir unpack target %s: %w", target, err)
		}
		*dirTimes = append(*dirTimes, packDirTime{Path: target, ModTime: hdr.ModTime})
	case tar.TypeReg, 0:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("mkdir unpack parent %s: %w", filepath.Dir(target), err)
		}
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("replace unpack target %s: %w", target, err)
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return fmt.Errorf("create unpack target %s: %w", target, err)
		}
		_, copyErr := io.Copy(f, r)
		closeErr := f.Close()
		if copyErr != nil {
			return fmt.Errorf("write unpack target %s: %w", target, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close unpack target %s: %w", target, closeErr)
		}
		_ = os.Chtimes(target, hdr.ModTime, hdr.ModTime)
	case tar.TypeSymlink:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("mkdir unpack parent %s: %w", filepath.Dir(target), err)
		}
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("replace unpack symlink %s: %w", target, err)
		}
		if err := os.Symlink(hdr.Linkname, target); err != nil {
			return fmt.Errorf("create unpack symlink %s: %w", target, err)
		}
	default:
		return fmt.Errorf("unsupported pack entry type %d for %s", hdr.Typeflag, hdr.Name)
	}
	return nil
}

func removeManifestPaths(localRoot string, paths []string) error {
	for _, archivePath := range paths {
		archivePath, err := canonicalArchivePath(archivePath)
		if err != nil {
			return fmt.Errorf("invalid manifest path %q: %w", archivePath, err)
		}
		rel := strings.TrimPrefix(archivePath, "/")
		if rel == "" {
			return fmt.Errorf("refusing to replace local overlay root from pack manifest")
		}
		target, err := safeOverlayTarget(localRoot, rel)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("replace unpack root %s: %w", target, err)
		}
	}
	return nil
}

func packEntryRel(name string) (string, error) {
	if !strings.HasPrefix(name, packArchiveEntryPrefix) {
		return "", fmt.Errorf("unsupported pack entry name %q", name)
	}
	rel := strings.TrimPrefix(name, packArchiveEntryPrefix)
	if rel == "" {
		return "", fmt.Errorf("empty pack entry path")
	}
	if strings.ContainsRune(rel, '\x00') || strings.Contains(rel, "\\") || strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("unsafe pack entry path %q", rel)
	}
	cleaned := path.Clean(rel)
	if cleaned == "." || cleaned != rel {
		return "", fmt.Errorf("unclean pack entry path %q", rel)
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("unsafe pack entry path %q", rel)
		}
	}
	return rel, nil
}

func safeOverlayTarget(localRoot, rel string) (string, error) {
	overlayRoot := filepath.Join(localRoot, "overlay")
	target := filepath.Join(overlayRoot, filepath.FromSlash(rel))
	cleanRoot := filepath.Clean(overlayRoot)
	cleanTarget := filepath.Clean(target)
	if cleanTarget != cleanRoot && !strings.HasPrefix(cleanTarget, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("pack entry %q escapes local overlay root", rel)
	}
	return cleanTarget, nil
}

func clientForRemoteArchiveArg(defaultClient *client.Client, raw string) (*client.Client, string, error) {
	rp, ok := ParseRemote(raw)
	if !ok {
		return nil, "", fmt.Errorf("remote archive path required (use :/path/to/archive.tar.gz)")
	}
	if rp.Path == "/" || strings.HasSuffix(rp.Path, "/") {
		return nil, "", fmt.Errorf("remote archive path must be a file, got %q", raw)
	}
	c := defaultClient
	if rp.Context != "" {
		var err error
		c, err = newFSClientForContext(rp.Context)
		if err != nil {
			return nil, "", err
		}
	}
	return c, rp.Path, nil
}

func clientForPackArchiveArg(defaultClient *client.Client, raw string, remoteRoot string, profile string) (*client.Client, string, error) {
	if strings.TrimSpace(raw) != "" {
		return clientForRemoteArchiveArg(defaultClient, raw)
	}
	if profile != "coding-agent" {
		return nil, "", fmt.Errorf("default pack archive requires --profile=coding-agent or an explicit remote archive path")
	}
	archivePath, err := defaultCodingAgentPackArchivePath(remoteRoot)
	if err != nil {
		return nil, "", err
	}
	return defaultClient, archivePath, nil
}

func defaultCodingAgentPackArchivePath(remoteRoot string) (string, error) {
	remoteRoot, err := mountpath.NormalizeRoot(strings.TrimSpace(remoteRoot))
	if err != nil {
		return "", fmt.Errorf("drive9 pack: %w", err)
	}
	sum := sha256.Sum256([]byte(remoteRoot))
	hash := hex.EncodeToString(sum[:8])
	label := path.Base(remoteRoot)
	if label == "." || label == "/" || label == "" {
		label = "root"
	}
	label = safePackArchiveLabel(label)
	return fmt.Sprintf("%s/%s-%s.tar.gz", codingAgentPackRoot, label, hash), nil
}

func safePackArchiveLabel(label string) string {
	var b strings.Builder
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		return "root"
	}
	return out
}

func resolveMountedPackTarget(target string) (mountedPackTarget, error) {
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return mountedPackTarget{}, fmt.Errorf("resolve mount target: %w", err)
	}
	candidate := filepath.Clean(absTarget)
	for {
		state, _, err := mountstate.ReadProcessState(candidate)
		if err == nil {
			if strings.TrimSpace(state.LocalRoot) == "" {
				return mountedPackTarget{}, fmt.Errorf("drive9 mount metadata for %q does not include local_root; remount with --profile=coding-agent --local-root", candidate)
			}
			if strings.TrimSpace(state.RemoteRoot) == "" {
				return mountedPackTarget{}, fmt.Errorf("drive9 mount metadata for %q does not include remote_root; remount with a newer drive9 CLI", candidate)
			}
			absMount, rel, ok, err := relToMountedTarget(absTarget, state.MountPoint)
			if err != nil {
				return mountedPackTarget{}, err
			}
			if !ok {
				absMount, rel, ok, err = relToMountedTarget(absTarget, candidate)
				if err != nil {
					return mountedPackTarget{}, err
				}
			}
			if !ok {
				return mountedPackTarget{}, fmt.Errorf("target %q is outside drive9 mount %q", target, candidate)
			}
			remoteRoot, err := mountpath.NormalizeRoot(state.RemoteRoot)
			if err != nil {
				return mountedPackTarget{}, err
			}
			return mountedPackTarget{
				MountPoint: absMount,
				MountRel:   rel,
				RemoteRoot: remoteRoot,
				Profile:    state.Profile,
				LocalRoot:  state.LocalRoot,
			}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return mountedPackTarget{}, err
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			break
		}
		candidate = parent
	}
	return mountedPackTarget{}, fmt.Errorf("target %q is not inside a drive9 mount with readable mount metadata", target)
}

func humanizePackBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
