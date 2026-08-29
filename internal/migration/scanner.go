package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"golang.org/x/sys/unix"
)

const MaxSourceReadBufferBytes = 256 << 10

var (
	ErrSourceChanged     = errors.New("source changed during stable read")
	ErrUnsafeSourcePath  = errors.New("unsafe source path")
	ErrUnsupportedSource = errors.New("unsupported source entry")
)

type fileIdentity struct {
	version SourceVersion
	nlink   uint64
	blocks  int64
}

type scannedDirectory struct {
	name    string
	version SourceVersion
}

type rootedSourceFile struct {
	scanner  *Scanner
	root     *os.Root
	file     *os.File
	name     string
	relative string
	expected SourceVersion
}

type DeepRead struct {
	Version        SourceVersion
	Size           int64
	ChecksumSHA256 string
}

type ScanResult struct {
	Complete       bool
	Entries        map[string]SourceEntry
	Findings       []Finding
	EntryCount     int
	DirectoryCount int
	LogicalBytes   int64
	rootVersion    SourceVersion
}

type sourceWalkResult struct {
	Findings       []Finding
	EntryCount     int
	DirectoryCount int
	FileCount      int
	LogicalBytes   int64
	RootVersion    SourceVersion
}

// Scanner reads one mounted EBS Source Root without mutating it.
type Scanner struct {
	root        string
	bufferSize  int
	identity    func(string, os.FileInfo) (fileIdentity, error)
	openFile    func(*os.Root, string) (*os.File, error)
	beforeEntry func(string)
	afterRead   func(string)
}

func NewScanner(root string) (*Scanner, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("%w: source root must be clean and absolute", ErrUnsafeSourcePath)
	}
	return &Scanner{root: root, bufferSize: MaxSourceReadBufferBytes, identity: defaultFileIdentity, openFile: openRootSourceFile}, nil
}

func openRootSourceFile(root *os.Root, relative string) (*os.File, error) {
	return root.OpenFile(relative, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
}

func defaultFileIdentity(_ string, info os.FileInfo) (fileIdentity, error) {
	dev, inode, ctimeNS, nlink, blocks, ok := platformStatFields(info)
	if !ok {
		return fileIdentity{}, errors.New("source identity unavailable")
	}
	kind := entryKind(info.Mode())
	return fileIdentity{
		version: SourceVersion{
			Device: dev, Inode: inode, Kind: kind, Size: info.Size(),
			MtimeNS: info.ModTime().UnixNano(), CtimeNS: ctimeNS, Mode: uint32(info.Mode()),
		},
		nlink:  nlink,
		blocks: blocks,
	}, nil
}

func entryKind(mode os.FileMode) EntryKind {
	switch {
	case mode.IsRegular():
		return EntryRegular
	case mode.IsDir():
		return EntryDirectory
	case mode&os.ModeSymlink != 0:
		return EntrySymlink
	default:
		return EntrySpecial
	}
}

func (s *Scanner) Scan(ctx context.Context) (ScanResult, error) {
	result := ScanResult{Entries: make(map[string]SourceEntry)}
	accumulator := newScanAccumulator()
	hardlinks := make(map[string]struct{})
	var directories []scannedDirectory
	walk, err := s.walkSource(ctx, func(entry SourceEntry) error {
		accumulator.add(entry.Path, entry.LocalPath, entry)
		return nil
	}, func(directory scannedDirectory) error {
		directories = append(directories, directory)
		return nil
	}, nil, func(key string) bool {
		if _, exists := hardlinks[key]; exists {
			return false
		}
		hardlinks[key] = struct{}{}
		return true
	})
	result.rootVersion = walk.RootVersion
	result.Findings = append(walk.Findings, accumulator.findings...)
	if err != nil {
		return result, err
	}
	if err := s.validateScannedDirectories(ctx, directories, walk.RootVersion); err != nil {
		return result, err
	}
	result.Entries = accumulator.entries
	result.EntryCount = len(result.Entries)
	for _, entry := range result.Entries {
		if entry.Kind == EntryDirectory {
			result.DirectoryCount++
		}
		if entry.Kind == EntryRegular {
			result.LogicalBytes += entry.Version.Size
		}
	}
	result.Complete = true
	return result, nil
}

func (s *Scanner) walkSource(ctx context.Context, visitEntry func(SourceEntry) error, visitDirectory func(scannedDirectory) error, visitFinding func(Finding) error, selectHardlinkPrimary func(string) bool) (sourceWalkResult, error) {
	var result sourceWalkResult
	recordFinding := func(finding Finding) error {
		if visitFinding != nil {
			return visitFinding(finding)
		}
		result.Findings = append(result.Findings, finding)
		return nil
	}
	root, rootInfo, err := s.openRoot()
	if err != nil {
		return result, err
	}
	defer func() { _ = root.Close() }()
	rootIdentity, err := s.identity(s.root, rootInfo)
	if err != nil {
		return result, fmt.Errorf("source root identity: %w", err)
	}
	result.RootVersion = rootIdentity.version
	if visitDirectory != nil {
		if err := visitDirectory(scannedDirectory{name: ".", version: rootIdentity.version}); err != nil {
			return result, err
		}
	}
	err = fs.WalkDir(root.FS(), ".", func(relative string, directory fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return rootedSourceChangeError(walkErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		name := filepath.Join(s.root, filepath.FromSlash(relative))
		if s.beforeEntry != nil {
			s.beforeEntry(name)
		}
		canonical, ok := canonicalSourcePath(relative)
		if !ok {
			if err := recordFinding(Finding{Kind: FindingInvalidUTF8, Severity: SeverityBlocker}); err != nil {
				return err
			}
			if directory.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rootName := filepath.FromSlash(relative)
		info, err := root.Lstat(rootName)
		if err != nil {
			return rootedSourceChangeError(err)
		}
		identity, err := s.identity(name, info)
		if err != nil {
			if err := recordFinding(Finding{Path: canonical, Kind: FindingSourceIdentity, Severity: SeverityBlocker}); err != nil {
				return err
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if identity.version.Device != rootIdentity.version.Device {
			if err := recordFinding(Finding{Path: canonical, Kind: FindingNestedMount, Severity: SeverityBlocker}); err != nil {
				return err
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if identity.version.Kind == EntryDirectory {
			if visitDirectory != nil {
				if err := visitDirectory(scannedDirectory{name: rootName, version: identity.version}); err != nil {
					return err
				}
			}
		}
		localPath := "/" + filepath.ToSlash(relative)
		entry := SourceEntry{Path: canonical, LocalPath: localPath, Kind: identity.version.Kind, Version: identity.version, Mode: uint32(info.Mode().Perm())}
		switch entry.Kind {
		case EntryRegular:
			if identity.nlink > 1 {
				entry.HardlinkKey = fmt.Sprintf("%d:%d", identity.version.Device, identity.version.Inode)
				if selectHardlinkPrimary != nil {
					entry.HardlinkPrimary = selectHardlinkPrimary(entry.HardlinkKey)
				}
			}
			if info.Size() > 0 && identity.blocks >= 0 && identity.blocks*512 < info.Size() {
				if err := recordFinding(Finding{Path: canonical, Kind: FindingSparseFile, Severity: SeverityWarning}); err != nil {
					return err
				}
			}
		case EntryDirectory:
		case EntrySymlink:
			target, err := root.Readlink(rootName)
			if err != nil {
				return rootedSourceChangeError(err)
			}
			entry.LinkTarget = target
			sum := sha256.Sum256([]byte(target))
			entry.ChecksumSHA256 = hex.EncodeToString(sum[:])
			if !utf8.ValidString(target) {
				if err := recordFinding(Finding{Path: canonical, Kind: FindingInvalidUTF8, Severity: SeverityBlocker}); err != nil {
					return err
				}
			} else if s.symlinkTargetNeedsWarning(root, rootName, target) {
				if err := recordFinding(Finding{Path: canonical, Kind: FindingSymlinkTarget, Severity: SeverityWarning}); err != nil {
					return err
				}
			}
		case EntrySpecial:
			if err := recordFinding(Finding{Path: canonical, Kind: FindingSpecialFile, Severity: SeverityBlocker}); err != nil {
				return err
			}
		}
		if info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
			if err := recordFinding(Finding{Path: canonical, Kind: FindingModeBits, Severity: SeverityWarning}); err != nil {
				return err
			}
		}
		if visitEntry != nil {
			if err := visitEntry(entry); err != nil {
				return err
			}
		}
		result.EntryCount++
		if entry.Kind == EntryDirectory {
			result.DirectoryCount++
		}
		if entry.Kind == EntryRegular {
			result.FileCount++
			result.LogicalBytes += entry.Version.Size
		}
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("scan source namespace: %w", err)
	}
	rootAfter, statErr := os.Lstat(s.root)
	if statErr != nil || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return result, ErrSourceChanged
	}
	return result, nil
}

func (s *Scanner) validateScannedDirectories(ctx context.Context, directories []scannedDirectory, rootVersion SourceVersion) error {
	root, rootInfo, err := s.openRoot()
	if err != nil {
		return ErrSourceChanged
	}
	defer func() { _ = root.Close() }()
	rootIdentity, err := s.identity(s.root, rootInfo)
	if err != nil || rootIdentity.version != rootVersion {
		return ErrSourceChanged
	}
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, statErr := root.Lstat(directory.name)
		if statErr != nil {
			return ErrSourceChanged
		}
		name := s.root
		if directory.name != "." {
			name = filepath.Join(s.root, directory.name)
		}
		identity, identityErr := s.identity(name, info)
		if identityErr != nil || identity.version != directory.version {
			return ErrSourceChanged
		}
	}
	rootAfter, statErr := os.Lstat(s.root)
	if statErr != nil || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return ErrSourceChanged
	}
	return nil
}

func (s *Scanner) validateSnapshot(ctx context.Context, snapshot ScanResult) error {
	if !snapshot.Complete || snapshot.rootVersion.Kind != EntryDirectory {
		return ErrSourceChanged
	}
	root, rootInfo, err := s.openRoot()
	if err != nil {
		return ErrSourceChanged
	}
	defer func() { _ = root.Close() }()
	rootIdentity, err := s.identity(s.root, rootInfo)
	if err != nil || rootIdentity.version != snapshot.rootVersion {
		return ErrSourceChanged
	}
	directories := []scannedDirectory{{name: ".", version: snapshot.rootVersion}}
	for _, entry := range snapshot.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		localPath := entry.LocalPath
		if localPath == "" {
			localPath = entry.Path
		}
		name, err := s.resolvePath(localPath)
		if err != nil {
			return err
		}
		relative := filepath.FromSlash(strings.TrimPrefix(localPath, "/"))
		if err := s.validateAncestors(root, relative); err != nil {
			return err
		}
		info, err := root.Lstat(relative)
		if err != nil {
			return ErrSourceChanged
		}
		identity, err := s.identity(name, info)
		if err != nil || identity.version != entry.Version {
			return ErrSourceChanged
		}
		if entry.Kind == EntryDirectory {
			directories = append(directories, scannedDirectory{name: relative, version: entry.Version})
		}
	}
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := root.Lstat(directory.name)
		if err != nil {
			return ErrSourceChanged
		}
		name := s.root
		if directory.name != "." {
			name = filepath.Join(s.root, directory.name)
		}
		identity, err := s.identity(name, info)
		if err != nil || identity.version != directory.version {
			return ErrSourceChanged
		}
	}
	rootAfter, err := os.Lstat(s.root)
	if err != nil || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return ErrSourceChanged
	}
	return nil
}

const maxSymlinkTargetHops = 255

// symlinkTargetNeedsWarning resolves only path and link metadata under the
// Source Root. It never opens target content or walks a target directory.
func (s *Scanner) symlinkTargetNeedsWarning(root *os.Root, linkName, target string) bool {
	if target == "" || filepath.IsAbs(target) {
		return true
	}
	resolved := make([]string, 0)
	relativeParent := filepath.Dir(linkName)
	if relativeParent != "." {
		resolved = append(resolved, strings.Split(relativeParent, string(filepath.Separator))...)
	}
	pending := strings.Split(target, string(filepath.Separator))
	hops := 0
	for len(pending) > 0 {
		part := pending[0]
		pending = pending[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			if len(resolved) == 0 {
				return true
			}
			resolved = resolved[:len(resolved)-1]
			continue
		}

		components := append(append([]string(nil), resolved...), part)
		current := filepath.Join(components...)
		info, err := root.Lstat(current)
		if err != nil {
			return true
		}
		if info.Mode()&os.ModeSymlink != 0 {
			hops++
			if hops > maxSymlinkTargetHops {
				return true
			}
			next, err := root.Readlink(current)
			if err != nil || next == "" || filepath.IsAbs(next) {
				return true
			}
			pending = append(strings.Split(next, string(filepath.Separator)), pending...)
			continue
		}
		if len(pending) > 0 && !info.IsDir() {
			return true
		}
		resolved = append(resolved, part)
	}
	return false
}

func rootedSourceChangeError(err error) error {
	if err == nil {
		return nil
	}
	return ErrSourceChanged
}

func (s *Scanner) openRoot() (*os.Root, os.FileInfo, error) {
	before, err := os.Lstat(s.root)
	if err != nil {
		return nil, nil, fmt.Errorf("lstat source root: %w", err)
	}
	if !before.IsDir() {
		return nil, nil, errors.New("source root is not a directory")
	}
	root, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, nil, fmt.Errorf("open source root: %w", err)
	}
	opened, openErr := root.Lstat(".")
	after, pathErr := os.Lstat(s.root)
	if openErr != nil || pathErr != nil || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(opened, after) {
		_ = root.Close()
		return nil, nil, ErrSourceChanged
	}
	return root, opened, nil
}

type scanAccumulator struct {
	entries   map[string]SourceEntry
	spellings map[string]string
	findings  []Finding
}

func newScanAccumulator() *scanAccumulator {
	return &scanAccumulator{entries: make(map[string]SourceEntry), spellings: make(map[string]string)}
}

func (a *scanAccumulator) add(canonical, original string, entry SourceEntry) {
	if previous, exists := a.spellings[canonical]; exists && previous != original {
		a.findings = append(a.findings, Finding{Path: canonical, Kind: FindingNFCCollision, Severity: SeverityBlocker})
		return
	}
	a.spellings[canonical] = original
	a.entries[canonical] = entry
}

func canonicalSourcePath(relative string) (string, bool) {
	if !utf8.ValidString(relative) {
		return "", false
	}
	canonical, err := pathutil.Canonicalize("/" + filepath.ToSlash(relative))
	if err != nil {
		return "", false
	}
	return canonical, true
}

func (s *Scanner) resolvePath(sourcePath string) (string, error) {
	if !strings.HasPrefix(sourcePath, "/") || sourcePath == "/" || filepath.ToSlash(filepath.Clean(filepath.FromSlash(sourcePath))) != sourcePath {
		return "", ErrUnsafeSourcePath
	}
	resolved := filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(sourcePath, "/")))
	relative, err := filepath.Rel(s.root, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", ErrUnsafeSourcePath
	}
	return resolved, nil
}

func (s *Scanner) openRootForPath(sourcePath string) (*os.Root, string, string, error) {
	name, err := s.resolvePath(sourcePath)
	if err != nil {
		return nil, "", "", err
	}
	root, _, err := s.openRoot()
	if err != nil {
		return nil, "", "", err
	}
	relative := filepath.FromSlash(strings.TrimPrefix(sourcePath, "/"))
	return root, relative, name, nil
}

func (s *Scanner) validateAncestors(root *os.Root, relative string) error {
	parts := strings.Split(relative, string(filepath.Separator))
	current := ""
	for _, part := range parts[:len(parts)-1] {
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrSourceChanged
		}
	}
	return nil
}

func (s *Scanner) openStableSource(sourcePath string, expected SourceVersion) (*rootedSourceFile, error) {
	root, relative, name, err := s.openRootForPath(sourcePath)
	if err != nil {
		return nil, err
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = root.Close()
		}
	}()
	if err := s.validateAncestors(root, relative); err != nil {
		return nil, err
	}
	before, err := root.Lstat(relative)
	if err != nil {
		return nil, rootedSourceChangeError(err)
	}
	beforeIdentity, err := s.identity(name, before)
	if err != nil || beforeIdentity.version != expected {
		return nil, ErrSourceChanged
	}
	file, err := s.openFile(root, relative)
	if err != nil {
		return nil, rootedSourceChangeError(err)
	}
	result := &rootedSourceFile{scanner: s, root: root, file: file, name: name, relative: relative, expected: expected}
	if err := result.validate(); err != nil {
		_ = file.Close()
		return nil, err
	}
	closeRoot = false
	return result, nil
}

func (s *Scanner) validateSourcePath(sourcePath string, expected SourceVersion) error {
	root, relative, name, err := s.openRootForPath(sourcePath)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	if err := s.validateAncestors(root, relative); err != nil {
		return err
	}
	info, err := root.Lstat(relative)
	if err != nil {
		return rootedSourceChangeError(err)
	}
	identity, err := s.identity(name, info)
	if err != nil || identity.version != expected {
		return ErrSourceChanged
	}
	return nil
}

func (f *rootedSourceFile) validate() error {
	if err := f.scanner.validateAncestors(f.root, f.relative); err != nil {
		return err
	}
	opened, openErr := f.file.Stat()
	current, pathErr := f.root.Lstat(f.relative)
	if openErr != nil || pathErr != nil {
		return ErrSourceChanged
	}
	openedIdentity, openIdentityErr := f.scanner.identity(f.name, opened)
	pathIdentity, pathIdentityErr := f.scanner.identity(f.name, current)
	if openIdentityErr != nil || pathIdentityErr != nil || openedIdentity.version != f.expected || pathIdentity.version != f.expected {
		return ErrSourceChanged
	}
	return nil
}

func (f *rootedSourceFile) close() error {
	fileErr := f.file.Close()
	rootErr := f.root.Close()
	if fileErr != nil {
		return fileErr
	}
	return rootErr
}

// ReadStableEntry reads an entry through its original local spelling while preserving its normalized logical path.
func (s *Scanner) ReadStableEntry(ctx context.Context, entry SourceEntry) (DeepRead, error) {
	localPath := entry.LocalPath
	if localPath == "" {
		localPath = entry.Path
	}
	return s.ReadStable(ctx, localPath, entry.Version)
}

// ReadStable hashes a regular file through lstat/open/read/lstat token checks.
func (s *Scanner) ReadStable(ctx context.Context, sourcePath string, expected SourceVersion) (DeepRead, error) {
	if _, err := s.resolvePath(sourcePath); err != nil {
		return DeepRead{}, err
	}
	if expected.Kind != EntryRegular {
		return DeepRead{}, ErrUnsupportedSource
	}
	rooted, err := s.openStableSource(sourcePath, expected)
	if err != nil {
		return DeepRead{}, err
	}
	defer func() { _ = rooted.close() }()
	hash := sha256.New()
	bufferSize := s.bufferSize
	if bufferSize <= 0 || bufferSize > MaxSourceReadBufferBytes {
		bufferSize = MaxSourceReadBufferBytes
	}
	buffer := make([]byte, bufferSize)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return DeepRead{}, err
		}
		count, readErr := rooted.file.Read(buffer)
		if count > 0 {
			_, _ = hash.Write(buffer[:count])
			size += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return DeepRead{}, readErr
		}
	}
	if s.afterRead != nil {
		s.afterRead(sourcePath)
	}
	if err := rooted.validate(); err != nil || size != expected.Size {
		return DeepRead{}, ErrSourceChanged
	}
	return DeepRead{Version: expected, Size: size, ChecksumSHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}
