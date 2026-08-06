package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
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
}

// Scanner reads one mounted EBS Source Root without mutating it.
type Scanner struct {
	root       string
	bufferSize int
	identity   func(string, os.FileInfo) (fileIdentity, error)
	afterRead  func(string)
}

func NewScanner(root string) (*Scanner, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("%w: source root must be clean and absolute", ErrUnsafeSourcePath)
	}
	return &Scanner{root: root, bufferSize: MaxSourceReadBufferBytes, identity: defaultFileIdentity}, nil
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
	rootInfo, err := os.Lstat(s.root)
	if err != nil {
		return result, fmt.Errorf("lstat source root: %w", err)
	}
	if !rootInfo.IsDir() {
		return result, fmt.Errorf("source root is not a directory")
	}
	rootIdentity, err := s.identity(s.root, rootInfo)
	if err != nil {
		return result, fmt.Errorf("source root identity: %w", err)
	}
	accumulator := newScanAccumulator()
	hardlinks := make(map[string]struct{})
	err = filepath.WalkDir(s.root, func(name string, directory os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == s.root {
			return nil
		}
		relative, err := filepath.Rel(s.root, name)
		if err != nil {
			return err
		}
		canonical, ok := canonicalSourcePath(relative)
		if !ok {
			accumulator.findings = append(accumulator.findings, Finding{Kind: FindingInvalidUTF8, Severity: SeverityBlocker})
			if directory.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := os.Lstat(name)
		if err != nil {
			return err
		}
		identity, err := s.identity(name, info)
		if err != nil {
			accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingSourceIdentity, Severity: SeverityBlocker})
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if identity.version.Device != rootIdentity.version.Device {
			accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingNestedMount, Severity: SeverityBlocker})
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		entry := SourceEntry{Path: canonical, Kind: identity.version.Kind, Version: identity.version, Mode: uint32(info.Mode().Perm())}
		switch entry.Kind {
		case EntryRegular:
			if identity.nlink > 1 {
				entry.HardlinkKey = fmt.Sprintf("%d:%d", identity.version.Device, identity.version.Inode)
				if _, exists := hardlinks[entry.HardlinkKey]; !exists {
					entry.HardlinkPrimary = true
					hardlinks[entry.HardlinkKey] = struct{}{}
				}
			}
			if info.Size() > 0 && identity.blocks >= 0 && identity.blocks*512 < info.Size() {
				accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingSparseFile, Severity: SeverityWarning})
			}
		case EntryDirectory:
		case EntrySymlink:
			target, err := os.Readlink(name)
			if err != nil {
				return err
			}
			entry.LinkTarget = target
			sum := sha256.Sum256([]byte(target))
			entry.ChecksumSHA256 = hex.EncodeToString(sum[:])
			if !utf8.ValidString(target) {
				accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingInvalidUTF8, Severity: SeverityBlocker})
			} else if filepath.IsAbs(target) || strings.HasPrefix(filepath.ToSlash(target), "../") {
				accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingSymlinkTarget, Severity: SeverityWarning})
			}
		case EntrySpecial:
			accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingSpecialFile, Severity: SeverityBlocker})
		}
		if info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
			accumulator.findings = append(accumulator.findings, Finding{Path: canonical, Kind: FindingModeBits, Severity: SeverityWarning})
		}
		accumulator.add(canonical, "/"+filepath.ToSlash(relative), entry)
		return nil
	})
	if err != nil {
		result.Findings = accumulator.findings
		return result, fmt.Errorf("scan source namespace: %w", err)
	}
	result.Entries = accumulator.entries
	result.Findings = accumulator.findings
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
	canonical := norm.NFC.String("/" + filepath.ToSlash(relative))
	if pathpkg.Clean(canonical) != canonical {
		return "", false
	}
	return canonical, true
}

func (s *Scanner) resolvePath(sourcePath string) (string, error) {
	if !strings.HasPrefix(sourcePath, "/") || sourcePath == "/" || pathpkg.Clean(sourcePath) != sourcePath {
		return "", ErrUnsafeSourcePath
	}
	resolved := filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(sourcePath, "/")))
	relative, err := filepath.Rel(s.root, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", ErrUnsafeSourcePath
	}
	return resolved, nil
}

// ReadStable hashes a regular file through lstat/open/read/lstat token checks.
func (s *Scanner) ReadStable(ctx context.Context, sourcePath string, expected SourceVersion) (DeepRead, error) {
	name, err := s.resolvePath(sourcePath)
	if err != nil {
		return DeepRead{}, err
	}
	if expected.Kind != EntryRegular {
		return DeepRead{}, ErrUnsupportedSource
	}
	before, err := os.Lstat(name)
	if err != nil {
		return DeepRead{}, err
	}
	beforeIdentity, err := s.identity(name, before)
	if err != nil {
		return DeepRead{}, err
	}
	if beforeIdentity.version != expected {
		return DeepRead{}, ErrSourceChanged
	}
	file, err := os.Open(name)
	if err != nil {
		return DeepRead{}, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return DeepRead{}, err
	}
	openedIdentity, err := s.identity(name, opened)
	if err != nil || openedIdentity.version != expected {
		return DeepRead{}, ErrSourceChanged
	}
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
		count, readErr := file.Read(buffer)
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
	openedAfter, openErr := file.Stat()
	pathAfter, pathErr := os.Lstat(name)
	if openErr != nil || pathErr != nil {
		return DeepRead{}, ErrSourceChanged
	}
	openIdentity, openIdentityErr := s.identity(name, openedAfter)
	pathIdentity, pathIdentityErr := s.identity(name, pathAfter)
	if openIdentityErr != nil || pathIdentityErr != nil || openIdentity.version != expected || pathIdentity.version != expected || size != expected.Size {
		return DeepRead{}, ErrSourceChanged
	}
	return DeepRead{Version: expected, Size: size, ChecksumSHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}
