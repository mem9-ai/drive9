package migration

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

var (
	ErrIncompleteInventory = errors.New("incomplete migration inventory")
	ErrTargetChanged       = errors.New("target changed during inventory")
)

type TargetScan struct {
	Complete bool
	Entries  map[string]TargetEntry
}

// TargetScanner inventories exactly one bounded Drive9 Prefix.
type TargetScanner struct {
	api    *client.Client
	prefix string
}

func NewTargetScanner(api *client.Client, prefix string) (*TargetScanner, error) {
	if api == nil {
		return nil, errors.New("target scanner requires a client")
	}
	clean, err := validateTargetPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &TargetScanner{api: api, prefix: clean}, nil
}

type listedTarget struct {
	logical string
	remote  string
	info    client.FileInfo
}

func (s *TargetScanner) Scan(ctx context.Context, checksumPaths map[string]struct{}) (TargetScan, error) {
	result := TargetScan{Entries: make(map[string]TargetEntry)}
	entries := make(map[string]TargetEntry)
	queue := []listedTarget{{remote: listPath(s.prefix)}}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		directory := queue[0]
		queue = queue[1:]
		listed, err := s.api.ListCtx(ctx, directory.remote)
		if err != nil {
			if directory.logical == "" && client.IsNotFound(err) {
				result.Complete = true
				return result, nil
			}
			return result, fmt.Errorf("list target %q: %w", directory.logical, err)
		}
		observed, err := s.validateListing(directory.logical, listed, entries)
		if err != nil {
			return result, err
		}
		deep, shallow := splitTargetStats(observed, checksumPaths)
		for _, group := range []struct {
			items    []listedTarget
			checksum bool
		}{{shallow, false}, {deep, true}} {
			for first := 0; first < len(group.items); first += client.MaxBatchStatPaths {
				last := min(first+client.MaxBatchStatPaths, len(group.items))
				if err := s.statBatch(ctx, group.items[first:last], group.checksum, entries, &queue); err != nil {
					return result, err
				}
			}
		}
	}
	result.Complete = true
	result.Entries = entries
	return result, nil
}

func (s *TargetScanner) validateListing(parent string, listed []client.FileInfo, prior map[string]TargetEntry) ([]listedTarget, error) {
	seen := make(map[string]struct{}, len(listed))
	result := make([]listedTarget, 0, len(listed))
	for _, info := range listed {
		if !safeTargetName(info.Name) {
			return nil, fmt.Errorf("unsafe target entry name under %q", parent)
		}
		logical := "/" + info.Name
		if parent != "" {
			logical = parent + "/" + info.Name
		}
		canonical, err := pathutil.Canonicalize(logical)
		if err != nil || canonical != logical {
			return nil, fmt.Errorf("unsafe target path under %q", parent)
		}
		if _, duplicate := seen[logical]; duplicate {
			return nil, fmt.Errorf("duplicate target entry %q", logical)
		}
		seen[logical] = struct{}{}
		if _, duplicate := prior[logical]; duplicate {
			return nil, fmt.Errorf("duplicate target path %q", logical)
		}
		if s.prefix == "/" && logical == ControlPrefix {
			if !info.IsDir {
				return nil, fmt.Errorf("reserved control path is not a directory")
			}
			continue
		}
		remote := targetRemotePath(s.prefix, logical, info.IsDir)
		result = append(result, listedTarget{logical: logical, remote: remote, info: info})
	}
	return result, nil
}

func splitTargetStats(items []listedTarget, checksumPaths map[string]struct{}) (deep, shallow []listedTarget) {
	for _, item := range items {
		_, checksum := checksumPaths[item.logical]
		if checksum && !item.info.IsDir {
			deep = append(deep, item)
		} else {
			shallow = append(shallow, item)
		}
	}
	return deep, shallow
}

func (s *TargetScanner) statBatch(ctx context.Context, items []listedTarget, checksum bool, entries map[string]TargetEntry, queue *[]listedTarget) error {
	paths := make([]string, len(items))
	for i := range items {
		paths[i] = items[i].remote
	}
	stats, err := s.api.BatchStatWithOptionsCtx(ctx, paths, client.BatchStatOptions{IncludeChecksum: checksum})
	if err != nil {
		return fmt.Errorf("stat target batch: %w", err)
	}
	for i, stat := range stats {
		item := items[i]
		if stat.Path != item.remote {
			return fmt.Errorf("%w: stat path mismatch for %s", ErrTargetChanged, item.logical)
		}
		if !stat.OK() {
			if stat.Status == 404 {
				return fmt.Errorf("%w: %s disappeared", ErrTargetChanged, item.logical)
			}
			return &client.StatusError{StatusCode: stat.Status, Message: fmt.Sprintf("stat target %q returned status %d", item.logical, stat.Status)}
		}
		if targetObservationChanged(item.info, stat) {
			return fmt.Errorf("%w: %s", ErrTargetChanged, item.logical)
		}
		entry := TargetEntry{
			Path: item.logical, Kind: targetKind(stat), Size: stat.Size, Mode: stat.Mode & 0o777,
			HasMode: stat.HasMode, Revision: stat.Revision, ResourceID: stat.ResourceID,
			Nlink: stat.Nlink, ChecksumSHA256: stat.ChecksumSHA256,
		}
		entries[item.logical] = entry
		if entry.Kind == EntryDirectory {
			*queue = append(*queue, listedTarget{logical: item.logical, remote: item.remote})
		}
	}
	return nil
}

func targetObservationChanged(listed client.FileInfo, stat client.BatchStatResult) bool {
	return listed.IsDir != stat.IsDir || listed.Size != stat.Size ||
		(listed.Revision != 0 && listed.Revision != stat.Revision) ||
		(listed.ResourceID != "" && listed.ResourceID != stat.ResourceID) ||
		(listed.HasMode && (!stat.HasMode || listed.Mode != stat.Mode)) ||
		(listed.Nlink != 0 && listed.Nlink != stat.Nlink)
}

func targetKind(stat client.BatchStatResult) EntryKind {
	if stat.IsDir {
		return EntryDirectory
	}
	if !stat.HasMode {
		return EntrySpecial
	}
	switch stat.Mode & 0o170000 {
	case 0, 0o100000:
		return EntryRegular
	case 0o120000:
		return EntrySymlink
	default:
		return EntrySpecial
	}
}

func safeTargetName(name string) bool {
	return name != "" && name != "." && name != ".." && utf8.ValidString(name) && norm.NFC.String(name) == name &&
		!strings.ContainsAny(name, "/\\\x00")
}

func targetRemotePath(prefix, logical string, directory bool) string {
	remote := path.Join(prefix, strings.TrimPrefix(logical, "/"))
	if directory && remote != "/" {
		remote += "/"
	}
	return remote
}

// BuildRound creates diff work only from two complete namespace observations.
func BuildRound(id string, mode RoundMode, started time.Time, source ScanResult, target TargetScan) (Round, error) {
	round := Round{ID: id, Mode: mode, StartedAt: started}
	if !source.Complete || !target.Complete {
		return round, ErrIncompleteInventory
	}
	round.ScanComplete = true
	round.CompletedAt = time.Now()
	round.Source = source.Entries
	round.Target = target.Entries
	round.Findings = append(append([]Finding(nil), source.Findings...), diffSnapshots(source.Entries, target.Entries)...)
	sort.Slice(round.Findings, func(i, j int) bool {
		if round.Findings[i].Path == round.Findings[j].Path {
			return round.Findings[i].Kind < round.Findings[j].Kind
		}
		return round.Findings[i].Path < round.Findings[j].Path
	})
	round.Converged = true
	for _, finding := range round.Findings {
		if finding.Severity == SeverityBlocker {
			round.Converged = false
			break
		}
	}
	return round, nil
}

func diffSnapshots(source map[string]SourceEntry, target map[string]TargetEntry) []Finding {
	findings := make([]Finding, 0)
	seen := make(map[string]struct{})
	ownedLinks := targetOwnedLinkCounts(source, target)
	add := func(path string, kind FindingKind) {
		key := path + "\x00" + string(kind)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		findings = append(findings, Finding{Path: path, Kind: kind, Severity: SeverityBlocker})
	}
	resourceOwners := make(map[string]string)
	groupResources := make(map[string]string)
	sourcePaths := make([]string, 0, len(source))
	for sourcePath := range source {
		sourcePaths = append(sourcePaths, sourcePath)
	}
	sort.Strings(sourcePaths)
	for _, sourcePath := range sourcePaths {
		sourceEntry := source[sourcePath]
		targetEntry, exists := target[sourcePath]
		if !exists {
			add(sourcePath, FindingSourceOnly)
			continue
		}
		if sourceEntry.Kind != targetEntry.Kind {
			add(sourcePath, FindingType)
			continue
		}
		if targetEntry.ResourceID == "" {
			add(sourcePath, FindingIdentity)
		}
		if !targetEntry.HasMode || sourceEntry.Mode&0o777 != targetEntry.Mode&0o777 {
			add(sourcePath, FindingMetadata)
		}
		if sourceEntry.Kind != EntryDirectory && targetEntry.Revision <= 0 {
			add(sourcePath, FindingRevision)
		}
		if sourceEntry.Kind != EntryDirectory && (sourceEntry.Version.Size != targetEntry.Size ||
			sourceEntry.ChecksumSHA256 != "" && sourceEntry.ChecksumSHA256 != targetEntry.ChecksumSHA256) {
			add(sourcePath, FindingContent)
		}
		if sourceEntry.Kind == EntryRegular && targetEntry.ResourceID != "" {
			owner := targetLinkSourceOwner(sourceEntry)
			if sourceEntry.HardlinkKey != "" {
				if prior := groupResources[owner]; prior != "" && prior != targetEntry.ResourceID {
					add(sourcePath, FindingIdentity)
				} else {
					groupResources[owner] = targetEntry.ResourceID
				}
			}
			if prior := resourceOwners[targetEntry.ResourceID]; prior != "" && prior != owner {
				add(sourcePath, FindingIdentity)
			} else {
				resourceOwners[targetEntry.ResourceID] = owner
			}
			if targetEntry.Nlink == 0 || targetEntry.Nlink != ownedLinks[targetLinkOwner{source: owner, resource: targetEntry.ResourceID}] {
				add(sourcePath, FindingIdentity)
			}
		}
	}
	targetPaths := make([]string, 0, len(target))
	for targetPath := range target {
		targetPaths = append(targetPaths, targetPath)
	}
	sort.Strings(targetPaths)
	for _, targetPath := range targetPaths {
		if _, exists := source[targetPath]; !exists {
			add(targetPath, FindingTargetOnly)
		}
	}
	return findings
}

type targetLinkOwner struct {
	source   string
	resource string
}

func targetLinkSourceOwner(entry SourceEntry) string {
	if entry.HardlinkKey != "" {
		return "hardlink:" + entry.HardlinkKey
	}
	return "path:" + entry.Path
}

func targetOwnedLinkCounts(source map[string]SourceEntry, target map[string]TargetEntry) map[targetLinkOwner]uint32 {
	counts := make(map[targetLinkOwner]uint32)
	for path, targetEntry := range target {
		sourceEntry, exists := source[path]
		if !exists || sourceEntry.Kind != EntryRegular || targetEntry.Kind != EntryRegular || targetEntry.ResourceID == "" {
			continue
		}
		key := targetLinkOwner{source: targetLinkSourceOwner(sourceEntry), resource: targetEntry.ResourceID}
		counts[key]++
	}
	return counts
}
