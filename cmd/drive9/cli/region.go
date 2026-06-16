package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

var errRegionManifestUnavailable = errors.New("region manifest unavailable")

const (
	EnvRegionCode        = "DRIVE9_REGION_CODE"
	EnvRegionManifestURL = "DRIVE9_REGION_MANIFEST_URL"

	defaultRegionManifestURL = "https://drive9.ai/manifest/regions/drive9-regions.json"
)

var fallbackRegionManifest = RegionManifest{
	Service: "drive9",
	Default: &RegionManifestDefault{
		RegionCode: "aws-ap-southeast-1",
		Mode:       RegionModeTiDBCloudStarter,
	},
	Regions: []RegionManifestEntry{
		{
			RegionCode: "aws-ap-southeast-1",
			Mode:       RegionModeTiDBCloudStarter,
			ServerURL:  defaultServerURL,
		},
	},
}

type RegionManifest struct {
	Service string                 `json:"service"`
	Default *RegionManifestDefault `json:"default,omitempty"`
	Regions []RegionManifestEntry  `json:"regions"`
}

type RegionManifestDefault struct {
	RegionCode string `json:"region_code"`
	Mode       string `json:"mode"`
}

type RegionManifestEntry struct {
	RegionCode    string            `json:"region_code"`
	Mode          string            `json:"mode"`
	ServerURL     string            `json:"server_url"`
	CloudProvider string            `json:"cloud_provider,omitempty"`
	TiDBRegion    string            `json:"tidb_region,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func Region(args []string) error {
	if len(args) == 0 || IsHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, regionUsage())
		return nil
	}
	switch args[0] {
	case "ls", "list":
		return regionListCmd(args[1:])
	default:
		return fmt.Errorf("unknown region command %q\n%s", args[0], regionUsage())
	}
}

func regionUsage() string {
	return `usage: drive9 region <list|ls>
  list [--json] [--manifest-url <url>]   list create regions from the drive9 manifest`
}

func regionListCmd(args []string) error {
	asJSON := false
	manifestURL := regionManifestURL()
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--json":
			asJSON = true
		case "--manifest-url":
			if i+1 >= len(args) {
				return fmt.Errorf("--manifest-url requires an argument")
			}
			i++
			manifestURL = strings.TrimSpace(args[i])
		default:
			return fmt.Errorf("unknown flag %q\nusage: drive9 region list [--json] [--manifest-url URL]", args[i])
		}
	}
	if manifestURL == "" {
		return fmt.Errorf("region manifest URL is empty")
	}
	manifest, err := fetchRegionManifest(context.Background(), manifestURL)
	if err != nil {
		if !errors.Is(err, errRegionManifestUnavailable) {
			return err
		}
		manifest = fallbackRegionManifestCopy()
	}
	sortRegionManifestEntries(manifest.Regions)
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(manifest)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "REGION\tMODE\tSERVER")
	for _, entry := range manifest.Regions {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", entry.RegionCode, regionModeLabel(entry.Mode), entry.ServerURL)
	}
	return w.Flush()
}

func fallbackRegionManifestCopy() *RegionManifest {
	manifest := fallbackRegionManifest
	if fallbackRegionManifest.Default != nil {
		defaultEntry := *fallbackRegionManifest.Default
		manifest.Default = &defaultEntry
	}
	manifest.Regions = append([]RegionManifestEntry(nil), fallbackRegionManifest.Regions...)
	return &manifest
}

func regionManifestURL() string {
	if raw := strings.TrimSpace(os.Getenv(EnvRegionManifestURL)); raw != "" {
		return raw
	}
	return defaultRegionManifestURL
}

func fetchRegionManifest(ctx context.Context, manifestURL string) (*RegionManifest, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build region manifest request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetch region manifest: %v", errRegionManifestUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: fetch region manifest: HTTP %d", errRegionManifestUnavailable, resp.StatusCode)
	}
	var manifest RegionManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode region manifest: %w", err)
	}
	if err := validateRegionManifest(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func validateRegionManifest(manifest *RegionManifest) error {
	if manifest == nil {
		return fmt.Errorf("region manifest is required")
	}
	if len(manifest.Regions) == 0 {
		return fmt.Errorf("region manifest has no regions")
	}
	seen := map[string]int{}
	for i, entry := range manifest.Regions {
		if strings.TrimSpace(entry.RegionCode) == "" {
			return fmt.Errorf("region manifest entry %d missing region_code", i)
		}
		if strings.TrimSpace(entry.Mode) == "" {
			return fmt.Errorf("region manifest entry %d missing mode", i)
		}
		if strings.TrimSpace(entry.ServerURL) == "" {
			return fmt.Errorf("region manifest entry %d missing server_url", i)
		}
		mode := strings.TrimSpace(entry.Mode)
		key := strings.TrimSpace(entry.RegionCode) + "\x00" + mode
		if first, ok := seen[key]; ok {
			return fmt.Errorf("region manifest entries %d and %d duplicate region_code %q mode %q", first, i, strings.TrimSpace(entry.RegionCode), mode)
		}
		seen[key] = i
	}
	return nil
}

func sortRegionManifestEntries(entries []RegionManifestEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].RegionCode != entries[j].RegionCode {
			return entries[i].RegionCode < entries[j].RegionCode
		}
		if entries[i].Mode != entries[j].Mode {
			return entries[i].Mode < entries[j].Mode
		}
		return entries[i].ServerURL < entries[j].ServerURL
	})
}

func regionModeLabel(mode string) string {
	switch strings.TrimSpace(mode) {
	case RegionModeTiDBCloudStarter:
		return "Anonymous"
	case RegionModeTiDBCloudNative:
		return "TiDBCloud"
	default:
		return strings.TrimSpace(mode)
	}
}
