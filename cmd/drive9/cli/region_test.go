package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegionListTextAndJSON(t *testing.T) {
	manifest := newRegionManifestTestServer(t, []RegionManifestEntry{
		{
			RegionCode:    "ali-ap-southeast-1",
			Mode:          RegionModeTiDBCloudNative,
			ServerURL:     "https://native-sg.example",
			CloudProvider: "alicloud",
			TiDBRegion:    "ap-southeast-1",
		},
		{
			RegionCode:    "aws-us-east-1",
			Mode:          RegionModeTiDBCloudStarter,
			ServerURL:     "https://api.drive9.ai",
			CloudProvider: "aws",
			TiDBRegion:    "us-east-1",
		},
	})
	defer manifest.Close()

	textOut, err := captureStdoutE(t, func() error {
		return Region([]string{"list", "--manifest-url", manifest.URL})
	})
	if err != nil {
		t.Fatalf("region list: %v", err)
	}
	for _, want := range []string{
		"REGION",
		"MODE",
		"SERVER",
		"ali-ap-southeast-1",
		"TiDBCloud",
		"Anonymous",
		"https://api.drive9.ai",
	} {
		if !strings.Contains(textOut, want) {
			t.Fatalf("text output = %q, want substring %q", textOut, want)
		}
	}
	if strings.Contains(textOut, "NAME") {
		t.Fatalf("text output = %q, want no NAME column", textOut)
	}
	if strings.Contains(textOut, "tidb_cloud_native") || strings.Contains(textOut, "tidb_cloud_starter") {
		t.Fatalf("text output = %q, want mapped mode labels", textOut)
	}

	jsonOut, err := captureStdoutE(t, func() error {
		return Region([]string{"list", "--manifest-url", manifest.URL, "--json"})
	})
	if err != nil {
		t.Fatalf("region list --json: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(jsonOut), &raw); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, jsonOut)
	}
	if _, ok := raw["schema"]; ok {
		t.Fatalf("json output unexpectedly contains schema: %#v", raw)
	}
	if raw["service"] != "drive9" {
		t.Fatalf("service = %#v, want drive9", raw["service"])
	}
	regions, ok := raw["regions"].([]any)
	if !ok || len(regions) != 2 {
		t.Fatalf("regions = %#v, want 2 entries", raw["regions"])
	}
}

func TestRegionModeLabel(t *testing.T) {
	cases := []struct {
		mode string
		want string
	}{
		{mode: RegionModeTiDBCloudStarter, want: "Anonymous"},
		{mode: RegionModeTiDBCloudNative, want: "TiDBCloud"},
		{mode: "custom", want: "custom"},
	}
	for _, tc := range cases {
		if got := regionModeLabel(tc.mode); got != tc.want {
			t.Fatalf("regionModeLabel(%q) = %q, want %q", tc.mode, got, tc.want)
		}
	}
}

func TestRegionListFallsBackWhenManifestUnavailable(t *testing.T) {
	textOut, err := captureStdoutE(t, func() error {
		return Region([]string{"list", "--manifest-url", "http://127.0.0.1:1/drive9-regions.json"})
	})
	if err != nil {
		t.Fatalf("region list fallback: %v", err)
	}
	for _, want := range []string{
		"REGION",
		"MODE",
		"SERVER",
		"aws-ap-southeast-1",
		"Anonymous",
		"https://api.drive9.ai",
	} {
		if !strings.Contains(textOut, want) {
			t.Fatalf("text output = %q, want substring %q", textOut, want)
		}
	}
}

func TestRegionListJSONFallsBackWhenManifestUnavailable(t *testing.T) {
	jsonOut, err := captureStdoutE(t, func() error {
		return Region([]string{"list", "--manifest-url", "http://127.0.0.1:1/drive9-regions.json", "--json"})
	})
	if err != nil {
		t.Fatalf("region list --json fallback: %v", err)
	}
	var manifest RegionManifest
	if err := json.Unmarshal([]byte(jsonOut), &manifest); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, jsonOut)
	}
	if manifest.Default == nil || manifest.Default.RegionCode != "aws-ap-southeast-1" || manifest.Default.Mode != RegionModeTiDBCloudStarter {
		t.Fatalf("fallback default = %#v", manifest.Default)
	}
	if len(manifest.Regions) != 1 {
		t.Fatalf("fallback regions len = %d, want 1", len(manifest.Regions))
	}
	entry := manifest.Regions[0]
	if entry.RegionCode != "aws-ap-southeast-1" || entry.Mode != RegionModeTiDBCloudStarter || entry.ServerURL != defaultServerURL {
		t.Fatalf("fallback region = %#v", entry)
	}
}

func TestValidateRegionManifestAllowsSameRegionDifferentModes(t *testing.T) {
	err := validateRegionManifest(&RegionManifest{
		Service: "drive9",
		Default: &RegionManifestDefault{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudStarter,
		},
		Regions: []RegionManifestEntry{
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudStarter, ServerURL: "https://starter.example"},
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudNative, ServerURL: "https://native.example"},
		},
	})
	if err != nil {
		t.Fatalf("validateRegionManifest: %v", err)
	}
}

func TestValidateRegionManifestRejectsMissingDefaultEntry(t *testing.T) {
	err := validateRegionManifest(&RegionManifest{
		Service: "drive9",
		Default: &RegionManifestDefault{
			RegionCode: "aws-us-east-1",
			Mode:       RegionModeTiDBCloudNative,
		},
		Regions: []RegionManifestEntry{
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudStarter, ServerURL: "https://starter.example"},
		},
	})
	if err == nil {
		t.Fatal("validateRegionManifest error = nil, want missing default entry error")
	}
	if !strings.Contains(err.Error(), "region manifest default") {
		t.Fatalf("validateRegionManifest error = %q", err)
	}
}

func TestValidateRegionManifestRejectsDuplicateRegionMode(t *testing.T) {
	err := validateRegionManifest(&RegionManifest{
		Service: "drive9",
		Regions: []RegionManifestEntry{
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudStarter, ServerURL: "https://starter-a.example"},
			{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudStarter, ServerURL: "https://starter-b.example"},
		},
	})
	if err == nil {
		t.Fatal("validateRegionManifest error = nil, want duplicate key error")
	}
	if !strings.Contains(err.Error(), "duplicate region_code") {
		t.Fatalf("validateRegionManifest error = %q", err)
	}
}

func TestSelectRegionServerMatchesRegionAndExactMode(t *testing.T) {
	entry, err := selectRegionServer([]RegionManifestEntry{
		{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudStarter, ServerURL: "https://starter.example"},
		{RegionCode: "aws-us-east-1", Mode: RegionModeTiDBCloudNative, ServerURL: "https://native.example"},
	}, "aws-us-east-1", RegionModeTiDBCloudStarter)
	if err != nil {
		t.Fatalf("selectRegionServer: %v", err)
	}
	if entry.ServerURL != "https://starter.example" {
		t.Fatalf("server = %q, want starter", entry.ServerURL)
	}
}

func TestSelectRegionServerDoesNotAcceptLegacyNativeMode(t *testing.T) {
	legacyNativeMode := strings.Replace(RegionModeTiDBCloudNative, "tidb_cloud", "tidbcloud", 1)
	_, err := selectRegionServer([]RegionManifestEntry{
		{RegionCode: "aws-us-east-1", Mode: legacyNativeMode, ServerURL: "https://legacy-native.example"},
	}, "aws-us-east-1", RegionModeTiDBCloudNative)
	if err == nil {
		t.Fatal("selectRegionServer error = nil, want unsupported mode error")
	}
	if !strings.Contains(err.Error(), `does not support mode "tidb_cloud_native"`) {
		t.Fatalf("selectRegionServer error = %q", err)
	}
}

func TestRegionListRejectsInvalidManifest(t *testing.T) {
	manifest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"service":"drive9","regions":[{"region_code":"aws-us-east-1","mode":"tidb_cloud_native"}]}`))
	}))
	defer manifest.Close()

	_, err := captureStdoutE(t, func() error {
		return Region([]string{"list", "--manifest-url", manifest.URL})
	})
	if err == nil {
		t.Fatal("region list error = nil, want invalid manifest error")
	}
	if !strings.Contains(err.Error(), "missing server_url") {
		t.Fatalf("region list error = %q", err)
	}
}
