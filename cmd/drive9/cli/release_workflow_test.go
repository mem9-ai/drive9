package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

func TestCLIReleasePublishesLegacyManifests(t *testing.T) {
	data, err := os.ReadFile("../../../.github/workflows/release-cli.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string }
		}
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	var generate, upload string
	for _, step := range workflow.Jobs["release"].Steps {
		if strings.Contains(step.Run, "drive9-fe") {
			t.Fatal("release still depends on committing binaries to the website repository")
		}
		switch step.Name {
		case "Generate SHA256SUMS":
			generate = step.Run
		case "Upload CLI binaries to S3 (latest)":
			upload = step.Run
		}
	}
	if generate == "" || upload == "" {
		t.Fatal("missing release generation/upload step")
	}
	for _, name := range []string{"drive9-*", "kubectl-drive9-migration-*"} {
		if !strings.Contains(upload, "--include \""+name+"\"") {
			t.Errorf("S3 binary upload omits %s", name)
		}
	}
	// The self-updater refuses a manifest it cannot authenticate, so a release that skips
	// signing is a release nobody can update to.
	var signStep, manifestUpload string
	for _, step := range workflow.Jobs["release"].Steps {
		switch step.Name {
		case "Sign release manifest":
			signStep = step.Run
		case "Publish release manifests to S3 (latest, last)":
			manifestUpload = step.Run
		}
	}
	if signStep == "" {
		t.Fatal("release workflow has no manifest signing step")
	}
	for _, want := range []string{"drive9-release-sign", "DRIVE9_RELEASE_SIGNING_KEY", "exit 1"} {
		if !strings.Contains(signStep, want) {
			t.Errorf("signing step omits %q", want)
		}
	}
	// Signing is not enough: the signature must verify against the key the release binary
	// embeds, read from the same file the build embeds.
	for _, want := range []string{"-verify", "cmd/drive9/cli/update_key.txt", "dist/release.txt"} {
		if !strings.Contains(signStep, want) {
			t.Errorf("signing step does not sign/verify the full release pair: missing %q", want)
		}
	}
	// Binaries first, manifests last: the updater reads the manifest pair in two requests and
	// both sort ahead of the artifacts.
	if manifestUpload == "" {
		t.Fatal("release workflow has no separate manifest upload step")
	}
	if strings.Contains(upload, `--include "checksums.txt"`) || strings.Contains(upload, `--include "SHA256SUMS"`) {
		t.Error("binary upload still publishes the manifest, so the manifest can appear before its artifacts")
	}
	for _, name := range []string{"checksums.txt", "checksums.txt.sig", "release.txt", "release.txt.sig", "SHA256SUMS", "version"} {
		if !strings.Contains(manifestUpload, `--include "`+name+`"`) {
			t.Errorf("manifest upload omits %s", name)
		}
	}
	if !strings.Contains(manifestUpload, "max-age=60") {
		t.Error("manifest upload should use a short cache TTL so a rollback propagates")
	}
	for _, failure := range []string{"none", "checksum mismatch", "missing manifest"} {
		t.Run(failure, func(t *testing.T) {
			// The workflow runs on Ubuntu; keep its structural checks above
			// available on developer machines without these Unix tools.
			for _, tool := range []string{"bash", "sha256sum"} {
				if _, err := exec.LookPath(tool); err != nil {
					t.Skipf("release shell validation requires %s: %v", tool, err)
				}
			}
			root := t.TempDir()
			dist := filepath.Join(root, "dist")
			if err := os.Mkdir(dist, 0700); err != nil {
				t.Fatal(err)
			}
			var all strings.Builder
			for _, artifact := range []struct{ binary, manifest string }{
				{"drive9-linux-amd64", "checksums.txt"},
				{"kubectl-drive9-migration-windows-arm64.exe", "migration-kube-plugin-checksums.txt"},
			} {
				payload := []byte(artifact.binary)
				sum := sha256.Sum256(payload)
				line := fmt.Sprintf("%x  %s\n", sum, artifact.binary)
				all.WriteString(line)
				manifestBody := line
				if failure == "checksum mismatch" && artifact.manifest == "checksums.txt" {
					payload = []byte("changed binary")
				}
				if err := os.WriteFile(filepath.Join(dist, artifact.binary), payload, 0600); err != nil {
					t.Fatal(err)
				}
				if failure != "missing manifest" || artifact.manifest != "checksums.txt" {
					if err := os.WriteFile(filepath.Join(dist, artifact.manifest), []byte(manifestBody), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			command := exec.Command("bash", "-e", "-c", generate)
			command.Dir = root
			output, err := command.CombinedOutput()
			if (err != nil) != (failure != "none") {
				t.Fatalf("manifest validation: %v\n%s", err, output)
			}
			if failure == "none" {
				generated, err := os.ReadFile(filepath.Join(dist, "SHA256SUMS"))
				if err != nil || string(generated) != all.String() {
					t.Fatalf("combined manifest: %q, %v", generated, err)
				}
			}
		})
	}
}

// The signing step verifies the manifest against the key file the build embeds. It has to
// hand that file over untouched: if it normalizes separators first, a key file the shipped
// binary cannot parse still passes the check, and the release publishes with a CLI that can
// never update again. The parser accepts commas or whitespace, so no normalization is needed.
func TestReleaseWorkflowVerifiesTheEmbeddedKeyFileVerbatim(t *testing.T) {
	data, err := os.ReadFile("../../../.github/workflows/release-cli.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string }
		}
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	var signStep string
	for _, step := range workflow.Jobs["release"].Steps {
		if step.Name == "Sign release manifest" {
			signStep = step.Run
		}
	}
	if signStep == "" {
		t.Fatal("release workflow has no manifest signing step")
	}
	for _, forbidden := range []string{"tr -s", "sed 's/^,/", "paste -sd"} {
		if strings.Contains(signStep, forbidden) {
			t.Errorf("signing step rewrites the key file separators (%q), so it verifies a key set the shipped binary never parses", forbidden)
		}
	}
	if !strings.Contains(signStep, "cat cmd/drive9/cli/update_key.txt") {
		t.Error("signing step does not read the embedded key file verbatim")
	}

	// One key per line and comma-separated are the same key set, which is what lets the file
	// be passed through as-is.
	rotating := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x37}, ed25519.SeedSize))
	secondPublic := base64.StdEncoding.EncodeToString(rotating.Public().(ed25519.PublicKey))
	for _, fileBody := range []string{
		updateTestSigningPublicKey + "\n" + secondPublic + "\n",
		updateTestSigningPublicKey + "," + secondPublic + "\n",
	} {
		keys, err := releasesign.ParsePublicKeys(strings.TrimSuffix(fileBody, "\n"))
		if err != nil {
			t.Fatalf("key file %q: %v", fileBody, err)
		}
		if len(keys) != 2 {
			t.Fatalf("key file %q parsed %d keys, want 2", fileBody, len(keys))
		}
	}
}
