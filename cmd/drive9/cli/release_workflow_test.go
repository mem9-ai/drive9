package cli

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
	for _, name := range []string{"drive9-*", "kubectl-drive9-migration-*", "version", "SHA256SUMS", "checksums.txt", "migration-kube-plugin-checksums.txt"} {
		if !strings.Contains(upload, "--include \""+name+"\"") {
			t.Errorf("S3 upload omits %s", name)
		}
	}
	for _, failure := range []string{"none", "checksum mismatch", "missing manifest"} {
		t.Run(failure, func(t *testing.T) {
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
				if failure == "checksum mismatch" && artifact.manifest == "checksums.txt" {
					payload = []byte("changed binary")
				}
				if err := os.WriteFile(filepath.Join(dist, artifact.binary), payload, 0600); err != nil {
					t.Fatal(err)
				}
				if failure != "missing manifest" || artifact.manifest != "checksums.txt" {
					if err := os.WriteFile(filepath.Join(dist, artifact.manifest), []byte(line), 0600); err != nil {
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
