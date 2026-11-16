package cli

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/releasesign"
)

func TestUpdateThroughLegacyReleaseRedirect(t *testing.T) {
	for _, platform := range []string{"linux", "windows"} {
		for _, failure := range []string{"none", "missing checksums", "missing signature", "forged signature", "checksum mismatch", "missing binary", "hash versions require force", "forced hash update"} {
			t.Run(platform+"/"+failure, func(t *testing.T) {
				t.Setenv("HOME", t.TempDir())
				artifact := updateArtifactName(platform, "amd64")
				payload := []byte("new executable fixture")
				sum := fmt.Sprintf("%x", sha256.Sum256(payload))
				target := filepath.Join(t.TempDir(), artifact)
				if err := os.WriteFile(target, []byte("old executable"), 0700); err != nil {
					t.Fatal(err)
				}
				// The updater now reads the signed manifest for both the version and the
				// checksums, so the legacy origin serves the same shapes the release job
				// publishes: checksums.txt with a version line, and its detached signature.
				version := "v0.2.0"
				if strings.Contains(failure, "hash") {
					version = "ab9c86b"
				}
				checksum := sum
				if failure == "checksum mismatch" {
					checksum = strings.Repeat("0", 64)
				}
				manifest := updateTestManifest(version, artifact, checksum)
				signature, signErr := releasesign.Sign(manifest, updateTestSigningPrivateKey)
				if signErr != nil {
					t.Fatalf("sign manifest: %v", signErr)
				}
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/latest/version":
						_, _ = fmt.Fprintln(w, version)
					case "/latest/checksums.txt":
						if failure == "missing checksums" {
							http.Error(w, "missing", http.StatusForbidden)
							return
						}
						_, _ = w.Write(manifest)
					case "/latest/" + releasesign.SignatureName:
						if failure == "missing signature" {
							http.NotFound(w, r)
							return
						}
						if failure == "forged signature" {
							forged, _ := releasesign.Sign([]byte("another manifest"), updateTestSigningPrivateKey)
							_, _ = w.Write(forged)
							return
						}
						_, _ = w.Write(signature)
					case "/latest/" + artifact:
						if failure == "missing binary" {
							http.NotFound(w, r)
							return
						}
						_, _ = w.Write(payload)
					default:
						http.NotFound(w, r)
					}
				}))
				defer origin.Close()
				legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !strings.HasPrefix(r.URL.Path, "/releases/") {
						http.NotFound(w, r)
						return
					}
					http.Redirect(w, r, origin.URL+"/latest/"+strings.TrimPrefix(r.URL.Path, "/releases/"), http.StatusFound)
				}))
				defer legacy.Close()
				// Exercise the unchanged updater with no CLI arguments; inject
				// only local origins and a scratch executable, never replace self.
				var args []string
				currentVersion := "v0.1.0"
				if strings.Contains(failure, "hash") {
					currentVersion = "42a316a"
				}
				if failure == "forced hash update" {
					args = []string{"--force"}
				}
				var stdout bytes.Buffer
				err := updateWithDeps(args, updateDeps{
					baseURL: legacy.URL, currentVersion: currentVersion, goos: platform, goarch: "amd64",
					httpClient: legacy.Client(), allowInsecureBaseURL: true,
					executable: func() (string, error) { return target, nil },
					stdout:     &stdout, replaceExecutable: os.Rename,
				})
				wantError := failure == "missing checksums" || failure == "missing signature" || failure == "forged signature" || failure == "checksum mismatch" || failure == "missing binary"
				if (err != nil) != wantError {
					t.Fatalf("update through redirect: %v", err)
				}
				want := payload
				if wantError || failure == "hash versions require force" {
					want = []byte("old executable")
				}
				got, err := os.ReadFile(target)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("installed executable=%q err=%v", got, err)
				}
			})
		}
	}
}
