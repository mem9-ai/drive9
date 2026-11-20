//go:build failpoint

package staging

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
)

func TestFixedShadowFailedMarkerSaveAndReleaseRetry(t *testing.T) {
	for _, phase := range []string{"fixedShadowBeforeSave", "fixedShadowAfterReleaseRemove"} {
		for _, reopen := range []bool{false, true} {
			t.Run(phase+map[bool]string{true: "/reopen", false: "/live"}[reopen], func(t *testing.T) {
				dir := t.TempDir()
				s := fixedShadowFixture(t, dir)
				if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
					t.Fatal(err)
				}
				gen := s.ActiveGeneration("/f")
				old := ""
				if phase == "fixedShadowAfterReleaseRemove" {
					var err error
					old, err = s.ProtectFixedShadow("/f", gen)
					if err != nil {
						t.Fatal(err)
					}
				}
				name := "github.com/mem9-ai/drive9/pkg/fusecomp/staging/" + phase
				if err := failpoint.Enable(name, "return(true)"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = failpoint.Disable(name) })
				if phase == "fixedShadowBeforeSave" {
					if _, err := s.ProtectFixedShadow("/f", gen); err == nil {
						t.Fatal("save failure not injected")
					}
				} else {
					if err := s.ReleaseFixedShadowAttempt("/f", old); err == nil {
						t.Fatal("release failure not injected")
					}
				}
				if err := failpoint.Disable(name); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(dir, HashPath("/f")+fixedShadowSuffix)); !os.IsNotExist(err) {
					t.Fatal("marker still present", err)
				}
				if reopen {
					s.Close()
					s = fixedShadowFixture(t, dir)
					gen = s.EnsureActiveGeneration("/f")
				}
				if old != "" {
					if err := s.ReleaseFixedShadowAttempt("/f", old); err != nil {
						t.Fatal(err)
					}
				}
				current, err := s.ProtectFixedShadow("/f", gen)
				if err != nil || current == old {
					t.Fatal(current, err)
				}
				if old != "" {
					if err := s.ReleaseFixedShadowAttempt("/f", old); err != nil {
						t.Fatal(err)
					}
				}
				if got, err := s.ReadAll("/f"); err != nil || string(got) != "bytes" {
					t.Fatal(string(got), err)
				}
				s.Close()
				fresh := fixedShadowFixture(t, dir)
				if got, err := fresh.ProtectFixedShadow("/f", fresh.EnsureActiveGeneration("/f")); err != nil || got != current {
					t.Fatal(got, err)
				}
			})
		}
	}
}
