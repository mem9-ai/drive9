package staging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

func fixedShadowFixture(t *testing.T, dir string) *ShadowStore {
	t.Helper()
	s, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.EnableFixedUploadRecovery(); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestFixedShadowIdentitySurvivesReopenAndCleanup(t *testing.T) {
	dir := t.TempDir()
	s := fixedShadowFixture(t, dir)
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	id, err := s.ProtectFixedShadow("/f", s.ActiveGeneration("/f"))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	fresh := fixedShadowFixture(t, dir)
	gen := fresh.EnsureActiveGeneration("/f")
	if again, err := fresh.ProtectFixedShadow("/f", gen); err != nil || again != id {
		t.Fatal(again, err)
	}
	for range 2 {
		if removed, err := fresh.RetireFixedShadow("/f", id); err != nil || !removed {
			t.Fatal(removed, err)
		}
	}
	if fresh.Has("/f") {
		t.Fatal("retained confirmed image")
	}
	if err := fresh.WriteFull("/f", []byte("newer"), 2); err != nil {
		t.Fatal(err)
	}
	if removed, err := fresh.RetireFixedShadow("/f", id); err != nil || removed {
		t.Fatal("removed new incarnation", removed, err)
	}
	if got, err := fresh.ReadAll("/f"); err != nil || string(got) != "newer" {
		t.Fatal(string(got), err)
	}
}
func TestFixedShadowMutationsInvalidateBeforeReopen(t *testing.T) {
	for _, kind := range []string{"ensure", "write-at", "full", "stream", "truncate", "extents", "rename", "remove"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			s := fixedShadowFixture(t, dir)
			if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
				t.Fatal(err)
			}
			id, err := s.ProtectFixedShadow("/f", s.ActiveGeneration("/f"))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "ensure":
				err = s.Ensure("/f", 6, 1)
			case "write-at":
				_, err = s.WriteAt("/f", 0, []byte("n"), 1)
			case "full":
				err = s.WriteFull("/f", []byte("newer"), 1)
			case "stream":
				_, err = s.WriteStream("/f", bytes.NewReader([]byte("newer")), 1)
			case "truncate":
				err = s.Truncate("/f", 4, 1)
			case "extents":
				buf := writebuffer.NewWriteBuffer("/f", 0, 4<<20)
				_, err = buf.Write(0, []byte("newer"))
				if err == nil {
					err = s.WriteExtents("/f", buf, 1)
				}
			case "rename":
				if !s.Rename("/f", "/other") {
					t.Fatal("rename failed")
				}
				err = s.WriteFull("/f", []byte("newer"), 1)
			case "remove":
				s.Remove("/f")
				err = s.WriteFull("/f", []byte("newer"), 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			expected, err := s.ReadAll("/f")
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			fresh := fixedShadowFixture(t, dir)
			if removed, err := fresh.RetireFixedShadow("/f", id); err != nil || removed {
				t.Fatal("removed newer bytes", removed, err)
			}
			if got, err := fresh.ReadAll("/f"); err != nil || !bytes.Equal(got, expected) {
				t.Fatal(string(got), err)
			}
		})
	}
}
func TestFixedShadowMarkerFailureRejectsWriteAndCleanup(t *testing.T) {
	dir := t.TempDir()
	s := fixedShadowFixture(t, dir)
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	gen := s.ActiveGeneration("/f")
	id, err := s.ProtectFixedShadow("/f", gen)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, HashPath("/f")+fixedShadowSuffix)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(marker, "block"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteAt("/f", 0, []byte("newer"), 1); err == nil {
		t.Fatal("wrote despite failed invalidation")
	}
	if removed, err := s.RetireFixedShadow("/f", id); err == nil || removed {
		t.Fatal(removed, err)
	}
	if got, err := s.ReadAll("/f"); err != nil || string(got) != "bytes" || s.ActiveGeneration("/f") != gen {
		t.Fatal(string(got), err)
	}
}
func TestFixedShadowCanceledAttemptRenewsIdentity(t *testing.T) {
	s := fixedShadowFixture(t, t.TempDir())
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	gen := s.ActiveGeneration("/f")
	id, err := s.ProtectFixedShadow("/f", gen)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseFixedShadowAttempt("/f", id); err != nil {
		t.Fatal(err)
	}
	newer, err := s.ProtectFixedShadow("/f", gen)
	if err != nil || newer == id {
		t.Fatal(newer, err)
	}
	if err := s.ReleaseFixedShadowAttempt("/f", id); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ProtectFixedShadow("/f", gen); err != nil || got != newer {
		t.Fatal(got, err)
	}
}

func TestFixedShadowCleanupProtectsPendingOwnerAndRetriesFailure(t *testing.T) {
	for _, scenario := range []string{"normal", "new-metadata", "new-shadow", "metadata-remove-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			s := fixedShadowFixture(t, filepath.Join(dir, "shadow"))
			idx, err := NewPendingIndex(filepath.Join(dir, "index"))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
				t.Fatal(err)
			}
			gen, err := idx.Put("/f", 5, PendingNew)
			if err != nil {
				t.Fatal(err)
			}
			token, err := s.ProtectFixedShadow("/f", s.ActiveGeneration("/f"))
			if err != nil {
				t.Fatal(err)
			}
			blocker := filepath.Join(idx.Dir(), HashPath("/f")+".meta")
			switch scenario {
			case "new-metadata":
				if _, err := idx.Put("/f", 5, PendingNew); err != nil {
					t.Fatal(err)
				}
			case "new-shadow":
				if err := s.WriteFull("/f", []byte("newer"), 1); err != nil {
					t.Fatal(err)
				}
			case "metadata-remove-failure":
				if err := os.Remove(blocker); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(blocker, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(blocker, "block"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			retired, err := idx.RetireFixedShadowUpload("/f", gen, s, token)
			switch scenario {
			case "normal":
				if err != nil || !retired || s.Has("/f") || idx.HasPending("/f") {
					t.Fatal(retired, err)
				}
			case "new-metadata":
				if err != nil || retired || !s.Has("/f") || !idx.HasPending("/f") {
					t.Fatal(retired, err)
				}
			case "new-shadow":
				if err == nil || retired || !s.Has("/f") || !idx.HasPending("/f") {
					t.Fatal(retired, err)
				}
			case "metadata-remove-failure":
				if err == nil || retired || s.Has("/f") || !idx.HasPending("/f") {
					t.Fatal(retired, err)
				}
				if err := os.RemoveAll(blocker); err != nil {
					t.Fatal(err)
				}
				if retired, err := idx.RetireFixedShadowUpload("/f", gen, s, token); err != nil || !retired || idx.HasPending("/f") {
					t.Fatal(retired, err)
				}
			}
		})
	}
}
func TestFixedShadowRejectedQuotaKeepsOriginalToken(t *testing.T) {
	s, err := NewShadowStoreWithQuota(t.TempDir(), 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	gen := s.ActiveGeneration("/f")
	token, err := s.ProtectFixedShadow("/f", gen)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFull("/f", []byte("longer"), 1); err == nil {
		t.Fatal("quota ignored")
	}
	if again, err := s.ProtectFixedShadow("/f", gen); err != nil || again != token {
		t.Fatal(again, err)
	}
	if retired, err := s.RetireFixedShadow("/f", token); err != nil || !retired {
		t.Fatal(retired, err)
	}
}

func TestFixedShadowRetireWithPinnedReader(t *testing.T) {
	s := fixedShadowFixture(t, t.TempDir())
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	token, err := s.ProtectFixedShadow("/f", s.ActiveGeneration("/f"))
	if err != nil {
		t.Fatal(err)
	}
	pin, ok := s.PinIfExists("/f")
	if !ok {
		t.Fatal("missing pin")
	}
	if removed, err := s.RetireFixedShadow("/f", token); err != nil || !removed {
		t.Fatal(removed, err)
	}
	if err := s.WriteFull("/f", []byte("newer"), 1); err != nil {
		t.Fatal(err)
	}
	s.Unpin(pin)
	if got, err := s.ReadAll("/f"); err != nil || string(got) != "newer" {
		t.Fatal(string(got), err)
	}
}
func TestFixedShadowMainDoesNotTouchMarkers(t *testing.T) {
	dir := t.TempDir()
	s, err := NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	marker := filepath.Join(dir, HashPath("/f")+fixedShadowSuffix)
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(marker, "block"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteFull("/f", []byte("bytes"), 1); err != nil {
		t.Fatal(err)
	}
	s.Remove("/f")
	if _, err := os.Stat(marker); err != nil || s.Has("/f") {
		t.Fatal(err)
	}
}
