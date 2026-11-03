package fuse

import (
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestParseOwner(t *testing.T) {
	for raw, want := range map[string]Owner{"1000:1000": {1000, 1000}, "1000": {1000, 1000}, "0:5": {0, 5}} {
		got, err := ParseOwner(raw)
		if err != nil || *got != want {
			t.Errorf("ParseOwner(%q) = %v, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"", "agent", "1000:x", "-1:1", "1:2:3"} {
		if _, err := ParseOwner(raw); err == nil {
			t.Errorf("ParseOwner(%q) accepted", raw)
		}
	}
}

// The mount owner is the reply's fallback; an inode's stored owner (a
// mount-local chown) wins — v1 DefaultOwner's "files without a stored
// owner" rule.
func TestDefaultOwnerAppliesOnlyToFilesWithoutStoredOwner(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)
	f.uid, f.gid = 1000, 1001

	var out fuse.Attr
	f.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: 0o644}, 1, &out)
	if out.Uid != 1000 || out.Gid != 1001 {
		t.Fatalf("default owner = %d:%d", out.Uid, out.Gid)
	}
	f.inodes.UpdateOwner(1, 501, 20, true, true)
	f.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: 0o644}, 1, &out)
	if out.Uid != 501 || out.Gid != 20 {
		t.Fatalf("stored owner = %d:%d", out.Uid, out.Gid)
	}
}

// The public options carry the sandbox owner and the credential policy
// into the internal config.
func TestMountConfigCarriesOwnerAndCredentialPolicy(t *testing.T) {
	owner := &Owner{UID: 1000, GID: 1001}
	cfg := mountConfigFromOptions(&MountOptions{
		Server: "https://drive9.example", DefaultOwner: owner, OmitProcessStateCredentials: true,
	})
	if cfg.DefaultOwner == nil || cfg.DefaultOwner.UID != 1000 || cfg.DefaultOwner.GID != 1001 {
		t.Fatalf("DefaultOwner = %+v", cfg.DefaultOwner)
	}
	if !cfg.OmitProcessStateCredentials {
		t.Fatal("OmitProcessStateCredentials not carried into the config")
	}
	state := newMountProcessState(&MountOptions{
		Server: "https://drive9.example", OmitProcessStateCredentials: true,
	}, "/mnt", 1, "api_key", "", time.Unix(1, 0).UTC())
	if state.Server != "" {
		t.Fatalf("process state server = %q, want empty", state.Server)
	}
}
