package staging

import (
	"os"
	"testing"
)

func TestFixedCacheSeedSurvivesReopenAndChangesWithImage(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put("/f", []byte("first"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}
	gen := cache.MetaGeneration("/f")
	seed, err := cache.FixedUploadSeed("/f", gen)
	if err != nil || seed == "" {
		t.Fatal(seed, err)
	}
	again, err := cache.FixedUploadSeed("/f", gen)
	if err != nil || again != seed {
		t.Fatal(again, err)
	}
	reopened, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	again, err = reopened.FixedUploadSeed("/f", gen)
	if err != nil || again != seed {
		t.Fatal(again, err)
	}
	if err := reopened.UpdateMode("/f", 0600); err != nil {
		t.Fatal(err)
	}
	changed := reopened.MetaGeneration("/f")
	next, err := reopened.FixedUploadSeed("/f", changed)
	if err != nil || next == seed {
		t.Fatal(next, err)
	}
	if _, err := reopened.FixedUploadSeed("/f", gen); err == nil {
		t.Fatal("accepted stale owner")
	}
	if data, ok := reopened.Get("/f"); !ok || string(data) != "first" {
		t.Fatal("seed changed payload")
	}
}

func TestFixedCacheSeedRefusesMissingOrCorruptMetadata(t *testing.T) {
	for _, kind := range []string{"missing", "corrupt", "mismatch", "invalid-seed"} {
		t.Run(kind, func(t *testing.T) {
			c, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put("/f", []byte("bytes"), 5, PendingNew); err != nil {
				t.Fatal(err)
			}
			gen := c.MetaGeneration("/f")
			var raw []byte
			switch kind {
			case "missing":
				if err := os.Remove(c.MetaFile("/f")); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				raw = []byte("{")
			case "mismatch":
				raw = []byte(`{"path":"/other","generation":1}`)
			case "invalid-seed":
				raw = []byte(`{"path":"/f","generation":1,"fixed_upload_seed_generation":1,"fixed_upload_seed":"bad/id"}`)
			}
			if raw != nil {
				if err := os.WriteFile(c.MetaFile("/f"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if seed, err := c.FixedUploadSeed("/f", gen); err == nil || seed != "" {
				t.Fatal(seed, err)
			}
			if c.MetaGeneration("/f") != gen {
				t.Fatal("failure changed image")
			}
		})
	}
}

func TestFixedCacheSeedRenewalPreservesImageAndReplays(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PutWithBaseRev("/f", []byte("bytes"), 5, PendingOverwrite, 4); err != nil {
		t.Fatal(err)
	}
	gen := c.MetaGeneration("/f")
	seed, err := c.FixedUploadSeed("/f", gen)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RenewFixedUploadSeedAfterAbort("/f", gen, seed); err != nil {
		t.Fatal(err)
	}
	next, err := c.FixedUploadSeed("/f", gen)
	if err != nil || next == seed {
		t.Fatal(next, err)
	}
	if err := c.RenewFixedUploadSeedAfterAbort("/f", gen, seed); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reopened.FixedUploadSeed("/f", gen)
	if err != nil || again != next {
		t.Fatal(again, err)
	}
	meta, data, ok := reopened.GetMetaAndView("/f")
	if !ok || string(data) != "bytes" || meta.BaseRev != 4 || meta.Generation != gen {
		t.Fatal("changed image")
	}
	if err := reopened.Put("/f", []byte("newer"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}
	newGen := reopened.MetaGeneration("/f")
	current, err := reopened.FixedUploadSeed("/f", newGen)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.RenewFixedUploadSeedAfterAbort("/f", gen, next); err != nil {
		t.Fatal(err)
	}
	if after, err := reopened.FixedUploadSeed("/f", newGen); err != nil || after != current {
		t.Fatal("renewed newer attempt", after, err)
	}
}

func TestFixedCacheSeedRenewalRejectsCorruptMetadata(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put("/f", []byte("bytes"), 5, PendingNew); err != nil {
		t.Fatal(err)
	}
	gen := c.MetaGeneration("/f")
	seed, err := c.FixedUploadSeed("/f", gen)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(c.MetaFile("/f"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.MetaFile("/f"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.RenewFixedUploadSeedAfterAbort("/f", gen, seed); err == nil {
		t.Fatal("advanced corrupt metadata")
	}
	if err := os.WriteFile(c.MetaFile("/f"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if after, err := c.FixedUploadSeed("/f", gen); err != nil || after != seed {
		t.Fatal(after, err)
	}
	if err := c.RenewFixedUploadSeedAfterAbort("/f", 0, seed); err == nil {
		t.Fatal("zero generation accepted")
	}
	if err := c.RenewFixedUploadSeedAfterAbort("/f", gen, "bad/id"); err == nil {
		t.Fatal("invalid attempt accepted")
	}
}
