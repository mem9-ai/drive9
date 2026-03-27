package tenant

import "testing"

func TestNormalizeProvider(t *testing.T) {
	for _, p := range []string{ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudStarter} {
		got, err := NormalizeProvider(p)
		if err != nil {
			t.Fatalf("provider %s should be accepted: %v", p, err)
		}
		if got != p {
			t.Fatalf("expected %s got %s", p, got)
		}
	}
	if _, err := NormalizeProvider("bad-provider"); err == nil {
		t.Fatal("expected error for invalid provider")
	}
}

func TestSmallInDB(t *testing.T) {
	if !SmallInDB(ProviderTiDBZero) {
		t.Fatal("tidb_zero should store small files in db")
	}
	if !SmallInDB(ProviderTiDBCloudStarter) {
		t.Fatal("tidb_cloud_starter should store small files in db")
	}
	if SmallInDB(ProviderDB9) {
		t.Fatal("db9 should not store small files in db")
	}
}
