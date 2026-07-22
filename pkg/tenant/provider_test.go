package tenant

import "testing"

func TestNormalizeProvider(t *testing.T) {
	for _, p := range []string{ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudNative} {
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
	if _, err := NormalizeProvider(ProviderTiDBCloudStarterLegacy); err == nil {
		t.Fatal("legacy starter provider should not be accepted for new provisioning")
	}
}

func TestSmallInDB(t *testing.T) {
	for _, provider := range []string{ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudStarterLegacy} {
		if !SmallInDB(provider) {
			t.Fatalf("%s should store small files in db", provider)
		}
	}
	if SmallInDB(ProviderDB9) {
		t.Fatal("db9 should not store small files in db")
	}
}

func TestUsesTiDBAutoEmbedding(t *testing.T) {
	for _, provider := range []string{ProviderTiDBZero, ProviderTiDBCloudNative, ProviderTiDBCloudStarterLegacy} {
		if !UsesTiDBAutoEmbedding(provider) {
			t.Fatalf("provider %s should use TiDB auto-embedding mode", provider)
		}
	}
	if UsesTiDBAutoEmbedding(ProviderDB9) {
		t.Fatal("db9 should remain on app-managed embedding")
	}
}

func TestSupportsClusterDelete(t *testing.T) {
	for _, provider := range []string{ProviderTiDBCloudNative, ProviderTiDBCloudStarterLegacy} {
		if !SupportsClusterDelete(provider) {
			t.Fatalf("%s should support cluster delete", provider)
		}
	}
	for _, provider := range []string{ProviderDB9, ProviderTiDBZero} {
		if SupportsClusterDelete(provider) {
			t.Fatalf("%s should not support cluster delete", provider)
		}
	}
}

func TestUsesTiDBCloudNativeCredentials(t *testing.T) {
	for _, provider := range []string{ProviderTiDBCloudNative, ProviderTiDBCloudNativeShared} {
		if !UsesTiDBCloudNativeCredentials(provider) {
			t.Fatalf("%s should use the TiDB Cloud native credential family", provider)
		}
	}
	for _, provider := range []string{ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudStarterLegacy} {
		if UsesTiDBCloudNativeCredentials(provider) {
			t.Fatalf("%s should not use the TiDB Cloud native credential family", provider)
		}
	}
}
