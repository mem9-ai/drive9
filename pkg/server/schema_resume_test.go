package server

import (
	"context"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestStartTenantSchemaInitResumeUsesLegacyProvisioner(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant-legacy-resume"
	db, cipher := newLegacySchemaResumeTenant(t, tenantID)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudStarterLegacy}
	srv := NewWithConfig(Config{
		Meta:                     db.Meta,
		Pool:                     db.Pool,
		LegacyStarterProvisioner: prov,
	})
	t.Cleanup(srv.Close)

	srv.startTenantSchemaInitResume(ctx, meta.Tenant{
		ID:               tenantID,
		Provider:         tenant.ProviderTiDBCloudStarterLegacy,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: cipher,
		DBName:           db.DBName,
		DBTLS:            false,
	})
	srv.forkWorkerWG.Wait()
	tenantMeta, err := db.Meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if tenantMeta.Status != meta.TenantActive {
		t.Fatalf("tenant status = %s, want %s", tenantMeta.Status, meta.TenantActive)
	}
}

func TestStartTenantSchemaInitResumeSkipsMissingLegacyProvisioner(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant-legacy-resume-missing-provisioner"
	db, cipher := newLegacySchemaResumeTenant(t, tenantID)
	srv := NewWithConfig(Config{
		Meta: db.Meta,
		Pool: db.Pool,
	})
	t.Cleanup(srv.Close)

	srv.startTenantSchemaInitResume(ctx, meta.Tenant{
		ID:               tenantID,
		Provider:         tenant.ProviderTiDBCloudStarterLegacy,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: cipher,
		DBName:           db.DBName,
		DBTLS:            false,
	})
	srv.forkWorkerWG.Wait()
	tenantMeta, err := db.Meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if tenantMeta.Status != meta.TenantProvisioning {
		t.Fatalf("tenant status = %s, want %s", tenantMeta.Status, meta.TenantProvisioning)
	}
}

func newLegacySchemaResumeTenant(t *testing.T, tenantID string) (*testDBInfo, []byte) {
	t.Helper()
	db := newTenantDeleteDBInfo(t)
	ctx := context.Background()
	cipher, err := db.Pool.Encrypt(ctx, []byte(db.DBPass))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.Meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantProvisioning,
		Provider:         tenant.ProviderTiDBCloudStarterLegacy,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: cipher,
		DBName:           db.DBName,
		DBTLS:            false,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	return db, cipher
}
