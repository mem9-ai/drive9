package meta

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOrgObjectBackendCRUD(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	rec := &OrgObjectBackend{
		ID:             "obb_test1",
		OrganizationID: "org-1",
		Scheme:         "s3",
		Bucket:         "example",
		CredentialKind: ObjectCredentialStatic,
		AccessKeyID:    "AKIATEST",
		SecretCipher:   []byte("cipher"),
	}
	if err := s.InsertOrgObjectBackend(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOrgObjectBackend(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bucket != "example" || got.AccessKeyID != "AKIATEST" || len(got.SecretCipher) == 0 {
		t.Fatalf("got %+v", got)
	}
	byBucket, err := s.GetOrgObjectBackendByBucket(ctx, "org-1", "s3", "example")
	if err != nil || byBucket.ID != rec.ID {
		t.Fatalf("by bucket: %+v err=%v", byBucket, err)
	}
	list, err := s.ListOrgObjectBackends(ctx, "org-1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if err := s.InsertOrgObjectBackend(ctx, rec); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate err=%v", err)
	}
	if err := s.DeleteOrgObjectBackend(ctx, rec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetOrgObjectBackend(ctx, rec.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestSetTenantObjectNamespaceID(t *testing.T) {
	s := newControlStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	insertTiDBCloudBindingTenant(t, s, "ns-tenant", TenantKindLive, TenantActive, "cluster-ns", "", now)
	if err := s.UpsertTenantTiDBCloudOrgBinding(ctx, &TenantTiDBCloudOrgBinding{
		TenantID: "ns-tenant", OrganizationID: "org-1", ClusterID: "cluster-ns",
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTenantObjectNamespaceID(ctx, "ns-tenant", "customer-1"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTenantTiDBCloudOrgBinding(ctx, "ns-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if got.ObjectNamespaceID != "customer-1" {
		t.Fatalf("namespace=%q", got.ObjectNamespaceID)
	}
	if err := s.SetTenantObjectNamespaceID(ctx, "ns-tenant", ""); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetTenantTiDBCloudOrgBinding(ctx, "ns-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if got.ObjectNamespaceID != "" {
		t.Fatalf("cleared namespace=%q", got.ObjectNamespaceID)
	}
}

func TestValidateOrgObjectBackend(t *testing.T) {
	err := validateOrgObjectBackend(&OrgObjectBackend{
		ID: "obb_1", OrganizationID: "org", Scheme: "s3", Bucket: "b",
		CredentialKind: ObjectCredentialRole,
	})
	if err == nil || err.Error() != "role_arn is required for role credentials" {
		t.Fatalf("err=%v", err)
	}
	err = validateOrgObjectBackend(&OrgObjectBackend{
		ID: "obb_1", OrganizationID: "org", Scheme: "s3", Bucket: "b",
		CredentialKind: ObjectCredentialStatic,
	})
	if err == nil || err.Error() != "access_key_id is required for static credentials" {
		t.Fatalf("err=%v", err)
	}
}
