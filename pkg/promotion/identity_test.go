package promotion

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMigrationIDRoundTripAndAliases(t *testing.T) {
	want := MigrationIdentity{AllocationEpoch: 42, AllocationSequence: 99}
	id, err := EncodeMigrationID(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMigrationID(id)
	if err != nil {
		t.Fatalf("DecodeMigrationID: %v", err)
	}
	if got != want {
		t.Fatalf("identity = %+v, want %+v", got, want)
	}
	for _, invalid := range []string{
		strings.ToUpper(id),
		id + "=",
		"p2_" + strings.TrimPrefix(id, "p1_"),
		"p1_a_aaaaaaaaaaaaa",
	} {
		if _, err := DecodeMigrationID(invalid); !errors.Is(err, ErrInvalidMigrationID) {
			t.Fatalf("DecodeMigrationID(%q) error = %v, want ErrInvalidMigrationID", invalid, err)
		}
	}
}

func TestAllocationProofBindsTenantTargetAndIdentity(t *testing.T) {
	signer, err := NewProofSigner([]byte("abcdef0123456789abcdef0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := EncodeMigrationID(MigrationIdentity{AllocationEpoch: 7, AllocationSequence: 11})
	if err != nil {
		t.Fatal(err)
	}
	claims := AllocationProofClaims{
		Version:                      "p1",
		TenantID:                     "tenant-a",
		MigrationID:                  id,
		AllocationEpoch:              7,
		AllocationSequence:           11,
		Target:                       "/published/tree",
		ExpectedTargetAbsent:         true,
		AllocationIdempotencyKeyHash: checksum("allocate-key"),
		AllocationRequestDigest:      checksum("request"),
		CreateBeforeUnixMilli:        time.Date(2026, 9, 21, 13, 0, 0, 0, time.UTC).UnixMilli(),
	}
	proof, err := signer.Sign(claims)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	got, err := signer.Verify(proof)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != claims {
		t.Fatalf("claims = %+v, want %+v", got, claims)
	}

	parts := strings.Split(proof, ".")
	parts[2] = strings.Repeat("A", len(parts[2]))
	if _, err := signer.Verify(strings.Join(parts, ".")); !errors.Is(err, ErrInvalidAllocationProof) {
		t.Fatalf("tampered proof error = %v, want ErrInvalidAllocationProof", err)
	}
}

func TestAllocationProofKeyRotationRetainsOldVerification(t *testing.T) {
	oldKey := []byte("old-proof-key-0123456789abcdef012345")
	newKey := []byte("new-proof-key-0123456789abcdef012345")
	oldSigner, err := NewProofSignerWithKeyring("old", oldKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := EncodeMigrationID(MigrationIdentity{AllocationEpoch: 7, AllocationSequence: 11})
	if err != nil {
		t.Fatal(err)
	}
	claims := AllocationProofClaims{
		Version: "p1", TenantID: "tenant-a", MigrationID: id,
		AllocationEpoch: 7, AllocationSequence: 11, Target: "/published/tree",
		ExpectedTargetAbsent: true, AllocationIdempotencyKeyHash: checksum("allocate-key"),
		AllocationRequestDigest: checksum("request"), CreateBeforeUnixMilli: time.Now().Add(time.Hour).UnixMilli(),
	}
	proof, err := oldSigner.Sign(claims)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := NewProofSignerWithKeyring("new", newKey, map[string][]byte{"old": oldKey})
	if err != nil {
		t.Fatal(err)
	}
	got, err := rotated.Verify(proof)
	if err != nil {
		t.Fatalf("Verify old proof after rotation: %v", err)
	}
	if got != claims {
		t.Fatalf("claims = %+v, want %+v", got, claims)
	}
	newProof, err := rotated.Sign(claims)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(proof, "old.") || !strings.HasPrefix(newProof, "new.") {
		t.Fatalf("proof key versions missing: old=%q new=%q", proof, newProof)
	}
	if _, err := oldSigner.Verify(newProof); !errors.Is(err, ErrInvalidAllocationProof) {
		t.Fatalf("old keyring verified new proof: %v", err)
	}
	parts := strings.Split(proof, ".")
	parts[0] = "new"
	if _, err := rotated.Verify(strings.Join(parts, ".")); !errors.Is(err, ErrInvalidAllocationProof) {
		t.Fatalf("renamed proof key version error = %v, want ErrInvalidAllocationProof", err)
	}
}

func TestAllocationProofRejectsMismatchedIDTuple(t *testing.T) {
	signer, err := NewProofSigner([]byte("abcdef0123456789abcdef0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := EncodeMigrationID(MigrationIdentity{AllocationEpoch: 7, AllocationSequence: 11})
	if err != nil {
		t.Fatal(err)
	}
	claims := AllocationProofClaims{
		Version:                      "p1",
		TenantID:                     "tenant-a",
		MigrationID:                  id,
		AllocationEpoch:              7,
		AllocationSequence:           12,
		Target:                       "/published/tree",
		ExpectedTargetAbsent:         true,
		AllocationIdempotencyKeyHash: checksum("allocate-key"),
		AllocationRequestDigest:      checksum("request"),
		CreateBeforeUnixMilli:        time.Now().Add(time.Hour).UnixMilli(),
	}
	if _, err := signer.Sign(claims); !errors.Is(err, ErrInvalidAllocationProof) {
		t.Fatalf("mismatched tuple error = %v, want ErrInvalidAllocationProof", err)
	}
}
