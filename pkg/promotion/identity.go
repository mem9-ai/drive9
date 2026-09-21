package promotion

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	migrationIDVersion = "p1"
	encodedUint64Len   = 13
	defaultProofKeyID  = "k1"
)

var migrationBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// MigrationIdentity is the tenant-scoped durable identity encoded in a public
// migration ID. Tenant identity comes from authenticated request context.
type MigrationIdentity struct {
	AllocationEpoch    uint64
	AllocationSequence uint64
}

// EncodeMigrationID returns the only accepted p1 representation.
func EncodeMigrationID(identity MigrationIdentity) (string, error) {
	if identity.AllocationEpoch == 0 || identity.AllocationSequence == 0 {
		return "", fmt.Errorf("%w: epoch and sequence must be positive", ErrInvalidMigrationID)
	}
	return migrationIDVersion + "_" + encodeUint64(identity.AllocationEpoch) + "_" + encodeUint64(identity.AllocationSequence), nil
}

// DecodeMigrationID strictly rejects aliases, padding, case changes, unknown
// versions, zero values, and malformed/overflowed components.
func DecodeMigrationID(value string) (MigrationIdentity, error) {
	parts := strings.Split(value, "_")
	if len(parts) != 3 || parts[0] != migrationIDVersion || len(parts[1]) != encodedUint64Len || len(parts[2]) != encodedUint64Len {
		return MigrationIdentity{}, ErrInvalidMigrationID
	}
	epoch, err := decodeUint64(parts[1])
	if err != nil {
		return MigrationIdentity{}, ErrInvalidMigrationID
	}
	sequence, err := decodeUint64(parts[2])
	if err != nil || epoch == 0 || sequence == 0 {
		return MigrationIdentity{}, ErrInvalidMigrationID
	}
	identity := MigrationIdentity{AllocationEpoch: epoch, AllocationSequence: sequence}
	canonical, _ := EncodeMigrationID(identity)
	if subtle.ConstantTimeCompare([]byte(value), []byte(canonical)) != 1 {
		return MigrationIdentity{}, ErrInvalidMigrationID
	}
	return identity, nil
}

// AllocationProofClaims are the immutable claims authenticated before any
// claim/import lookup or mutation.
type AllocationProofClaims struct {
	Version                      string `json:"version"`
	TenantID                     string `json:"tenant_id"`
	MigrationID                  string `json:"migration_id"`
	AllocationEpoch              uint64 `json:"allocation_epoch"`
	AllocationSequence           uint64 `json:"allocation_sequence"`
	Target                       string `json:"target"`
	ExpectedTargetAbsent         bool   `json:"expected_target_absent"`
	AllocationIdempotencyKeyHash string `json:"allocation_idempotency_key_hash"`
	AllocationRequestDigest      string `json:"allocation_request_digest"`
	CreateBeforeUnixMilli        int64  `json:"create_before_unix_milli"`
}

// ProofSigner signs bounded allocation proofs with one active key and verifies
// them with a versioned keyring. Verification keys must remain installed for
// the documented allocation/terminal recovery window so key rotation does not
// invalidate an accepted proof or change exact retry bytes.
type ProofSigner struct {
	signingKeyID string
	signingKey   []byte
	verifyKeys   map[string][]byte
}

func NewProofSigner(secret []byte) (*ProofSigner, error) {
	return NewProofSignerWithKeyring(defaultProofKeyID, secret, nil)

}

// NewProofSignerWithKeyring constructs a signer with an explicit active key
// version and retained verification-only keys. The active key is always added
// to the verification keyring; a conflicting duplicate is rejected.
func NewProofSignerWithKeyring(signingKeyID string, signingKey []byte, verificationKeys map[string][]byte) (*ProofSigner, error) {
	if !validProofKeyID(signingKeyID) || len(signingKey) < sha256.Size {
		return nil, fmt.Errorf("%w: proof signing key requires a canonical id and at least %d bytes", ErrInvalidAllocationProof, sha256.Size)
	}
	keys := make(map[string][]byte, len(verificationKeys)+1)
	for keyID, key := range verificationKeys {
		if !validProofKeyID(keyID) || len(key) < sha256.Size {
			return nil, fmt.Errorf("%w: invalid verification key %q", ErrInvalidAllocationProof, keyID)
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	if existing, ok := keys[signingKeyID]; ok && !bytes.Equal(existing, signingKey) {
		return nil, fmt.Errorf("%w: signing key %q conflicts with verification key", ErrInvalidAllocationProof, signingKeyID)
	}
	keys[signingKeyID] = append([]byte(nil), signingKey...)
	return &ProofSigner{
		signingKeyID: signingKeyID,
		signingKey:   append([]byte(nil), signingKey...),
		verifyKeys:   keys,
	}, nil
}

// Sign authenticates claims after enforcing the canonical ID and target.
func (s *ProofSigner) Sign(claims AllocationProofClaims) (string, error) {
	if err := validateProofClaims(claims); err != nil {
		return "", err
	}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal allocation proof: %w", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	signature := proofMAC(s.signingKey, s.signingKeyID, raw)
	return s.signingKeyID + "." + payload + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// Verify validates the MAC and every self-consistency invariant. It does not
// apply create_before; accepted-row recovery must be allowed after expiry.
func (s *ProofSigner) Verify(proof string) (AllocationProofClaims, error) {
	parts := strings.Split(proof, ".")
	if len(parts) != 3 || !validProofKeyID(parts[0]) {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	key, ok := s.verifyKeys[parts[0]]
	if !ok {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	if subtle.ConstantTimeCompare(signature, proofMAC(key, parts[0], raw)) != 1 {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	var claims AllocationProofClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	if err := validateProofClaims(claims); err != nil {
		return AllocationProofClaims{}, err
	}
	canonical, err := json.Marshal(claims)
	if err != nil || subtle.ConstantTimeCompare(raw, canonical) != 1 {
		return AllocationProofClaims{}, ErrInvalidAllocationProof
	}
	return claims, nil
}

func proofMAC(key []byte, keyID string, raw []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(keyID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(raw)
	return mac.Sum(nil)
}

func validProofKeyID(value string) bool {
	if value == "" || len(value) > 32 {
		return false
	}
	for _, ch := range value {
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' {
			return false
		}
	}
	return true
}

// CreateBefore returns the proof deadline as a UTC time.
func (c AllocationProofClaims) CreateBefore() time.Time {
	return time.UnixMilli(c.CreateBeforeUnixMilli).UTC()
}

func validateProofClaims(claims AllocationProofClaims) error {
	if claims.Version != migrationIDVersion || claims.TenantID == "" || !claims.ExpectedTargetAbsent ||
		claims.AllocationIdempotencyKeyHash == "" || claims.AllocationRequestDigest == "" || claims.CreateBeforeUnixMilli <= 0 {
		return ErrInvalidAllocationProof
	}
	identity, err := DecodeMigrationID(claims.MigrationID)
	if err != nil || identity.AllocationEpoch != claims.AllocationEpoch || identity.AllocationSequence != claims.AllocationSequence {
		return ErrInvalidAllocationProof
	}
	target, err := canonicalTarget(claims.Target)
	if err != nil || target != claims.Target {
		return ErrInvalidAllocationProof
	}
	return nil
}

func encodeUint64(value uint64) string {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	return strings.ToLower(migrationBase32.EncodeToString(raw[:]))
}

func decodeUint64(value string) (uint64, error) {
	if strings.ToLower(value) != value || strings.Contains(value, "=") {
		return 0, ErrInvalidMigrationID
	}
	raw, err := migrationBase32.DecodeString(strings.ToUpper(value))
	if err != nil || len(raw) != 8 {
		return 0, ErrInvalidMigrationID
	}
	decoded := binary.BigEndian.Uint64(raw)
	if decoded == math.MaxUint64 {
		// MaxUint64 cannot be incremented by the allocation transaction and is
		// therefore never a valid issued epoch or sequence.
		return 0, ErrInvalidMigrationID
	}
	return decoded, nil
}
