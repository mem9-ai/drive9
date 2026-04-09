package tidbcloud

import (
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"testing"
)

func TestParseZeroInstanceID(t *testing.T) {
	// Build a valid 24-byte payload: 8-byte big-endian cluster ID + 16-byte UUID.
	var payload [24]byte
	binary.BigEndian.PutUint64(payload[:8], 12345)
	// bytes [8:24] are random UUID (leave as zero for test).
	encoded := base64.RawURLEncoding.EncodeToString(payload[:])

	clusterID, err := ParseZeroInstanceID(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if clusterID != 12345 {
		t.Fatalf("got cluster ID %d, want 12345", clusterID)
	}
}

func TestParseZeroInstanceID_InvalidBase64(t *testing.T) {
	_, err := ParseZeroInstanceID("!!!not-base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}

func TestParseZeroInstanceID_WrongLength(t *testing.T) {
	// 16 bytes instead of 24.
	short := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	_, err := ParseZeroInstanceID(short)
	if err == nil {
		t.Fatal("expected error for wrong payload length")
	}
}

func TestParseHeaders_ClusterID(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set(HeaderClusterID, "99999")

	target, err := ParseHeaders(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target == nil {
		t.Fatal("expected non-nil target")
		return
	}
	if target.Type != TargetCluster {
		t.Fatalf("got type %s, want %s", target.Type, TargetCluster)
	}
	if target.ClusterID != "99999" {
		t.Fatalf("got cluster ID %s, want 99999", target.ClusterID)
	}
}

func TestParseHeaders_InstanceID(t *testing.T) {
	var payload [24]byte
	binary.BigEndian.PutUint64(payload[:8], 77777)
	encoded := base64.RawURLEncoding.EncodeToString(payload[:])

	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set(HeaderZeroInstanceID, encoded)

	target, err := ParseHeaders(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target == nil {
		t.Fatal("expected non-nil target")
		return
	}
	if target.Type != TargetZeroInstance {
		t.Fatalf("got type %s, want %s", target.Type, TargetZeroInstance)
	}
	if target.InstanceID != encoded {
		t.Fatalf("got instance ID %s, want %s", target.InstanceID, encoded)
	}
	if target.ClusterID != "77777" {
		t.Fatalf("got cluster ID %s, want 77777", target.ClusterID)
	}
}

func TestParseHeaders_ClusterIDTakesPrecedence(t *testing.T) {
	var payload [24]byte
	binary.BigEndian.PutUint64(payload[:8], 11111)
	encoded := base64.RawURLEncoding.EncodeToString(payload[:])

	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set(HeaderClusterID, "22222")
	r.Header.Set(HeaderZeroInstanceID, encoded)

	target, err := ParseHeaders(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target == nil {
		t.Fatal("expected non-nil target")
		return
	}
	if target.Type != TargetCluster {
		t.Fatalf("cluster ID should take precedence, got type %s", target.Type)
	}
	if target.ClusterID != "22222" {
		t.Fatalf("got cluster ID %s, want 22222", target.ClusterID)
	}
}

func TestParseHeaders_NoHeaders(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	target, err := ParseHeaders(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != nil {
		t.Fatalf("expected nil target, got %+v", target)
	}
}

func TestParseHeaders_InvalidInstanceID(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set(HeaderZeroInstanceID, "bad-instance-id!!!")
	_, err := ParseHeaders(r)
	if err == nil {
		t.Fatal("expected error for invalid instance ID")
	}
}

func TestHeaderForTarget(t *testing.T) {
	tests := []struct {
		typ  TargetType
		want string
	}{
		{TargetZeroInstance, HeaderZeroInstanceID},
		{TargetCluster, HeaderClusterID},
		{TargetType("unknown"), "X-TIDBCLOUD-*"},
	}
	for _, tt := range tests {
		got := HeaderForTarget(tt.typ)
		if got != tt.want {
			t.Errorf("HeaderForTarget(%s) = %s, want %s", tt.typ, got, tt.want)
		}
	}
}
