// Package tidbcloud implements TiDB Cloud control-plane integration for the
// tidbcloud-native provider, including header parsing and type definitions.
package tidbcloud

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	// HeaderZeroInstanceID is the request header for zero-instance routing.
	HeaderZeroInstanceID = "X-TIDBCLOUD-ZERO-INSTANCE-ID"
	// HeaderClusterID is the request header for cluster-id routing.
	HeaderClusterID = "X-TIDBCLOUD-CLUSTER-ID"
)

// TargetType distinguishes the two header-driven routing paths.
type TargetType string

const (
	TargetZeroInstance TargetType = "instance"
	TargetCluster      TargetType = "cluster"
)

// ResolvedTarget carries the parsed header values from an incoming request.
type ResolvedTarget struct {
	Type       TargetType
	InstanceID string // non-empty when Type == TargetZeroInstance
	ClusterID  string // non-empty when Type == TargetCluster
}

// ParseHeaders extracts X-TIDBCLOUD-* headers from the request.
// It returns nil if no tidbcloud headers are present.
// For zero-instance headers, the cluster ID is extracted from the instance ID.
func ParseHeaders(r *http.Request) (*ResolvedTarget, error) {
	clusterID := strings.TrimSpace(r.Header.Get(HeaderClusterID))
	instanceID := strings.TrimSpace(r.Header.Get(HeaderZeroInstanceID))

	// Cluster-ID takes precedence per design.
	if clusterID != "" {
		return &ResolvedTarget{Type: TargetCluster, ClusterID: clusterID}, nil
	}
	if instanceID != "" {
		cid, err := ParseZeroInstanceID(instanceID)
		if err != nil {
			return nil, fmt.Errorf("invalid zero instance id: %w", err)
		}
		return &ResolvedTarget{
			Type:       TargetZeroInstance,
			InstanceID: instanceID,
			ClusterID:  strconv.FormatUint(cid, 10),
		}, nil
	}
	return nil, nil
}

// HeaderForTarget returns the canonical header name for the given target type.
func HeaderForTarget(t TargetType) string {
	switch t {
	case TargetZeroInstance:
		return HeaderZeroInstanceID
	case TargetCluster:
		return HeaderClusterID
	default:
		return "X-TIDBCLOUD-*"
	}
}

// ParseZeroInstanceID extracts the cluster ID (uint64) from a zero-instance ID.
//
// A zero-instance ID is a 32-character base64url (no padding) string encoding a
// 24-byte payload:
//
//	bytes [0:8]  — cluster ID (uint64, big-endian)
//	bytes [8:24] — random UUID secret
func ParseZeroInstanceID(instanceID string) (uint64, error) {
	payload, err := base64.RawURLEncoding.DecodeString(instanceID)
	if err != nil {
		return 0, fmt.Errorf("decode instance id: %w", err)
	}
	if len(payload) != 24 {
		return 0, fmt.Errorf("unexpected payload length %d, want 24", len(payload))
	}
	return binary.BigEndian.Uint64(payload[:8]), nil
}
