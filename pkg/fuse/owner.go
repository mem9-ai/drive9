package fuse

import (
	"fmt"
	"strconv"
	"strings"
)

// Owner is a numeric uid/gid pair.
type Owner struct {
	UID uint32
	GID uint32
}

// ParseOwner parses "UID:GID" (or a bare "UID", which uses the same GID).
func ParseOwner(raw string) (*Owner, error) {
	uidText, gidText, hasGID := strings.Cut(strings.TrimSpace(raw), ":")
	if !hasGID {
		gidText = uidText
	}
	uid, err := strconv.ParseUint(uidText, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid owner %q: want UID:GID", raw)
	}
	gid, err := strconv.ParseUint(gidText, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid owner %q: want UID:GID", raw)
	}
	return &Owner{UID: uint32(uid), GID: uint32(gid)}, nil
}
