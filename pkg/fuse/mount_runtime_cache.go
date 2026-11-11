package fuse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// mountRuntimeGeneration identifies one runtime in a switch journal. Both
// fields are required together. Zero preserves the ordinary mount namespace.
// The switch owner must serialize construction/recovery of a given generation;
// a distinct cache directory is not a lock or permission grant.
type mountRuntimeGeneration struct {
	MountID    string
	Generation uint64
}

func mountRuntimeCacheHashes(cfg MountConfig, binding viewprotocol.Binding, generation mountRuntimeGeneration) (string, string, error) {
	if err := binding.Validate(); err != nil {
		return "", "", err
	}
	if (generation.MountID == "") != (generation.Generation == 0) ||
		generation.MountID != "" && viewprotocol.ValidateOperationID(generation.MountID) != nil {
		return "", "", fmt.Errorf("fuse: invalid mount runtime generation")
	}
	if binding.LayerID != "" && (cfg.LayerRef != "" || cfg.CheckpointRef != "") {
		return "", "", fmt.Errorf("fuse: fixed view cannot also select a legacy layer or checkpoint")
	}
	writeHash := MountHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot)
	if cfg.LayerRef != "" {
		writeHash = MountLayerHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot, cfg.LayerRef, cfg.CheckpointRef)
	}
	readHash := MountReadCacheHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg))
	if generation.MountID == "" && binding.LayerID == "" {
		return writeHash, readHash, nil
	}

	// Include the actual immutable client binding, not a user-supplied Layer
	// name. Revisiting a view in a later generation must not recover a prior
	// generation's dirty writes. The credential-derived read namespace is
	// retained too. Only digests are used in filesystem paths.
	scope, err := json.Marshal(struct {
		Mount mountRuntimeGeneration
		View  viewprotocol.Binding
	}{generation, binding})
	if err != nil {
		return "", "", err
	}
	hash := func(base string) string {
		digest := sha256.Sum256(append([]byte("view-runtime-v1\x00"+base+"\x00"), scope...))
		return hex.EncodeToString(digest[:])
	}
	return hash(writeHash), hash(readHash), nil
}
