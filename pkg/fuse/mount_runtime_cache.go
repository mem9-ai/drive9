package fuse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// mountRuntimeGeneration scopes per-generation staging/read cache directories
// across in-place view switches. Generation 1 on main preserves the legacy
// MountHash / MountReadCacheHash paths for WAL crash recovery compatibility.
type mountRuntimeGeneration struct {
	MountID    string
	Generation uint64
}

func mountRuntimeCacheHashes(cfg MountConfig, remoteRoot string, binding viewprotocol.SwitchBinding, generation mountRuntimeGeneration) (string, string, error) {
	if err := binding.Validate(); err != nil {
		return "", "", err
	}
	if (generation.MountID == "") != (generation.Generation == 0) ||
		generation.MountID != "" && viewprotocol.ValidateOperationID(generation.MountID) != nil {
		return "", "", fmt.Errorf("fuse: invalid mount runtime generation")
	}
	writeHash := MountHash(cfg.Server, cfg.MountPoint, remoteRoot)
	if binding.LayerID != "" && generation.Generation <= 1 {
		writeHash = MountLayerHash(cfg.Server, cfg.MountPoint, remoteRoot, binding.LayerID)
		readHash := MountReadCacheHashWithLayer(cfg.Server, cfg.MountPoint, remoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg), binding.LayerID)
		return writeHash, readHash, nil
	}
	readHash := MountReadCacheHash(cfg.Server, cfg.MountPoint, remoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg))
	if binding.LayerID == "" && generation.Generation <= 1 {
		return writeHash, readHash, nil
	}
	scope, err := json.Marshal(struct {
		Mount mountRuntimeGeneration
		View  viewprotocol.SwitchBinding
	}{generation, binding})
	if err != nil {
		return "", "", err
	}
	hash := func(base string) string {
		digest := sha256.Sum256(append([]byte("view-runtime-v36\x00"+base+"\x00"), scope...))
		return hex.EncodeToString(digest[:])
	}
	return hash(writeHash), hash(readHash), nil
}
