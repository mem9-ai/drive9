package fuse

import (
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func mountRuntimeCacheHashes(cfg MountConfig, remoteRoot string, binding mountstate.SwitchBinding) (string, string, error) {
	if err := binding.Validate(); err != nil {
		return "", "", err
	}
	if binding.LayerID != "" {
		writeHash := MountLayerHash(cfg.Server, cfg.MountPoint, remoteRoot, binding.LayerID)
		readHash := MountReadCacheHashWithLayer(cfg.Server, cfg.MountPoint, remoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg), binding.LayerID)
		return writeHash, readHash, nil
	}
	writeHash := MountHash(cfg.Server, cfg.MountPoint, remoteRoot)
	readHash := MountReadCacheHash(cfg.Server, cfg.MountPoint, remoteRoot, mountCredentialKind(&cfg), mountCredentialSecret(&cfg))
	return writeHash, readHash, nil
}
