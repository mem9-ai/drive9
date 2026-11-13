package staging

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// FixedUploadSeed persists the attempt identity on the existing cache image
// before initiation. Disk is read under the path lock so an atomic-write error
// after rename cannot cause the next call to replace an already-saved identity.
func (c *WriteBackCache) FixedUploadSeed(path string, generation uint64) (string, error) {
	pl := c.AcquirePathLock(path)
	defer c.ReleasePathLock(path, pl)
	c.mu.Lock()
	current, ok := c.metas[path]
	if !ok || generation == 0 || current.Generation != generation {
		c.mu.Unlock()
		return "", fmt.Errorf("fixed upload cache generation changed")
	}
	updated := cloneWriteBackMeta(current)
	c.mu.Unlock()
	raw, err := os.ReadFile(c.MetaFile(path))
	if err != nil {
		return "", err
	}
	var disk WriteBackMeta
	if err := json.Unmarshal(raw, &disk); err != nil {
		return "", err
	}
	if disk.Path != path || disk.Generation != generation {
		return "", fmt.Errorf("fixed upload cache metadata changed")
	}
	if disk.FixedUploadSeedGeneration == generation {
		if viewprotocol.ValidateOperationID(disk.FixedUploadSeed) != nil {
			return "", fmt.Errorf("invalid fixed upload seed")
		}
		updated.FixedUploadSeed = disk.FixedUploadSeed
	} else {
		updated.FixedUploadSeed = "cache-" + rand.Text()
	}
	updated.FixedUploadSeedGeneration = generation
	raw, err = json.Marshal(updated)
	if err != nil {
		return "", err
	}
	if err := atomicWrite(c.MetaFile(path), raw); err != nil {
		return "", err
	}
	c.mu.Lock()
	c.metas[path] = &updated
	c.mu.Unlock()
	return updated.FixedUploadSeed, nil
}
