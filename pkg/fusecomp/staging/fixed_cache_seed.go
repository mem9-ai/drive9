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

// RenewFixedUploadSeedAfterAbort advances only the canceled attempt on the
// same local image. The caller must first confirm ABORTED or EXPIRED and its
// reservation release. An unknown abort must keep the old seed for replay.
func (c *WriteBackCache) RenewFixedUploadSeedAfterAbort(path string, generation uint64, oldSeed string) error {
	if generation == 0 || viewprotocol.ValidateOperationID(oldSeed) != nil {
		return fmt.Errorf("invalid fixed upload attempt")
	}
	pl := c.AcquirePathLock(path)
	defer c.ReleasePathLock(path, pl)
	c.mu.Lock()
	current, ok := c.metas[path]
	if !ok || current.Generation != generation {
		c.mu.Unlock()
		return nil
	}
	updated := cloneWriteBackMeta(current)
	c.mu.Unlock()
	raw, err := os.ReadFile(c.MetaFile(path))
	if err != nil {
		return err
	}
	var disk WriteBackMeta
	if err := json.Unmarshal(raw, &disk); err != nil {
		return err
	}
	if disk.Path != path || disk.Generation != generation || disk.FixedUploadSeedGeneration != generation || viewprotocol.ValidateOperationID(disk.FixedUploadSeed) != nil {
		return fmt.Errorf("fixed upload attempt metadata changed")
	}
	if disk.FixedUploadSeed != oldSeed {
		return nil
	} // An earlier atomic rename already advanced it.
	updated.FixedUploadSeed = "cache-" + rand.Text()
	updated.FixedUploadSeedGeneration = generation
	raw, err = json.Marshal(updated)
	if err != nil {
		return err
	}
	if err := atomicWrite(c.MetaFile(path), raw); err != nil {
		return err
	}
	c.mu.Lock()
	c.metas[path] = &updated
	c.mu.Unlock()
	return nil
}
