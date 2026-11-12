package staging

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const fixedUploadGenerationFile = "fixed-upload-generation"

// fixedUploadGenerationFloor prevents a completed upload's generation from
// being reused when its metadata was removed before the completion WAL settled.
// Only fixed-upload cleanup writes this file; ordinary publications still use
// the existing in-memory counter. As with the metadata directory itself, one
// index/cache instance owns the directory at a time.
type fixedUploadGenerationFloor struct {
	mu         sync.Mutex
	generation uint64
}

// load runs only during construction, before any publication is admitted.
func (f *fixedUploadGenerationFloor) load(dir string) error {
	data, err := os.ReadFile(filepath.Join(dir, fixedUploadGenerationFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read fixed upload generation: %w", err)
	}
	if len(data) != 12 || binary.LittleEndian.Uint32(data[8:]) != crc32.ChecksumIEEE(data[:8]) {
		return fmt.Errorf("invalid fixed upload generation")
	}
	generation := binary.LittleEndian.Uint64(data[:8])
	if generation == 0 || generation == math.MaxUint64 {
		return fmt.Errorf("invalid fixed upload generation")
	}
	f.generation = generation
	return nil
}

func (f *fixedUploadGenerationFloor) advance(dir string, generation uint64, next *atomic.Uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if generation == 0 || generation == math.MaxUint64 {
		return fmt.Errorf("invalid fixed upload generation")
	}
	if generation > f.generation {
		var data [12]byte
		binary.LittleEndian.PutUint64(data[:8], generation)
		binary.LittleEndian.PutUint32(data[8:], crc32.ChecksumIEEE(data[:8]))
		if err := atomicWrite(filepath.Join(dir, fixedUploadGenerationFile), data[:]); err != nil {
			return fmt.Errorf("save fixed upload generation: %w", err)
		}
		f.generation = generation
	}
	for previous := next.Load(); previous < f.generation; previous = next.Load() {
		if next.CompareAndSwap(previous, f.generation) {
			break
		}
	}
	return nil
}
