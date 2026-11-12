package staging

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFixedUploadGenerationFloorConcurrentAndOlderRetirement(t *testing.T) {
	dir := t.TempDir()
	var floor fixedUploadGenerationFloor
	var next atomic.Uint64
	var wg sync.WaitGroup
	for gen := uint64(1); gen <= 16; gen++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := floor.advance(dir, gen, &next); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := floor.advance(dir, 1, &next); err != nil {
		t.Fatal(err)
	}
	var fresh fixedUploadGenerationFloor
	if err := fresh.load(dir); err != nil {
		t.Fatal(err)
	}
	if fresh.generation != 16 || next.Load() != 16 {
		t.Fatal(fresh.generation, next.Load())
	}
}

func TestFixedUploadGenerationFloorRejectsDamagedState(t *testing.T) {
	for _, kind := range []string{"short", "checksum", "zero", "exhausted", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, fixedUploadGenerationFile)
			data := make([]byte, 12)
			binary.LittleEndian.PutUint64(data[:8], 3)
			if kind == "zero" {
				binary.LittleEndian.PutUint64(data[:8], 0)
			}
			if kind == "exhausted" {
				binary.LittleEndian.PutUint64(data[:8], math.MaxUint64)
			}
			binary.LittleEndian.PutUint32(data[8:], crc32.ChecksumIEEE(data[:8]))
			if kind == "checksum" {
				data[0] ^= 1
			}
			if kind == "short" {
				data = data[:7]
			}
			if kind == "directory" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewPendingIndex(dir); err == nil {
				t.Fatal("pending accepted damaged generation")
			}
			if _, err := NewWriteBackCache(dir); err == nil {
				t.Fatal("cache accepted damaged generation")
			}
		})
	}
}

func TestFixedUploadRetirementAdvancesEmptyLiveCounter(t *testing.T) {
	idx, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.RetireFixedUploadGeneration("/absent", 7); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.Put("/absent", 1, PendingNew)
	if err != nil || gen <= 7 {
		t.Fatal(gen, err)
	}
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireFixedUploadGeneration("/absent", 7); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("/absent", []byte("a"), 1, PendingNew); err != nil {
		t.Fatal(err)
	}
	if m, _ := c.GetMeta("/absent"); m.Generation <= 7 {
		t.Fatal(m.Generation)
	}
}
