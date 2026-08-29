// Command migration-scale-fixture creates deterministic EBS migration sizing fixtures.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type fixtureConfig struct {
	Root         string
	Entries      int
	Files        int
	LogicalBytes int64
	Sparse       bool
}

type fixtureResult struct {
	Root         string    `json:"root"`
	Entries      int       `json:"entries"`
	Files        int       `json:"files"`
	Directories  int       `json:"directories"`
	LogicalBytes int64     `json:"logical_bytes"`
	Sparse       bool      `json:"sparse"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
}

func main() {
	root := flag.String("root", "", "new or empty absolute fixture directory")
	profile := flag.String("profile", "observed", "fixture profile: observed or full")
	entries := flag.Int("entries", -1, "override total non-root namespace entries")
	files := flag.Int("files", -1, "override regular file count")
	logicalBytes := flag.Int64("logical-bytes", -1, "override total regular-file logical bytes")
	sparse := flag.Bool("sparse", false, "use sparse files; faster but not representative of physical I/O")
	flag.Parse()
	config, err := fixtureProfile(*profile)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	config = applyFixtureOverrides(config, *entries, *files, *logicalBytes)
	config.Root, config.Sparse = *root, *sparse
	result, err := createFixture(config)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func fixtureProfile(profile string) (fixtureConfig, error) {
	switch profile {
	case "observed":
		return fixtureConfig{Entries: 299853, Files: 257213, LogicalBytes: 3328748350}, nil
	case "full":
		return fixtureConfig{Entries: 6000000, Files: 5140000, LogicalBytes: 62 << 30}, nil
	default:
		return fixtureConfig{}, fmt.Errorf("unsupported fixture profile %q", profile)
	}
}

func applyFixtureOverrides(config fixtureConfig, entries, files int, logicalBytes int64) fixtureConfig {
	if entries >= 0 {
		config.Entries = entries
	}
	if files >= 0 {
		config.Files = files
	}
	if logicalBytes >= 0 {
		config.LogicalBytes = logicalBytes
	}
	return config
}

func createFixture(config fixtureConfig) (fixtureResult, error) {
	if config.Root == "" || !filepath.IsAbs(config.Root) || filepath.Clean(config.Root) != config.Root {
		return fixtureResult{}, errors.New("--root must be a clean absolute path")
	}
	if config.Entries < 0 || config.Files < 0 || config.Files > config.Entries || config.LogicalBytes < 0 {
		return fixtureResult{}, errors.New("entries/files/logical-bytes are inconsistent")
	}
	if err := ensureEmptyRoot(config.Root); err != nil {
		return fixtureResult{}, err
	}
	result := fixtureResult{
		Root: config.Root, Entries: config.Entries, Files: config.Files,
		Directories: config.Entries - config.Files, LogicalBytes: config.LogicalBytes,
		Sparse: config.Sparse, StartedAt: time.Now().UTC(),
	}
	for index := 0; index < result.Directories; index++ {
		path := filepath.Join(config.Root, fmt.Sprintf("dir-%09d", index))
		if err := os.Mkdir(path, 0o755); err != nil {
			return fixtureResult{}, fmt.Errorf("create directory %d: %w", index, err)
		}
	}
	baseSize, remainder := int64(0), int64(0)
	if config.Files > 0 {
		baseSize = config.LogicalBytes / int64(config.Files)
		remainder = config.LogicalBytes % int64(config.Files)
	}
	buffer := make([]byte, 64<<10)
	for index := range buffer {
		buffer[index] = byte(index % 251)
	}
	for index := 0; index < config.Files; index++ {
		parent := config.Root
		if result.Directories > 0 {
			parent = filepath.Join(config.Root, fmt.Sprintf("dir-%09d", index%result.Directories))
		}
		size := baseSize
		if int64(index) < remainder {
			size++
		}
		path := filepath.Join(parent, fmt.Sprintf("file-%09d", index))
		if err := writeFixtureFile(path, size, config.Sparse, buffer); err != nil {
			return fixtureResult{}, fmt.Errorf("create file %d: %w", index, err)
		}
		if (index+1)%100000 == 0 {
			_, _ = fmt.Fprintf(os.Stderr, "created %d/%d files\n", index+1, config.Files)
		}
	}
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func ensureEmptyRoot(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(root, 0o755)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("fixture root must be a real directory")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("fixture root must be empty; the generator never deletes existing data")
	}
	return nil
}

func writeFixtureFile(path string, size int64, sparse bool, buffer []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if sparse {
		err = file.Truncate(size)
	} else {
		remaining := size
		for remaining > 0 {
			chunk := int64(len(buffer))
			if remaining < chunk {
				chunk = remaining
			}
			if _, writeErr := file.Write(buffer[:chunk]); writeErr != nil {
				err = writeErr
				break
			}
			remaining -= chunk
		}
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if errors.Is(err, io.ErrShortWrite) {
		return fmt.Errorf("short write: %w", err)
	}
	return err
}
