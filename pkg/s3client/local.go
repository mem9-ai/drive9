package s3client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// LocalS3Client implements S3Client using the local filesystem.
// Used for testing and development without real S3.
type LocalS3Client struct {
	rootDir string
	baseURL string // base URL for presigned URLs (e.g. "http://localhost:9091/s3")
	mu      sync.Mutex
	uploads map[string]*localUpload // uploadID → upload state
}

type localUpload struct {
	key   string
	parts map[int]*localPart // partNumber → part
}

type localPart struct {
	size int64
	etag string
}

// NewLocal creates a LocalS3Client rooted at the given directory.
// baseURL is used to construct presigned URLs that can be resolved locally.
func NewLocal(rootDir, baseURL string) (*LocalS3Client, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create s3 root: %w", err)
	}
	markS3ClientAvailable()
	return &LocalS3Client{
		rootDir: rootDir,
		baseURL: baseURL,
		uploads: make(map[string]*localUpload),
	}, nil
}

func (c *LocalS3Client) objectPath(key string) string {
	return filepath.Join(c.rootDir, "objects", key)
}

func (c *LocalS3Client) partPath(key, uploadID string, partNumber int) string {
	return filepath.Join(c.rootDir, "parts", uploadID, fmt.Sprintf("%05d", partNumber))
}

func (c *LocalS3Client) CreateMultipartUpload(ctx context.Context, key string, algo ChecksumAlgo, encOpts EncryptionOpts) (*MultipartUpload, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("create_multipart_upload", result, start) }()

	uploadID := fmt.Sprintf("upload-%x", sha256.Sum256([]byte(key+time.Now().String())))[:24]

	partsDir := filepath.Join(c.rootDir, "parts", uploadID)
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		result = "error"
		return nil, fmt.Errorf("create parts dir: %w", err)
	}

	c.mu.Lock()
	c.uploads[uploadID] = &localUpload{key: key, parts: make(map[int]*localPart)}
	c.mu.Unlock()

	return &MultipartUpload{UploadID: uploadID, Key: key}, nil
}

func (c *LocalS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, algo ChecksumAlgo, checksumValue string, ttl time.Duration) (*UploadPartURL, error) {
	start := time.Now()
	metricResult := "ok"
	defer func() { recordS3Operation("presign_upload_part", metricResult, start) }()

	url := fmt.Sprintf("%s/upload/%s/%d", c.baseURL, uploadID, partNumber)
	var headers map[string]string
	if checksumValue != "" {
		switch algo {
		case ChecksumAlgoCRC32C:
			headers = map[string]string{"x-amz-checksum-crc32c": checksumValue}
		default:
			headers = map[string]string{"x-amz-checksum-sha256": checksumValue}
		}
	}
	urlResult := &UploadPartURL{
		Number:    partNumber,
		URL:       url,
		Size:      partSize,
		Headers:   headers,
		ExpiresAt: time.Now().Add(ttl),
	}
	switch algo {
	case ChecksumAlgoCRC32C:
		urlResult.ChecksumCRC32C = checksumValue
	default:
		urlResult.ChecksumSHA256 = checksumValue
	}
	return urlResult, nil
}

// UploadPart directly writes a part (used by the local presigned URL handler).
func (c *LocalS3Client) UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader) (string, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("upload_part", result, start) }()

	c.mu.Lock()
	upload, ok := c.uploads[uploadID]
	c.mu.Unlock()
	if !ok {
		result = "not_found"
		return "", fmt.Errorf("upload not found: %s", uploadID)
	}

	path := c.partPath(upload.key, uploadID, partNumber)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		result = "error"
		return "", err
	}
	f, err := os.Create(path)
	if err != nil {
		result = "error"
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		result = "error"
		return "", fmt.Errorf("write part body: %w", err)
	}
	if err := f.Close(); err != nil {
		result = "error"
		return "", err
	}

	sum := h.Sum(nil)
	etag := hex.EncodeToString(sum[:16])

	c.mu.Lock()
	upload.parts[partNumber] = &localPart{size: n, etag: etag}
	c.mu.Unlock()

	return etag, nil
}

func (c *LocalS3Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []Part) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("complete_multipart_upload", result, start) }()

	c.mu.Lock()
	upload, ok := c.uploads[uploadID]
	c.mu.Unlock()
	if !ok {
		result = "not_found"
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	// Sort parts by number
	sorted := make([]Part, len(parts))
	copy(sorted, parts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Number < sorted[j].Number })

	// Assemble final object from parts
	objPath := c.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		result = "error"
		return err
	}
	f, err := os.Create(objPath)
	if err != nil {
		result = "error"
		return err
	}
	defer func() { _ = f.Close() }()

	for _, p := range sorted {
		partFile := c.partPath(upload.key, uploadID, p.Number)
		part, err := os.Open(partFile)
		if err != nil {
			result = "error"
			return fmt.Errorf("open part %d: %w", p.Number, err)
		}
		if _, err := io.Copy(f, part); err != nil {
			_ = part.Close()
			result = "error"
			return err
		}
		if err := part.Close(); err != nil {
			result = "error"
			return err
		}
	}

	// Cleanup parts
	partsDir := filepath.Join(c.rootDir, "parts", uploadID)
	_ = os.RemoveAll(partsDir)

	c.mu.Lock()
	delete(c.uploads, uploadID)
	c.mu.Unlock()

	return nil
}

func (c *LocalS3Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	start := time.Now()
	defer func() { recordS3Operation("abort_multipart_upload", "ok", start) }()

	partsDir := filepath.Join(c.rootDir, "parts", uploadID)
	_ = os.RemoveAll(partsDir)

	c.mu.Lock()
	delete(c.uploads, uploadID)
	c.mu.Unlock()

	return nil
}

func (c *LocalS3Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("list_parts", result, start) }()

	c.mu.Lock()
	defer c.mu.Unlock()

	upload, ok := c.uploads[uploadID]
	if !ok {
		result = "not_found"
		return nil, fmt.Errorf("upload not found: %s", uploadID)
	}

	var parts []Part
	for num, p := range upload.parts {
		parts = append(parts, Part{Number: num, Size: p.size, ETag: p.etag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	return parts, nil
}

func (c *LocalS3Client) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	start := time.Now()
	defer func() { recordS3Operation("presign_get_object", "ok", start) }()

	url := fmt.Sprintf("%s/objects/%s", c.baseURL, key)
	return url, nil
}

func (c *LocalS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64, encOpts EncryptionOpts) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("put_object", result, start) }()

	objPath := c.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		result = "error"
		return err
	}
	f, err := os.Create(objPath)
	if err != nil {
		result = "error"
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, body); err != nil {
		result = "error"
		return err
	}
	if err := f.Close(); err != nil {
		result = "error"
		return err
	}
	return nil
}

func (c *LocalS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("get_object", result, start) }()

	rc, err := os.Open(c.objectPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			result = "not_found"
		} else {
			result = "error"
		}
		return nil, err
	}
	return rc, nil
}

func (c *LocalS3Client) DeleteObject(ctx context.Context, key string) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("delete_object", result, start) }()

	err := os.Remove(c.objectPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			result = "not_found"
		} else {
			result = "error"
		}
		return err
	}
	return nil
}

func (c *LocalS3Client) DeletePrefix(ctx context.Context, prefix string) (PrefixDeleteResult, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("delete_prefix", result, start) }()

	prefix = strings.TrimLeft(prefix, "/")
	var out PrefixDeleteResult
	objectsRoot := filepath.Join(c.rootDir, "objects")
	targetRoot := filepath.Join(objectsRoot, filepath.FromSlash(prefix))
	if prefix == "" {
		targetRoot = objectsRoot
	}
	cleanObjectsRoot, err := filepath.Abs(objectsRoot)
	if err != nil {
		result = "error"
		return out, err
	}
	cleanTargetRoot, err := filepath.Abs(targetRoot)
	if err != nil {
		result = "error"
		return out, err
	}
	rel, err := filepath.Rel(cleanObjectsRoot, cleanTargetRoot)
	if err != nil {
		result = "error"
		return out, err
	}
	if rel == ".." || strings.HasPrefix(rel, "../") || filepath.IsAbs(rel) {
		result = "error"
		return out, fmt.Errorf("prefix escapes local s3 root: %q", prefix)
	}
	if err := filepath.WalkDir(targetRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		out.DeletedObjects++
		return nil
	}); err != nil && !os.IsNotExist(err) {
		result = "error"
		return out, err
	}

	c.mu.Lock()
	uploadIDs := make([]string, 0)
	for uploadID, upload := range c.uploads {
		if prefix == "" || strings.HasPrefix(upload.key, prefix) {
			uploadIDs = append(uploadIDs, uploadID)
			delete(c.uploads, uploadID)
		}
	}
	c.mu.Unlock()
	for _, uploadID := range uploadIDs {
		_ = os.RemoveAll(filepath.Join(c.rootDir, "parts", uploadID))
		out.AbortedMultipartUploads++
	}
	return out, nil
}

func (c *LocalS3Client) UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("upload_part_copy", result, start) }()

	c.mu.Lock()
	upload, ok := c.uploads[uploadID]
	c.mu.Unlock()
	if !ok {
		result = "not_found"
		return "", fmt.Errorf("upload not found: %s", uploadID)
	}

	// Read source range from the existing object file
	srcPath := c.objectPath(sourceKey)
	f, err := os.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			result = "not_found"
		} else {
			result = "error"
		}
		return "", fmt.Errorf("open source object: %w", err)
	}
	defer func() { _ = f.Close() }()

	path := c.partPath(upload.key, uploadID, partNumber)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		result = "error"
		return "", err
	}
	part, err := os.Create(path)
	if err != nil {
		result = "error"
		return "", err
	}
	defer func() { _ = part.Close() }()

	size := endByte - startByte + 1
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(part, h), io.NewSectionReader(f, startByte, size))
	if err != nil {
		result = "error"
		return "", fmt.Errorf("copy source range: %w", err)
	}
	if err := part.Close(); err != nil {
		result = "error"
		return "", err
	}

	sum := h.Sum(nil)
	etag := hex.EncodeToString(sum[:16])

	c.mu.Lock()
	upload.parts[partNumber] = &localPart{size: n, etag: etag}
	c.mu.Unlock()

	return etag, nil
}

func (c *LocalS3Client) PresignGetObjectRange(ctx context.Context, key string, startByte, endByte int64, ttl time.Duration) (string, error) {
	start := time.Now()
	defer func() { recordS3Operation("presign_get_object_range", "ok", start) }()

	url := fmt.Sprintf("%s/objects/%s?range=%d-%d", c.baseURL, key, startByte, endByte)
	return url, nil
}

// Verify interface compliance.
var _ S3Client = (*LocalS3Client)(nil)
