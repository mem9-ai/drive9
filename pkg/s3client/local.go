package s3client

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// LocalS3Client implements S3Client using the local filesystem.
// Used for testing and development without real S3.
type LocalS3Client struct {
	rootDir    string
	baseURL    string // base URL for presigned URLs (e.g. "http://localhost:9091/s3")
	signingKey []byte // HMAC-SHA256 key for presigned URL signatures
	mu         sync.Mutex
	uploads    map[string]*localUpload // uploadID → upload state
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
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	return &LocalS3Client{
		rootDir:    rootDir,
		baseURL:    baseURL,
		signingKey: key,
		uploads:    make(map[string]*localUpload),
	}, nil
}

func (c *LocalS3Client) objectPath(key string) string {
	return filepath.Join(c.rootDir, "objects", key)
}

func (c *LocalS3Client) partPath(key, uploadID string, partNumber int) string {
	return filepath.Join(c.rootDir, "parts", uploadID, fmt.Sprintf("%05d", partNumber))
}

func (c *LocalS3Client) CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error) {
	uploadID := fmt.Sprintf("upload-%x", sha256.Sum256([]byte(key+time.Now().String())))[:24]

	partsDir := filepath.Join(c.rootDir, "parts", uploadID)
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create parts dir: %w", err)
	}

	c.mu.Lock()
	c.uploads[uploadID] = &localUpload{key: key, parts: make(map[int]*localPart)}
	c.mu.Unlock()

	return &MultipartUpload{UploadID: uploadID, Key: key}, nil
}

func (c *LocalS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, ttl time.Duration) (*UploadPartURL, error) {
	exp := time.Now().Add(ttl)
	path := fmt.Sprintf("/upload/%s/%d", uploadID, partNumber)
	url := fmt.Sprintf("%s%s?exp=%d&sig=%s", c.baseURL, path, exp.Unix(), c.sign(path, exp))
	return &UploadPartURL{
		Number:    partNumber,
		URL:       url,
		Size:      partSize,
		ExpiresAt: exp,
	}, nil
}

// UploadPart directly writes a part (used by the local presigned URL handler).
func (c *LocalS3Client) UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader) (string, error) {
	c.mu.Lock()
	upload, ok := c.uploads[uploadID]
	c.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("upload not found: %s", uploadID)
	}

	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("read part body: %w", err)
	}

	path := c.partPath(upload.key, uploadID, partNumber)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}

	h := sha256.Sum256(data)
	etag := hex.EncodeToString(h[:16])

	c.mu.Lock()
	upload.parts[partNumber] = &localPart{size: int64(len(data)), etag: etag}
	c.mu.Unlock()

	return etag, nil
}

func (c *LocalS3Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []Part) error {
	c.mu.Lock()
	upload, ok := c.uploads[uploadID]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	// Sort parts by number
	sorted := make([]Part, len(parts))
	copy(sorted, parts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Number < sorted[j].Number })

	// Assemble final object from parts
	objPath := c.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(objPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for _, p := range sorted {
		partFile := c.partPath(upload.key, uploadID, p.Number)
		data, err := os.ReadFile(partFile)
		if err != nil {
			return fmt.Errorf("read part %d: %w", p.Number, err)
		}
		if _, err := f.Write(data); err != nil {
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
	partsDir := filepath.Join(c.rootDir, "parts", uploadID)
	_ = os.RemoveAll(partsDir)

	c.mu.Lock()
	delete(c.uploads, uploadID)
	c.mu.Unlock()

	return nil
}

func (c *LocalS3Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	upload, ok := c.uploads[uploadID]
	if !ok {
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
	exp := time.Now().Add(ttl)
	path := fmt.Sprintf("/objects/%s", key)
	url := fmt.Sprintf("%s%s?exp=%d&sig=%s", c.baseURL, path, exp.Unix(), c.sign(path, exp))
	return url, nil
}

func (c *LocalS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	objPath := c.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	return os.WriteFile(objPath, data, 0o644)
}

func (c *LocalS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return os.Open(c.objectPath(key))
}

func (c *LocalS3Client) DeleteObject(ctx context.Context, key string) error {
	return os.Remove(c.objectPath(key))
}

// sign produces an HMAC-SHA256 signature for a path and expiry.
func (c *LocalS3Client) sign(path string, exp time.Time) string {
	mac := hmac.New(sha256.New, c.signingKey)
	mac.Write([]byte(fmt.Sprintf("%s\n%d", path, exp.Unix())))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature checks the sig and exp query params on an incoming request path.
// Returns true if the signature is valid and not expired.
func (c *LocalS3Client) VerifySignature(path, sig, expStr string) bool {
	expUnix, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return false
	}
	if time.Now().Unix() > expUnix {
		return false
	}
	expected := c.sign(path, time.Unix(expUnix, 0))
	return hmac.Equal([]byte(sig), []byte(expected))
}

// Verify interface compliance.
var _ S3Client = (*LocalS3Client)(nil)
