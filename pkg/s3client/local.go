package s3client

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Presigned-URL query parameters minted and verified by the local object-store
// mock. Unlike S3/GCS/Azure the local gateway has no cloud provider to sign its
// URLs, so it signs them itself: without a signature GET /s3/objects/{key}
// would hand any caller that can guess a key the bytes of another tenant's
// object, and PUT /s3/upload/{uploadID}/{part} would accept anonymous writes.
const (
	localPresignExpiresParam   = "x-drive9-expires"
	localPresignSignatureParam = "x-drive9-signature"
	localPresignDefaultTTL     = 10 * time.Minute
)

// All LocalS3Client instances in one process share the signing key: a
// presigned URL minted by one tenant backend must still verify after that
// backend is evicted and reopened, which creates a new client for the same
// storage. The key is process-local, so URLs die with the process.
var (
	localPresignKeyOnce sync.Once
	localPresignKey     []byte
	localPresignKeyErr  error
)

// ErrInvalidObjectKey marks a key that would address a path outside the local
// object-store root.
var ErrInvalidObjectKey = errors.New("invalid object key")

func sharedLocalPresignKey() ([]byte, error) {
	localPresignKeyOnce.Do(func() {
		localPresignKey = make([]byte, 32)
		_, localPresignKeyErr = rand.Read(localPresignKey)
	})
	return localPresignKey, localPresignKeyErr
}

// LocalS3Client implements S3Client using the local filesystem.
// Used for testing and development without real S3.
type LocalS3Client struct {
	rootDir string
	baseURL string // base URL for presigned URLs (e.g. "http://localhost:9091/s3")
	signKey []byte
	now     func() time.Time
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
	// Canonicalize the root: the signature binds it, and the pool can hand the
	// same storage through a relative and an absolute path.
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve s3 root: %w", err)
	}
	rootDir = absRoot
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create s3 root: %w", err)
	}
	markS3ClientAvailable()
	signKey, err := sharedLocalPresignKey()
	if err != nil {
		return nil, fmt.Errorf("generate local s3 presign key: %w", err)
	}
	return &LocalS3Client{
		rootDir: rootDir,
		baseURL: baseURL,
		signKey: signKey,
		now:     time.Now,
		uploads: make(map[string]*localUpload),
	}, nil
}

// objectPath maps an object key to its path under the objects root.
//
// Keys reach this method from persisted storage_ref values and from presigned
// URLs, so the mapped path is verified to stay inside the objects root: a key
// such as "../../etc/passwd" must never resolve to a host file. Both callers
// and tests rely on the containment check, so it lives here rather than in the
// individual operations.
func (c *LocalS3Client) objectPath(key string) (string, error) {
	if !validObjectKey(key) {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	root := filepath.Join(c.rootDir, "objects")
	path := filepath.Join(root, filepath.FromSlash(key))
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	return path, nil
}

// ValidateObjectKey reports whether key is a well-formed object key the local
// object store (and any S3-compatible backend) can address: non-empty,
// relative, and free of "..", "." and empty segments, backslashes and NUL
// bytes. Callers that accept a key from outside the process use it to reject
// references that would escape a tenant's namespace; the local client enforces
// the same rule on every object operation.
func ValidateObjectKey(key string) error {
	if !validObjectKey(key) {
		return fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	return nil
}

// validObjectKey reports whether key is a well-formed object key: non-empty,
// relative, and free of "..", "." and empty segments, backslashes and NUL
// bytes.
func validObjectKey(key string) bool {
	if key == "" || strings.ContainsRune(key, 0) || strings.Contains(key, `\`) {
		return false
	}
	if strings.HasPrefix(key, "/") {
		return false
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// RootDir is the local mock S3 root.
func (c *LocalS3Client) RootDir() string { return c.rootDir }

// ObjectsDir is where PutObject stores keys (root/objects).
func (c *LocalS3Client) ObjectsDir() string { return filepath.Join(c.rootDir, "objects") }

func (c *LocalS3Client) nowTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// signLocalPresign is the canonical signature for one presigned request: the
// storage root, HTTP method, handler-relative path, the extra query the URL
// was minted with (the range selector, or empty), and the expiry. Binding the
// storage root keeps a URL for one tenant from being replayed against another
// tenant's backend after the /s3/{tenant}/ proxy strips the prefix; binding the
// rest stops cross-method, cross-key, and expired replays.
func (c *LocalS3Client) signLocalPresign(method, path, extra string, expires int64) string {
	mac := hmac.New(sha256.New, c.signKey)
	_, _ = fmt.Fprintf(mac, "%s\n%s\n%s\n%s\n%d", c.rootDir, method, path, extra, expires)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// presignLocalRequest returns baseURL+path with an expiry and signature
// appended. extraQuery is the optional query fragment that is part of the
// signature (e.g. "range=0-1023").
func (c *LocalS3Client) presignLocalRequest(method, path, extraQuery string, ttl time.Duration) string {
	if ttl <= 0 {
		ttl = localPresignDefaultTTL
	}
	expires := c.nowTime().Add(ttl).Unix()
	signature := c.signLocalPresign(method, path, extraQuery, expires)
	query := extraQuery
	if query != "" {
		query += "&"
	}
	query += localPresignExpiresParam + "=" + strconv.FormatInt(expires, 10) +
		"&" + localPresignSignatureParam + "=" + signature
	return c.baseURL + path + "?" + query
}

// authorizeLocalPresign verifies the expiry and signature of an incoming
// presigned request before any object bytes are read or written.
func (c *LocalS3Client) authorizeLocalPresign(r *http.Request, extraQuery string) bool {
	query := r.URL.Query()
	expiresRaw := query.Get(localPresignExpiresParam)
	signature := query.Get(localPresignSignatureParam)
	if expiresRaw == "" || signature == "" {
		return false
	}
	expires, err := strconv.ParseInt(expiresRaw, 10, 64)
	if err != nil {
		return false
	}
	if c.nowTime().Unix() > expires {
		return false
	}
	want := c.signLocalPresign(r.Method, r.URL.Path, extraQuery, expires)
	return hmac.Equal([]byte(signature), []byte(want))
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

	if !validObjectKey(key) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	url := c.presignLocalRequest(http.MethodPut, fmt.Sprintf("/upload/%s/%d", uploadID, partNumber), "", ttl)
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
	objPath, err := c.objectPath(key)
	if err != nil {
		result = "error"
		return err
	}
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

	if !validObjectKey(key) {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	return c.presignLocalRequest(http.MethodGet, "/objects/"+key, "", ttl), nil
}

func (c *LocalS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64, encOpts EncryptionOpts) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("put_object", result, start) }()

	objPath, err := c.objectPath(key)
	if err != nil {
		result = "error"
		return err
	}
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

	rc, err := c.openObject(key)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, ErrInvalidObjectKey) {
			result = "not_found"
		} else {
			result = "error"
		}
		return nil, err
	}
	return rc, nil
}

// openObject opens an object file after containment validation.
func (c *LocalS3Client) openObject(key string) (io.ReadCloser, error) {
	path, err := c.objectPath(key)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func (c *LocalS3Client) DeleteObject(ctx context.Context, key string) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("delete_object", result, start) }()

	path, err := c.objectPath(key)
	if err != nil {
		result = "error"
		return err
	}
	err = os.Remove(path)
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
	srcPath, err := c.objectPath(sourceKey)
	if err != nil {
		result = "error"
		return "", err
	}
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

	if !validObjectKey(key) {
		return "", fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	rangeQuery := fmt.Sprintf("range=%d-%d", startByte, endByte)
	return c.presignLocalRequest(http.MethodGet, "/objects/"+key, rangeQuery, ttl), nil
}

// Verify interface compliance.
var _ S3Client = (*LocalS3Client)(nil)
