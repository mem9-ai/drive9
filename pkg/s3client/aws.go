package s3client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type AWSS3Client struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

type AWSConfig struct {
	Region          string
	Bucket          string
	Prefix          string // key prefix, e.g. "tenants/<id>/" — keys already contain "blobs/"
	RoleARN         string
	Endpoint        string
	ForcePathStyle  bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func (cfg AWSConfig) validate() error {
	if cfg.Bucket == "" {
		return fmt.Errorf("s3 bucket is required")
	}
	hasAccessKey := cfg.AccessKeyID != ""
	hasSecretKey := cfg.SecretAccessKey != ""
	if hasAccessKey != hasSecretKey {
		return fmt.Errorf("s3 access key id and secret access key must be set together")
	}
	if cfg.SessionToken != "" && !hasAccessKey {
		return fmt.Errorf("s3 session token requires access key id and secret access key")
	}
	return nil
}

// Validate checks whether the S3 config has a legal combination of required
// fields before any SDK clients are constructed.
func (cfg AWSConfig) Validate() error {
	return cfg.validate()
}

// CredentialLogValue reports the credential source label for startup logs.
func CredentialLogValue(accessKeyID string) string {
	if accessKeyID != "" {
		return "static"
	}
	return "default-credentials"
}

// RoleLogValue reports the role-assumption label for startup logs.
func RoleLogValue(roleARN string) string {
	if roleARN == "" {
		return "none"
	}
	return roleARN
}

func staticCredentialsProvider(cfg AWSConfig) (aws.CredentialsProvider, bool, error) {
	// 1. Explicit static credentials take highest priority.
	accessKeyID := cfg.AccessKeyID
	secretAccessKey := cfg.SecretAccessKey
	sessionToken := cfg.SessionToken

	if accessKeyID != "" {
		return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken), true, nil
	}

	if isAliyunEndpoint(cfg.Endpoint) {
		// 2. ACK RRSA: exchange OIDC token for temporary STS credentials.
		if p, ok := rrsaCredentialsProvider(); ok {
			return p, true, nil
		}
		// 3. Fall back to ALIBABA_CLOUD_ACCESS_KEY_ID / SECRET env vars.
		accessKeyID, secretAccessKey, sessionToken = aliyunCredentials()
		if accessKeyID != "" {
			return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken), true, nil
		}
	}

	return nil, false, nil
}

func applyS3Options(cfg AWSConfig) func(*s3.Options) {
	return func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	}
}

func NewAWS(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{}
	}
	// S3 data-plane calls fan out more than control-plane API calls:
	// multipart uploads use uploadMaxConcurrency workers and FUSE reads can
	// issue direct range/prefetch requests concurrently. Keep this pool 2x
	// the dat9 client pool so hot S3 paths avoid repeated TLS handshakes.
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 128

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
	}

	provider, ok, err := staticCredentialsProvider(cfg)
	if err != nil {
		return nil, err
	}
	if ok {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(provider))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	if cfg.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		awsCfg.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleARN,
				func(o *stscreds.AssumeRoleOptions) {
					o.RoleSessionName = "drive9-server"
				},
			),
		)
	}

	client := s3.NewFromConfig(awsCfg, applyS3Options(cfg))
	markS3ClientAvailable()
	return &AWSS3Client{
		client:  client,
		presign: s3.NewPresignClient(client),
		bucket:  cfg.Bucket,
		prefix:  normalizePrefix(cfg.Prefix),
	}, nil
}

func normalizePrefix(p string) string {
	p = strings.TrimLeft(p, "/")
	if p == "" {
		return ""
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

func (c *AWSS3Client) fullKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + key
}

func (c *AWSS3Client) CreateMultipartUpload(ctx context.Context, key string, algo ChecksumAlgo, encOpts EncryptionOpts) (*MultipartUpload, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("create_multipart_upload", result, start) }()

	in := &s3.CreateMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	}
	if err := applyEncryptionToCreateMultipartUploadInput(in, encOpts); err != nil {
		result = "error"
		return nil, err
	}
	awsAlgo, ok, err := checksumAlgorithmForAWS(algo)
	if err != nil {
		result = "error"
		return nil, err
	}
	if ok {
		in.ChecksumAlgorithm = awsAlgo
	}
	out, err := c.client.CreateMultipartUpload(ctx, in)
	if err != nil {
		result = "error"
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}
	return &MultipartUpload{
		UploadID: aws.ToString(out.UploadId),
		Key:      key,
	}, nil
}

func checksumAlgorithmForAWS(algo ChecksumAlgo) (types.ChecksumAlgorithm, bool, error) {
	switch algo {
	case ChecksumAlgoNone:
		return "", false, nil
	case ChecksumAlgoCRC32C:
		return types.ChecksumAlgorithmCrc32c, true, nil
	case ChecksumAlgoSHA256:
		return types.ChecksumAlgorithmSha256, true, nil
	default:
		return "", false, fmt.Errorf("unsupported checksum algorithm: %q", algo)
	}
}

func (c *AWSS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, algo ChecksumAlgo, checksumValue string, ttl time.Duration) (*UploadPartURL, error) {
	start := time.Now()
	metricResult := "ok"
	defer func() { recordS3Operation("presign_upload_part", metricResult, start) }()

	if ttl > UploadTTL {
		ttl = UploadTTL
	}
	in := &s3.UploadPartInput{
		Bucket:        &c.bucket,
		Key:           aws.String(c.fullKey(key)),
		UploadId:      &uploadID,
		PartNumber:    aws.Int32(int32(partNumber)),
		ContentLength: aws.Int64(partSize),
	}
	var headerKey string
	if checksumValue != "" {
		switch algo {
		case ChecksumAlgoCRC32C:
			in.ChecksumCRC32C = aws.String(checksumValue)
			headerKey = "x-amz-checksum-crc32c"
		default:
			in.ChecksumSHA256 = aws.String(checksumValue)
			headerKey = "x-amz-checksum-sha256"
		}
	}
	partPresigner := v4.NewSigner(func(o *v4.SignerOptions) {
		o.DisableURIPathEscaping = true
		o.DisableHeaderHoisting = true
	})
	out, err := c.presign.PresignUploadPart(ctx, in,
		s3.WithPresignExpires(ttl),
		func(o *s3.PresignOptions) { o.Presigner = partPresigner },
	)
	if err != nil {
		metricResult = "error"
		return nil, fmt.Errorf("presign upload part: %w", err)
	}
	headers := flattenSignedHeaders(out.SignedHeader)
	if checksumValue != "" {
		headers[headerKey] = checksumValue
	}
	urlResult := &UploadPartURL{
		Number:    partNumber,
		URL:       out.URL,
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

func flattenSignedHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) == 0 {
			continue
		}
		if strings.EqualFold(k, "host") {
			continue
		}
		out[strings.ToLower(k)] = vs[0]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *AWSS3Client) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []Part) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("complete_multipart_upload", result, start) }()

	completed := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		cp := types.CompletedPart{
			PartNumber: aws.Int32(int32(p.Number)),
			ETag:       aws.String(p.ETag),
		}
		if p.ChecksumCRC32C != "" {
			cp.ChecksumCRC32C = aws.String(p.ChecksumCRC32C)
		} else if p.ChecksumSHA256 != "" {
			cp.ChecksumSHA256 = aws.String(p.ChecksumSHA256)
		}
		completed[i] = cp
	}
	_, err := c.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      aws.String(c.fullKey(key)),
		UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	if err != nil {
		result = "error"
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (c *AWSS3Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("abort_multipart_upload", result, start) }()

	_, err := c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      aws.String(c.fullKey(key)),
		UploadId: &uploadID,
	})
	if err != nil {
		result = "error"
		return fmt.Errorf("abort multipart upload: %w", err)
	}
	return nil
}

func (c *AWSS3Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("list_parts", result, start) }()

	var parts []Part
	var partMarker *string

	for {
		out, err := c.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           &c.bucket,
			Key:              aws.String(c.fullKey(key)),
			UploadId:         &uploadID,
			PartNumberMarker: partMarker,
		})
		if err != nil {
			result = "error"
			return nil, fmt.Errorf("list parts: %w", err)
		}
		for _, p := range out.Parts {
			parts = append(parts, Part{
				Number:         int(aws.ToInt32(p.PartNumber)),
				Size:           aws.ToInt64(p.Size),
				ETag:           aws.ToString(p.ETag),
				ChecksumSHA256: aws.ToString(p.ChecksumSHA256),
				ChecksumCRC32C: aws.ToString(p.ChecksumCRC32C),
			})
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		partMarker = out.NextPartNumberMarker
	}
	return parts, nil
}

func (c *AWSS3Client) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("presign_get_object", result, start) }()

	if ttl > DownloadTTL {
		ttl = DownloadTTL
	}
	out, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		result = "error"
		return "", fmt.Errorf("presign get object: %w", err)
	}
	return out.URL, nil
}

func (c *AWSS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64, encOpts EncryptionOpts) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("put_object", result, start) }()

	in := &s3.PutObjectInput{
		Bucket:        &c.bucket,
		Key:           aws.String(c.fullKey(key)),
		Body:          body,
		ContentLength: aws.Int64(size),
	}
	if err := applyEncryptionToPutObjectInput(in, encOpts); err != nil {
		result = "error"
		return err
	}
	_, err := c.client.PutObject(ctx, in)
	if err != nil {
		result = "error"
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func applyEncryptionToCreateMultipartUploadInput(in *s3.CreateMultipartUploadInput, encOpts EncryptionOpts) error {
	fields, err := awsEncryptionFields(encOpts)
	if err != nil {
		return err
	}
	if fields.serverSideEncryption == "" {
		return nil
	}
	in.ServerSideEncryption = fields.serverSideEncryption
	in.SSEKMSKeyId = fields.kmsKeyID
	in.BucketKeyEnabled = fields.bucketKeyEnabled
	in.SSEKMSEncryptionContext = fields.encryptionContext
	return nil
}

func applyEncryptionToPutObjectInput(in *s3.PutObjectInput, encOpts EncryptionOpts) error {
	fields, err := awsEncryptionFields(encOpts)
	if err != nil {
		return err
	}
	if fields.serverSideEncryption == "" {
		return nil
	}
	in.ServerSideEncryption = fields.serverSideEncryption
	in.SSEKMSKeyId = fields.kmsKeyID
	in.BucketKeyEnabled = fields.bucketKeyEnabled
	in.SSEKMSEncryptionContext = fields.encryptionContext
	return nil
}

type awsEncryptionFieldSet struct {
	serverSideEncryption types.ServerSideEncryption
	kmsKeyID             *string
	bucketKeyEnabled     *bool
	encryptionContext    *string
}

func awsEncryptionFields(encOpts EncryptionOpts) (awsEncryptionFieldSet, error) {
	var fields awsEncryptionFieldSet
	switch encOpts.Mode {
	case "", EncryptionModeLegacy, EncryptionModeNone:
		if err := rejectUnusedEncryptionFields(encOpts); err != nil {
			return fields, err
		}
		return fields, nil
	case EncryptionModeSSES3:
		if err := rejectUnusedEncryptionFields(encOpts); err != nil {
			return fields, err
		}
		fields.serverSideEncryption = types.ServerSideEncryptionAes256
		return fields, nil
	case EncryptionModeSSEKMS:
		if encOpts.KMSKeyID == "" {
			return fields, fmt.Errorf("sse-kms encryption requires KMS key ID")
		}
		fields.serverSideEncryption = types.ServerSideEncryptionAwsKms
		fields.kmsKeyID = aws.String(encOpts.KMSKeyID)
		fields.bucketKeyEnabled = aws.Bool(encOpts.BucketKeyEnabled)
	case EncryptionModeDSSEKMS:
		if encOpts.KMSKeyID == "" {
			return fields, fmt.Errorf("dsse-kms encryption requires KMS key ID")
		}
		if encOpts.BucketKeyEnabled {
			return fields, fmt.Errorf("bucket key is not supported for dsse-kms encryption")
		}
		fields.serverSideEncryption = types.ServerSideEncryptionAwsKmsDsse
		fields.kmsKeyID = aws.String(encOpts.KMSKeyID)
	default:
		return fields, fmt.Errorf("unsupported encryption mode: %q", encOpts.Mode)
	}
	contextValue, err := encodeKMSEncryptionContext(encOpts.EncryptionContext)
	if err != nil {
		return fields, err
	}
	fields.encryptionContext = contextValue
	return fields, nil
}

func rejectUnusedEncryptionFields(encOpts EncryptionOpts) error {
	if encOpts.KMSKeyID != "" {
		return fmt.Errorf("KMS key ID is not supported for %s encryption", noEncryptionModeLabel(encOpts.Mode))
	}
	if encOpts.BucketKeyEnabled {
		return fmt.Errorf("bucket key is not supported for %s encryption", noEncryptionModeLabel(encOpts.Mode))
	}
	if len(encOpts.EncryptionContext) != 0 {
		return fmt.Errorf("encryption context is not supported for %s encryption", noEncryptionModeLabel(encOpts.Mode))
	}
	return nil
}

func noEncryptionModeLabel(mode EncryptionMode) string {
	if mode == "" {
		return "zero-value"
	}
	return string(mode)
}

func encodeKMSEncryptionContext(contextMap map[string]string) (*string, error) {
	if len(contextMap) == 0 {
		return nil, nil
	}
	payload, err := json.Marshal(contextMap)
	if err != nil {
		return nil, fmt.Errorf("encode KMS encryption context: %w", err)
	}
	return aws.String(base64.StdEncoding.EncodeToString(payload)), nil
}

func (c *AWSS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("get_object", result, start) }()

	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		result = "error"
		return nil, fmt.Errorf("get object: %w", err)
	}
	return out.Body, nil
}

func (c *AWSS3Client) DeleteObject(ctx context.Context, key string) error {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("delete_object", result, start) }()

	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		result = "error"
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (c *AWSS3Client) UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("upload_part_copy", result, start) }()

	copySource := fmt.Sprintf("%s/%s", c.bucket, c.fullKey(sourceKey))
	copyRange := fmt.Sprintf("bytes=%d-%d", startByte, endByte)

	out, err := c.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:          &c.bucket,
		Key:             aws.String(c.fullKey(destKey)),
		UploadId:        &uploadID,
		PartNumber:      aws.Int32(int32(partNumber)),
		CopySource:      aws.String(copySource),
		CopySourceRange: aws.String(copyRange),
	})
	if err != nil {
		result = "error"
		return "", fmt.Errorf("upload part copy: %w", err)
	}
	return aws.ToString(out.CopyPartResult.ETag), nil
}

func (c *AWSS3Client) PresignGetObjectRange(ctx context.Context, key string, startByte, endByte int64, ttl time.Duration) (string, error) {
	start := time.Now()
	result := "ok"
	defer func() { recordS3Operation("presign_get_object_range", result, start) }()

	if ttl > DownloadTTL {
		ttl = DownloadTTL
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
	out, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
		Range:  aws.String(rangeHeader),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		result = "error"
		return "", fmt.Errorf("presign get object range: %w", err)
	}
	return out.URL, nil
}

var _ S3Client = (*AWSS3Client)(nil)
