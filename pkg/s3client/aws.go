package s3client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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
	Region  string
	Bucket  string
	Prefix  string // key prefix, e.g. "tenants/<id>/" — keys already contain "blobs/"
	RoleARN string
}

func NewAWS(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
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

	client := s3.NewFromConfig(awsCfg)
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

func (c *AWSS3Client) CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error) {
	out, err := c.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            &c.bucket,
		Key:               aws.String(c.fullKey(key)),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}
	return &MultipartUpload{
		UploadID: aws.ToString(out.UploadId),
		Key:      key,
	}, nil
}

func (c *AWSS3Client) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, checksumSHA256 string, ttl time.Duration) (*UploadPartURL, error) {
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
	if checksumSHA256 != "" {
		in.ChecksumSHA256 = aws.String(checksumSHA256)
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
		return nil, fmt.Errorf("presign upload part: %w", err)
	}
	headers := flattenSignedHeaders(out.SignedHeader)
	if checksumSHA256 != "" {
		headers[strings.ToLower("x-amz-checksum-sha256")] = checksumSHA256
	}
	return &UploadPartURL{
		Number:         partNumber,
		URL:            out.URL,
		Size:           partSize,
		ChecksumSHA256: checksumSHA256,
		Headers:        headers,
		ExpiresAt:      time.Now().Add(ttl),
	}, nil
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
	completed := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		cp := types.CompletedPart{
			PartNumber: aws.Int32(int32(p.Number)),
			ETag:       aws.String(p.ETag),
		}
		if p.ChecksumSHA256 != "" {
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
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (c *AWSS3Client) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	_, err := c.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      aws.String(c.fullKey(key)),
		UploadId: &uploadID,
	})
	if err != nil {
		return fmt.Errorf("abort multipart upload: %w", err)
	}
	return nil
}

func (c *AWSS3Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
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
			return nil, fmt.Errorf("list parts: %w", err)
		}
		for _, p := range out.Parts {
			parts = append(parts, Part{
				Number:         int(aws.ToInt32(p.PartNumber)),
				Size:           aws.ToInt64(p.Size),
				ETag:           aws.ToString(p.ETag),
				ChecksumSHA256: aws.ToString(p.ChecksumSHA256),
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
	if ttl > DownloadTTL {
		ttl = DownloadTTL
	}
	out, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign get object: %w", err)
	}
	return out.URL, nil
}

func (c *AWSS3Client) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &c.bucket,
		Key:           aws.String(c.fullKey(key)),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (c *AWSS3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	return out.Body, nil
}

func (c *AWSS3Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(c.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (c *AWSS3Client) UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error) {
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
		return "", fmt.Errorf("upload part copy: %w", err)
	}
	return aws.ToString(out.CopyPartResult.ETag), nil
}

func (c *AWSS3Client) PresignGetObjectRange(ctx context.Context, key string, startByte, endByte int64, ttl time.Duration) (string, error) {
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
		return "", fmt.Errorf("presign get object range: %w", err)
	}
	return out.URL, nil
}

var _ S3Client = (*AWSS3Client)(nil)
