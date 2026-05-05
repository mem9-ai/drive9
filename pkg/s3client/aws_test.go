package s3client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestAWSConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     AWSConfig
		wantErr bool
	}{
		{
			name:    "missing bucket",
			cfg:     AWSConfig{},
			wantErr: true,
		},
		{
			name: "access key without secret",
			cfg: AWSConfig{
				Bucket:      "bucket",
				AccessKeyID: "ak",
			},
			wantErr: true,
		},
		{
			name: "secret without access key",
			cfg: AWSConfig{
				Bucket:          "bucket",
				SecretAccessKey: "sk",
			},
			wantErr: true,
		},
		{
			name: "session token without static credentials",
			cfg: AWSConfig{
				Bucket:       "bucket",
				SessionToken: "token",
			},
			wantErr: true,
		},
		{
			name: "static credentials",
			cfg: AWSConfig{
				Bucket:          "bucket",
				AccessKeyID:     "ak",
				SecretAccessKey: "sk",
			},
		},
		{
			name: "static credentials with session token",
			cfg: AWSConfig{
				Bucket:          "bucket",
				AccessKeyID:     "ak",
				SecretAccessKey: "sk",
				SessionToken:    "token",
			},
		},
		{
			name: "default credentials chain",
			cfg: AWSConfig{
				Bucket: "bucket",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStaticCredentialsProviderFromAWSConfig(t *testing.T) {
	provider, ok, err := staticCredentialsProvider(AWSConfig{
		AccessKeyID:     "test-ak",
		SecretAccessKey: "test-sk",
		SessionToken:    "test-token",
	})
	if err != nil {
		t.Fatalf("staticCredentialsProvider() error = %v", err)
	}
	if !ok {
		t.Fatal("staticCredentialsProvider() ok = false, want true")
	}

	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "test-ak" {
		t.Fatalf("AccessKeyID = %q, want %q", creds.AccessKeyID, "test-ak")
	}
	if creds.SecretAccessKey != "test-sk" {
		t.Fatalf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "test-sk")
	}
	if creds.SessionToken != "test-token" {
		t.Fatalf("SessionToken = %q, want %q", creds.SessionToken, "test-token")
	}

	provider, ok, err = staticCredentialsProvider(AWSConfig{})
	if err != nil {
		t.Fatalf("staticCredentialsProvider(empty) error = %v", err)
	}
	if ok {
		t.Fatal("staticCredentialsProvider(empty) ok = true, want false")
	}
	if provider != nil {
		t.Fatalf("staticCredentialsProvider(empty) provider = %#v, want nil", provider)
	}
}

func TestS3LogValueHelpers(t *testing.T) {
	if got := CredentialLogValue("ak"); got != "static" {
		t.Fatalf("CredentialLogValue(static) = %q, want %q", got, "static")
	}
	if got := CredentialLogValue(""); got != "default-credentials" {
		t.Fatalf("CredentialLogValue(default) = %q, want %q", got, "default-credentials")
	}
	if got := RoleLogValue(""); got != "none" {
		t.Fatalf("RoleLogValue(empty) = %q, want %q", got, "none")
	}
	if got := RoleLogValue("arn:aws:iam::123456789012:role/test"); got != "arn:aws:iam::123456789012:role/test" {
		t.Fatalf("RoleLogValue(arn) = %q, want %q", got, "arn:aws:iam::123456789012:role/test")
	}
}

func TestApplyS3Options(t *testing.T) {
	opts := s3.Options{}
	applyS3Options(AWSConfig{
		Endpoint:       "https://minio.example:9000",
		ForcePathStyle: true,
	})(&opts)

	if opts.BaseEndpoint == nil {
		t.Fatal("BaseEndpoint = nil, want non-nil")
	}
	if got := *opts.BaseEndpoint; got != "https://minio.example:9000" {
		t.Fatalf("BaseEndpoint = %q, want %q", got, "https://minio.example:9000")
	}
	if !opts.UsePathStyle {
		t.Fatal("UsePathStyle = false, want true")
	}

	opts = s3.Options{}
	applyS3Options(AWSConfig{})(&opts)
	if opts.BaseEndpoint != nil {
		t.Fatal("BaseEndpoint != nil, want nil")
	}
	if opts.UsePathStyle {
		t.Fatal("UsePathStyle = true, want false")
	}
}

func TestApplyEncryptionZeroValueNoop(t *testing.T) {
	putInput := &s3.PutObjectInput{}
	if err := applyEncryptionToPutObjectInput(putInput, EncryptionOpts{}); err != nil {
		t.Fatalf("apply put encryption error = %v", err)
	}
	assertNoEncryptionHeaders(t, putInput.ServerSideEncryption, putInput.SSEKMSKeyId, putInput.BucketKeyEnabled, putInput.SSEKMSEncryptionContext)

	createInput := &s3.CreateMultipartUploadInput{}
	if err := applyEncryptionToCreateMultipartUploadInput(createInput, EncryptionOpts{}); err != nil {
		t.Fatalf("apply create encryption error = %v", err)
	}
	assertNoEncryptionHeaders(t, createInput.ServerSideEncryption, createInput.SSEKMSKeyId, createInput.BucketKeyEnabled, createInput.SSEKMSEncryptionContext)
}

func TestApplyEncryptionLegacyAndNoneNoop(t *testing.T) {
	for _, mode := range []EncryptionMode{EncryptionModeLegacy, EncryptionModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			input := &s3.PutObjectInput{}
			err := applyEncryptionToPutObjectInput(input, EncryptionOpts{
				Mode:             mode,
				KMSKeyID:         "ignored",
				BucketKeyEnabled: true,
			})
			if err != nil {
				t.Fatalf("apply encryption error = %v", err)
			}
			assertNoEncryptionHeaders(t, input.ServerSideEncryption, input.SSEKMSKeyId, input.BucketKeyEnabled, input.SSEKMSEncryptionContext)
		})
	}
}

func TestApplyEncryptionSSES3(t *testing.T) {
	input := &s3.PutObjectInput{}
	err := applyEncryptionToPutObjectInput(input, EncryptionOpts{Mode: EncryptionModeSSES3})
	if err != nil {
		t.Fatalf("apply encryption error = %v", err)
	}
	if input.ServerSideEncryption != types.ServerSideEncryptionAes256 {
		t.Fatalf("ServerSideEncryption = %q, want %q", input.ServerSideEncryption, types.ServerSideEncryptionAes256)
	}
	if input.SSEKMSKeyId != nil {
		t.Fatalf("SSEKMSKeyId = %q, want nil", aws.ToString(input.SSEKMSKeyId))
	}
	if input.BucketKeyEnabled != nil {
		t.Fatalf("BucketKeyEnabled = %v, want nil", aws.ToBool(input.BucketKeyEnabled))
	}
	if input.SSEKMSEncryptionContext != nil {
		t.Fatalf("SSEKMSEncryptionContext = %q, want nil", aws.ToString(input.SSEKMSEncryptionContext))
	}
}

func TestApplyEncryptionSSES3RejectsUnsupportedOptions(t *testing.T) {
	tests := []struct {
		name string
		opts EncryptionOpts
	}{
		{name: "bucket key", opts: EncryptionOpts{Mode: EncryptionModeSSES3, BucketKeyEnabled: true}},
		{name: "encryption context", opts: EncryptionOpts{Mode: EncryptionModeSSES3, EncryptionContext: map[string]string{"tenant_id": "tenant-1"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyEncryptionToPutObjectInput(&s3.PutObjectInput{}, tt.opts)
			if err == nil {
				t.Fatal("apply encryption error = nil, want error")
			}
		})
	}
}

func TestApplyEncryptionSSEKMS(t *testing.T) {
	contextMap := map[string]string{
		"tenant_id":  "tenant-1",
		"object_key": "blobs/object",
	}
	opts := EncryptionOpts{
		Mode:              EncryptionModeSSEKMS,
		KMSKeyID:          "arn:aws:kms:ap-southeast-1:123456789012:key/test",
		BucketKeyEnabled:  true,
		EncryptionContext: contextMap,
	}

	putInput := &s3.PutObjectInput{}
	if err := applyEncryptionToPutObjectInput(putInput, opts); err != nil {
		t.Fatalf("apply put encryption error = %v", err)
	}
	assertSSEKMSHeaders(t, putInput.ServerSideEncryption, putInput.SSEKMSKeyId, putInput.BucketKeyEnabled, putInput.SSEKMSEncryptionContext, opts.KMSKeyID, contextMap)

	createInput := &s3.CreateMultipartUploadInput{}
	if err := applyEncryptionToCreateMultipartUploadInput(createInput, opts); err != nil {
		t.Fatalf("apply create encryption error = %v", err)
	}
	assertSSEKMSHeaders(t, createInput.ServerSideEncryption, createInput.SSEKMSKeyId, createInput.BucketKeyEnabled, createInput.SSEKMSEncryptionContext, opts.KMSKeyID, contextMap)
}

func TestApplyEncryptionSSEKMSBucketKeyFalseIsExplicit(t *testing.T) {
	input := &s3.PutObjectInput{}
	err := applyEncryptionToPutObjectInput(input, EncryptionOpts{
		Mode:     EncryptionModeSSEKMS,
		KMSKeyID: "arn:aws:kms:ap-southeast-1:123456789012:key/test",
	})
	if err != nil {
		t.Fatalf("apply encryption error = %v", err)
	}
	if input.BucketKeyEnabled == nil {
		t.Fatal("BucketKeyEnabled = nil, want explicit false")
	}
	if aws.ToBool(input.BucketKeyEnabled) {
		t.Fatal("BucketKeyEnabled = true, want false")
	}
}

func TestApplyEncryptionDSSEKMS(t *testing.T) {
	input := &s3.CreateMultipartUploadInput{}
	err := applyEncryptionToCreateMultipartUploadInput(input, EncryptionOpts{
		Mode:     EncryptionModeDSSEKMS,
		KMSKeyID: "arn:aws:kms:ap-southeast-1:123456789012:key/test",
	})
	if err != nil {
		t.Fatalf("apply encryption error = %v", err)
	}
	if input.ServerSideEncryption != types.ServerSideEncryptionAwsKmsDsse {
		t.Fatalf("ServerSideEncryption = %q, want %q", input.ServerSideEncryption, types.ServerSideEncryptionAwsKmsDsse)
	}
	if aws.ToString(input.SSEKMSKeyId) != "arn:aws:kms:ap-southeast-1:123456789012:key/test" {
		t.Fatalf("SSEKMSKeyId = %q, want test key", aws.ToString(input.SSEKMSKeyId))
	}
	if input.BucketKeyEnabled != nil {
		t.Fatalf("BucketKeyEnabled = %v, want nil", aws.ToBool(input.BucketKeyEnabled))
	}
}

func TestApplyEncryptionRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts EncryptionOpts
	}{
		{name: "sse kms missing key", opts: EncryptionOpts{Mode: EncryptionModeSSEKMS}},
		{name: "dsse kms missing key", opts: EncryptionOpts{Mode: EncryptionModeDSSEKMS}},
		{name: "dsse kms bucket key", opts: EncryptionOpts{Mode: EncryptionModeDSSEKMS, KMSKeyID: "key", BucketKeyEnabled: true}},
		{name: "unknown mode", opts: EncryptionOpts{Mode: EncryptionMode("unknown")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyEncryptionToPutObjectInput(&s3.PutObjectInput{}, tt.opts)
			if err == nil {
				t.Fatal("apply encryption error = nil, want error")
			}
		})
	}
}

func assertNoEncryptionHeaders(t *testing.T, mode types.ServerSideEncryption, keyID *string, bucketKey *bool, contextValue *string) {
	t.Helper()
	if mode != "" {
		t.Fatalf("ServerSideEncryption = %q, want empty", mode)
	}
	if keyID != nil {
		t.Fatalf("SSEKMSKeyId = %q, want nil", aws.ToString(keyID))
	}
	if bucketKey != nil {
		t.Fatalf("BucketKeyEnabled = %v, want nil", aws.ToBool(bucketKey))
	}
	if contextValue != nil {
		t.Fatalf("SSEKMSEncryptionContext = %q, want nil", aws.ToString(contextValue))
	}
}

func assertSSEKMSHeaders(t *testing.T, mode types.ServerSideEncryption, keyID *string, bucketKey *bool, contextValue *string, wantKey string, wantContext map[string]string) {
	t.Helper()
	if mode != types.ServerSideEncryptionAwsKms {
		t.Fatalf("ServerSideEncryption = %q, want %q", mode, types.ServerSideEncryptionAwsKms)
	}
	if aws.ToString(keyID) != wantKey {
		t.Fatalf("SSEKMSKeyId = %q, want %q", aws.ToString(keyID), wantKey)
	}
	if !aws.ToBool(bucketKey) {
		t.Fatal("BucketKeyEnabled = false, want true")
	}
	if contextValue == nil {
		t.Fatal("SSEKMSEncryptionContext = nil, want encoded context")
	}
	decoded, err := base64.StdEncoding.DecodeString(aws.ToString(contextValue))
	if err != nil {
		t.Fatalf("decode context: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Fatalf("unmarshal context: %v", err)
	}
	if len(got) != len(wantContext) {
		t.Fatalf("context len = %d, want %d", len(got), len(wantContext))
	}
	for k, want := range wantContext {
		if got[k] != want {
			t.Fatalf("context[%q] = %q, want %q", k, got[k], want)
		}
	}
}
