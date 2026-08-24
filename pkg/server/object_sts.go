package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	alists "github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tcprofile "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	tcsts "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
	"golang.org/x/oauth2/google"

	"github.com/mem9-ai/drive9/pkg/meta"
)

var (
	errNoObjectBackend        = errors.New("no object backend is configured for this bucket")
	errURIOutsideNamespace    = errors.New("uri is outside the tenant object namespace")
	errAmbiguousObjectBackend = errors.New("multiple object backends match this bucket; pass ?endpoint= to disambiguate")
)

type objectMintTarget struct {
	Backend *meta.OrgObjectBackend
	Allowed string
}

func (s *Server) mintObjectSession(ctx context.Context, backend *meta.OrgObjectBackend, prefix string, write bool) (*objectCredentialsResponse, error) {
	secret := ""
	if len(backend.SecretCipher) > 0 {
		plain, err := s.pool.Decrypt(ctx, backend.SecretCipher)
		if err != nil {
			return nil, fmt.Errorf("decrypt object backend secret: %w", err)
		}
		secret = string(plain)
	}
	externalID := ""
	if len(backend.ExternalIDCipher) > 0 {
		plain, err := s.pool.Decrypt(ctx, backend.ExternalIDCipher)
		if err != nil {
			return nil, fmt.Errorf("decrypt object backend external id: %w", err)
		}
		externalID = string(plain)
	}
	ttl := objectSessionTTL(backend.MaxSessionTTLSec)
	creds, err := mintObjectSessionWithSecret(ctx, backend, secret, externalID, prefix, write, ttl)
	if err != nil {
		return nil, err
	}
	creds.Endpoint = backend.Endpoint
	creds.Region = backend.Region
	creds.ForcePathStyle = backend.ForcePathStyle
	creds.Scheme = backend.Scheme
	creds.Bucket = backend.Bucket
	creds.Prefix = prefix
	if backend.AccountID != "" && creds.Account == "" {
		creds.Account = backend.AccountID
	}
	return creds, nil
}

func mintObjectSessionWithSecret(ctx context.Context, backend *meta.OrgObjectBackend, secret, externalID, prefix string, write bool, ttl int) (*objectCredentialsResponse, error) {
	if err := validateSTSEndpoint(backend.STSEndpoint); err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(backend.Scheme)) {
	case "s3":
		return mintAWSS3Session(ctx, backend, secret, externalID, prefix, write, ttl)
	case "cos":
		return mintTencentCOSSession(ctx, backend, secret, externalID, prefix, write, ttl)
	case "tos":
		return mintVolcengineTOSSession(ctx, backend, secret, externalID, prefix, write, ttl)
	case "oss":
		return mintAliyunOSSSession(ctx, backend, secret, externalID, prefix, write, ttl)
	case "az":
		return mintAzureSession(backend, secret, write, ttl)
	case "gs":
		return mintGCSSession(ctx, secret, write)
	default:
		return nil, fmt.Errorf("unsupported object scheme %q", backend.Scheme)
	}
}

func objectSessionTTL(sec int) int {
	if sec <= 0 {
		return 3600
	}
	if sec > 43200 {
		return 43200
	}
	return sec
}

func mintAWSS3Session(ctx context.Context, backend *meta.OrgObjectBackend, secret, externalID, prefix string, write bool, ttl int) (*objectCredentialsResponse, error) {
	policy := awsSessionPolicy(backend.Bucket, prefix, write)
	region := strings.TrimSpace(backend.Region)
	if region == "" {
		region = "us-east-1"
	}
	var cfg aws.Config
	var err error
	if strings.TrimSpace(backend.AccessKeyID) == "" {
		if backend.CredentialKind != meta.ObjectCredentialRole {
			return nil, fmt.Errorf("object backend is missing access_key_id")
		}
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(backend.AccessKeyID, secret, "")),
			config.WithRegion(region))
	}
	if err != nil {
		return nil, fmt.Errorf("load sts config: %w", err)
	}
	cli := awssts.NewFromConfig(cfg, awsSTSClientOptions(backend.STSEndpoint)...)
	var ak, sk, tok string
	var exp *time.Time
	switch backend.CredentialKind {
	case meta.ObjectCredentialRole:
		in := &awssts.AssumeRoleInput{
			RoleArn:         aws.String(backend.RoleARN),
			RoleSessionName: aws.String("drive9-object"),
			DurationSeconds: aws.Int32(int32(ttl)),
			Policy:          aws.String(policy),
		}
		if externalID != "" {
			in.ExternalId = aws.String(externalID)
		}
		out, err := cli.AssumeRole(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("assume role: %w", err)
		}
		ak = aws.ToString(out.Credentials.AccessKeyId)
		sk = aws.ToString(out.Credentials.SecretAccessKey)
		tok = aws.ToString(out.Credentials.SessionToken)
		if out.Credentials.Expiration != nil {
			t := out.Credentials.Expiration.UTC()
			exp = &t
		}
	default:
		out, err := cli.GetFederationToken(ctx, &awssts.GetFederationTokenInput{
			Name:            aws.String("drive9-object"),
			DurationSeconds: aws.Int32(int32(ttl)),
			Policy:          aws.String(policy),
		})
		if err != nil {
			return nil, fmt.Errorf("get federation token: %w", err)
		}
		ak = aws.ToString(out.Credentials.AccessKeyId)
		sk = aws.ToString(out.Credentials.SecretAccessKey)
		tok = aws.ToString(out.Credentials.SessionToken)
		if out.Credentials.Expiration != nil {
			t := out.Credentials.Expiration.UTC()
			exp = &t
		}
	}
	return hmacSession(ak, sk, tok, exp), nil
}

func awsSTSClientOptions(stsEndpoint string) []func(*awssts.Options) {
	ep := strings.TrimSpace(stsEndpoint)
	if ep == "" {
		return nil
	}
	return []func(*awssts.Options){func(o *awssts.Options) {
		o.BaseEndpoint = aws.String(ep)
	}}
}

func mintTencentCOSSession(ctx context.Context, backend *meta.OrgObjectBackend, secret, externalID, prefix string, write bool, ttl int) (*objectCredentialsResponse, error) {
	if strings.TrimSpace(backend.AccessKeyID) == "" || secret == "" {
		return nil, fmt.Errorf("cos mint requires access_key_id and secret_access_key")
	}
	appID, err := cosAppID(backend.AccountID, backend.Bucket)
	if err != nil {
		return nil, err
	}
	region := strings.TrimSpace(backend.Region)
	if region == "" {
		return nil, fmt.Errorf("cos mint requires region")
	}
	policy := cosSessionPolicy(region, appID, backend.Bucket, prefix, write)
	cpf := tcprofile.NewClientProfile()
	if ep := stsHost(backend.STSEndpoint); ep != "" {
		cpf.HttpProfile.Endpoint = ep
	}
	cli, err := tcsts.NewClient(tccommon.NewCredential(backend.AccessKeyID, secret), region, cpf)
	if err != nil {
		return nil, fmt.Errorf("create tencent sts client: %w", err)
	}
	if backend.CredentialKind == meta.ObjectCredentialRole || strings.TrimSpace(backend.RoleARN) != "" {
		if strings.TrimSpace(backend.RoleARN) == "" {
			return nil, fmt.Errorf("cos role mint requires role_arn")
		}
		req := tcsts.NewAssumeRoleRequest()
		req.RoleArn = tccommon.StringPtr(backend.RoleARN)
		req.RoleSessionName = tccommon.StringPtr("drive9object")
		req.DurationSeconds = tccommon.Uint64Ptr(uint64(ttl))
		req.Policy = tccommon.StringPtr(policy)
		if externalID != "" {
			req.ExternalId = tccommon.StringPtr(externalID)
		}
		out, err := cli.AssumeRoleWithContext(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("tencent assume role: %w", err)
		}
		return tencentCredentials(out.Response.Credentials, out.Response.Expiration)
	}
	req := tcsts.NewGetFederationTokenRequest()
	req.Name = tccommon.StringPtr("drive9object")
	req.DurationSeconds = tccommon.Uint64Ptr(uint64(ttl))
	req.Policy = tccommon.StringPtr(policy)
	out, err := cli.GetFederationTokenWithContext(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("tencent get federation token: %w", err)
	}
	return tencentCredentials(out.Response.Credentials, out.Response.Expiration)
}

func tencentCredentials(creds *tcsts.Credentials, expiration *string) (*objectCredentialsResponse, error) {
	if creds == nil {
		return nil, fmt.Errorf("tencent sts returned empty credentials")
	}
	var exp *time.Time
	if expiration != nil && strings.TrimSpace(*expiration) != "" {
		if t, err := time.Parse(time.RFC3339, strings.TrimSpace(*expiration)); err == nil {
			t = t.UTC()
			exp = &t
		}
	}
	return hmacSession(derefString(creds.TmpSecretId), derefString(creds.TmpSecretKey), derefString(creds.Token), exp), nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func mintAliyunOSSSession(ctx context.Context, backend *meta.OrgObjectBackend, secret, externalID, prefix string, write bool, ttl int) (*objectCredentialsResponse, error) {
	_ = ctx
	if strings.TrimSpace(backend.RoleARN) == "" {
		return nil, fmt.Errorf("oss mint requires role_arn (RAM AssumeRole); OSS has no GetFederationToken")
	}
	if strings.TrimSpace(backend.AccessKeyID) == "" || secret == "" {
		return nil, fmt.Errorf("oss mint requires access_key_id and secret_access_key to call AssumeRole")
	}
	region := strings.TrimSpace(backend.Region)
	if region == "" {
		region = "cn-hangzhou"
	}
	cli, err := alists.NewClientWithAccessKey(region, backend.AccessKeyID, secret)
	if err != nil {
		return nil, fmt.Errorf("create aliyun sts client: %w", err)
	}
	if ep := stsHost(backend.STSEndpoint); ep != "" {
		cli.Domain = ep
	}
	req := alists.CreateAssumeRoleRequest()
	req.Scheme = "https"
	req.RoleArn = backend.RoleARN
	req.RoleSessionName = "drive9-object"
	req.DurationSeconds = requests.NewInteger(ttl)
	req.Policy = ossSessionPolicy(backend.Bucket, prefix, write)
	if externalID != "" {
		req.ExternalId = externalID
	}
	out, err := cli.AssumeRole(req)
	if err != nil {
		return nil, fmt.Errorf("aliyun assume role: %w", err)
	}
	var exp *time.Time
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(out.Credentials.Expiration)); err == nil {
		t = t.UTC()
		exp = &t
	}
	return hmacSession(out.Credentials.AccessKeyId, out.Credentials.AccessKeySecret, out.Credentials.SecurityToken, exp), nil
}

func mintAzureSession(backend *meta.OrgObjectBackend, secret string, write bool, ttl int) (*objectCredentialsResponse, error) {
	account := strings.TrimSpace(backend.AccountID)
	if account == "" {
		account = strings.TrimSpace(backend.AccessKeyID)
	}
	if account == "" || secret == "" {
		return nil, fmt.Errorf("azure mint requires storage account name and account key")
	}
	if strings.TrimSpace(backend.Prefix) != "" {
		return nil, fmt.Errorf("azure minted credentials are container-scoped; backend prefix must be empty (one container per tenant)")
	}
	cred, err := azblob.NewSharedKeyCredential(account, secret)
	if err != nil {
		return nil, fmt.Errorf("azure shared key: %w", err)
	}
	perms := sas.ContainerPermissions{Read: true, List: true}
	if write {
		perms.Add = true
		perms.Create = true
		perms.Write = true
		perms.Delete = true
	}
	now := time.Now().UTC()
	values := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now.Add(-1 * time.Minute),
		ExpiryTime:    now.Add(time.Duration(ttl) * time.Second),
		Permissions:   perms.String(),
		ContainerName: backend.Bucket,
	}
	qp, err := values.SignWithSharedKey(cred)
	if err != nil {
		return nil, fmt.Errorf("sign azure sas: %w", err)
	}
	base := strings.TrimRight(strings.TrimSpace(backend.Endpoint), "/")
	if base == "" {
		base = fmt.Sprintf("https://%s.blob.core.windows.net", account)
	}
	sasURL := fmt.Sprintf("%s/%s?%s", base, url.PathEscape(backend.Bucket), qp.Encode())
	exp := values.ExpiryTime
	return &objectCredentialsResponse{
		SASURL:     sasURL,
		Account:    account,
		Expiration: exp.Format(time.RFC3339),
	}, nil
}

func mintGCSSession(ctx context.Context, secret string, write bool) (*objectCredentialsResponse, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("gcs mint requires a service-account JSON key")
	}
	scope := "https://www.googleapis.com/auth/devstorage.read_only"
	if write {
		scope = "https://www.googleapis.com/auth/devstorage.read_write"
	}
	conf, err := google.JWTConfigFromJSON([]byte(secret), scope)
	if err != nil {
		return nil, fmt.Errorf("parse gcs service account json: %w", err)
	}
	tok, err := conf.TokenSource(ctx).Token()
	if err != nil {
		return nil, fmt.Errorf("mint gcs access token: %w", err)
	}
	resp := &objectCredentialsResponse{AccessToken: tok.AccessToken}
	if !tok.Expiry.IsZero() {
		resp.Expiration = tok.Expiry.UTC().Format(time.RFC3339)
	}
	return resp, nil
}

func hmacSession(ak, sk, tok string, exp *time.Time) *objectCredentialsResponse {
	resp := &objectCredentialsResponse{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		SessionToken:    tok,
	}
	if exp != nil {
		resp.Expiration = exp.Format(time.RFC3339)
	}
	return resp
}

func matchOrgObjectBackend(rows []meta.OrgObjectBackend, scheme, bucket, key, endpoint, ns string) (*objectMintTarget, error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	bucket = strings.TrimSpace(bucket)
	key = strings.Trim(key, "/")
	endpoint = normalizeObjectEndpoint(endpoint)
	ns = strings.Trim(ns, "/")
	if ns == "" {
		return nil, fmt.Errorf("object namespace is not configured for this tenant")
	}
	if len(rows) == 0 {
		return nil, errNoObjectBackend
	}

	pick := func(requireEmptyEndpoint bool, requireEndpoint string) []objectMintTarget {
		var out []objectMintTarget
		for i := range rows {
			b := &rows[i]
			if strings.ToLower(b.Scheme) != scheme || b.Bucket != bucket {
				continue
			}
			be := normalizeObjectEndpoint(b.Endpoint)
			if requireEmptyEndpoint && be != "" {
				continue
			}
			if requireEndpoint != "" && be != requireEndpoint {
				continue
			}
			allowed, ok := backendAllowedPrefix(scheme, bucket, ns, b)
			if !ok {
				continue
			}
			if !objectURIInScope(scheme, bucket, key, ns, b) {
				continue
			}
			out = append(out, objectMintTarget{Backend: b, Allowed: allowed})
		}
		return out
	}

	var candidates []objectMintTarget
	if endpoint != "" {
		candidates = pick(false, endpoint)
	} else {
		candidates = pick(true, "")
		if len(candidates) == 0 {
			fallback := pick(false, "")
			seen := map[string]struct{}{}
			for _, c := range fallback {
				seen[normalizeObjectEndpoint(c.Backend.Endpoint)] = struct{}{}
			}
			if len(seen) > 1 {
				return nil, errAmbiguousObjectBackend
			}
			candidates = fallback
		}
	}
	if len(candidates) == 0 {
		if anyBucketMatch(rows, scheme, bucket) {
			return nil, errURIOutsideNamespace
		}
		return nil, errNoObjectBackend
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if len(c.Backend.Prefix) > len(best.Backend.Prefix) {
			best = c
		}
	}
	return &best, nil
}

func anyBucketMatch(rows []meta.OrgObjectBackend, scheme, bucket string) bool {
	for i := range rows {
		if strings.ToLower(rows[i].Scheme) == scheme && rows[i].Bucket == bucket {
			return true
		}
	}
	return false
}

func backendAllowedPrefix(scheme, bucket, ns string, b *meta.OrgObjectBackend) (string, bool) {
	switch scheme {
	case "az", "gs":
		if strings.TrimSpace(b.Prefix) != "" || b.Bucket != ns {
			return "", false
		}
		return "", true
	default:
		allowed := ns
		if b.Prefix != "" {
			allowed = strings.Trim(b.Prefix, "/") + "/" + ns
		}
		_ = bucket
		return allowed, true
	}
}

func objectURIInScope(scheme, bucket, key, ns string, b *meta.OrgObjectBackend) bool {
	switch scheme {
	case "az", "gs":
		return strings.TrimSpace(b.Prefix) == "" && b.Bucket == ns
	default:
		allowed := ns
		if b.Prefix != "" {
			allowed = strings.Trim(b.Prefix, "/") + "/" + ns
		}
		_ = bucket
		return objectKeyInNamespace(key, allowed)
	}
}

func validateSTSEndpoint(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("sts_endpoint must be an absolute URL")
	}
	if u.User != nil {
		return fmt.Errorf("sts_endpoint must not contain userinfo")
	}
	host := strings.ToLower(u.Hostname())
	if isBlockedSTSHost(host) {
		return fmt.Errorf("sts_endpoint host is not allowed")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if host == "127.0.0.1" || host == "localhost" || host == "::1" {
			return nil
		}
		return fmt.Errorf("sts_endpoint must use https:// (http:// is only allowed for loopback)")
	default:
		return fmt.Errorf("sts_endpoint scheme %q is not allowed", u.Scheme)
	}
}

func isBlockedSTSHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	switch host {
	case "169.254.169.254", "metadata.google.internal", "metadata", "metadata.google.com":
		return true
	}
	return strings.HasPrefix(host, "169.254.") || strings.HasPrefix(host, "fe80:")
}

func normalizeObjectEndpoint(raw string) string {
	return meta.NormalizeObjectStoreEndpoint(raw)
}

func stsHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err == nil && u.Host != "" {
		return u.Host
	}
	return strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
}

func cosAppID(accountID, bucket string) (string, error) {
	if id := strings.TrimSpace(accountID); id != "" {
		return id, nil
	}
	i := strings.LastIndex(bucket, "-")
	if i > 0 && i < len(bucket)-1 {
		suffix := bucket[i+1:]
		if isAllDigits(suffix) && len(suffix) >= 8 {
			return suffix, nil
		}
	}
	return "", fmt.Errorf("cos mint requires account_id (Tencent APPID)")
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func awsSessionPolicy(bucket, prefix string, write bool) string {
	return objectSessionPolicy(bucket, prefix, write)
}

func cosSessionPolicy(region, appID, bucket, prefix string, write bool) string {
	prefix = strings.Trim(prefix, "/")
	obj := []string{`"name/cos:GetObject"`, `"name/cos:HeadObject"`, `"name/cos:GetObjectTagging"`}
	if write {
		obj = append(obj, `"name/cos:PutObject"`, `"name/cos:PostObject"`, `"name/cos:DeleteObject"`,
			`"name/cos:InitiateMultipartUpload"`, `"name/cos:ListMultipartUploads"`, `"name/cos:ListParts"`,
			`"name/cos:UploadPart"`, `"name/cos:CompleteMultipartUpload"`, `"name/cos:AbortMultipartUpload"`)
	}
	objARN := fmt.Sprintf("qcs::cos:%s:uid/%s:%s/%s/*", region, appID, bucket, prefix)
	bucketARN := fmt.Sprintf("qcs::cos:%s:uid/%s:%s", region, appID, bucket)
	return fmt.Sprintf(`{"version":"2.0","statement":[{"effect":"allow","action":[%s],"resource":[%q]},{"effect":"allow","action":["name/cos:GetBucket","name/cos:HeadBucket"],"resource":[%q],"condition":{"string_like":{"cos:prefix":[%q,%q]}}}]}`,
		strings.Join(obj, ","), objARN, bucketARN, prefix, prefix+"/*")
}

func ossSessionPolicy(bucket, prefix string, write bool) string {
	prefix = strings.Trim(prefix, "/")
	actions := []string{`"oss:GetObject"`, `"oss:GetObjectMeta"`, `"oss:HeadObject"`}
	if write {
		actions = append(actions, `"oss:PutObject"`, `"oss:DeleteObject"`, `"oss:InitiateMultipartUpload"`,
			`"oss:UploadPart"`, `"oss:CompleteMultipartUpload"`, `"oss:AbortMultipartUpload"`,
			`"oss:ListParts"`, `"oss:ListMultipartUploads"`)
	}
	objARN := fmt.Sprintf("acs:oss:*:*:%s/%s/*", bucket, prefix)
	bucketARN := fmt.Sprintf("acs:oss:*:*:%s", bucket)
	return fmt.Sprintf(`{"Version":"1","Statement":[{"Effect":"Allow","Action":[%s],"Resource":[%q]},{"Effect":"Allow","Action":["oss:ListObjects","oss:GetBucket"],"Resource":[%q],"Condition":{"StringLike":{"oss:Prefix":[%q,%q]}}}]}`,
		strings.Join(actions, ","), objARN, bucketARN, prefix, prefix+"/*")
}

func tosSessionPolicy(bucket, prefix string, write bool) string {
	prefix = strings.Trim(prefix, "/")
	actions := []string{`"tos:GetObject"`, `"tos:HeadObject"`, `"tos:GetObjectMeta"`}
	if write {
		actions = append(actions, `"tos:PutObject"`, `"tos:DeleteObject"`, `"tos:AbortMultipartUpload"`,
			`"tos:ListMultipartUploadParts"`, `"tos:CreateMultipartUpload"`, `"tos:UploadPart"`, `"tos:CompleteMultipartUpload"`)
	}
	objARN := fmt.Sprintf("trn:tos:::%s/%s/*", bucket, prefix)
	bucketARN := fmt.Sprintf("trn:tos:::%s", bucket)
	return fmt.Sprintf(`{"Statement":[{"Effect":"Allow","Action":[%s],"Resource":[%q]},{"Effect":"Allow","Action":["tos:ListBucket"],"Resource":[%q],"Condition":{"StringLike":{"tos:prefix":[%q,%q]}}}]}`,
		strings.Join(actions, ","), objARN, bucketARN, prefix, prefix+"/*")
}

func canonicalObjectScheme(scheme string) string {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	switch scheme {
	case "gcs":
		return "gs"
	case "azure":
		return "az"
	default:
		return scheme
	}
}

func validateAdminObjectBackend(rec *meta.OrgObjectBackend) error {
	if rec == nil {
		return fmt.Errorf("org object backend is required")
	}
	rec.Scheme = canonicalObjectScheme(rec.Scheme)
	if rec.Scheme == "" || strings.TrimSpace(rec.Bucket) == "" {
		return fmt.Errorf("scheme and bucket are required")
	}
	if !mintableObjectScheme(rec.Scheme) {
		return fmt.Errorf("scheme must be s3, cos, tos, oss, gs, or az")
	}
	if strings.Contains(rec.Prefix, "..") {
		return fmt.Errorf("prefix must not contain parent-directory segments")
	}
	if err := validateSTSEndpoint(rec.STSEndpoint); err != nil {
		return err
	}
	if err := validateObjectStoreEndpoint(rec.Endpoint); err != nil {
		return err
	}
	if strings.ContainsAny(rec.Prefix, "*?") {
		return fmt.Errorf("prefix must not contain wildcard characters")
	}
	hasSecret := len(rec.SecretCipher) > 0
	switch rec.Scheme {
	case "tos", "oss":
		if strings.TrimSpace(rec.RoleARN) == "" {
			return fmt.Errorf("%s mint requires role_arn", rec.Scheme)
		}
		if strings.TrimSpace(rec.AccessKeyID) == "" || !hasSecret {
			return fmt.Errorf("%s mint requires access_key_id and secret_access_key", rec.Scheme)
		}
	case "cos":
		if strings.TrimSpace(rec.AccessKeyID) == "" || !hasSecret {
			return fmt.Errorf("cos mint requires access_key_id and secret_access_key")
		}
		if rec.CredentialKind == meta.ObjectCredentialRole && strings.TrimSpace(rec.RoleARN) == "" {
			return fmt.Errorf("cos role mint requires role_arn")
		}
	case "s3":
		if rec.CredentialKind == meta.ObjectCredentialRole {
			if strings.TrimSpace(rec.RoleARN) == "" {
				return fmt.Errorf("role_arn is required for credential_kind=role")
			}
		} else if strings.TrimSpace(rec.AccessKeyID) == "" || !hasSecret {
			return fmt.Errorf("access_key_id and secret_access_key are required for credential_kind=static")
		}
	case "az", "gs":
		if rec.CredentialKind == meta.ObjectCredentialRole {
			return fmt.Errorf("%s backends do not support credential_kind=role", rec.Scheme)
		}
		if strings.TrimSpace(rec.Prefix) != "" {
			return fmt.Errorf("%s isolation is per-container/bucket; backend prefix must be empty", rec.Scheme)
		}
		if !hasSecret {
			if rec.Scheme == "gs" {
				return fmt.Errorf("secret_access_key must be the GCS service-account JSON")
			}
			return fmt.Errorf("azure requires account name (access_key_id or account_id) and account key")
		}
		if rec.Scheme == "az" && strings.TrimSpace(rec.AccessKeyID) == "" && strings.TrimSpace(rec.AccountID) == "" {
			return fmt.Errorf("azure requires account name (access_key_id or account_id) and account key")
		}
	}
	return nil
}

func validateObjectStoreEndpoint(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("endpoint must be an absolute URL")
	}
	if u.User != nil {
		return fmt.Errorf("endpoint must not contain userinfo")
	}
	host := strings.ToLower(u.Hostname())
	if isBlockedSTSHost(host) {
		return fmt.Errorf("endpoint host is not allowed")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if host == "127.0.0.1" || host == "localhost" || host == "::1" {
			return nil
		}
		return fmt.Errorf("endpoint must use https:// (http:// is only allowed for loopback)")
	default:
		return fmt.Errorf("endpoint scheme %q is not allowed", u.Scheme)
	}
}

func mintableObjectScheme(scheme string) bool {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "s3", "cos", "tos", "oss", "gs", "az":
		return true
	default:
		return false
	}
}
