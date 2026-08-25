package server

import (
	"net/url"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
)

func TestMatchOrgObjectBackendLongestPrefix(t *testing.T) {
	rows := []meta.OrgObjectBackend{
		{Scheme: "s3", Bucket: "b", Prefix: "", CredentialKind: meta.ObjectCredentialStatic},
		{Scheme: "s3", Bucket: "b", Prefix: "drive9", CredentialKind: meta.ObjectCredentialStatic},
	}
	got, err := matchOrgObjectBackend(rows, "s3", "b", "drive9/cust/a.txt", "", "cust")
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.Prefix != "drive9" || got.Allowed != "drive9/cust" {
		t.Fatalf("got prefix=%q allowed=%q", got.Backend.Prefix, got.Allowed)
	}
	got, err = matchOrgObjectBackend(rows, "s3", "b", "cust/a.txt", "", "cust")
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend.Prefix != "" || got.Allowed != "cust" {
		t.Fatalf("root match prefix=%q allowed=%q", got.Backend.Prefix, got.Allowed)
	}
}

func TestMatchOrgObjectBackendEmptyEndpointDoesNotMatchCustomURI(t *testing.T) {
	rows := []meta.OrgObjectBackend{
		{Scheme: "s3", Bucket: "b", Endpoint: "", CredentialKind: meta.ObjectCredentialStatic},
		{Scheme: "s3", Bucket: "b", Endpoint: "http://127.0.0.1:9000", CredentialKind: meta.ObjectCredentialStatic},
	}
	got, err := matchOrgObjectBackend(rows, "s3", "b", "cust/a", "http://127.0.0.1:9000", "cust")
	if err != nil || got.Backend.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got, err = matchOrgObjectBackend(rows, "s3", "b", "cust/a", "", "cust")
	if err != nil || got.Backend.Endpoint != "" {
		t.Fatalf("empty uri endpoint should prefer empty backend: %+v err=%v", got, err)
	}
}

func TestValidateSTSEndpoint(t *testing.T) {
	if err := validateSTSEndpoint(""); err != nil {
		t.Fatal(err)
	}
	if err := validateSTSEndpoint("https://sts.amazonaws.com"); err != nil {
		t.Fatal(err)
	}
	if err := validateSTSEndpoint("sts.tencentcloudapi.com"); err != nil {
		t.Fatal(err)
	}
	if err := validateSTSEndpoint("http://127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	if err := validateSTSEndpoint("http://169.254.169.254/"); err == nil {
		t.Fatal("metadata http must fail")
	}
	if err := validateSTSEndpoint("https://169.254.169.254/"); err == nil {
		t.Fatal("metadata https must fail")
	}
	if err := validateSTSEndpoint("http://sts.example.com"); err == nil {
		t.Fatal("plain http remote must fail")
	}
}

func TestMatchOrgObjectBackendEndpointDisambiguate(t *testing.T) {
	rows := []meta.OrgObjectBackend{
		{Scheme: "s3", Bucket: "b", Endpoint: "https://s3.amazonaws.com", CredentialKind: meta.ObjectCredentialStatic},
		{Scheme: "s3", Bucket: "b", Endpoint: "http://127.0.0.1:9000", CredentialKind: meta.ObjectCredentialStatic},
	}
	_, err := matchOrgObjectBackend(rows, "s3", "b", "cust/a", "", "cust")
	if err != errAmbiguousObjectBackend {
		t.Fatalf("err=%v", err)
	}
	got, err := matchOrgObjectBackend(rows, "s3", "b", "cust/a", "http://127.0.0.1:9000", "cust")
	if err != nil || got.Backend.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestMatchOrgObjectBackendAzureRequiresDedicatedContainer(t *testing.T) {
	rows := []meta.OrgObjectBackend{
		{Scheme: "az", Bucket: "cust", Prefix: "", CredentialKind: meta.ObjectCredentialStatic},
	}
	got, err := matchOrgObjectBackend(rows, "az", "cust", "file.txt", "", "cust")
	if err != nil || got.Backend.Bucket != "cust" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	_, err = matchOrgObjectBackend(rows, "az", "cust", "file.txt", "", "other")
	if err != errURIOutsideNamespace && err != errNoObjectBackend {
		t.Fatalf("err=%v", err)
	}
}

func TestCOSAndTOSAndOSSPolicies(t *testing.T) {
	cos := cosSessionPolicy("ap-guangzhou", "1250000000", "example-1250000000", "cust", true)
	if !strings.Contains(cos, "qcs::cos:ap-guangzhou:uid/1250000000:example-1250000000/cust/*") {
		t.Fatalf("cos policy=%s", cos)
	}
	if !strings.Contains(cos, "name/cos:PutObject") || !strings.Contains(cos, `"version":"2.0"`) {
		t.Fatalf("cos policy=%s", cos)
	}
	tos := tosSessionPolicy("tbkt", "cust", false)
	if strings.Contains(tos, "tos:PutObject") || !strings.Contains(tos, "trn:tos:::tbkt/cust/*") {
		t.Fatalf("tos policy=%s", tos)
	}
	oss := ossSessionPolicy("obkt", "cust", true)
	if !strings.Contains(oss, "acs:oss:*:*:obkt/cust/*") || !strings.Contains(oss, "oss:PutObject") {
		t.Fatalf("oss policy=%s", oss)
	}
	if !strings.Contains(oss, "oss:InitiateMultipartUpload") || !strings.Contains(oss, "oss:UploadPart") || !strings.Contains(oss, "oss:CompleteMultipartUpload") {
		t.Fatalf("oss multipart missing: %s", oss)
	}
}

func TestCOSAppIDFromBucket(t *testing.T) {
	got, err := cosAppID("", "examplebucket-1250000000")
	if err != nil || got != "1250000000" {
		t.Fatalf("got %q err=%v", got, err)
	}
	if _, err := cosAppID("", "plain"); err == nil {
		t.Fatal("expected missing appid")
	}
	got, err = cosAppID("125111", "plain")
	if err != nil || got != "125111" {
		t.Fatalf("account id got %q err=%v", got, err)
	}
}

func TestAWSSTSClientOptionsCustomEndpoint(t *testing.T) {
	opts := awsSTSClientOptions("https://sts.minio.local")
	if len(opts) != 1 {
		t.Fatalf("opts=%d", len(opts))
	}
	if awsSTSClientOptions("") != nil {
		t.Fatal("empty endpoint should skip options")
	}
}

func TestMintTOSAndOSSRequireRole(t *testing.T) {
	_, err := mintVolcengineTOSSession(t.Context(), &meta.OrgObjectBackend{Scheme: "tos", Bucket: "b", AccessKeyID: "ak"}, "sk", "", "p", false, 3600)
	if err == nil || !strings.Contains(err.Error(), "role_arn") {
		t.Fatalf("tos err=%v", err)
	}
	_, err = mintAliyunOSSSession(t.Context(), &meta.OrgObjectBackend{Scheme: "oss", Bucket: "b", AccessKeyID: "ak"}, "sk", "", "p", false, 3600)
	if err == nil || !strings.Contains(err.Error(), "role_arn") {
		t.Fatalf("oss err=%v", err)
	}
}

func TestVolcengineSTSRegionFromHost(t *testing.T) {
	if got := volcengineSTSRegion("cn-beijing", "sts.cn-shanghai.volcengineapi.com"); got != "cn-shanghai" {
		t.Fatalf("got %s", got)
	}
	if got := volcengineSTSRegion("", "sts.volcengineapi.com"); got != "cn-north-1" {
		t.Fatalf("got %s", got)
	}
}

func TestCanonicalQueryStable(t *testing.T) {
	got := canonicalQuery(url.Values{"Action": {"AssumeRole"}, "Version": {"2018-01-01"}})
	if got != "Action=AssumeRole&Version=2018-01-01" {
		t.Fatalf("got %q", got)
	}
	spaced := canonicalQuery(url.Values{"Policy": {`{"Statement": []}`}})
	if strings.Contains(spaced, "+") || !strings.Contains(spaced, "%20") {
		t.Fatalf("spaces must be %%20 not +: %q", spaced)
	}
}

func TestValidateAdminObjectBackend(t *testing.T) {
	err := validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "tos", Bucket: "b", CredentialKind: meta.ObjectCredentialStatic, AccessKeyID: "ak",
	})
	if err == nil || !strings.Contains(err.Error(), "role_arn") {
		t.Fatalf("err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "tos", Bucket: "b", CredentialKind: meta.ObjectCredentialRole, RoleARN: "arn:tos", AccessKeyID: "ak",
	})
	if err == nil || !strings.Contains(err.Error(), "secret_access_key") {
		t.Fatalf("tos role without secret err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "oss", Bucket: "b", CredentialKind: meta.ObjectCredentialRole, RoleARN: "arn:oss", AccessKeyID: "ak",
	})
	if err == nil || !strings.Contains(err.Error(), "secret_access_key") {
		t.Fatalf("oss role without secret err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "cos", Bucket: "b", CredentialKind: meta.ObjectCredentialRole, RoleARN: "arn:cos", AccessKeyID: "ak",
	})
	if err == nil || !strings.Contains(err.Error(), "secret_access_key") {
		t.Fatalf("cos role without secret err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "s3", Bucket: "b", CredentialKind: meta.ObjectCredentialRole, RoleARN: "arn:aws:iam::1:role/r",
	})
	if err != nil {
		t.Fatalf("aws role without stored keys should be allowed: %v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "gcs", Bucket: "b", Prefix: "x", CredentialKind: meta.ObjectCredentialStatic,
	})
	if err == nil || !strings.Contains(err.Error(), "prefix") {
		t.Fatalf("err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "s3", Bucket: "b", Prefix: "ten*", CredentialKind: meta.ObjectCredentialStatic, AccessKeyID: "ak", SecretCipher: []byte{1},
	})
	if err == nil || !strings.Contains(err.Error(), "wildcard") {
		t.Fatalf("prefix wildcard err=%v", err)
	}
	err = validateAdminObjectBackend(&meta.OrgObjectBackend{
		Scheme: "s3", Bucket: "b", Endpoint: "http://169.254.169.254/", CredentialKind: meta.ObjectCredentialStatic, AccessKeyID: "ak", SecretCipher: []byte{1},
	})
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("blocked endpoint err=%v", err)
	}
	if got := canonicalizeObjectStoreEndpoint("minio.example:9000"); got != "https://minio.example:9000" {
		t.Fatalf("canonicalize=%q", got)
	}
}
