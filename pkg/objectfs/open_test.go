package objectfs

import (
	"strings"
	"testing"
)

func TestConnectionString(t *testing.T) {
	must := func(raw string) Location {
		t.Helper()
		loc, err := Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return loc
	}
	tests := []struct {
		in   string
		want string
	}{
		{"s3://bucket/dir/a.txt?region=us-east-1", ":s3,provider=AWS,env_auth=true,directory_markers=true,region=us-east-1:bucket/dir/a.txt"},
		{"s3://bucket", ":s3,provider=AWS,env_auth=true,directory_markers=true:bucket"},
		{"s3://bucket/", ":s3,provider=AWS,env_auth=true,directory_markers=true:bucket"},
		{"cos://b/k", ":s3,provider=TencentCOS,env_auth=true,directory_markers=true:b/k"},
		{"oss://b/k", ":s3,provider=Alibaba,env_auth=true,directory_markers=true:b/k"},
		{"tos://b/k?region=cn-beijing", `:s3,provider=Other,env_auth=true,directory_markers=true,region=cn-beijing,endpoint="https://tos-s3-cn-beijing.volces.com":b/k`},
		{"s3://b/k?provider=aliyun&endpoint=https://oss.example.com&forcePathStyle=true", `:s3,provider=Alibaba,env_auth=true,directory_markers=true,endpoint="https://oss.example.com",force_path_style=true:b/k`},
		{"s3://b/k?endpoint=http://127.0.0.1:9000&forcePathStyle=true", `:s3,provider=AWS,env_auth=true,directory_markers=true,endpoint="http://127.0.0.1:9000",force_path_style=true,use_unsigned_payload=true:b/k`},
		{`s3://b/k?region=us-east-1,extra`, `:s3,provider=AWS,env_auth=true,directory_markers=true,region="us-east-1,extra":b/k`},
		{"gs://bucket/dir/a.txt", ":gcs,env_auth=true:bucket/dir/a.txt"},
		{"gcs://bucket/k", ":gcs,env_auth=true:bucket/k"},
		{"gs://bucket/k?endpoint=http://127.0.0.1:4443", `:gcs,env_auth=true,endpoint="http://127.0.0.1:4443":bucket/k`},
		{"az://container/k", ":azureblob,env_auth=true:container/k"},
		{"azure://container/k?account=myacct", ":azureblob,env_auth=true,account=myacct:container/k"},
		{"az://c/k?account=devstoreaccount1&endpoint=http://127.0.0.1:10000/devstoreaccount1", `:azureblob,env_auth=true,account=devstoreaccount1,endpoint="http://127.0.0.1:10000/devstoreaccount1":c/k`},
	}
	for _, tt := range tests {
		got, err := connectionString(must(tt.in), SessionCredentials{})
		if err != nil {
			t.Errorf("connectionString(%q) err=%v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("connectionString(%q)=\n  %q\nwant %q", tt.in, got, tt.want)
		}
	}
}

func TestConnectionStringSessionUsesExplicitKeys(t *testing.T) {
	loc, err := Parse("s3://bucket/k?region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := connectionString(loc, SessionCredentials{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret",
		SessionToken:    "token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "env_auth=false") || !strings.Contains(got, "access_key_id=AKIATEST") || !strings.Contains(got, "session_token=token") || !strings.Contains(got, "no_check_bucket=true") {
		t.Fatalf("got %q, want explicit session keys and no_check_bucket", got)
	}
	if strings.Contains(got, "env_auth=true") {
		t.Fatalf("session creds must not keep env_auth=true: %q", got)
	}
}

func TestConnectionStringGCSAccessTokenAndAzureSAS(t *testing.T) {
	gcs, err := Parse("gs://bucket/k")
	if err != nil {
		t.Fatal(err)
	}
	got, err := connectionString(gcs, SessionCredentials{AccessToken: "ya29.tok"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "env_auth=false") || !strings.Contains(got, "access_token=ya29.tok") {
		t.Fatalf("gcs got %q", got)
	}
	az, err := Parse("az://container/k")
	if err != nil {
		t.Fatal(err)
	}
	got, err = connectionString(az, SessionCredentials{SASURL: "https://acct.blob.core.windows.net/container?sig=1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "env_auth=false") || !strings.Contains(got, "sas_url=") {
		t.Fatalf("azure got %q", got)
	}
}

func TestConnectionStringTOSRequiresRegion(t *testing.T) {
	loc, err := Parse("tos://b/k")
	if err != nil {
		t.Fatal(err)
	}
	_, err = connectionString(loc, SessionCredentials{})
	if err == nil || !strings.Contains(err.Error(), "region required") {
		t.Fatalf("err=%v, want region required", err)
	}
}

func TestConnectionStringEmptyBucket(t *testing.T) {
	_, err := connectionString(Location{Raw: "s3://", Scheme: SchemeS3}, SessionCredentials{})
	if err == nil || !strings.Contains(err.Error(), "empty bucket") {
		t.Fatalf("err=%v, want empty bucket", err)
	}
}
