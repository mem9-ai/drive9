package extent

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenStorageFilePrefix(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(&Credential{
		Scheme:   SchemeFile,
		Endpoint: dir,
		Prefix:   "t/abc/",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/x", bytes.NewReader([]byte("hi"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "t/abc/chunks/x")); err != nil {
		t.Fatalf("expected prefixed key: %v", err)
	}
}

func TestOpenStorageRejectsDrive9Proxy(t *testing.T) {
	_, err := OpenStorage(&Credential{
		Scheme:   "drive9",
		Endpoint: "http://example/v1/extent/blocks",
	}, nil)
	if err == nil {
		t.Fatal("expected error for drive9 block proxy scheme")
	}
}

func TestOpenStorageS3StaticConstructs(t *testing.T) {
	st, err := OpenStorage(&Credential{
		Scheme:          SchemeS3,
		Endpoint:        "http://127.0.0.1:19000",
		Bucket:          "drive9-local",
		Prefix:          "t/abc/",
		AccessKeyID:     "drive9minio",
		SecretAccessKey: "drive9minio",
		ForcePathStyle:  true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.(*refreshingStore)
	if !ok {
		t.Fatalf("type=%T, want *refreshingStore", st)
	}
	if rs.scheme != SchemeS3 {
		t.Fatalf("scheme=%s, want s3", rs.scheme)
	}
}
