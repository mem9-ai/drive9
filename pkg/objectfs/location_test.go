package objectfs

import (
	"errors"
	"strings"
	"testing"
)

func TestParseObjectURI(t *testing.T) {
	loc, err := Parse("s3://bucket/dir/a.txt?region=us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if loc.Scheme != SchemeS3 || loc.Bucket != "bucket" || loc.Path != "dir/a.txt" {
		t.Fatalf("got %+v", loc)
	}
	if loc.Query[QueryRegion] != "us-east-1" {
		t.Fatalf("region=%q", loc.Query[QueryRegion])
	}
}

func TestParseRejectsUserinfo(t *testing.T) {
	_, err := Parse("s3://AKIA:secret@bucket/k")
	if !errors.Is(err, ErrInvalidLocation) {
		t.Fatalf("err=%v", err)
	}
}

func TestParseRejectsUnknownQuery(t *testing.T) {
	_, err := Parse("s3://b/k?profile=prod")
	if err == nil || !strings.Contains(err.Error(), "unknown query") {
		t.Fatalf("err=%v", err)
	}
}

func TestParseRejectsDrive9(t *testing.T) {
	_, err := Parse("drive9://abc")
	if !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("err=%v", err)
	}
}

func TestParseBucketRootIsDir(t *testing.T) {
	loc, err := Parse("s3://bucket")
	if err != nil {
		t.Fatal(err)
	}
	if !loc.DirHint || loc.Path != "" {
		t.Fatalf("got %+v", loc)
	}
}

func TestParseCanonicalizesAliases(t *testing.T) {
	gs, err := Parse("gcs://bucket/k")
	if err != nil {
		t.Fatal(err)
	}
	if gs.Scheme != SchemeGS || gs.Bucket != "bucket" || gs.Path != "k" {
		t.Fatalf("gcs:// = %+v", gs)
	}
	az, err := Parse("azure://container/k?account=acct")
	if err != nil {
		t.Fatal(err)
	}
	if az.Scheme != SchemeAZ || az.Bucket != "container" || az.Query[QueryAccount] != "acct" {
		t.Fatalf("azure:// = %+v", az)
	}
}

func TestParseRejectsS3QueryOnGS(t *testing.T) {
	_, err := Parse("gs://b/k?forcePathStyle=true")
	if err == nil || !strings.Contains(err.Error(), "unknown query") {
		t.Fatalf("err=%v", err)
	}
}

func TestParseRejectsEndpointUserinfo(t *testing.T) {
	_, err := Parse("s3://b/k?endpoint=https://user:pass@s3.example.com")
	if err == nil || !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("err=%v", err)
	}
}

func TestParseRejectsVersionID(t *testing.T) {
	_, err := Parse("s3://b/k?versionId=v1")
	if err == nil || !strings.Contains(err.Error(), "unknown query") {
		t.Fatalf("err=%v", err)
	}
}
