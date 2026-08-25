package cli

import (
	"errors"
	"strings"
	"testing"
)

func TestParseGolden(t *testing.T) {
	tests := []struct {
		in      string
		kind    Kind
		scheme  Scheme
		context string
		path    string
		bucket  string
		local   string
		dirHint bool
		wantErr string
	}{
		{in: "-", kind: KindStdin},
		{in: ":/", kind: KindDrive9, scheme: SchemeDrive9, path: "/"},
		{in: ":/abc", kind: KindDrive9, scheme: SchemeDrive9, path: "/abc"},
		{in: ":/dir/", kind: KindDrive9, scheme: SchemeDrive9, path: "/dir/"},
		{in: ":abc", kind: KindDrive9, scheme: SchemeDrive9, path: "/abc"},
		{in: "work:foo", kind: KindLocal, local: "work:foo"},
		{in: "drive9://abc", kind: KindDrive9, scheme: SchemeDrive9, path: "/abc"},
		{in: "drive9:///abc", kind: KindDrive9, scheme: SchemeDrive9, path: "/abc"},
		{in: "drive9://", kind: KindDrive9, scheme: SchemeDrive9, path: "/"},
		{in: "drive9:///", kind: KindDrive9, scheme: SchemeDrive9, path: "/"},
		{in: "drive9:////foo/", kind: KindDrive9, scheme: SchemeDrive9, path: "/foo/"},
		{in: "work:/foo", kind: KindDrive9, scheme: SchemeDrive9, context: "work", path: "/foo"},
		{in: "drive9:/foo", kind: KindDrive9, scheme: SchemeDrive9, context: "drive9", path: "/foo"},
		{in: "drive9://work/foo", kind: KindDrive9, scheme: SchemeDrive9, path: "/work/foo"},
		{in: "drive9://alice@/foo", wantErr: "userinfo"},
		{in: "drive9://foo?profile=x", wantErr: "query"},
		{in: "S3://bucket/k", kind: KindObject, scheme: SchemeS3, bucket: "bucket", path: "k"},
		{in: "s3://bucket/dir/a.txt", kind: KindObject, scheme: SchemeS3, bucket: "bucket", path: "dir/a.txt"},
		{in: "s3://bucket", kind: KindObject, scheme: SchemeS3, bucket: "bucket", path: "", dirHint: true},
		{in: "s3://bucket/", kind: KindObject, scheme: SchemeS3, bucket: "bucket", path: "", dirHint: true},
		{in: "s3://", wantErr: "empty bucket"},
		{in: "s3:///key", wantErr: "empty bucket"},
		{in: "s3:/inbox/a.txt", kind: KindDrive9, scheme: SchemeDrive9, context: "s3", path: "/inbox/a.txt"},
		{in: "cos://b/k", kind: KindObject, scheme: SchemeCOS, bucket: "b", path: "k"},
		{in: "tos://b/k", kind: KindObject, scheme: SchemeTOS, bucket: "b", path: "k"},
		{in: "oss://b/k", kind: KindObject, scheme: SchemeOSS, bucket: "b", path: "k"},
		{in: "gs://bucket/k", kind: KindObject, scheme: SchemeGS, bucket: "bucket", path: "k"},
		{in: "gcs://bucket/k", kind: KindObject, scheme: SchemeGS, bucket: "bucket", path: "k"},
		{in: "az://container/k", kind: KindObject, scheme: SchemeAZ, bucket: "container", path: "k"},
		{in: "azure://container/k?account=acct", kind: KindObject, scheme: SchemeAZ, bucket: "container", path: "k"},
		{in: "gs://b/k?profile=prod", wantErr: "unknown query"},
		{in: "az://c/k?region=eastus", wantErr: "unknown query"},
		{in: "s3://b/k?context=work", wantErr: "unknown query"},
		{in: "s3://b/foo%2Fbar", kind: KindObject, scheme: SchemeS3, bucket: "b", path: "foo/bar"},
		{in: "s3://b/a+b", kind: KindObject, scheme: SchemeS3, bucket: "b", path: "a+b"},
		{in: "C:/tmp/a", kind: KindLocal, local: "C:/tmp/a"},
		{in: "./a.txt", kind: KindLocal, local: "./a.txt"},
		{in: "/tmp/a", kind: KindLocal, local: "/tmp/a"},
		{in: "file:///tmp/a", kind: KindLocal, scheme: SchemeFile, local: "/tmp/a", path: "/tmp/a"},
		{in: "file://localhost/tmp/a", kind: KindLocal, scheme: SchemeFile, local: "/tmp/a", path: "/tmp/a"},
		{in: "file://other/tmp/a", wantErr: "host"},
		{in: "https://x.com/a", kind: KindDrive9, scheme: SchemeDrive9, context: "https", path: "//x.com/a"},
		{in: "s3://b/k?region=us-east-1", kind: KindObject, scheme: SchemeS3, bucket: "b", path: "k"},
		{in: "s3://b/k?profile=prod", wantErr: "unknown query"},
		{in: "s3://b/k?region=a&region=b", wantErr: "duplicate"},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if tt.wantErr != "" {
			if err == nil {
				t.Errorf("Parse(%q) err=nil, want %q", tt.in, tt.wantErr)
				continue
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Errorf("Parse(%q) err=%v, want substring %q", tt.in, err, tt.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) unexpected err=%v", tt.in, err)
			continue
		}
		if got.Kind != tt.kind {
			t.Errorf("Parse(%q) kind=%v, want %v", tt.in, got.Kind, tt.kind)
		}
		if tt.scheme != "" && got.Scheme != tt.scheme {
			t.Errorf("Parse(%q) scheme=%q, want %q", tt.in, got.Scheme, tt.scheme)
		}
		if got.Context != tt.context {
			t.Errorf("Parse(%q) context=%q, want %q", tt.in, got.Context, tt.context)
		}
		if tt.path != "" || tt.kind == KindDrive9 || tt.kind == KindObject {
			if got.Path != tt.path {
				t.Errorf("Parse(%q) path=%q, want %q", tt.in, got.Path, tt.path)
			}
		}
		if got.Bucket != tt.bucket {
			t.Errorf("Parse(%q) bucket=%q, want %q", tt.in, got.Bucket, tt.bucket)
		}
		if tt.local != "" && got.Local != tt.local {
			t.Errorf("Parse(%q) local=%q, want %q", tt.in, got.Local, tt.local)
		}
		if got.DirHint != tt.dirHint {
			t.Errorf("Parse(%q) dirHint=%v, want %v", tt.in, got.DirHint, tt.dirHint)
		}
		if tt.kind == KindObject && got.Kind != KindObject {
			t.Errorf("Parse(%q) registered object classified as %v", tt.in, got.Kind)
		}
	}
}

func TestParseNeverLocalForRegisteredObject(t *testing.T) {
	for _, in := range []string{"s3://b/k", "cos://b/k", "tos://b/k", "oss://b/k", "S3://B/k", "gs://b/k", "az://c/k", "gcs://b/k", "azure://c/k"} {
		got, err := Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if got.Kind != KindObject {
			t.Fatalf("Parse(%q) kind=%v, want object", in, got.Kind)
		}
	}
}

func TestReservedContextNameIsCaseInsensitive(t *testing.T) {
	if !IsReservedContextName("S3") || !IsReservedContextName("Gs") {
		t.Fatal("scheme aliases must be reserved regardless of case")
	}
}

func TestParseInvalidUserinfo(t *testing.T) {
	_, err := Parse("s3://AKIA:secret@bucket/k")
	if err == nil || !errors.Is(err, ErrInvalidLocation) {
		t.Fatalf("want ErrInvalidLocation, got %v", err)
	}
}

func TestPromoteBareFSArg(t *testing.T) {
	abs, err := Parse("/cli-foo")
	if err != nil {
		t.Fatal(err)
	}
	got := promoteBareFSArg(abs)
	if got.Kind != KindDrive9 || got.Path != "/cli-foo" {
		t.Fatalf("promote /cli-foo = %+v", got)
	}
	rel, _ := Parse("./foo")
	got = promoteBareFSArg(rel)
	if got.Kind != KindLocal {
		t.Fatalf("promote ./foo kind=%v", got.Kind)
	}
	win, _ := Parse("C:/foo")
	got = promoteBareFSArg(win)
	if got.Kind != KindLocal {
		t.Fatalf("promote C:/foo kind=%v", got.Kind)
	}
	file, _ := Parse("file:///tmp/a")
	got = promoteBareFSArg(file)
	if got.Kind != KindLocal || got.Scheme != SchemeFile {
		t.Fatalf("promote file:// = %+v", got)
	}
}
