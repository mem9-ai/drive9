package s3client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// localPresignedRequest turns a minted presigned URL into a request the
// LocalS3Client handler sees: the server mounts the handler under /s3 with
// StripPrefix, so the handler-relative path is what the signature covers.
func localPresignedRequest(t *testing.T, presigned string) *http.Request {
	t.Helper()
	u, err := url.Parse(presigned)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}
	path := strings.TrimPrefix(u.Path, "/s3")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	req := httptest.NewRequest(http.MethodGet, path+"?"+u.RawQuery, nil)
	req.URL.Path = path
	req.URL.RawQuery = u.RawQuery
	return req
}

func newPresignTestClient(t *testing.T) *LocalS3Client {
	t.Helper()
	c := newTestClient(t)
	if err := c.PutObject(context.Background(), "tenant-a/blob", strings.NewReader("secret-bytes"), 12, EncryptionOpts{}); err != nil {
		t.Fatalf("put object: %v", err)
	}
	return c
}

// An unsigned GET used to serve any object whose key could be guessed. The
// route is unauthenticated, so the signature is the only authorization.
func TestLocalHandlerRejectsUnsignedGet(t *testing.T) {
	c := newPresignTestClient(t)
	req := httptest.NewRequest(http.MethodGet, "/objects/tenant-a/blob", nil)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if strings.Contains(rec.Body.String(), "secret-bytes") {
		t.Fatal("unsigned request leaked object bytes")
	}
}

func TestLocalHandlerServesSignedGet(t *testing.T) {
	c := newPresignTestClient(t)
	presigned, err := c.PresignGetObject(context.Background(), "tenant-a/blob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req := localPresignedRequest(t, presigned)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Body.String(); got != "secret-bytes" {
		t.Fatalf("body = %q, want %q", got, "secret-bytes")
	}
}

// A signature minted for one key must not authorize another tenant's object:
// that is the cross-tenant read the gateway had no check for.
func TestLocalHandlerRejectsCrossKeyReplay(t *testing.T) {
	c := newPresignTestClient(t)
	presigned, err := c.PresignGetObject(context.Background(), "tenant-a/blob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req := localPresignedRequest(t, presigned)
	req.URL.Path = "/objects/tenant-b/blob"
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

// The signature binds the method, so a GET URL cannot be replayed as a write.
func TestLocalHandlerRejectsCrossMethodReplay(t *testing.T) {
	c := newPresignTestClient(t)
	presigned, err := c.PresignGetObject(context.Background(), "tenant-a/blob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req := localPresignedRequest(t, presigned)
	req.Method = http.MethodPut
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 403/405", rec.Code)
	}
}

func TestLocalHandlerRejectsExpiredGet(t *testing.T) {
	c := newPresignTestClient(t)
	presigned, err := c.PresignGetObject(context.Background(), "tenant-a/blob", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	req := localPresignedRequest(t, presigned)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

// The range selector is signed: widening it after minting must not work.
func TestLocalHandlerRejectsTamperedRange(t *testing.T) {
	c := newPresignTestClient(t)
	presigned, err := c.PresignGetObjectRange(context.Background(), "tenant-a/blob", 0, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req := localPresignedRequest(t, presigned)
	q := req.URL.Query()
	q.Set("range", "0-11")
	req.URL.RawQuery = q.Encode()
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestLocalHandlerRejectsUnsignedUploadPart(t *testing.T) {
	c := newPresignTestClient(t)
	req := httptest.NewRequest(http.MethodPut, "/upload/upload-1/1", strings.NewReader("payload"))
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestLocalHandlerServesSignedUploadPart(t *testing.T) {
	c := newPresignTestClient(t)
	ctx := context.Background()
	upload, err := c.CreateMultipartUpload(ctx, "blobs/signed-part", ChecksumAlgoSHA256, EncryptionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	part, err := c.PresignUploadPart(ctx, "blobs/signed-part", upload.UploadID, 1, 7, ChecksumAlgoSHA256, "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req := localPresignedRequest(t, part.URL)
	req.Method = http.MethodPut
	req.Body = http.NoBody
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// Object keys that would escape the objects root are rejected before any file
// is touched, so a stored storage_ref cannot address a host file.
func TestLocalObjectPathRejectsTraversal(t *testing.T) {
	c := newTestClient(t)
	for _, key := range []string{"../etc/passwd", "a/../../etc/passwd", "/etc/passwd", "a//b", "", "a\\b"} {
		if _, err := c.GetObject(context.Background(), key); err == nil {
			t.Fatalf("GetObject(%q) succeeded, want rejection", key)
		}
	}
}

func TestLocalPresignRejectsTraversalKey(t *testing.T) {
	c := newTestClient(t)
	if _, err := c.PresignGetObject(context.Background(), "../../etc/passwd", time.Minute); err == nil {
		t.Fatal("PresignGetObject accepted a traversal key")
	}
}
