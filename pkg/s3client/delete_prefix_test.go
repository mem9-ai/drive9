package s3client

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

const listUploadsXML = `<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bucket</Bucket>
  <IsTruncated>false</IsTruncated>
</ListMultipartUploadsResult>`

// deleteObjectsProbe is a stand-in S3 endpoint. It holds two objects across two
// pages, records what the Multi-Object Delete carried, and — like a real store —
// stops listing an object once it has been deleted, so the caller can be checked
// for a pagination loop.
type deleteObjectsProbe struct {
	rejectBatch bool // answer the batch call the way MinIO does without Content-MD5

	mu        sync.Mutex
	keys      []string
	md5Header string
	batchBody []byte
	batches   int
	oneByOne  []string
	page1     int
	page2     int
}

func newDeleteObjectsProbe(rejectBatch bool) *deleteObjectsProbe {
	return &deleteObjectsProbe{rejectBatch: rejectBatch, keys: []string{"t/tenant/a", "t/tenant/b"}}
}

func (p *deleteObjectsProbe) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, listUploadsXML)
		case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
			p.serveList(w, r)
		case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
			p.serveDeleteObjects(w, r)
		case r.Method == http.MethodDelete:
			p.mu.Lock()
			key := strings.TrimPrefix(r.URL.Path, "/bucket/")
			p.oneByOne = append(p.oneByOne, key)
			p.remove(key)
			p.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// serveList answers ListObjectsV2 with the first remaining key on page one and
// the last one on page two, so deleting a key between the two calls shifts the
// listing the way a real bucket does.
func (p *deleteObjectsProbe) serveList(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	page := 1
	if r.URL.Query().Get("continuation-token") == "page2" {
		page = 2
	}
	contents := ""
	truncated := false
	next := ""
	if page == 1 {
		p.page1++
		if len(p.keys) > 0 {
			contents = objectXML(p.keys[0])
			if len(p.keys) > 1 {
				truncated, next = true, "page2"
			}
		}
	} else {
		p.page2++
		if len(p.keys) > 0 {
			contents = objectXML(p.keys[len(p.keys)-1])
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name><Prefix>t/tenant/</Prefix><MaxKeys>1</MaxKeys>
  <IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken>
  %s
</ListBucketResult>`, truncated, next, contents)
}

func objectXML(key string) string {
	return fmt.Sprintf(`<Contents><Key>%s</Key><Size>4</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e"</ETag><StorageClass>STANDARD</StorageClass></Contents>`, key)
}

func (p *deleteObjectsProbe) serveDeleteObjects(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.batches++
	p.md5Header = r.Header.Get("Content-MD5")
	p.batchBody = body
	reject := p.rejectBatch
	if !reject {
		for _, key := range p.keysIn(body) {
			p.remove(key)
		}
	}
	p.mu.Unlock()
	if reject {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InvalidRequest</Code><Message>Missing required header for this request: Content-Md5.</Message><RequestId>r</RequestId><HostId>h</HostId></Error>`)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
}

func (p *deleteObjectsProbe) keysIn(body []byte) []string {
	var out []string
	for _, key := range []string{"t/tenant/a", "t/tenant/b"} {
		if strings.Contains(string(body), key) {
			out = append(out, key)
		}
	}
	return out
}

func (p *deleteObjectsProbe) remove(key string) {
	kept := p.keys[:0]
	for _, k := range p.keys {
		if k != key {
			kept = append(kept, k)
		}
	}
	p.keys = kept
}

func (p *deleteObjectsProbe) snapshot() (batches, page1, page2 int, oneByOne []string, md5Header string, body []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batches, p.page1, p.page2, append([]string(nil), p.oneByOne...), p.md5Header, append([]byte(nil), p.batchBody...)
}

func newProbeClient(t *testing.T, probe *deleteObjectsProbe) *AWSS3Client {
	t.Helper()
	srv := httptest.NewServer(probe.handler())
	t.Cleanup(srv.Close)
	c, err := New(context.Background(), AWSConfig{
		Region:          "us-east-1",
		Bucket:          "bucket",
		Prefix:          "t/tenant/",
		Endpoint:        srv.URL,
		ForcePathStyle:  true,
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// S3 requires Content-MD5 on Multi-Object Delete: MinIO rejects a request
// without it (400 MissingContentMD5) and AWS documents it as required. The
// header must match the body we send, and pagination must advance.
func TestDeletePrefixSendsContentMD5(t *testing.T) {
	probe := newDeleteObjectsProbe(false)
	c := newProbeClient(t, probe)

	res, err := c.DeletePrefix(context.Background(), "")
	if err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	if res.DeletedObjects != 2 {
		t.Fatalf("deleted=%d, want 2", res.DeletedObjects)
	}
	batches, page1, page2, oneByOne, md5Header, body := probe.snapshot()
	if batches != 2 {
		t.Fatalf("batch calls=%d, want 2 (one per page)", batches)
	}
	if page1 != 1 || page2 != 1 {
		t.Fatalf("list calls page1=%d page2=%d, want 1 and 1", page1, page2)
	}
	if len(oneByOne) != 0 {
		t.Fatalf("unexpected one-by-one deletes: %v", oneByOne)
	}
	if md5Header == "" {
		t.Fatal("Multi-Object Delete carried no Content-MD5 header")
	}
	sum := md5.Sum(body)
	if want := base64.StdEncoding.EncodeToString(sum[:]); md5Header != want {
		t.Fatalf("Content-MD5=%q, want %q for a %d byte body", md5Header, want, len(body))
	}
}

// A store that still refuses the batch (a proxy stripping the header, an
// implementation wanting a different checksum) must not leave the tenant's
// objects behind: DeletePrefix deletes them one by one, and still advances
// through the pages exactly once.
func TestDeletePrefixFallsBackWhenBatchIsRejected(t *testing.T) {
	probe := newDeleteObjectsProbe(true)
	c := newProbeClient(t, probe)

	res, err := c.DeletePrefix(context.Background(), "")
	if err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	if res.DeletedObjects != 2 {
		t.Fatalf("deleted=%d, want 2 via the fallback", res.DeletedObjects)
	}
	batches, page1, page2, oneByOne, _, _ := probe.snapshot()
	if batches != 2 {
		t.Fatalf("batch calls=%d, want 2 (one per page)", batches)
	}
	if page1 != 1 || page2 != 1 {
		t.Fatalf("list calls page1=%d page2=%d, want 1 and 1", page1, page2)
	}
	if len(oneByOne) != 2 {
		t.Fatalf("one-by-one deletes=%v, want both keys", oneByOne)
	}
}

func TestIsDeleteObjectsCompatibilityError(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"operation error S3: DeleteObjects, https response error StatusCode: 400, api error MissingContentMD5: Missing required header for this request: Content-Md5.", true},
		{"operation error S3: DeleteObjects, https response error StatusCode: 400, api error InvalidRequest: Missing required header for this request: Content-MD5", true},
		{"api error BadDigest: The Content-MD5 you specified did not match what we received.", true},
		{"api error MalformedXML: The XML you provided was not well-formed", true},
		{"api error AccessDenied: Access Denied", false},
		{"api error NoSuchBucket: The specified bucket does not exist", false},
		{"operation error S3: DeleteObjects, dial tcp: i/o timeout", false},
	}
	for _, tc := range cases {
		if got := isDeleteObjectsCompatibilityError(errString(tc.msg)); got != tc.want {
			t.Errorf("isDeleteObjectsCompatibilityError(%q)=%v, want %v", tc.msg, got, tc.want)
		}
	}
	if isDeleteObjectsCompatibilityError(nil) {
		t.Error("nil error must not be a compatibility error")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
