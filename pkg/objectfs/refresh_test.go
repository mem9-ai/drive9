package objectfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
)

func TestSessionRefreshWait(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	got := sessionRefreshWait(now.Add(time.Hour), now)
	if got != 45*time.Minute {
		t.Fatalf("1h ttl wait=%s want 45m", got)
	}
	got = sessionRefreshWait(now.Add(12*time.Hour), now)
	if got != 12*time.Hour-15*time.Minute {
		t.Fatalf("12h ttl wait=%s", got)
	}
	got = sessionRefreshWait(now.Add(-time.Second), now)
	if got != sessionRefreshMinWait {
		t.Fatalf("expired wait=%s", got)
	}
}

func TestParseSessionExpiry(t *testing.T) {
	got := ParseSessionExpiry("2026-08-24T13:00:00Z")
	if got.UTC().Format(time.RFC3339) != "2026-08-24T13:00:00Z" {
		t.Fatalf("got %s", got)
	}
	if !ParseSessionExpiry("").IsZero() || !ParseSessionExpiry("nope").IsZero() {
		t.Fatal("empty/invalid should be zero")
	}
}

func TestEffectiveSessionExpiryDefault(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	got := effectiveSessionExpiry(time.Time{}, now)
	if !got.Equal(now.Add(sessionDefaultTTL)) {
		t.Fatalf("got %s", got)
	}
}

func TestApplySessionEmpty(t *testing.T) {
	err := applySession(context.Background(), nil, SessionCredentials{})
	if err == nil {
		t.Fatal("expected error")
	}
}

type setRecorder struct {
	name string
	mu   sync.Mutex
	opts []map[string]string
	err  error
}

func (s *setRecorder) Name() string { return s.name }
func (s *setRecorder) Root() string { return "bucket" }
func (s *setRecorder) String() string {
	return s.name + ":bucket"
}
func (s *setRecorder) Precision() time.Duration { return time.Second }
func (s *setRecorder) Hashes() hash.Set         { return hash.Set(0) }
func (s *setRecorder) Features() *fs.Features   { return &fs.Features{} }
func (s *setRecorder) List(context.Context, string) (fs.DirEntries, error) {
	return nil, fs.ErrorDirNotFound
}
func (s *setRecorder) NewObject(context.Context, string) (fs.Object, error) {
	return nil, fs.ErrorObjectNotFound
}
func (s *setRecorder) Put(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) (fs.Object, error) {
	return nil, errors.New("unused")
}
func (s *setRecorder) Mkdir(context.Context, string) error { return nil }
func (s *setRecorder) Rmdir(context.Context, string) error { return nil }
func (s *setRecorder) Command(_ context.Context, name string, _ []string, opt map[string]string) (interface{}, error) {
	if name != "set" {
		return nil, fs.ErrorCommandNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make(map[string]string, len(opt))
	for k, v := range opt {
		copied[k] = v
	}
	s.opts = append(s.opts, copied)
	return nil, s.err
}

var _ fs.Commander = (*setRecorder)(nil)
var _ fs.Fs = (*setRecorder)(nil)

func TestApplySessionCallsRcloneSetInPlace(t *testing.T) {
	rec := &setRecorder{name: "s3{abc}"}
	err := applySession(context.Background(), rec, SessionCredentials{
		AccessKeyID:     "AKIANEW",
		SecretAccessKey: "secret",
		SessionToken:    "tok2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Name() != "s3{abc}" {
		t.Fatalf("Name() changed to %q", rec.Name())
	}
	if len(rec.opts) != 1 {
		t.Fatalf("set calls=%d", len(rec.opts))
	}
	got := rec.opts[0]
	if got["access_key_id"] != "AKIANEW" || got["secret_access_key"] != "secret" || got["session_token"] != "tok2" || got["env_auth"] != "false" {
		t.Fatalf("opts=%v", got)
	}
}

func TestApplySessionSetMissing(t *testing.T) {
	rec := &setRecorder{name: "local", err: fs.ErrorCommandNotFound}
	err := applySession(context.Background(), rec, SessionCredentials{
		AccessKeyID:     "AKIA",
		SecretAccessKey: "s",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOnTheFlyS3NameIncludesKeys(t *testing.T) {
	configfile.Install()
	ctx := context.Background()
	f1, err := fs.NewFs(ctx, s3TestSpec("AKIAOLD"))
	if err != nil {
		t.Fatal(err)
	}
	f2, err := fs.NewFs(ctx, s3TestSpec("AKIANEW"))
	if err != nil {
		t.Fatal(err)
	}
	if f1.Name() == f2.Name() {
		t.Fatalf("expected on-the-fly config hash in Name() when keys change; both %q", f1.Name())
	}
	if operations.SameConfig(f1, f2) {
		t.Fatal("different keys must not SameConfig; Copy would incorrectly assume server-side copy")
	}
}

func TestApplySessionPreservesRcloneS3Name(t *testing.T) {
	configfile.Install()
	ctx := context.Background()
	f, err := fs.NewFs(ctx, s3TestSpec("AKIAOLD"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.(fs.Commander); !ok {
		t.Fatal("rclone s3 must implement Commander for in-place STS refresh")
	}
	before := f.Name()
	err = applySession(ctx, f, SessionCredentials{
		AccessKeyID:     "AKIANEW",
		SecretAccessKey: "newsecret",
		SessionToken:    "newtok",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.Name() != before {
		t.Fatalf("in-place set changed Name() %q -> %q; same-bucket copy would fall back to download+upload", before, f.Name())
	}
	if !operations.SameConfig(f, f) {
		t.Fatal("SameConfig must stay true on the same Fs after set")
	}
}

func s3TestSpec(accessKey string) string {
	return fmt.Sprintf(`:s3,provider=AWS,env_auth=false,access_key_id=%s,secret_access_key=secret,session_token=tok,endpoint="http://127.0.0.1:1",no_check_bucket=true:bucket`, accessKey)
}

func TestBearerTransportOverwritesAuthorization(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	tr := &bearerTransport{}
	tr.set("fresh-token")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer stale")
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got != "Bearer fresh-token" {
		t.Fatalf("Authorization=%q", got)
	}
}

func TestRewriteAzureSASQueryKeepsAPIParams(t *testing.T) {
	reqURL, err := url.Parse("https://acct.blob.core.windows.net/c/blob?restype=container&comp=list&prefix=a/&sv=old&sig=oldsig")
	if err != nil {
		t.Fatal(err)
	}
	sas, err := url.Parse("https://acct.blob.core.windows.net/c?sv=2024-11-04&ss=b&srt=sco&sp=rwl&se=2026-01-01T00:00:00Z&sig=newsig")
	if err != nil {
		t.Fatal(err)
	}
	rewriteAzureSASQuery(reqURL, sas)
	q := reqURL.Query()
	if q.Get("restype") != "container" || q.Get("comp") != "list" || q.Get("prefix") != "a/" {
		t.Fatalf("api params lost: %s", reqURL.RawQuery)
	}
	if q.Get("sig") != "newsig" || q.Get("sv") != "2024-11-04" {
		t.Fatalf("sas not applied: %s", reqURL.RawQuery)
	}
}

func TestRewriteAzureSASRequestHostMismatch(t *testing.T) {
	installAzureSASFilter("https://acct.blob.core.windows.net/c?sv=1&sig=abc")
	req, err := http.NewRequest(http.MethodGet, "https://minio.example/bucket/key?sig=notazure", nil)
	if err != nil {
		t.Fatal(err)
	}
	rewriteAzureSASRequest(req)
	if req.URL.Query().Get("sig") != "notazure" {
		t.Fatalf("rewrote non-azure URL: %s", req.URL)
	}
}

func TestRcloneTransportIsFilterable(t *testing.T) {
	rt := fshttp.NewTransport(context.Background())
	if _, ok := rt.(*fshttp.Transport); !ok {
		t.Fatalf("rclone fshttp.NewTransport type %T; Azure SAS refresh filter will not install", rt)
	}
}

func TestRewriteAzureSASRequestHostMatch(t *testing.T) {
	installAzureSASFilter("https://acct.blob.core.windows.net/c?sv=2024-11-04&sig=new")
	req, err := http.NewRequest(http.MethodGet, "https://acct.blob.core.windows.net/c/blob?sv=old&sig=old&comp=list", nil)
	if err != nil {
		t.Fatal(err)
	}
	rewriteAzureSASRequest(req)
	q := req.URL.Query()
	if q.Get("sig") != "new" || q.Get("sv") != "2024-11-04" {
		t.Fatalf("sas not applied: %s", req.URL)
	}
	if q.Get("comp") != "list" {
		t.Fatalf("api param lost: %s", req.URL)
	}
}

func TestApplySessionGCSRequiresBoundFs(t *testing.T) {
	err := applySession(context.Background(), nil, SessionCredentials{AccessToken: "ya29.tok"})
	if !errors.Is(err, errGCSAuthUnbound) {
		t.Fatalf("err=%v", err)
	}
	rec := &setRecorder{name: "gcs"}
	auth := &sessionAuth{}
	sessionAuthByFs.Store(rec, auth)
	t.Cleanup(func() { sessionAuthByFs.Delete(rec) })
	err = applySession(context.Background(), rec, SessionCredentials{AccessToken: "ya29.fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if auth.gcs.get() != "ya29.fresh" {
		t.Fatalf("token=%q", auth.gcs.get())
	}
}

func TestApplySessionAzureSkipCommander(t *testing.T) {
	err := applySession(context.Background(), nil, SessionCredentials{
		SASURL: "https://acct.blob.core.windows.net/c?sv=1&sig=n",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAzureSASIsPerContainer(t *testing.T) {
	installAzureSASFilter("https://acct.blob.core.windows.net/a?sv=1&sig=sa")
	installAzureSASFilter("https://acct.blob.core.windows.net/b?sv=1&sig=sb")
	reqA, err := http.NewRequest(http.MethodGet, "https://acct.blob.core.windows.net/a/blob?comp=list", nil)
	if err != nil {
		t.Fatal(err)
	}
	rewriteAzureSASRequest(reqA)
	if reqA.URL.Query().Get("sig") != "sa" {
		t.Fatalf("a sig=%q", reqA.URL.Query().Get("sig"))
	}
	reqB, err := http.NewRequest(http.MethodGet, "https://acct.blob.core.windows.net/b/blob?comp=list", nil)
	if err != nil {
		t.Fatal(err)
	}
	rewriteAzureSASRequest(reqB)
	if reqB.URL.Query().Get("sig") != "sb" {
		t.Fatalf("b sig=%q", reqB.URL.Query().Get("sig"))
	}
}
