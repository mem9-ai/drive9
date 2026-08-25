package objectfs

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"golang.org/x/oauth2"

	"github.com/mem9-ai/drive9/pkg/logger"
)

type sessionAuthCtxKey struct{}

type sessionAuth struct {
	gcs bearerTransport
}

var (
	sessionAuthByFs sync.Map // fs.Fs -> *sessionAuth

	azureSASMu      sync.RWMutex
	azureSASByKey   = map[string]*url.URL{}
	azureFilterOnce sync.Once
)

func attachSessionTransport(ctx context.Context, sess SessionCredentials) context.Context {
	auth := &sessionAuth{}
	if sess.AccessToken != "" {
		auth.gcs.set(sess.AccessToken)
		ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: &auth.gcs})
	}
	if sess.SASURL != "" {
		installAzureSASFilter(sess.SASURL)
	}
	return context.WithValue(ctx, sessionAuthCtxKey{}, auth)
}

func bindSessionAuth(ctx context.Context, f fs.Fs) {
	if f == nil {
		return
	}
	auth, _ := ctx.Value(sessionAuthCtxKey{}).(*sessionAuth)
	if auth == nil {
		return
	}
	sessionAuthByFs.Store(f, auth)
}

func setGCSAccessTokenOn(f fs.Fs, token string) error {
	if f == nil {
		return errGCSAuthUnbound
	}
	v, ok := sessionAuthByFs.Load(f)
	if !ok {
		return errGCSAuthUnbound
	}
	v.(*sessionAuth).gcs.set(token)
	return nil
}

var errGCSAuthUnbound = errors.New("objectfs: no GCS session transport bound to this filesystem")

type bearerTransport struct {
	mu    sync.RWMutex
	token string
}

func (t *bearerTransport) set(token string) {
	t.mu.Lock()
	t.token = token
	t.mu.Unlock()
}

func (t *bearerTransport) get() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.RLock()
	token := t.token
	t.mu.RUnlock()
	if token != "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return http.DefaultTransport.RoundTrip(req)
}

func installAzureSASFilter(sasURL string) {
	setAzureSAS(sasURL)
	azureFilterOnce.Do(func() {
		rt := fshttp.NewTransport(context.Background())
		t, ok := rt.(*fshttp.Transport)
		if !ok {
			logger.Warn(context.Background(), "object mount cannot install Azure SAS refresh filter; unexpected rclone transport")
			return
		}
		t.SetRequestFilter(rewriteAzureSASRequest)
	})
}

func setAzureSAS(sasURL string) {
	parsed, err := url.Parse(strings.TrimSpace(sasURL))
	if err != nil || parsed == nil || parsed.Host == "" {
		logger.Warn(context.Background(), "object mount ignoring unparseable Azure SAS URL")
		return
	}
	key := azureSASKey(parsed.Host, parsed.Path)
	azureSASMu.Lock()
	azureSASByKey[key] = parsed
	azureSASMu.Unlock()
}

func azureSASKey(host, path string) string {
	return strings.ToLower(strings.TrimSpace(host)) + "|" + strings.Trim(path, "/")
}

func lookupAzureSAS(reqURL *url.URL) *url.URL {
	if reqURL == nil {
		return nil
	}
	host := strings.ToLower(reqURL.Host)
	segs := strings.Split(strings.Trim(reqURL.Path, "/"), "/")
	azureSASMu.RLock()
	defer azureSASMu.RUnlock()
	n := len(segs)
	if n > 2 {
		n = 2
	}
	for ; n >= 1; n-- {
		if segs[0] == "" {
			break
		}
		key := host + "|" + strings.Join(segs[:n], "/")
		if sas := azureSASByKey[key]; sas != nil {
			return sas
		}
	}
	return nil
}

func rewriteAzureSASRequest(req *http.Request) {
	if req == nil || req.URL == nil {
		return
	}
	sas := lookupAzureSAS(req.URL)
	if sas == nil {
		return
	}
	rewriteAzureSASQuery(req.URL, sas)
}

func rewriteAzureSASQuery(reqURL *url.URL, sas *url.URL) {
	q := reqURL.Query()
	for k := range azureSASQueryKeys {
		q.Del(k)
	}
	for k, vs := range sas.Query() {
		q.Del(k)
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	reqURL.RawQuery = q.Encode()
}

// azureSASQueryKeys are the SAS parameters Azure puts on blob URLs.
// API params such as restype/comp/prefix are left in place.
var azureSASQueryKeys = map[string]struct{}{
	"sv": {}, "ss": {}, "srt": {}, "sp": {}, "se": {}, "st": {}, "spr": {},
	"sig": {}, "sip": {}, "sr": {}, "si": {}, "sdd": {},
	"sks": {}, "skt": {}, "ske": {}, "sktid": {}, "skoid": {}, "skv": {},
	"rscc": {}, "rscd": {}, "rsce": {}, "rscl": {}, "rsct": {},
}
