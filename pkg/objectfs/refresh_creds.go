package objectfs

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs/fshttp"
	"go.uber.org/zap"
	"golang.org/x/oauth2"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// rclone GCS builds oauth2.NewClient(ctx, StaticTokenSource(snapshot)).
// The snapshot cannot be updated, but oauth2 uses ctx's HTTPClient as the
// Base transport and sets Authorization before calling it. Overwriting the
// header here refreshes GCS without NewFs.
//
// rclone Azure bakes the SAS into the service client URL. It talks HTTP
// through the process-wide fshttp transport; a request filter rewrites the
// SAS query on matching hosts.

var (
	gcsBearer       bearerTransport
	azureSASMu      sync.RWMutex
	azureSASURL     *url.URL
	azureFilterOnce sync.Once
)

func attachSessionTransport(ctx context.Context, sess SessionCredentials) context.Context {
	if sess.AccessToken != "" {
		ctx = withGCSTokenTransport(ctx, sess.AccessToken)
	}
	if sess.SASURL != "" {
		installAzureSASFilter(sess.SASURL)
	}
	return ctx
}

func withGCSTokenTransport(ctx context.Context, token string) context.Context {
	setGCSAccessToken(token)
	return context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: &gcsBearer})
}

func setGCSAccessToken(token string) {
	gcsBearer.set(token)
}

type bearerTransport struct {
	mu    sync.RWMutex
	token string
}

func (t *bearerTransport) set(token string) {
	t.mu.Lock()
	t.token = token
	t.mu.Unlock()
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
		if err != nil {
			logger.Warn(context.Background(), "object mount ignoring unparseable Azure SAS URL", zap.Error(err))
		}
		return
	}
	azureSASMu.Lock()
	azureSASURL = parsed
	azureSASMu.Unlock()
}

func rewriteAzureSASRequest(req *http.Request) {
	if req == nil || req.URL == nil {
		return
	}
	azureSASMu.RLock()
	sas := azureSASURL
	azureSASMu.RUnlock()
	if sas == nil {
		return
	}
	if !azureSASHostMatch(req.URL.Host, sas.Host) {
		return
	}
	rewriteAzureSASQuery(req.URL, sas)
}

func azureSASHostMatch(reqHost, sasHost string) bool {
	return strings.EqualFold(reqHost, sasHost)
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
