package viewprotocol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/stretchr/testify/require"
)

func TestLayerIDAndNameDisjointNamespaces(t *testing.T) {
	validID := "layer_01j8z4r9w3m5n7p9q2s4t6v8xz"
	require.True(t, IsCanonicalLayerID(validID))
	require.NoError(t, ValidateLayerID(validID))

	for _, badID := range []string{
		"",
		"main",
		"my-layer",
		"layer_short",
		"layer_01J8Z4R9W3M5N7P9Q2S4T6V8XZ", // uppercase forbidden
		"layer_01j8z4r9w3m5n7p9q2s4t6v8xi", // 'i' excluded from Crockford base32
		"layer_01j8z4r9w3m5n7p9q2s4t6v8xl", // 'l' excluded
		"layer_01j8z4r9w3m5n7p9q2s4t6v8xo", // 'o' excluded
		"layer_01j8z4r9w3m5n7p9q2s4t6v8xu", // 'u' excluded
	} {
		require.False(t, IsCanonicalLayerID(badID), "expected invalid id %q", badID)
		require.ErrorIs(t, ValidateLayerID(badID), ErrInvalidLayerID)
	}

	name, err := NormalizeLayerName("  feature-branch-1  ")
	require.NoError(t, err)
	require.Equal(t, "feature-branch-1", name)

	empty, err := NormalizeLayerName("   ")
	require.NoError(t, err)
	require.Equal(t, "", empty)

	for _, badName := range []string{
		"main",
		"MAIN",
		"layer_anything",
		"LAYER_01j8z4r9w3m5n7p9q2s4t6v8xz",
		"tag:env=prod",
		"TAG:latest",
		"a/b",
		".",
		"..",
		"bad\nname",
	} {
		_, err := NormalizeLayerName(badName)
		require.ErrorIs(t, err, ErrInvalidLayerName, "expected invalid name %q", badName)
	}

	ref, err := NormalizeLayerRef("tag:env=staging")
	require.NoError(t, err)
	require.Equal(t, "tag:env=staging", ref)
	k, v, hasVal, isTag, err := ParseTagSelector(ref)
	require.NoError(t, err)
	require.True(t, isTag)
	require.True(t, hasVal)
	require.Equal(t, "env", k)
	require.Equal(t, "staging", v)

	_, err = NormalizeLayerRef("main")
	require.ErrorIs(t, err, ErrInvalidLayerRef)
}

func TestParseFenceHeaderAndViewRoute(t *testing.T) {
	h := make(http.Header)
	f, present, err := ParseFenceHeader(h)
	require.NoError(t, err)
	require.False(t, present)
	require.Zero(t, f)

	h.Set(FenceHeader, "7")
	f, present, err = ParseFenceHeader(h)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, int64(7), f)

	for _, bad := range []string{"", "0", "2", "-1", "+3", "03", " 3 "} {
		bh := make(http.Header)
		bh.Set(FenceHeader, bad)
		_, _, err := ParseFenceHeader(bh)
		require.ErrorIs(t, err, ErrInvalidFence, "fence=%q", bad)
	}

	validID := "layer_01j8z4r9w3m5n7p9q2s4t6v8xz"
	viewPath, err := FormatViewPath(validID, "/v1/fs/dir/grep")
	require.NoError(t, err)
	require.Equal(t, "/v1/views/"+validID+"/v1/fs/dir/grep", viewPath)

	for _, badInner := range []string{"", "v1/fs/a", "/v1", "/v1/views", "/v1/views?x", "/v1/views/" + validID + "/v1/fs/a", "/v1/fs/a?stat=1", "/s3/blob"} {
		_, err := FormatViewPath(validID, badInner)
		require.ErrorIs(t, err, ErrInvalidViewRoute, "inner=%q", badInner)
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com"+viewPath+"?stat=1", nil)
	inner, gotID, err := ParseViewRoute(req)
	require.NoError(t, err)
	require.Equal(t, validID, gotID)
	require.Equal(t, "/v1/fs/dir/grep", inner.URL.Path)
	require.Equal(t, "stat=1", inner.URL.RawQuery)

	for _, incomplete := range []*http.Request{nil, {}} {
		_, _, err := ParseViewRoute(incomplete)
		require.ErrorIs(t, err, ErrInvalidViewRoute)
	}

	for _, badTarget := range []string{
		"http://example.com/v1/views/" + validID + "/v1/views/" + validID + "/v1/fs/a",
		"http://example.com/v1/views/" + validID + "/v1/views",
		"http://example.com/v1/views/" + validID + "/v1",
		"http://example.com/v1/views",
	} {
		badReq := httptest.NewRequest(http.MethodGet, badTarget, nil)
		_, _, err = ParseViewRoute(badReq)
		require.ErrorIs(t, err, ErrInvalidViewRoute, "target=%q", badTarget)
	}
}

func TestClassifyClientAndViewDataRoutes(t *testing.T) {
	data, err := os.ReadFile("testdata/client_routes.json")
	require.NoError(t, err)
	var cases []struct{ Method, Target, Action string }
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)
	actions := map[string]RouteAction{"pass": RoutePassThrough, "rewrite": RouteRewriteView, "unsupported": RouteRejectUnsupported, "invalid": RouteRejectInvalid}
	for _, tc := range cases {
		t.Run(tc.Method+" "+tc.Target, func(t *testing.T) {
			uri, err := url.ParseRequestURI(tc.Target)
			require.NoError(t, err)
			want, ok := actions[tc.Action]
			require.True(t, ok, "unknown route action %q", tc.Action)
			require.Equal(t, want, ClassifyClientRoute(tc.Method, uri.EscapedPath(), uri.Query()))
			if want == RoutePassThrough {
				require.Equal(t, RouteRejectUnsupported, ClassifyViewDataRoute(tc.Method, uri.EscapedPath(), uri.Query()))
			}
		})
	}
}

func TestPathWithinBaseRootAndRedirectConfinement(t *testing.T) {
	for _, raw := range []string{"//", "///"} {
		root, err := CanonicalizeViewPath(raw)
		require.NoError(t, err)
		require.Equal(t, "/", root)
		require.True(t, PathWithinBaseRoot(root, "/workspace/a.txt"))
	}
	require.True(t, PathWithinBaseRoot("/", "/"))
	require.True(t, PathWithinBaseRoot("/", "/workspace/a.txt"))
	require.True(t, PathWithinBaseRoot("/workspace", "/workspace"))
	require.True(t, PathWithinBaseRoot("/workspace", "/workspace/sub/a.txt"))
	require.True(t, PathWithinBaseRoot("/workspace ", "/workspace /a.txt"))
	require.True(t, PathWithinBaseRoot("/caf\u00e9", "/cafe\u0301/file.txt"))

	// Parity with pathutil.Canonicalize on trailing-space sibling paths + empty/relative fail-closed.
	c1, err := pathutil.Canonicalize("/workspace")
	require.NoError(t, err)
	c2, err := pathutil.Canonicalize("/workspace ")
	require.NoError(t, err)
	require.NotEqual(t, c1, c2)
	require.False(t, PathWithinBaseRoot("/workspace", "/workspace "))
	require.False(t, PathWithinBaseRoot("/workspace ", "/workspace"))
	require.False(t, PathWithinBaseRoot("", "/secret"))
	require.False(t, PathWithinBaseRoot("/workspace", ""))
	require.False(t, PathWithinBaseRoot("/workspace", "workspace/a.txt"))
	require.False(t, PathWithinBaseRoot("workspace", "/workspace/a.txt"))
	require.False(t, PathWithinBaseRoot("/workspace", "/"))
	require.False(t, PathWithinBaseRoot("/workspace", "/workspace2/a.txt"))
	require.False(t, PathWithinBaseRoot("/workspace", "/workspace/../etc/passwd"))
	require.False(t, PathWithinBaseRoot("/workspace", "/workspace\\a.txt"))

	base, err := url.Parse("https://api.example.com")
	require.NoError(t, err)
	httpBase, err := url.Parse("http://api.example.com")
	require.NoError(t, err)

	s3URL, _ := url.Parse("https://s3.example.com/bucket/v1/key?X-Amz-Signature=placeholder")
	localS3, _ := url.Parse("/s3/tenant-1/blobs/f1")
	require.NoError(t, ValidateViewDownloadRedirect(http.MethodGet, base, s3URL))
	require.NoError(t, ValidateViewDownloadRedirect(http.MethodGet, base, localS3))

	// Non-GET (e.g. POST 307) must always be rejected even to S3.
	require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodPost, base, s3URL), ErrInvalidViewRedirect)

	for _, badLoc := range []string{
		"/v1/fs/f",
		"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/f",
		"/%76%31/fs/f",
		"///v1/fs/f",
		"/s3/../v1/fs/f",
		"/v1%2ffs/f",
		"https://api.example.com:443/v1/fs/f",
		"https://api.example.com./v1/fs/f",
		"https://api.example.com.:443/%76%32/uploads/initiate",
		"ftp://s3.example.com/bucket/key",
		"javascript:alert(1)",
		"https://user:pass@s3.example.com/bucket/key",
	} {
		loc, pErr := url.Parse(badLoc)
		require.NoError(t, pErr)
		require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, base, loc), ErrInvalidViewRedirect, "loc=%q", badLoc)
	}
	httpPort80, _ := url.Parse("http://api.example.com:80/v1/fs/f")
	require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, httpBase, httpPort80), ErrInvalidViewRedirect)

	// Same-host http -> https scheme upgrade and deployment path-prefix bypasses must be blocked.
	httpsUpgrade, _ := url.Parse("https://api.example.com/v1/fs/f")
	httpsUpgrade443, _ := url.Parse("https://api.example.com:443/v1/fs/f")
	require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, httpBase, httpsUpgrade), ErrInvalidViewRedirect)
	require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, httpBase, httpsUpgrade443), ErrInvalidViewRedirect)

	prefixedBase, err := url.Parse("https://api.example.com/drive9/")
	require.NoError(t, err)
	prefixedS3, _ := url.Parse("/drive9/s3/tenant-1/blobs/f1")
	require.NoError(t, ValidateViewDownloadRedirect(http.MethodGet, prefixedBase, prefixedS3))

	for _, badPrefixed := range []string{
		"/drive9/v1/fs/f",
		"/drive9/v2/uploads/initiate",
		"v1/fs/f",
		"https://api.example.com/drive9/%76%31/fs/f",
		"http://api.example.com/drive9/v1/fs/f",
	} {
		loc, pErr := url.Parse(badPrefixed)
		require.NoError(t, pErr)
		require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, prefixedBase, loc), ErrInvalidViewRedirect, "prefixed loc=%q", badPrefixed)
	}

	// Same-host different port and subdomain /v1 or /v2 redirects must also be rejected.
	portBase, err := url.Parse("https://api.example:8443")
	require.NoError(t, err)
	for _, badCross := range []string{
		"https://api.example:9443/v1/fs/f",
		"http://api.example:9443/v1/fs/f",
		"https://sub.api.example/v1/fs/f",
		"https://sub.api.example:9443/v2/uploads/initiate",
	} {
		loc, pErr := url.Parse(badCross)
		require.NoError(t, pErr)
		require.ErrorIs(t, ValidateViewDownloadRedirect(http.MethodGet, portBase, loc), ErrInvalidViewRedirect, "cross loc=%q", badCross)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type testAuthInjectingTransport struct {
	base  http.RoundTripper
	calls atomic.Int64
}

func (rt *testAuthInjectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.calls.Add(1)
	cloned := req.Clone(req.Context())
	cloned.Header.Set("Authorization", "wrapper-injected-auth")
	cloned.Header.Set("X-TiDB-Cloud-Authorization", "wrapper-injected-cloud-auth")
	cloned.Header.Set("X-TiDBCloud-Public-Key", "wrapper-pub")
	cloned.Header.Set("X-TiDBCloud-Private-Key", "wrapper-priv")
	return rt.base.RoundTrip(cloned)
}

func TestViewDownloadRedirectRealHTTPClient307SameHostAltPortAndSubdomain(t *testing.T) {
	var apiEscapedHits atomic.Int64
	var lastTargetHeaders atomic.Value
	var lastOriginCloudAuth atomic.Value

	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/fs/secret" || r.URL.Path == "/v2/uploads/initiate" {
			apiEscapedHits.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		lastTargetHeaders.Store(r.Header.Clone())
		w.Header().Set("Set-Cookie", "s3_poison=1; Domain=api.example; Path=/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("object-payload"))
	}))
	defer targetSrv.Close()

	var redirectLocation atomic.Value
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastOriginCloudAuth.Store(r.Header.Get("X-TiDB-Cloud-Authorization"))
		loc, _ := redirectLocation.Load().(string)
		w.Header().Set("Set-Cookie", "origin_poison=1; Domain=api.example; Path=/")
		w.Header().Set("Location", loc)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer originSrv.Close()

	originAddr := originSrv.Listener.Addr().String()
	_, originPort, err := net.SplitHostPort(originAddr)
	require.NoError(t, err)
	targetAddr := targetSrv.Listener.Addr().String()
	_, targetPort, err := net.SplitHostPort(targetAddr)
	require.NoError(t, err)

	baseOrigin, err := url.Parse("http://api.example:" + originPort)
	require.NoError(t, err)

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var dialer net.Dialer
			switch addr {
			case "api.example:" + originPort:
				return dialer.DialContext(ctx, network, originAddr)
			case "api.example:" + targetPort, "sub.api.example:" + targetPort, "sub.api.example:80":
				return dialer.DialContext(ctx, network, targetAddr)
			default:
				return dialer.DialContext(ctx, network, addr)
			}
		},
	}
	defer transport.CloseIdleConnections()

	client := NewViewDownloadHTTPClient(baseOrigin, transport)
	require.Nil(t, client.httpClient.Jar)

	type httpDoer interface {
		Do(req *http.Request) (*http.Response, error)
	}
	sendViewDownload := func(cl httpDoer) (*http.Response, error) {
		req, rErr := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/file.bin", nil)
		require.NoError(t, rErr)
		req.Header.Set("Authorization", "test-auth-header")
		req.Header.Set("Proxy-Authorization", "test-proxy-auth")
		req.Header.Set("Cookie", "session=test-cookie")
		req.Header.Set("X-API-Key", "test-key-header")
		req.Header.Set("X-TiDB-Cloud-Authorization", "test-tidbcloud-auth")
		req.Header.Set("X-TiDBCloud-Public-Key", "test-pub-id")
		req.Header.Set("X-TiDBCloud-Private-Key", "test-priv-id")
		req.Header.Set("X-Dat9-Actor", "internal-actor")
		req.Header.Set(FenceHeader, "3")
		req.Header.Set("Range", "bytes=0-13")
		req.Header.Set("If-None-Match", `"etag-v1"`)
		return cl.Do(req)
	}

	assertTargetSafeHeadersOnly := func(loc string) {
		got, _ := lastTargetHeaders.Load().(http.Header)
		require.NotNil(t, got, "loc=%s", loc)
		for _, forbidden := range []string{
			"Authorization",
			"Proxy-Authorization",
			"Cookie",
			"X-API-Key",
			"X-TiDB-Cloud-Authorization",
			"X-TiDBCloud-Public-Key",
			"X-TiDBCloud-Private-Key",
			"X-Dat9-Actor",
			FenceHeader,
		} {
			require.Empty(t, got.Get(forbidden), "forbidden header %s leaked to %s", forbidden, loc)
		}
		for k := range got {
			require.True(t, isAllowedRedirectDownloadHeader(k), "unexpected non-allowlisted header %q forwarded to %s", k, loc)
		}
		require.Equal(t, "bytes=0-13", got.Get("Range"))
		require.Equal(t, `"etag-v1"`, got.Get("If-None-Match"))
	}

	// 1. 307 to same-host different-port /v1 API route MUST be blocked before hitting targetSrv.
	redirectLocation.Store("http://api.example:" + targetPort + "/v1/fs/secret")
	resp, err := sendViewDownload(client)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.ErrorIs(t, err, ErrInvalidViewRedirect)
	require.Zero(t, apiEscapedHits.Load())

	// 2. 307 to subdomain /v1 API route MUST be blocked before hitting targetSrv.
	redirectLocation.Store("http://sub.api.example:" + targetPort + "/v1/fs/secret")
	resp, err = sendViewDownload(client)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.ErrorIs(t, err, ErrInvalidViewRedirect)
	require.Zero(t, apiEscapedHits.Load())

	// 3. 307 to same-host different-port and subdomain object URLs preserves Range/If-*
	//    while pruning ALL auth/custom headers via the minimal allowlist.
	for _, validObjectLoc := range []string{
		"http://api.example:" + targetPort + "/s3-bucket/object-1",
		"http://sub.api.example:" + targetPort + "/s3-bucket/object-2",
	} {
		redirectLocation.Store(validObjectLoc)
		resp, err = sendViewDownload(client)
		require.NoError(t, err, "loc=%s", validObjectLoc)
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, readErr)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "object-payload", string(body))
		assertTargetSafeHeadersOnly(validObjectLoc)
	}

	// 4. Non-nil CookieJar + auth-injecting origin RoundTripper wrapper defense-in-depth:
	//    - Hop 0 uses originWrapper to authenticate to Drive9;
	//    - Hop 1+ bypasses originWrapper completely (routing over cloned concrete *http.Transport)
	//      AND prunes Jar cookies and all custom auth headers at the wire boundary.
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	jar.SetCookies(baseOrigin, []*http.Cookie{
		{Name: "drive9_session", Value: "jar-session-value", Domain: "api.example", Path: "/"},
	})
	subOrigin, err := url.Parse("http://sub.api.example:" + targetPort)
	require.NoError(t, err)
	require.NotEmpty(t, jar.Cookies(subOrigin))

	originWrapper := &testAuthInjectingTransport{base: transport}
	wrappedOriginClient := NewViewDownloadHTTPClientWithOrigin(&http.Client{
		Transport: originWrapper,
		Jar:       jar,
	}, baseOrigin, transport)
	require.Nil(t, wrappedOriginClient.httpClient.Jar)

	manualJarClient := &http.Client{
		Transport:     NewViewDownloadSplitTransport(baseOrigin, originWrapper, transport),
		Jar:           jar,
		CheckRedirect: NewViewDownloadCheckRedirect(baseOrigin),
	}

	for _, cl := range []httpDoer{wrappedOriginClient, manualJarClient} {
		for _, validObjectLoc := range []string{
			"http://api.example:" + targetPort + "/s3-bucket/jar-check-1",
			"http://sub.api.example:" + targetPort + "/s3-bucket/jar-check-2",
		} {
			beforeCalls := originWrapper.calls.Load()
			redirectLocation.Store(validObjectLoc)
			resp, err = sendViewDownload(cl)
			require.NoError(t, err, "loc=%s", validObjectLoc)
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.NoError(t, readErr)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "object-payload", string(body))
			require.Equal(t, int64(1), originWrapper.calls.Load()-beforeCalls, "auth wrapper must run on Hop 0 only and never on redirect hops")
			require.Equal(t, "wrapper-injected-cloud-auth", lastOriginCloudAuth.Load())
			assertTargetSafeHeadersOnly(validObjectLoc)
			require.Len(t, jar.Cookies(baseOrigin), 1, "Set-Cookie from 307/target must not enter Jar")
		}

		// 5. Hop-0 off-origin requests and mismatched req.Host overrides MUST fail
		//    closed BEFORE originWrapper can inject credentials.
		for _, offOriginHop0 := range []string{
			"http://sub.api.example:" + targetPort + "/s3-bucket/direct-presigned",
			"http://api.example:" + targetPort + "/s3-bucket/alt-port-direct",
			"https://api.example:" + originPort + "/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/file.bin",
		} {
			beforeCalls := originWrapper.calls.Load()
			offReq, rErr := http.NewRequest(http.MethodGet, offOriginHop0, nil)
			require.NoError(t, rErr)
			offResp, offErr := cl.Do(offReq)
			if offResp != nil {
				_ = offResp.Body.Close()
			}
			require.ErrorIs(t, offErr, ErrInvalidViewRedirect, "hop0=%s", offOriginHop0)
			require.Equal(t, beforeCalls, originWrapper.calls.Load(), "originWrapper must NOT be called for off-origin Hop 0 %s", offOriginHop0)
		}

		for _, badHostOverride := range []string{
			"evil.example:" + originPort,
			"sub.api.example:" + originPort,
			"api.example:" + targetPort,
			"user@api.example:" + originPort,
		} {
			beforeCalls := originWrapper.calls.Load()
			hostReq, rErr := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/file.bin", nil)
			require.NoError(t, rErr)
			hostReq.Host = badHostOverride
			hostResp, hostErr := cl.Do(hostReq)
			if hostResp != nil {
				_ = hostResp.Body.Close()
			}
			require.ErrorIs(t, hostErr, ErrInvalidViewRedirect, "hostOverride=%s", badHostOverride)
			require.Equal(t, beforeCalls, originWrapper.calls.Load(), "originWrapper must NOT be called for bad req.Host %s", badHostOverride)
		}
	}
}

func generateEphemeralTestMTLS(t *testing.T, originHost, targetHost string) (rootPool *x509.CertPool, originCert, targetCert, clientCert tls.Certificate) {
	t.Helper()
	now := time.Now().Add(-time.Minute)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ephemeral-ca"},
		NotBefore:             now,
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	rootPool = x509.NewCertPool()
	rootPool.AddCert(caCert)

	issueLeaf := func(serial int64, host string) tls.Certificate {
		k, kErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, kErr)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: host},
			DNSNames:     []string{host},
			NotBefore:    now,
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, cErr := x509.CreateCertificate(rand.Reader, tmpl, caCert, &k.PublicKey, caKey)
		require.NoError(t, cErr)
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}
	}
	originCert = issueLeaf(2, originHost)
	targetCert = issueLeaf(3, targetHost)

	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "drive9-origin-mtls-client"},
		NotBefore:    now,
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	require.NoError(t, err)
	clientCert = tls.Certificate{Certificate: [][]byte{cliDER}, PrivateKey: cliKey}
	return rootPool, originCert, targetCert, clientCert
}

func TestViewDownloadHTTPS307ScrubsMTLSClientCertAndProxyAuth(t *testing.T) {
	// Issue strictly disjoint leaf certificates: originCert covers ONLY api.example,
	// and targetCert covers ONLY s3.example.
	rootPool, originCert, targetCert, clientCert := generateEphemeralTestMTLS(t, "api.example", "s3.example")

	var originPeerCertCount atomic.Int64
	var targetPeerCertCount atomic.Int64
	var targetHits atomic.Int64
	var getClientCertCalls atomic.Int64
	var getProxyHeaderCalls atomic.Int64
	var targetHeaders atomic.Value

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	targetTLS := &tls.Config{
		Certificates: []tls.Certificate{targetCert},
		ClientCAs:    rootPool,
		// Actively send TLS CertificateRequest during handshake so any un-scrubbed
		// Certificates or GetClientCertificate on the redirect transport would
		// transmit the client certificate to the object host.
		ClientAuth: tls.RequestClientCert,
	}
	targetSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			targetHits.Add(1)
			if r.TLS != nil {
				targetPeerCertCount.Store(int64(len(r.TLS.PeerCertificates)))
			}
			targetHeaders.Store(r.Header.Clone())
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("mtls-free-object"))
		}),
	}
	go func() { _ = targetSrv.Serve(tls.NewListener(targetLn, targetTLS)) }()
	defer func() { _ = targetSrv.Close() }()

	_, targetPort, err := net.SplitHostPort(targetLn.Addr().String())
	require.NoError(t, err)

	originLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	originTLS := &tls.Config{
		Certificates: []tls.Certificate{originCert},
		ClientCAs:    rootPool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}
	var afterHop0GetClientCertCalls atomic.Int64
	var relTargetPeerCertCount atomic.Int64
	var dynamicOriginLocation atomic.Value
	dynamicOriginLocation.Store("https://s3.example:" + targetPort + "/bucket/key?X-Amz-Signature=placeholder")
	originSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/s3-local/rel-object" {
				if r.TLS != nil {
					relTargetPeerCertCount.Store(int64(len(r.TLS.PeerCertificates)))
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("relative-target-object"))
				return
			}
			if r.TLS != nil {
				originPeerCertCount.Store(int64(len(r.TLS.PeerCertificates)))
			}
			afterHop0GetClientCertCalls.Store(getClientCertCalls.Load())
			loc, _ := dynamicOriginLocation.Load().(string)
			w.Header().Set("Location", loc)
			w.WriteHeader(http.StatusTemporaryRedirect)
		}),
	}
	go func() { _ = originSrv.Serve(tls.NewListener(originLn, originTLS)) }()
	defer func() { _ = originSrv.Close() }()

	_, originPort, err := net.SplitHostPort(originLn.Addr().String())
	require.NoError(t, err)

	baseOrigin, err := url.Parse("https://api.example:" + originPort)
	require.NoError(t, err)

	var proxyWithCreds atomic.Bool
	var originVerifyPeerCalls atomic.Int64
	mtlsTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            rootPool,
			ServerName:         "api.example",
			InsecureSkipVerify: true,
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				originVerifyPeerCalls.Add(1)
				if len(rawCerts) == 0 {
					return fmt.Errorf("missing server certificate")
				}
				leaf, pErr := x509.ParseCertificate(rawCerts[0])
				if pErr != nil {
					return pErr
				}
				_, vErr := leaf.Verify(x509.VerifyOptions{
					DNSName: "api.example",
					Roots:   rootPool,
				})
				return vErr
			},
			Certificates: []tls.Certificate{clientCert},
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				getClientCertCalls.Add(1)
				return &clientCert, nil
			},
			ClientSessionCache: tls.NewLRUClientSessionCache(16),
		},
		ProxyConnectHeader: http.Header{"Proxy-Authorization": []string{"proxy-secret-placeholder"}},
		GetProxyConnectHeader: func(context.Context, *url.URL, string) (http.Header, error) {
			getProxyHeaderCalls.Add(1)
			return http.Header{"Proxy-Authorization": []string{"proxy-dyn-placeholder"}}, nil
		},
		Proxy: func(req *http.Request) (*url.URL, error) {
			if proxyWithCreds.Load() && req != nil && req.URL != nil && req.URL.Hostname() == "s3.example" {
				return url.Parse("http://proxyuser:proxypass@127.0.0.1:3128")
			}
			return nil, nil
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			switch addr {
			case "api.example:" + originPort:
				return d.DialContext(ctx, network, originLn.Addr().String())
			case "s3.example:" + targetPort:
				return d.DialContext(ctx, network, targetLn.Addr().String())
			default:
				return d.DialContext(ctx, network, addr)
			}
		},
	}
	defer mtlsTransport.CloseIdleConnections()

	originWrapper := &testAuthInjectingTransport{base: mtlsTransport}
	dlClient := NewViewDownloadHTTPClientWithOrigin(&http.Client{
		Transport: originWrapper,
	}, baseOrigin, mtlsTransport)

	// Also verify sanitized redirect transport fields directly.
	splitRT, ok := dlClient.httpClient.Transport.(viewDownloadRoundTripper)
	require.True(t, ok)
	require.Nil(t, splitRT.redirect.TLSClientConfig.Certificates)
	require.Nil(t, splitRT.redirect.TLSClientConfig.GetClientCertificate)
	require.Nil(t, splitRT.redirect.TLSClientConfig.ClientSessionCache)
	require.Empty(t, splitRT.redirect.TLSClientConfig.ServerName, "ServerName must be cleared so redirect target negotiates its own SNI/SAN")
	require.False(t, splitRT.redirect.TLSClientConfig.InsecureSkipVerify, "InsecureSkipVerify MUST be forced false on redirect transport")
	require.Nil(t, splitRT.redirect.TLSClientConfig.VerifyPeerCertificate)
	require.NotNil(t, splitRT.redirect.TLSClientConfig.RootCAs, "RootCAs must be preserved on redirect transport")
	require.Nil(t, splitRT.redirect.ProxyConnectHeader)
	require.Nil(t, splitRT.redirect.GetProxyConnectHeader)
	require.Nil(t, splitRT.redirect.DialTLSContext)
	legacyDialTLSTransport := &http.Transport{
		DialTLS: func(string, string) (net.Conn, error) { return nil, errors.New("legacy DialTLS") }, //nolint:staticcheck // deliberately verifies deprecated DialTLS is scrubbed
	}
	scrubbedLegacy := sanitizeRedirectHTTPTransport(legacyDialTLSTransport)
	require.Nil(t, scrubbedLegacy.DialTLS)            //nolint:staticcheck // deliberately verifies deprecated DialTLS is scrubbed
	require.NotNil(t, legacyDialTLSTransport.DialTLS) //nolint:staticcheck // caller transport must remain untouched
	// Caller's original mtlsTransport MUST NOT be mutated.
	require.Equal(t, "api.example", mtlsTransport.TLSClientConfig.ServerName)
	require.True(t, mtlsTransport.TLSClientConfig.InsecureSkipVerify)
	require.NotNil(t, mtlsTransport.TLSClientConfig.VerifyPeerCertificate)
	require.Len(t, mtlsTransport.TLSClientConfig.Certificates, 1)
	require.NotNil(t, mtlsTransport.TLSClientConfig.GetClientCertificate)

	// 1. Real HTTPS 307: Hop 0 completes mTLS with clientCert; Hop 1 connects to
	//    targetSrv (which sends TLS CertificateRequest) with ZERO client certs!
	req, err := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin", nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=0-15")
	req.Header.Set("X-TiDB-Cloud-Authorization", "caller-cloud-auth")
	resp, err := dlClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "mtls-free-object", string(body))
	require.Equal(t, int64(1), originPeerCertCount.Load(), "Hop 0 must authenticate with mTLS client cert")
	require.Equal(t, int64(1), afterHop0GetClientCertCalls.Load(), "Hop 0 mTLS handshake invokes GetClientCertificate")
	require.Equal(t, int64(0), targetPeerCertCount.Load(), "Hop 1 MUST NOT present any mTLS client cert even when server requests one")
	require.Equal(t, afterHop0GetClientCertCalls.Load(), getClientCertCalls.Load(), "GetClientCertificate must never be called on redirect transport")
	require.Zero(t, getProxyHeaderCalls.Load())
	require.Equal(t, int64(1), originWrapper.calls.Load())
	gotHdr, _ := targetHeaders.Load().(http.Header)
	require.Equal(t, "bytes=0-15", gotHdr.Get("Range"))
	require.Empty(t, gotHdr.Get("X-TiDB-Cloud-Authorization"))
	require.Empty(t, gotHdr.Get("Authorization"))

	// 2. Direct Hop-0 call to external presigned HTTPS URL MUST fail closed before
	//    mtlsTransport or originWrapper is touched, AND MUST scrub query signatures
	//    from both inner errors and outer *url.Error.
	beforeWrapper := originWrapper.calls.Load()
	beforeTarget := targetHits.Load()
	const sensitiveQueryMarker = "TEST_ONLY_PRESIGNED_QUERY_SIG_DO_NOT_ECHO"
	directReq, err := http.NewRequest(http.MethodGet, "https://s3.example:"+targetPort+"/bucket/key?X-Amz-Signature="+sensitiveQueryMarker+"#frag-marker", nil)
	require.NoError(t, err)
	directResp, directErr := dlClient.Do(directReq)
	if directResp != nil {
		_ = directResp.Body.Close()
	}
	require.ErrorIs(t, directErr, ErrInvalidViewRedirect)
	require.NotContains(t, directErr.Error(), sensitiveQueryMarker)
	require.NotContains(t, directErr.Error(), "frag-marker")
	var uErr *url.Error
	require.ErrorAs(t, directErr, &uErr)
	require.Equal(t, "https://s3.example:"+targetPort+"/bucket/key", uErr.URL)
	require.Equal(t, beforeWrapper, originWrapper.calls.Load())
	require.Equal(t, beforeTarget, targetHits.Load())

	// 2b. DoRedirectTarget strictly requires an already-resolved absolute http(s)
	//     URL (rejecting ALL relative references), while ResolveRedirectLocation /
	//     DoRedirectLocation resolve raw Location values against the Hop-0 source
	//     request URL per RFC 9110 / RFC 3986 (preventing path-relative Location
	//     values like "next?sig=..." or "../secret" under /v1/views/... from
	//     escaping to "/next" or dropping a no-trailing-slash deployment prefix).
	preExtractedReq, err := http.NewRequest(http.MethodGet, "https://s3.example:"+targetPort+"/bucket/key?X-Amz-Signature="+sensitiveQueryMarker, nil)
	require.NoError(t, err)
	preExtractedReq.Header.Set("Authorization", "caller-auth-must-strip")
	preExtractedReq.Header.Set("Range", "bytes=0-15")
	preResp, err := dlClient.DoRedirectTarget(preExtractedReq)
	require.NoError(t, err)
	preBody, err := io.ReadAll(preResp.Body)
	_ = preResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "mtls-free-object", string(preBody))
	require.Equal(t, beforeWrapper, originWrapper.calls.Load(), "DoRedirectTarget must bypass originWrapper")
	require.Equal(t, int64(0), targetPeerCertCount.Load())

	for _, rejectedRel := range []string{
		"/s3-local/rel-object?X-Amz-Signature=" + sensitiveQueryMarker,
		"/v1/fs/secret?X-Amz-Signature=" + sensitiveQueryMarker,
		"next?X-Amz-Signature=" + sensitiveQueryMarker,
		"../secret?X-Amz-Signature=" + sensitiveQueryMarker,
	} {
		relURL, pErr := url.Parse(rejectedRel)
		require.NoError(t, pErr)
		badRelResp, badRelErr := dlClient.DoRedirectTarget(&http.Request{Method: "", URL: relURL})
		if badRelResp != nil {
			_ = badRelResp.Body.Close()
		}
		require.ErrorIs(t, badRelErr, ErrInvalidViewRedirect, "relative URL %q must be rejected by DoRedirectTarget", rejectedRel)
		require.NotContains(t, badRelErr.Error(), sensitiveQueryMarker)
	}

	hop0SourceURL, err := url.Parse(baseOrigin.String() + "/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/dir/file.bin")
	require.NoError(t, err)
	for _, escapedRelLoc := range []string{
		"next?X-Amz-Signature=" + sensitiveQueryMarker,
		"../secret?X-Amz-Signature=" + sensitiveQueryMarker,
		"?X-Amz-Signature=" + sensitiveQueryMarker,
		"/v1/fs/secret?X-Amz-Signature=" + sensitiveQueryMarker,
	} {
		_, rErr := dlClient.ResolveRedirectLocation(hop0SourceURL, escapedRelLoc)
		require.ErrorIs(t, rErr, ErrInvalidViewRedirect, "rawLoc=%q must resolve under /v1 and fail closed", escapedRelLoc)
		require.NotContains(t, rErr.Error(), sensitiveQueryMarker)

		locResp, locErr := dlClient.DoRedirectLocation(context.Background(), hop0SourceURL, escapedRelLoc, nil)
		if locResp != nil {
			_ = locResp.Body.Close()
		}
		require.ErrorIs(t, locErr, ErrInvalidViewRedirect, "DoRedirectLocation rawLoc=%q", escapedRelLoc)
		require.NotContains(t, locErr.Error(), sensitiveQueryMarker)
	}

	for _, validRelLoc := range []string{
		"/s3-local/rel-object?X-Amz-Signature=" + sensitiveQueryMarker,
		"../../../../../../s3-local/rel-object?X-Amz-Signature=" + sensitiveQueryMarker,
	} {
		relHdr := make(http.Header)
		relHdr.Set("Authorization", "caller-auth-must-strip")
		relResp, relErr := dlClient.DoRedirectLocation(context.Background(), hop0SourceURL, validRelLoc, relHdr)
		require.NoError(t, relErr, "validRelLoc=%q", validRelLoc)
		relBody, readErr := io.ReadAll(relResp.Body)
		_ = relResp.Body.Close()
		require.NoError(t, readErr)
		require.Equal(t, "relative-target-object", string(relBody))
		require.Equal(t, beforeWrapper, originWrapper.calls.Load(), "DoRedirectLocation must bypass originWrapper")
		require.Zero(t, relTargetPeerCertCount.Load(), "DoRedirectLocation must present zero mTLS client certs")
	}

	// No-trailing-slash deployment prefix on baseOrigin: ResolveRedirectLocation
	// must resolve against sourceURL (preserving "/drive9" prefix on dot-segment
	// traversal to "/drive9/s3-local/obj" while blocking "/drive9/v2/..." and "next").
	noSlashPrefixBase, err := url.Parse("https://api.example:" + originPort + "/drive9")
	require.NoError(t, err)
	prefixedDLClient := NewViewDownloadHTTPClientWithOrigin(&http.Client{Transport: originWrapper}, noSlashPrefixBase, mtlsTransport)
	prefixedSourceURL, err := url.Parse(noSlashPrefixBase.String() + "/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/dir/file.bin")
	require.NoError(t, err)
	resolvedPrefixedObj, err := prefixedDLClient.ResolveRedirectLocation(prefixedSourceURL, "../../../../../../s3-local/obj?X-Amz-Signature="+sensitiveQueryMarker)
	require.NoError(t, err)
	require.Equal(t, "/drive9/s3-local/obj", resolvedPrefixedObj.Path)
	for _, badPrefixedRel := range []string{
		"next?X-Amz-Signature=" + sensitiveQueryMarker,
		"../../../../../s3-local/obj?X-Amz-Signature=" + sensitiveQueryMarker,
		"../../../../../../v2/uploads/initiate?X-Amz-Signature=" + sensitiveQueryMarker,
	} {
		_, pErr := prefixedDLClient.ResolveRedirectLocation(prefixedSourceURL, badPrefixedRel)
		require.ErrorIs(t, pErr, ErrInvalidViewRedirect, "badPrefixedRel=%q", badPrefixedRel)
		require.NotContains(t, pErr.Error(), sensitiveQueryMarker)
	}

	// 2c. Empty req.Method ("", implicit GET in Go net/http) MUST succeed across 307 redirect!
	implicitGetURL, err := url.Parse(baseOrigin.String() + "/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin")
	require.NoError(t, err)
	implicitResp, err := dlClient.Do(&http.Request{Method: "", URL: implicitGetURL, Header: make(http.Header)})
	require.NoError(t, err)
	implicitBody, err := io.ReadAll(implicitResp.Body)
	_ = implicitResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, "mtls-free-object", string(implicitBody))
	require.Positive(t, originVerifyPeerCalls.Load())

	// 2d. Untrusted redirect HTTPS server MUST fail x509 verification even when
	//     origin transport used InsecureSkipVerify: true + VerifyPeerCertificate!
	_, untrustedCert, _, _ := generateEphemeralTestMTLS(t, "untrusted.s3.example", "other.example")
	untrustedLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	untrustedSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	}
	go func() {
		_ = untrustedSrv.Serve(tls.NewListener(untrustedLn, &tls.Config{Certificates: []tls.Certificate{untrustedCert}}))
	}()
	defer func() { _ = untrustedSrv.Close() }()
	dynamicOriginLocation.Store("https://" + untrustedLn.Addr().String() + "/bucket/untrusted?X-Amz-Signature=" + sensitiveQueryMarker)
	untrustedReq, err := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin", nil)
	require.NoError(t, err)
	untrustedResp, untrustedErr := dlClient.Do(untrustedReq)
	if untrustedResp != nil {
		_ = untrustedResp.Body.Close()
	}
	require.Error(t, untrustedErr, "redirect to untrusted self-signed HTTPS host must fail TLS verification")
	require.NotContains(t, untrustedErr.Error(), sensitiveQueryMarker)

	// 2e. Malformed Location header (invalid percent-escape before CheckRedirect)
	//     AND %w-wrapped *url.Error MUST NEVER echo presigned query parameters!
	dynamicOriginLocation.Store("https://s3.example:" + targetPort + "/bucket/bad%ZZ?X-Amz-Signature=" + sensitiveQueryMarker)
	malformedReq, err := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin", nil)
	require.NoError(t, err)
	malformedResp, malformedErr := dlClient.Do(malformedReq)
	if malformedResp != nil {
		_ = malformedResp.Body.Close()
	}
	require.ErrorIs(t, malformedErr, ErrInvalidViewRedirect)
	require.NotContains(t, malformedErr.Error(), sensitiveQueryMarker)

	rawStdlibParseErr := fmt.Errorf("outer context: %w", &url.Error{
		Op:  "Get",
		URL: "https://user:secret@s3.example/bucket/key?X-Amz-Signature=" + sensitiveQueryMarker + "#frag",
		Err: fmt.Errorf("failed to parse Location header %q: invalid URL escape", "https://s3.example/b\"x?X-Amz-Signature="+sensitiveQueryMarker+"%ZZ"),
	})
	scrubbedWrapped := SanitizeViewDownloadError(rawStdlibParseErr)
	require.NotContains(t, scrubbedWrapped.Error(), sensitiveQueryMarker)
	require.NotContains(t, scrubbedWrapped.Error(), "secret")
	var unwrappedUErr *url.Error
	require.ErrorAs(t, scrubbedWrapped, &unwrappedUErr)
	require.Equal(t, "https://s3.example/bucket/key", unwrappedUErr.URL)
	require.NotContains(t, unwrappedUErr.Err.Error(), sensitiveQueryMarker)

	// Unquoted, bracketed, userinfo-bearing, and single-quoted IPv6 URLs MUST
	// scrub query signatures, fragments, and userinfo completely without stopping
	// early at the IPv6 host closing ']'.
	for _, ipv6ErrMsg := range []string{
		"redirect failed for https://[2001:db8::1]:8443/bucket/key?X-Amz-Signature=" + sensitiveQueryMarker + "#frag6 after 1 hop",
		"failed [https://[2001:db8::1]:8443/bucket/key?X-Amz-Signature=" + sensitiveQueryMarker + "]",
		"failed https://user:secret@[2001:db8::1]:8443/bucket/key?X-Amz-Signature=" + sensitiveQueryMarker,
		"failed 'https://[2001:db8::1]:8443/bucket/key?X-Amz-Signature=" + sensitiveQueryMarker + "'",
	} {
		scrubbedIPv6 := SanitizeViewDownloadError(errors.New(ipv6ErrMsg))
		require.NotContains(t, scrubbedIPv6.Error(), sensitiveQueryMarker, "ipv6ErrMsg=%s", ipv6ErrMsg)
		require.NotContains(t, scrubbedIPv6.Error(), "frag6", "ipv6ErrMsg=%s", ipv6ErrMsg)
		require.NotContains(t, scrubbedIPv6.Error(), "secret", "ipv6ErrMsg=%s", ipv6ErrMsg)
		require.Contains(t, scrubbedIPv6.Error(), "https://[2001:db8::1]:8443/bucket/key", "ipv6ErrMsg=%s", ipv6ErrMsg)
	}

	// Custom origin RoundTripper returning arbitrary unstructured OR %w-wrapped
	// stdlib/sentinel errors (context.Canceled, context.DeadlineExceeded, io.EOF,
	// x509, *net.OpError) MUST NEVER echo raw outer text at ViewDownloadClient's
	// external error boundary while still preserving cancellation/timeout semantics.
	wrappedCases := []struct {
		name        string
		retErr      error
		wantIs      error
		wantTimeout bool
		wantSubstr  string
	}{
		{
			name:       "plain_custom",
			retErr:     errors.New("custom auth plugin failed: raw_secret=" + sensitiveQueryMarker),
			wantSubstr: "view download transport failure",
		},
		{
			name:   "wrapped_canceled",
			retErr: fmt.Errorf("custom wrapper raw_secret=%s: %w", sensitiveQueryMarker, context.Canceled),
			wantIs: context.Canceled,
		},
		{
			name:        "wrapped_deadline",
			retErr:      fmt.Errorf("custom wrapper raw_secret=%s: %w", sensitiveQueryMarker, context.DeadlineExceeded),
			wantIs:      context.DeadlineExceeded,
			wantTimeout: true,
		},
		{
			name:   "wrapped_eof",
			retErr: fmt.Errorf("custom wrapper raw_secret=%s: %w", sensitiveQueryMarker, io.ErrUnexpectedEOF),
			wantIs: io.ErrUnexpectedEOF,
		},
		{
			name:       "wrapped_x509",
			retErr:     fmt.Errorf("custom wrapper raw_secret=%s: %w", sensitiveQueryMarker, x509.UnknownAuthorityError{}),
			wantSubstr: "tls verification or handshake failure",
		},
		{
			name:       "wrapped_net_op",
			retErr:     fmt.Errorf("custom wrapper raw_secret=%s: %w", sensitiveQueryMarker, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("inner_secret=" + sensitiveQueryMarker)}),
			wantSubstr: "network transport failure",
		},
		{
			name: "poisoned_url_error_fields",
			retErr: &url.Error{
				Op:  "OpLeak_" + sensitiveQueryMarker,
				URL: "non-url-secret=" + sensitiveQueryMarker,
				Err: fmt.Errorf("outer_secret=%s: %w", sensitiveQueryMarker, context.Canceled),
			},
			wantIs: context.Canceled,
		},
	}
	for _, tc := range wrappedCases {
		customLeakClient := NewViewDownloadHTTPClientWithOrigin(&http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, tc.retErr
			}),
		}, baseOrigin, mtlsTransport)
		leakReq, rErr := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin", nil)
		require.NoError(t, rErr)
		leakResp, leakErr := customLeakClient.Do(leakReq)
		if leakResp != nil {
			_ = leakResp.Body.Close()
		}
		require.Error(t, leakErr, tc.name)
		require.NotContains(t, leakErr.Error(), sensitiveQueryMarker, tc.name)
		require.NotContains(t, leakErr.Error(), "raw_secret", tc.name)
		require.NotContains(t, leakErr.Error(), "inner_secret", tc.name)
		if tc.wantIs != nil {
			require.ErrorIs(t, leakErr, tc.wantIs, tc.name)
		}
		if tc.wantSubstr != "" {
			require.Contains(t, leakErr.Error(), tc.wantSubstr, tc.name)
		}
		var boundaryUErr *url.Error
		require.ErrorAs(t, leakErr, &boundaryUErr, tc.name)
		require.Equal(t, tc.wantTimeout, boundaryUErr.Timeout(), tc.name)
	}

	dynamicOriginLocation.Store("https://s3.example:" + targetPort + "/bucket/key?X-Amz-Signature=placeholder")

	// 3. Proxy returning userinfo credentials on redirect hop MUST fail closed with
	//    ErrInvalidViewRedirect and scrub presigned query parameters from *url.Error.
	proxyWithCreds.Store(true)
	reqProxy, err := http.NewRequest(http.MethodGet, baseOrigin.String()+"/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/blob.bin", nil)
	require.NoError(t, err)
	proxyResp, proxyErr := dlClient.Do(reqProxy)
	if proxyResp != nil {
		_ = proxyResp.Body.Close()
	}
	require.ErrorIs(t, proxyErr, ErrInvalidViewRedirect)
	require.NotContains(t, proxyErr.Error(), "placeholder")
	require.NotContains(t, proxyErr.Error(), "X-Amz-Signature")
}

func TestReservedTagPrefixPreservesKeyAndValueCase(t *testing.T) {
	key, value, hasValue, isTag, err := ParseTagSelector("TaG:Team=DB")
	require.NoError(t, err)
	require.True(t, isTag)
	require.True(t, hasValue)
	require.Equal(t, "Team", key)
	require.Equal(t, "DB", value)
	_, _, _, _, err = ParseTagSelector("TAG:")
	require.ErrorIs(t, err, ErrInvalidLayerRef)
}

func TestLayerNamesAndRefsUsePrintableUTF8(t *testing.T) {
	for _, s := range []string{"a\u0091b", "a\u202eb", string([]byte{0xff})} {
		_, err := NormalizeLayerName(s)
		require.ErrorIs(t, err, ErrInvalidLayerName)
		_, err = NormalizeLayerRef(s)
		require.ErrorIs(t, err, ErrInvalidLayerRef)
	}
	name, err := NormalizeLayerName("研究")
	require.NoError(t, err)
	require.Equal(t, "研究", name)
}
