package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
	"github.com/stretchr/testify/require"
)

func TestWithViewThreeSetRoutingPaginationAndRedirectIsolation(t *testing.T) {
	const lid = "layer_01j8z4r9w3m5n7p9q2s4t6v8xz"
	ctx := context.Background()

	objSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.Header.Get("Authorization"), "redirected S3 GET must scrub Authorization")
		require.Empty(t, r.Header.Get(viewprotocol.FenceHeader), "redirected S3 GET must scrub X-FS-Layer-Fence")
		_, _ = w.Write([]byte("s3-bytes"))
	}))
	defer objSrv.Close()

	var seenControlFence string
	var seenViewPaths []string
	var seenViewFences []string

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			seenControlFence = r.Header.Get(viewprotocol.FenceHeader)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "inline_threshold": 50000})
		case "/v1/layers/" + lid + "/renew-mount":
			seenControlFence = r.Header.Get(viewprotocol.FenceHeader)
			_ = json.NewEncoder(w).Encode(LayerMountLease{LayerID: lid, MountID: "m1", Fence: 3, MountLeaseTTLMS: 60000})
		case "/v1/views/" + lid + "/v1/fs/grep":
			seenViewPaths = append(seenViewPaths, r.URL.Path)
			seenViewFences = append(seenViewFences, r.Header.Get(viewprotocol.FenceHeader))
			_, _ = w.Write([]byte("literal-grep"))
		case "/v1/views/" + lid + "/v1/fs/find":
			seenViewPaths = append(seenViewPaths, r.URL.Path)
			_, _ = w.Write([]byte("literal-find"))
		case "/v1/views/" + lid + "/v1/fs/big.bin":
			w.Header().Set("Location", objSrv.URL+"/bucket/big.bin?X-Amz-Signature=placeholder")
			w.WriteHeader(http.StatusFound)
		case "/v1/views/" + lid + "/v1/fs/rebound.bin":
			w.Header().Set("Location", "/v1/fs/main-secret.txt")
			w.WriteHeader(http.StatusFound)
		case "/v1/views/" + lid + "/v1/fs/dir/":
			if r.URL.Query().Get("cursor") == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries":     []FileInfo{{Name: "a.txt", Size: 1}},
					"next_cursor": "cur-2",
				})
			} else {
				require.Equal(t, "cur-2", r.URL.Query().Get("cursor"))
				_ = json.NewEncoder(w).Encode(map[string]any{
					"entries": []FileInfo{{Name: "b.txt", Size: 2}},
				})
			}
		default:
			t.Fatalf("unexpected request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer apiSrv.Close()

	base := New(apiSrv.URL, "test-token-placeholder")
	_, err := base.WithView(ViewBinding{LayerID: "work-a"})
	require.ErrorIs(t, err, viewprotocol.ErrInvalidLayerID)
	_, err = base.WithView(ViewBinding{LayerID: lid, Fence: 2})
	require.ErrorIs(t, err, viewprotocol.ErrInvalidFence)

	vc, err := base.WithView(ViewBinding{LayerID: lid, Fence: 3})
	require.NoError(t, err)

	// Set A: /v1/status and /v1/layers/* pass through without /v1/views prefix and without X-FS-Layer-Fence.
	vc.Warm(ctx)
	require.Empty(t, seenControlFence)
	lease, err := vc.RenewLayerMount(ctx, lid, LayerMountRenewRequest{MountID: "m1", Fence: 3})
	require.NoError(t, err)
	require.Equal(t, int64(3), lease.Fence)
	require.Empty(t, seenControlFence)

	// Set B: literal filenames /grep and /find rewrite to /v1/views/{lid} with X-FS-Layer-Fence: 3.
	bGrep, err := vc.ReadCtx(ctx, "/grep")
	require.NoError(t, err)
	require.Equal(t, "literal-grep", string(bGrep))
	bFind, err := vc.ReadCtx(ctx, "/find")
	require.NoError(t, err)
	require.Equal(t, "literal-find", string(bFind))
	require.Equal(t, []string{"3"}, seenViewFences)

	// Paginated ListCtx follows next_cursor transparently.
	entries, err := vc.ListCtx(ctx, "/dir/")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, "a.txt", entries[0].Name)
	require.Equal(t, "b.txt", entries[1].Name)

	// S3 redirect isolated & rebounds to /v1/* blocked!
	s3Data, err := vc.ReadCtx(ctx, "/big.bin")
	require.NoError(t, err)
	require.Equal(t, "s3-bytes", string(s3Data))

	_, err = vc.ReadCtx(ctx, "/rebound.bin")
	require.ErrorIs(t, err, viewprotocol.ErrInvalidViewRedirect)

	// Set C: ?grep, ?find, SQL, SetMetadata(Description), BatchWrite(>100) rejected locally before network!
	_, err = vc.Grep("q", "/", 10)
	require.ErrorIs(t, err, viewprotocol.ErrUnsupportedInLayerView)
	_, err = vc.Find("/", url.Values{})
	require.ErrorIs(t, err, viewprotocol.ErrUnsupportedInLayerView)
	_, err = vc.SQL("SELECT 1")
	require.ErrorIs(t, err, viewprotocol.ErrUnsupportedInLayerView)
	desc := "nope"
	err = vc.SetMetadataCtx(ctx, "/grep", SetMetadataOptions{Description: &desc})
	require.ErrorIs(t, err, viewprotocol.ErrUnsupportedInLayerView)

	tooMany := make([]BatchWriteItem, MaxLayerViewBatchWriteItems+1)
	for i := range tooMany {
		tooMany[i] = BatchWriteItem{Path: "/a.txt", Data: []byte("x")}
	}
	_, err = vc.BatchWriteCtx(ctx, tooMany)
	require.Error(t, err)

	// WriteGuard MUST block GET /v1/uploads/{id}/resume (mints presigned PUT URLs),
	// while still permitting read-only GET /v1/uploads?path=...
	guardErr := errors.New("self-fenced write guard")
	guardedVC, err := base.WithView(ViewBinding{
		LayerID:    lid,
		Fence:      3,
		WriteGuard: func() error { return guardErr },
	})
	require.NoError(t, err)
	resumeReq, err := http.NewRequestWithContext(ctx, http.MethodGet, apiSrv.URL+"/v1/uploads/up-123/resume", nil)
	require.NoError(t, err)
	_, err = guardedVC.rewriteRequestForView(resumeReq)
	require.ErrorIs(t, err, guardErr)

	for _, path := range []string{"/v1/fs:batch-stat", "/v1/fs:batch-read-small", "/v1/fs:batch-write"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiSrv.URL+path, nil)
		require.NoError(t, err)
		out, err := guardedVC.rewriteRequestForView(req)
		if path == "/v1/fs:batch-write" {
			require.ErrorIs(t, err, guardErr)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, "/v1/views/"+lid+path, out.URL.Path)
	}

	listUpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, apiSrv.URL+"/v1/uploads?path=%2Fdir%2Fa.txt", nil)
	require.NoError(t, err)
	rewrittenListUp, err := guardedVC.rewriteRequestForView(listUpReq)
	require.NoError(t, err)
	require.Equal(t, "/v1/views/"+lid+"/v1/uploads", rewrittenListUp.URL.Path)
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("body read failed")
}

func (failingReadCloser) Close() error {
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
