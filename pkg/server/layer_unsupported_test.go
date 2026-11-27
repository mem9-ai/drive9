package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedServerLayerRoutesReturn501RequiresTiDBServer(t *testing.T) {
	s := newTestServer(t)
	for _, tc := range []struct {
		method string
		path   string
		code   string
	}{
		{http.MethodPost, "/v1/layers", viewprotocol.CodeLayerRequiresTiDBServer},
		{http.MethodGet, "/v1/layers", viewprotocol.CodeLayerRequiresTiDBServer},
		{http.MethodGet, "/v1/layers/work-a", viewprotocol.CodeLayerRequiresTiDBServer},
		{http.MethodGet, "/v1/views/layer_01j8z4r9w3m5n7p9q2s4t6v8xz/v1/fs/a.txt", viewprotocol.CodeLayerRequiresTiDBServer},
		{http.MethodGet, "/v1/fs/?grep=foo&layer=work-a", viewprotocol.CodeUnsupportedInLayerView},
		{http.MethodGet, "/v1/fs/?find=1&layer=work-a", viewprotocol.CodeUnsupportedInLayerView},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		require.Equal(t, http.StatusNotImplemented, w.Code, "%s %s", tc.method, tc.path)
		var body map[string]string
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Equal(t, tc.code, body["code"], "%s %s", tc.method, tc.path)
	}
}
