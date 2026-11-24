package client

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
	"github.com/stretchr/testify/require"
)

func TestViewClientSharedRouteCorpus(t *testing.T) {
	const origin = "https://example.invalid/deploy"
	const id = "layer_01j8z4r9w3m5n7p9q2s4t6v8xz"
	raw, err := os.ReadFile("../viewprotocol/testdata/client_routes.json")
	require.NoError(t, err)
	var cases []struct {
		Method, Target, Action string
		Mutates                bool
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	for _, tc := range cases {
		t.Run(tc.Method+" "+tc.Target, func(t *testing.T) {
			var calls int
			c, err := New(origin, "").WithView(ViewBinding{LayerID: id, Fence: 1, WriteGuard: func() error { calls++; return nil }})
			require.NoError(t, err)
			req, err := http.NewRequest(tc.Method, origin+tc.Target, nil)
			require.NoError(t, err)
			req.Header.Set(viewprotocol.FenceHeader, "99")
			got, err := c.rewriteRequestForView(req)
			switch tc.Action {
			case "pass":
				require.NoError(t, err)
				require.Empty(t, got.Header.Get(viewprotocol.FenceHeader))
				require.Equal(t, req.URL.String(), got.URL.String())
			case "rewrite":
				require.NoError(t, err)
				require.Equal(t, "1", got.Header.Get(viewprotocol.FenceHeader))
				require.Equal(t, origin+"/v1/views/"+id+tc.Target, got.URL.String())
			case "unsupported":
				require.ErrorIs(t, err, viewprotocol.ErrUnsupportedInLayerView)
			case "invalid":
				require.ErrorIs(t, err, ErrInvalidViewRequest)
			default:
				t.Fatalf("unexpected action %q", tc.Action)
			}
			if tc.Mutates {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}
