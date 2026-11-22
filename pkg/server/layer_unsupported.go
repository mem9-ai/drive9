package server

import (
	"encoding/json"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func (s *Server) handleLayerUnsupported(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": "LayerFS requires the TiDB server runtime",
		"code":  viewprotocol.CodeLayerRequiresTiDBServer,
	})
}

func writeUnsupportedLayerQuery(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": "operation unsupported in layer view",
		"code":  viewprotocol.CodeUnsupportedInLayerView,
	})
}
