//go:build !windows

package fuse

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func (b *fakeSwitchBackend) serveLayersSubpath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/layers/")
	parts := strings.SplitN(rest, "/", 2)
	layerID := parts[0]
	st, ok := b.layers[layerID]
	if !ok {
		for id, cand := range b.layers {
			if cand.Info.Name == layerID {
				layerID = id
				st = cand
				ok = true
				break
			}
		}
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_NOT_FOUND", "error": "layer not found"})
		return
	}
	sub := ""
	if len(parts) > 1 {
		sub = "/" + parts[1]
	}
	switch {
	case sub == "" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(st.Info)
	case sub == "/bind-mount" && r.Method == http.MethodPost:
		b.bindCalls++
		var req client.LayerMountBindRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if st.Info.Fence%2 == 1 {
			if st.OwnerMountID == req.MountID && st.LastBindOpID == req.OperationID {
				_ = json.NewEncoder(w).Encode(client.LayerMountLease{
					LayerID: layerID, MountID: req.MountID, Fence: st.Info.Fence, MountLeaseTTLMS: 30000,
				})
				return
			}
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_MOUNT_CONFLICT", "error": "active mount lease"})
			return
		}
		if req.ExpectedFence != st.Info.Fence {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_FENCE_MISMATCH", "error": "fence mismatch"})
			return
		}
		st.Info.Fence++
		st.Info.MountExpired = false
		st.OwnerMountID = req.MountID
		st.LastBindOpID = req.OperationID
		_ = json.NewEncoder(w).Encode(client.LayerMountLease{
			LayerID: layerID, MountID: req.MountID, Fence: st.Info.Fence, MountLeaseTTLMS: 30000,
		})
		if b.onBind != nil {
			b.onBind(layerID)
		}
	case sub == "/renew-mount" && r.Method == http.MethodPost:
		b.renewCnt++
		if b.onRenew != nil {
			b.onRenew(layerID)
		}
		var req client.LayerMountRenewRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if st.Info.Fence%2 != 1 || st.Info.Fence != req.Fence || st.OwnerMountID != req.MountID {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_FENCE_MISMATCH", "error": "lease revoked"})
			return
		}
		_ = json.NewEncoder(w).Encode(client.LayerMountLease{
			LayerID: layerID, MountID: req.MountID, Fence: st.Info.Fence, MountLeaseTTLMS: 30000,
		})
	case sub == "/unbind-mount" && r.Method == http.MethodPost:
		var req client.LayerMountUnbindRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if onUnbind := b.onUnbind; onUnbind != nil {
			b.mu.Unlock()
			onUnbind(layerID, req)
			b.mu.Lock()
		}
		b.unbindOps = append(b.unbindOps, req)
		if st.UnbindFailCnt > 0 {
			st.UnbindFailCnt--
			http.Error(w, `{"error":"transient unbind timeout"}`, http.StatusServiceUnavailable)
			return
		}
		switch req.Kind {
		case mountstate.UnbindKindExpire:
			if st.Info.Fence%2 == 1 && st.Info.MountExpired {
				st.Info.Fence++
				st.Info.MountExpired = false
				st.OwnerMountID = ""
			}
		case mountstate.UnbindKindOp:
			if st.Info.Fence%2 == 1 && st.OwnerMountID == req.MountID && req.OperationID != "" && st.LastBindOpID == req.OperationID {
				st.Info.Fence++
				st.OwnerMountID = ""
			} else if st.Info.Fence%2 == 0 && req.Fence >= 0 && req.Fence%2 == 0 && st.Info.Fence == req.Fence {
				st.Info.Fence += 2
			}
		case mountstate.UnbindKindFence:
			if st.Info.Fence%2 == 1 && st.Info.Fence == req.Fence && st.OwnerMountID == req.MountID {
				st.Info.Fence++
				st.OwnerMountID = ""
			}
		}
		_ = json.NewEncoder(w).Encode(client.LayerMountUnbindResult{LayerID: layerID, Fence: st.Info.Fence, Unbound: true})
	default:
		http.NotFound(w, r)
	}
}

func (b *fakeSwitchBackend) serveViewsSubpath(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/views/")
	parts := strings.SplitN(rest, "/", 2)
	layerID := parts[0]
	st, ok := b.layers[layerID]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_NOT_FOUND", "error": "layer not found"})
		return
	}
	inner := "/"
	if len(parts) > 1 {
		inner = "/" + parts[1]
	}
	if !strings.HasPrefix(inner, "/v1/fs") {
		http.NotFound(w, r)
		return
	}
	p := strings.TrimPrefix(inner, "/v1/fs")
	if p == "" {
		p = "/"
	}
	if !viewprotocol.PathWithinBaseRoot(st.Info.BaseRootPath, p) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "FORBIDDEN_OUTSIDE_BASE_ROOT", "error": "outside base_root_path"})
		return
	}
	if r.Method == http.MethodPut {
		rawFence := r.Header.Get("X-FS-Layer-Fence")
		fence, _ := strconv.ParseInt(rawFence, 10, 64)
		if st.Info.Fence%2 != 1 || st.Info.Fence != fence {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "LAYER_FENCE_MISMATCH", "error": "fence mismatch"})
			return
		}
		body, _ := io.ReadAll(r.Body)
		st.Files[p] = body
		w.Header().Set("ETag", `"2"`)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"revision": 2})
		return
	}
	if r.URL.Query().Has("list") {
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []map[string]any{}})
		if b.onViewList != nil {
			b.onViewList(layerID)
		}
		return
	}
	if p == st.Info.BaseRootPath || p == "/" {
		w.Header().Set("X-Drive9-Is-Dir", "true")
		w.WriteHeader(http.StatusOK)
		return
	}
	data, exists := st.Files[p]
	if !exists {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(data)
	}
}
