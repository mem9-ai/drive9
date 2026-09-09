package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func (s *Server) handleExtentMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	op := strings.TrimSpace(r.Header.Get("X-Drive9-Extent-Op"))
	if op == "" {
		op = strings.TrimSpace(r.URL.Query().Get("op"))
	}
	if op == "" {
		errJSON(w, http.StatusBadRequest, "missing extent op")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	body, errno, err := b.Store().RunExtentMetaOp(r.Context(), op, json.RawMessage(raw))
	if err != nil {
		logger.Error(r.Context(), "extent_meta_failed", eventFields(r.Context(), "extent_meta_failed", "op", op, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "extent meta failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if errno == int(syscall.EIO) {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(body)
}

func (s *Server) handleDataCredential(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if scope.IsScoped {
		errJSON(w, http.StatusForbidden, "scoped token cannot mint data credentials")
		return
	}
	b := backendFromRequest(r)
	var s3 s3client.S3Client
	if b != nil {
		s3 = b.S3()
	}
	if s3 == nil && s.localS3 != nil {
		s3 = s.localS3
	}
	cred, err := mintDataCredential(s3, scope.TenantID)
	if err != nil {
		errJSON(w, http.StatusNotImplemented, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cred)
}

func mintDataCredential(s3 s3client.S3Client, tenantID string) (*dataCredentialResponse, error) {
	prefix := "t/" + tenantID + "/"
	if local, ok := s3.(*s3client.LocalS3Client); ok && local != nil {
		return &dataCredentialResponse{
			Scheme:    extent.SchemeFile,
			Endpoint:  local.ObjectsDir(),
			Prefix:    prefix,
			ExpiresAt: "",
		}, nil
	}
	aws, ok := s3.(*s3client.AWSS3Client)
	if !ok || aws == nil {
		return nil, errNoExtentCredentials
	}
	if aws.CanMintSTS() {
		out, err := aws.MintTenantPrefix(context.Background(), tenantID, time.Hour)
		if err != nil {
			return nil, err
		}
		return dataCredentialFromS3(out), nil
	}
	if aws.CanMintStatic() {
		out, err := aws.MintStaticPrefix(tenantID)
		if err != nil {
			return nil, err
		}
		return dataCredentialFromS3(out), nil
	}
	return nil, errNoExtentCredentials
}

func dataCredentialFromS3(out *s3client.TenantPrefixCreds) *dataCredentialResponse {
	exp := ""
	if !out.Expiration.IsZero() {
		exp = out.Expiration.UTC().Format(time.RFC3339)
	}
	return &dataCredentialResponse{
		Scheme:          extent.SchemeS3,
		Endpoint:        out.Endpoint,
		Bucket:          out.Bucket,
		Prefix:          out.Prefix,
		AccessKeyID:     out.AccessKeyID,
		SecretAccessKey: out.SecretAccessKey,
		SessionToken:    out.SessionToken,
		Region:          out.Region,
		ForcePathStyle:  out.ForcePathStyle,
		ExpiresAt:       exp,
	}
}

var errNoExtentCredentials = fmt.Errorf("extent credentials unavailable: configure STS AssumeRole (DRIVE9_S3_ROLE_ARN) or static keys on an S3-compatible endpoint")

type dataCredentialResponse struct {
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket,omitempty"`
	Prefix          string `json:"prefix"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

func extentBlockKey(tenantID, key string) string {
	key = strings.TrimPrefix(key, "/")
	prefix := "t/" + tenantID + "/"
	if strings.HasPrefix(key, prefix) {
		return key
	}
	return prefix + key
}

func runExtentBlockGC(ctx context.Context, store *datastore.Store, s3 s3client.S3Client, tenantID string) {
	if store == nil || s3 == nil {
		return
	}
	keys, err := store.ListPendingBlockGC(ctx, 32)
	if err != nil || len(keys) == 0 {
		return
	}
	for _, key := range keys {
		full := extentBlockKey(tenantID, key)
		if err := s3.DeleteObject(ctx, full); err != nil && !extentObjectGone(err) {
			continue
		}
		_ = store.MarkBlockGCDone(ctx, key)
	}
}

func extentObjectGone(err error) bool {
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "nosuchkey") || strings.Contains(s, "not found") || strings.Contains(s, "not exist")
}

func runExtentSessionSweep(ctx context.Context, store *datastore.Store) {
	if store == nil {
		return
	}
	_, _ = store.SweepStaleExtentSessions(ctx, 16)
}

func runExtentCompactFallback(ctx context.Context, store *datastore.Store, s3 s3client.S3Client, tenantID string) {
	if store == nil || s3 == nil {
		return
	}
	ino, indx, taskID, err := store.ClaimCompactTask(ctx)
	if err != nil || taskID == "" || ino == 0 {
		return
	}
	st, err := extent.WrapS3(s3, "t/"+tenantID+"/")
	if err != nil {
		return
	}
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		Transport: extent.NewTransport(store.RunExtentMetaOp),
		Storage:   st,
	})
	if err != nil {
		return
	}
	_ = extent.ExecuteCompact(rt, ino, indx)
}
