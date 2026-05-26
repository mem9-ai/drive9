package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/slockoauth"
	"go.uber.org/zap"
)

const slockProvider = "slock"

const (
	maxExternalSubjectKeyBytes = 512
	maxExternalMetadataBytes   = 16 << 10
)

var slockHTMLTemplate = template.Must(template.New("slock").Parse(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>Drive9 Slock Login</title></head>
<body>
<h1>Drive9 tenant ready</h1>
<p>Tenant: <code>{{.TenantID}}</code></p>
<p>Status: <code>{{.Status}}</code></p>
<p>API key is only returned from the JSON callback response.</p>
</body>
</html>`))

type slockPrincipal struct {
	Provider string `json:"provider"`
	Type     string `json:"type"`
	ServerID string `json:"server_id"`
	Sub      string `json:"sub"`
}

type slockCallbackResponse struct {
	TenantID  string         `json:"tenant_id"`
	APIKey    string         `json:"api_key"`
	Status    string         `json:"status"`
	Principal slockPrincipal `json:"principal"`
}

func (s *Server) handleSlockLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.slockOAuth == nil {
		errJSON(w, http.StatusNotImplemented, "login with slock is not configured")
		return
	}
	http.Redirect(w, r, s.slockOAuth.LoginURL(), http.StatusFound)
}

func (s *Server) handleSlockCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.slockOAuth == nil {
		errJSON(w, http.StatusNotImplemented, "login with slock is not configured")
		return
	}
	if errMsg := strings.TrimSpace(r.URL.Query().Get("error")); errMsg != "" {
		errJSON(w, http.StatusBadRequest, "slock oauth error: "+errMsg)
		return
	}
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		errJSON(w, http.StatusBadRequest, "code is required")
		return
	}
	if err := s.ensureSlockProvisioningEnabled(); err != nil {
		errJSON(w, http.StatusNotFound, err.Error())
		return
	}

	tok, err := s.slockOAuth.ExchangeCode(r.Context(), code)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "slock_token_exchange_failed", "error", err)...)
		writeSlockOAuthError(w, err)
		return
	}
	info, err := s.slockOAuth.Userinfo(r.Context(), tok.AccessToken)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "slock_userinfo_failed", "error", err)...)
		writeSlockOAuthError(w, err)
		return
	}

	resp, err := s.slockProvisionForUserInfo(r.Context(), info)
	if err != nil {
		var pe *provisionTenantError
		if errors.As(err, &pe) {
			errJSON(w, pe.status, pe.message)
			return
		}
		errJSON(w, http.StatusInternalServerError, "slock tenant provision failed")
		return
	}
	if wantsJSON(r) {
		setSlockCallbackNoStoreHeaders(w)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	writeSlockHTML(w, resp)
}

func (s *Server) ensureSlockProvisioningEnabled() error {
	if s.meta == nil || s.pool == nil || len(s.tokenSecret) == 0 {
		return fmt.Errorf("provisioning not enabled")
	}
	if s.provisioner == nil {
		return fmt.Errorf("provisioner not configured")
	}
	return nil
}

func (s *Server) slockProvisionForUserInfo(ctx context.Context, info slockoauth.UserInfo) (*slockCallbackResponse, error) {
	subjectKey := slockSubjectKey(info)
	metadata, err := slockMetadataJSON(info)
	if err != nil {
		return nil, newProvisionTenantError(http.StatusInternalServerError, "failed to encode slock metadata", err)
	}
	if err := validateExternalIdentityPayload(subjectKey, metadata); err != nil {
		return nil, err
	}
	source := apiKeyIssueSource{Provider: slockProvider, SubjectKey: subjectKey, MetadataJSON: metadata}
	principal := slockPrincipal{Provider: slockProvider, Type: info.Type, ServerID: info.ServerID, Sub: info.Sub}

	var out *slockCallbackResponse
	err = s.meta.WithExternalBindingLock(ctx, slockProvider, subjectKey, func(lockCtx context.Context) error {
		binding, err := s.meta.GetExternalBinding(lockCtx, slockProvider, subjectKey)
		if err == nil {
			resp, reprovision, issueErr := s.slockIssueForBinding(lockCtx, binding, source, principal)
			if issueErr == nil && !reprovision {
				out = resp
				return nil
			}
			if !reprovision {
				return issueErr
			}
			if err := s.meta.DeleteExternalBinding(lockCtx, slockProvider, subjectKey); err != nil && !errors.Is(err, meta.ErrNotFound) {
				return newProvisionTenantError(http.StatusInternalServerError, "failed to delete stale external binding", err)
			}
			logger.Info(lockCtx, "server_event", eventFields(lockCtx, "slock_external_binding_deleted",
				"subject_key", subjectKey, "tenant_id", binding.TenantID)...)
		} else if !errors.Is(err, meta.ErrNotFound) {
			return newProvisionTenantError(http.StatusInternalServerError, "failed to load external binding", err)
		}

		res, err := s.provisionTenant(lockCtx, provisionTenantOptions{
			KeyName:      "slock",
			TokenVersion: 1,
			APIKeySource: source,
		})
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := s.meta.InsertExternalBinding(lockCtx, &meta.ExternalBinding{
			Provider:     slockProvider,
			SubjectKey:   subjectKey,
			TenantID:     res.TenantID,
			MetadataJSON: metadata,
			CreatedAt:    now,
			UpdatedAt:    now,
		}); err != nil {
			_ = s.meta.UpdateTenantStatus(context.Background(), res.TenantID, meta.TenantDeleted)
			return newProvisionTenantError(http.StatusInternalServerError, "failed to persist external binding", err)
		}
		s.startProvisionedTenantSchemaInit(lockCtx, res)
		logger.Info(lockCtx, "server_event", eventFields(lockCtx, "slock_external_binding_created",
			"tenant_id", res.TenantID, "subject_key", subjectKey)...)
		out = &slockCallbackResponse{
			TenantID:  res.TenantID,
			APIKey:    res.APIKey,
			Status:    string(res.Status),
			Principal: principal,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) slockIssueForBinding(ctx context.Context, binding *meta.ExternalBinding, source apiKeyIssueSource, principal slockPrincipal) (*slockCallbackResponse, bool, error) {
	t, err := s.meta.GetTenant(ctx, binding.TenantID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, true, nil
		}
		return nil, false, newProvisionTenantError(http.StatusInternalServerError, "failed to load bound tenant", err)
	}
	switch t.Status {
	case meta.TenantActive, meta.TenantProvisioning:
	case meta.TenantFailed, meta.TenantDeleted:
		return nil, true, nil
	case meta.TenantPending:
		if isStalePendingTenant(time.Now().UTC(), *t) {
			return nil, true, nil
		}
		return nil, false, newProvisionTenantError(http.StatusConflict, "bound tenant is still pending", fmt.Errorf("tenant status %s", t.Status))
	default:
		return nil, false, newProvisionTenantError(http.StatusForbidden, "bound tenant is unavailable", fmt.Errorf("tenant status %s", t.Status))
	}
	apiKey, _, err := s.rotateIssuedOwnerAPIKey(ctx, t.ID, "slock", source)
	if err != nil {
		logger.Error(ctx, "server_event", eventFields(ctx, "slock_api_key_issue_failed", "tenant_id", t.ID, "error", err)...)
		return nil, false, newProvisionTenantError(http.StatusInternalServerError, "failed to issue api key", err)
	}
	logger.Info(ctx, "server_event", eventFields(ctx, "slock_external_binding_resolved",
		"tenant_id", t.ID, "subject_key", binding.SubjectKey, "status", string(t.Status))...)
	return &slockCallbackResponse{
		TenantID:  t.ID,
		APIKey:    apiKey,
		Status:    string(t.Status),
		Principal: principal,
	}, false, nil
}

func slockSubjectKey(info slockoauth.UserInfo) string {
	return base64.RawURLEncoding.EncodeToString([]byte(info.ServerID)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(info.Sub))
}

func slockMetadataJSON(info slockoauth.UserInfo) ([]byte, error) {
	return json.Marshal(map[string]any{
		"server_id":          info.ServerID,
		"sub":                info.Sub,
		"principal_type":     info.Type,
		"server_slug":        info.ServerSlug,
		"preferred_username": info.PreferredUsername,
		"name":               info.Name,
		"client_id":          info.ClientID,
	})
}

func validateExternalIdentityPayload(subjectKey string, metadata []byte) *provisionTenantError {
	switch {
	case len(subjectKey) == 0:
		return newProvisionTenantError(http.StatusBadRequest, "external subject key is required", nil)
	case len(subjectKey) > maxExternalSubjectKeyBytes:
		return newProvisionTenantError(http.StatusBadRequest, "external subject key is too large", nil)
	case len(metadata) > maxExternalMetadataBytes:
		return newProvisionTenantError(http.StatusBadRequest, "external metadata is too large", nil)
	default:
		return nil
	}
}

func (s *Server) rotateIssuedOwnerAPIKey(ctx context.Context, tenantID, keyName string, source apiKeyIssueSource) (rawToken, apiKeyID string, err error) {
	rawToken, apiKeyID, err = s.issueOwnerAPIKey(ctx, tenantID, keyName, 0, source)
	if err != nil {
		return "", "", err
	}
	if err := s.meta.RevokeAPIKeysByIssuer(ctx, tenantID, source.Provider, source.SubjectKey, apiKeyID); err != nil {
		return "", "", err
	}
	return rawToken, apiKeyID, nil
}

func writeSlockOAuthError(w http.ResponseWriter, err error) {
	var oe slockoauth.OAuthError
	if errors.As(err, &oe) {
		errJSON(w, http.StatusBadGateway, oe.Error())
		return
	}
	errJSON(w, http.StatusBadGateway, "slock oauth request failed")
}

func wantsJSON(r *http.Request) bool {
	if strings.EqualFold(r.URL.Query().Get("format"), "json") {
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "application/json")
}

func writeSlockHTML(w http.ResponseWriter, resp *slockCallbackResponse) {
	setSlockCallbackNoStoreHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := slockHTMLTemplate.Execute(w, resp); err != nil {
		logger.Warn(context.Background(), "slock_html_render_failed", zap.Error(err))
	}
}

func setSlockCallbackNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}
