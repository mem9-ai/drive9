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

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/slockoauth"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

const slockProvider = "slock"

const (
	maxExternalSubjectKeyBytes = 512
	maxExternalMetadataBytes   = 16 << 10
)

var slockHTMLTemplate = template.Must(template.New("slock").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="https://drive9.ai/favicon.svg">
<title>drive9 — Login</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  color-scheme: dark;
  --bg: #0a0908;
  --ink: #ede8e0;
  --muted: #918c85;
  --faint: #48443f;
  --line: #1f1d1a;
  --paper: #131210;
  --warm: #1a1816;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { min-height: 100%; -webkit-font-smoothing: antialiased; }
body {
  min-height: 100vh;
  background: var(--bg);
  color: var(--ink);
  font-family: 'Inter', ui-sans-serif, system-ui, -apple-system, sans-serif;
  font-size: 17px;
  line-height: 1.6;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.page {
  width: min(780px, 100%);
}
.card {
  position: relative;
  background: var(--paper);
  border: 1px solid var(--line);
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(0,0,0,0.15), 0 12px 40px rgba(0,0,0,0.3);
}
.card::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent 0%, rgba(141,216,141,0.3) 50%, transparent 100%);
}
.header {
  padding: 32px 32px 24px;
  border-bottom: 1px solid var(--line);
}
.badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 20px;
  padding: 4px 12px;
  border: 1px solid var(--line);
  border-radius: 20px;
  background: var(--bg);
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  color: var(--muted);
  letter-spacing: 0.04em;
}
.badge-dot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: #8dd88d;
}
.header h1 {
  font-size: 28px;
  font-weight: 700;
  color: var(--ink);
  letter-spacing: -0.02em;
  margin-bottom: 10px;
}
.header p {
  font-size: 16px;
  color: var(--muted);
  line-height: 1.6;
}
.section {
  padding: 24px 32px;
  border-bottom: 1px solid var(--line);
}
.section:last-of-type {
  border-bottom: none;
}
.section-title {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--faint);
  margin-bottom: 16px;
}
.copy-row {
  position: relative;
  padding: 16px 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--bg);
  cursor: pointer;
  transition: border-color 0.3s, box-shadow 0.3s;
  user-select: none;
}
.copy-row:hover {
  border-color: #2a2520;
  box-shadow: 0 1px 2px rgba(0,0,0,0.2), 0 8px 32px rgba(0,0,0,0.4);
}
.copy-row::after {
  content: 'click to copy';
  position: absolute;
  bottom: 8px; right: 12px;
  font-family: 'Inter', sans-serif;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--faint);
  opacity: 0;
  transition: opacity 0.25s;
  background: var(--bg);
  padding: 2px 6px;
  border-radius: 4px;
}
.copy-row:hover::after { opacity: 1; }
.copy-row[data-copied]::after { content: 'copied'; opacity: 1; color: var(--ink); }
.copy-label {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--faint);
  margin-bottom: 8px;
}
.copy-value {
  font-family: 'JetBrains Mono', monospace;
  font-size: 15px;
  font-weight: 400;
  color: var(--ink);
  word-break: break-all;
  line-height: 1.5;
}
.info-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.info-item {
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--bg);
}
.info-label {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--faint);
  margin-bottom: 6px;
}
.info-value {
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
  font-weight: 500;
  color: var(--muted);
  word-break: break-all;
}
.cli-box {
  position: relative;
  padding: 20px 24px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--bg);
  cursor: pointer;
  transition: border-color 0.3s, box-shadow 0.3s;
  user-select: none;
}
.cli-box:hover {
  border-color: #2a2520;
  box-shadow: 0 1px 2px rgba(0,0,0,0.2), 0 8px 32px rgba(0,0,0,0.4);
}
.cli-box::after {
  content: 'click to copy';
  position: absolute;
  bottom: 8px; right: 12px;
  font-family: 'Inter', sans-serif;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--faint);
  opacity: 0;
  transition: opacity 0.25s;
  background: var(--bg);
  padding: 2px 6px;
  border-radius: 4px;
}
.cli-box:hover::after { opacity: 1; }
.cli-box[data-copied]::after { content: 'copied'; opacity: 1; color: var(--ink); }
.doc-box {
  padding: 16px 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--bg);
  font-size: 15px;
  line-height: 1.6;
  color: var(--muted);
}
.doc-box a {
  color: #8dd88d;
  text-decoration: none;
  transition: opacity 0.2s;
}
.doc-box a:hover {
  opacity: 0.8;
}
.cli-box code {
  font-family: 'JetBrains Mono', monospace;
  font-size: 15px;
  font-weight: 500;
  line-height: 1.7;
  color: var(--muted);
  white-space: pre-wrap;
  word-break: break-all;
}
.cli-box .cmd { color: var(--ink); }
.cli-box .arg { color: #8dd88d; }
.footer {
  padding: 20px 32px;
  text-align: center;
  border-top: 1px solid var(--line);
}
.footer a {
  color: var(--muted);
  text-decoration: none;
  font-size: 12px;
  transition: color 0.2s;
}
.footer a:hover { color: var(--ink); }
</style>
</head>
<body>
<div class="page">
  <div class="card">
    <div class="header">
      <div class="badge"><span class="badge-dot"></span> drive9</div>
      <h1>Login Successful</h1>
      <p>Your tenant is ready. Copy the credentials below to configure your CLI.</p>
    </div>

    <div class="section">
      <div class="section-title">API Key</div>
      <div class="copy-row" data-copy-text="{{.APIKey}}">
        <div class="copy-value">{{.APIKey}}</div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">Server URL</div>
      <div class="copy-row" data-copy-text="{{.ServerURL}}">
        <div class="copy-value">{{.ServerURL}}</div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">Identity</div>
      <div class="info-grid">
        <div class="info-item">
          <div class="info-label">Provider</div>
          <div class="info-value">{{.Principal.Provider}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Type</div>
          <div class="info-value">{{.Principal.Type}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Server ID</div>
          <div class="info-value">{{.Principal.ServerID}}</div>
        </div>
        <div class="info-item">
          <div class="info-label">Subject</div>
          <div class="info-value">{{.Principal.Sub}}</div>
        </div>
      </div>
    </div>

    <div class="section">
      <div class="section-title">Getting Started</div>
      <div class="doc-box">
        Read <a href="https://drive9.ai/skill.md" target="_blank" rel="noopener noreferrer">https://drive9.ai/skill.md</a> and follow the instructions to install and configure drive9.
      </div>
    </div>

    <div class="section">
      <div class="section-title">Quick Start</div>
      <div class="cli-box" data-copy-text="drive9 ctx add --api-key {{.APIKey}} --server {{.ServerURL}}">
        <code><span class="cmd">drive9</span> <span class="arg">ctx</span> <span class="arg">add</span> <span class="arg">--api-key</span> "{{.APIKey}}" <span class="arg">--server</span> {{.ServerURL}}</code>
      </div>
    </div>

    <div class="footer">
      <a href="https://drive9.ai">drive9.ai</a> — persistent filesystem for AI agents
    </div>
  </div>
</div>
<script>
(function() {
  function copy(el) {
    var text = el.dataset.copyText || el.textContent.trim();
    var done = function() {
      el.dataset.copied = 'true';
      setTimeout(function() { delete el.dataset.copied; }, 1500);
    };
    try {
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(done).catch(function() {
          fallbackCopy(text, done);
        });
      } else {
        fallbackCopy(text, done);
      }
    } catch(_) {
      fallbackCopy(text, done);
    }
  }
  function fallbackCopy(text, done) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.cssText = 'position:fixed;opacity:0';
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); done(); } catch(e) {}
    document.body.removeChild(ta);
  }
  document.querySelectorAll('.copy-row, .cli-box').forEach(function(el) {
    el.addEventListener('click', function() { copy(el); });
  });
})();
</script>
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
	ServerURL string         `json:"server_url"`
	Message   string         `json:"message"`
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
		writeSlockOAuthError(w, r, err)
		return
	}
	info, err := s.slockOAuth.Userinfo(r.Context(), tok.AccessToken)
	if err != nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "slock_userinfo_failed", "error", err)...)
		writeSlockOAuthError(w, r, err)
		return
	}

	resp, err := s.slockProvisionForUserInfo(r.Context(), info)
	if err != nil {
		var pe *provisionTenantError
		if errors.As(err, &pe) {
			if wantsJSON(r) {
				errJSON(w, pe.status, pe.message)
				return
			}
			setSlockCallbackNoStoreHeaders(w)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(pe.status)
			_, _ = fmt.Fprintf(w, "<!doctype html><html><body style='font-family:sans-serif;padding:40px;text-align:center;background:#0a0908;color:#ede8e0'><h1>Provisioning Error</h1><p style='color:#c75050'>%s</p></body></html>", template.HTMLEscapeString(pe.message))
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "slock_tenant_provision_failed", "error", err)...)
		if wantsJSON(r) {
			errJSON(w, backendErrorStatus(r.Context(), err), "slock tenant provision failed")
			return
		}
		setSlockCallbackNoStoreHeaders(w)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "<!doctype html><html><body style='font-family:sans-serif;padding:40px;text-align:center;background:#0a0908;color:#ede8e0'><h1>Provisioning Error</h1><p style='color:#c75050'>slock tenant provision failed</p></body></html>")
		return
	}
	resp.ServerURL = s.publicURL
	resp.Message = fmt.Sprintf(
		"Drive9 login successful. "+
			"Documentation: https://drive9.ai/ | Skill setup: https://drive9.ai/skill.md\n"+
			"Configure CLI: drive9 ctx add --api-key <API_KEY> --server %s",
		s.publicURL,
	)
	if wantsJSON(r) {
		setSlockCallbackNoStoreHeaders(w)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	writeSlockHTML(w, resp, s.publicURL)
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

	// Reuse existing active API key for the same issuer instead of rotating.
	existingKey, err := s.meta.GetActiveAPIKeyByIssuer(ctx, t.ID, source.Provider, source.SubjectKey)
	if err == nil {
		plain, decryptErr := poolDecryptToken(ctx, s.pool, existingKey.JWTCiphertext)
		if decryptErr == nil {
			// Verify the reused token is still valid with the current signing key.
			_, verifyErr := token.ParseAndVerifyToken(s.tokenSecret, string(plain))
			if verifyErr == nil {
				logger.Info(ctx, "server_event", eventFields(ctx, "slock_external_binding_resolved",
					"tenant_id", t.ID, "subject_key", binding.SubjectKey, "status", string(t.Status), "reuse", true)...)
				return &slockCallbackResponse{
					TenantID:  t.ID,
					APIKey:    string(plain),
					Status:    string(t.Status),
					Principal: principal,
				}, false, nil
			}
			logger.Warn(ctx, "server_event", eventFields(ctx, "slock_existing_key_verify_failed",
				"tenant_id", t.ID, "api_key_id", existingKey.ID, "error", verifyErr)...)
		} else {
			logger.Warn(ctx, "server_event", eventFields(ctx, "slock_existing_key_decrypt_failed",
				"tenant_id", t.ID, "api_key_id", existingKey.ID, "error", decryptErr)...)
		}
	} else if !errors.Is(err, meta.ErrNotFound) {
		logger.Error(ctx, "server_event", eventFields(ctx, "slock_get_active_key_failed",
			"tenant_id", t.ID, "error", err)...)
		return nil, false, newProvisionTenantError(http.StatusInternalServerError, "failed to lookup existing api key", err)
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
		_ = s.meta.RevokeAPIKey(ctx, tenantID, apiKeyID)
		return "", "", err
	}
	return rawToken, apiKeyID, nil
}

func writeSlockOAuthError(w http.ResponseWriter, r *http.Request, err error) {
	var oe slockoauth.OAuthError
	msg := "slock oauth request failed"
	if errors.As(err, &oe) {
		msg = oe.Error()
	}
	if wantsJSON(r) {
		errJSON(w, http.StatusBadGateway, msg)
		return
	}
	setSlockCallbackNoStoreHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusBadGateway)
	_, _ = fmt.Fprintf(w, "<!doctype html><html><body style='font-family:sans-serif;padding:40px;text-align:center;background:#0a0908;color:#ede8e0'><h1>Authentication Error</h1><p style='color:#c75050'>%s</p></body></html>", template.HTMLEscapeString(msg))
}

func wantsJSON(r *http.Request) bool {
	if strings.EqualFold(r.URL.Query().Get("format"), "json") {
		return true
	}
	return !strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/html")
}

func writeSlockHTML(w http.ResponseWriter, resp *slockCallbackResponse, serverURL string) {
	setSlockCallbackNoStoreHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := slockHTMLTemplate.Execute(w, struct {
		APIKey    string
		Principal slockPrincipal
		ServerURL string
	}{
		APIKey:    resp.APIKey,
		Principal: resp.Principal,
		ServerURL: serverURL,
	}); err != nil {
		logger.Warn(context.Background(), "slock_html_render_failed", zap.Error(err))
	}
}

func setSlockCallbackNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Referrer-Policy", "no-referrer")
}
