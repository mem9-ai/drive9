package mockserver

import (
	"net/http"
)

// handleDataCredential implements POST /v1/data-credential (spec §7.1).
// Branches: Config.S3Endpoint set → scheme "s3" (the simulator stack points
// this at the embedded rustfs); otherwise scheme "file" backed by the mock's
// objects directory. Static keys are handed out unconditionally — the mock's
// trust boundary is the whole process (the production gating env
// DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS does not apply).
func (s *Server) handleDataCredential(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tdb := tenantFrom(r)
	prefix := "t/" + tdb.id + "/"
	if s.cfg.S3Endpoint != "" {
		writeJSON(w, http.StatusOK, map[string]any{
			"scheme":           "s3",
			"endpoint":         s.cfg.S3Endpoint,
			"bucket":           s.cfg.S3Bucket,
			"prefix":           prefix,
			"tenant_id":        tdb.id,
			"access_key_id":    s.cfg.S3AK,
			"secret_access_key": s.cfg.S3SK,
			"force_path_style": true,
			"region":           "us-east-1",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scheme":    "file",
		"endpoint":  s.cfg.Dir + "/objects",
		"prefix":    prefix,
		"tenant_id": tdb.id,
	})
}
