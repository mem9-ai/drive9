package drivehttp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientStripsCredentialsOnCrossHostRedirect(t *testing.T) {
	var authorization, actor string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		actor = r.Header.Get("X-Dat9-Actor")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	req, err := http.NewRequest(http.MethodGet, source.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ApplyCredentials(req, "secret", "mount-a")
	resp, err := NewClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if authorization != "" || actor != "" {
		t.Fatalf("redirect leaked credentials: authorization=%q actor=%q", authorization, actor)
	}
}

func TestDecodeErrorBodySupportsNestedEnvelope(t *testing.T) {
	message, code := DecodeErrorBody(http.StatusConflict, []byte(`{"error":{"message":"target exists","code":"target_exists"}}`))
	if message != "target exists" || code != "target_exists" {
		t.Fatalf("decoded error = %q/%q", message, code)
	}
}
