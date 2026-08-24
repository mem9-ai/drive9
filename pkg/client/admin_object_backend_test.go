package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminObjectBackendClientSendsHeaders(t *testing.T) {
	var gotPath, gotMethod, gotPub, gotPriv string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotPub = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPriv = r.Header.Get("X-TiDBCloud-Private-Key")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/object-backends":
			_ = json.NewEncoder(w).Encode(map[string]any{"backends": []any{}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/object-backends/obb_1":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "obb_1", "scheme": "s3", "bucket": "b", "name": "prod"})
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "obb_1", "scheme": "s3", "bucket": "b"})
		case r.Method == http.MethodPatch:
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "obb_1", "scheme": "s3", "bucket": "b", "region": "us-west-2"})
		case r.Method == http.MethodDelete:
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "obb_1", "status": "deleted"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c := New(srv.URL, "")

	if _, err := c.AdminListObjectBackends(context.Background(), "pub", "priv"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodGet || gotPub != "pub" || gotPriv != "priv" {
		t.Fatalf("list %s pub=%s priv=%s", gotMethod, gotPub, gotPriv)
	}

	out, err := c.AdminCreateObjectBackend(context.Background(), AdminObjectBackendCreateRequest{
		PublicKey: "pub", PrivateKey: "priv", Scheme: "s3", Bucket: "b",
		AccessKeyID: "AKI", SecretAccessKey: "sec",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.ID != "obb_1" || gotPub != "pub" {
		t.Fatalf("create out=%+v headers pub=%s", out, gotPub)
	}

	got, err := c.AdminGetObjectBackend(context.Background(), "obb_1", "pub", "priv")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "prod" || gotPath != "/v1/admin/object-backends/obb_1" {
		t.Fatalf("get %+v path=%s", got, gotPath)
	}
	region := "us-west-2"
	updated, err := c.AdminUpdateObjectBackend(context.Background(), "obb_1", AdminObjectBackendUpdateRequest{
		PublicKey: "pub", PrivateKey: "priv", Region: &region,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Region != "us-west-2" || gotMethod != http.MethodPatch {
		t.Fatalf("update %+v method=%s", updated, gotMethod)
	}

	if err := c.AdminDeleteObjectBackend(context.Background(), "obb_1", "pub", "priv"); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/admin/object-backends/obb_1" || gotMethod != http.MethodDelete {
		t.Fatalf("delete %s %s", gotMethod, gotPath)
	}
}

func TestAdminObjectNamespaceClient(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		if r.Body != nil && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &gotBody)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": "t1", "namespace_id": "cust"})
	}))
	defer srv.Close()
	c := New(srv.URL, "")

	got, err := c.AdminGetObjectNamespace(context.Background(), "t1", "pub", "priv")
	if err != nil {
		t.Fatal(err)
	}
	if got.NamespaceID != "cust" || gotPath != "/v1/admin/tenants/t1/object-namespace" {
		t.Fatalf("get %+v path=%s", got, gotPath)
	}
	if _, err := c.AdminSetObjectNamespace(context.Background(), "t1", "cust", "pub", "priv"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPut || gotBody["namespace_id"] != "cust" {
		t.Fatalf("set method=%s body=%v", gotMethod, gotBody)
	}
	if err := c.AdminClearObjectNamespace(context.Background(), "t1", "pub", "priv"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("clear method=%s", gotMethod)
	}
}
