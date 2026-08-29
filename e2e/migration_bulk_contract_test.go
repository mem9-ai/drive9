package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestMigrationBulkLiveServerContract(t *testing.T) {
	if os.Getenv("DRIVE9_MIGRATION_CONTRACT") != "1" {
		t.Skip("set DRIVE9_MIGRATION_CONTRACT=1 for the live Server #115 gate")
	}
	base := strings.TrimRight(os.Getenv("DRIVE9_BASE"), "/")
	if base == "" {
		t.Fatal("DRIVE9_BASE is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	apiKey := os.Getenv("DRIVE9_API_KEY")
	if apiKey == "" {
		apiKey = provisionMigrationContractTenant(t, ctx, base)
	}
	waitMigrationContractTenantActive(t, ctx, base, apiKey)
	api := client.New(base, apiKey)
	if _, err := api.GetMigrationCapabilities(ctx); err != nil {
		t.Fatalf("migration capabilities: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	root := "/migration-contract-" + suffix + "/"
	unrelated := "/migration-contract-unrelated-" + suffix + "/"
	unrelated2 := "/migration-contract-unrelated-b-" + suffix + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_ = api.RemoveAllCtx(cleanupCtx, root)
		_ = api.RemoveAllCtx(cleanupCtx, unrelated)
		_ = api.RemoveAllCtx(cleanupCtx, unrelated2)
	})

	created, err := api.BatchMkdirCtx(ctx, []client.BatchMkdirItem{{Path: root, Mode: 0o700}})
	if err != nil || len(created) != 1 || !created[0].OK() || created[0].Created == nil || !*created[0].Created {
		t.Fatalf("create root results=%+v err=%v", created, err)
	}
	if _, err := api.BatchMkdirCtx(ctx, []client.BatchMkdirItem{
		{Path: unrelated, Mode: 0o755},
		{Path: unrelated2, Mode: 0o755},
	}); err != nil {
		t.Fatalf("create unrelated fixture: %v", err)
	}

	partial, err := api.BatchMkdirCtx(ctx, []client.BatchMkdirItem{
		{Path: root + "valid/", Mode: 0},
		{Path: root + "missing/child/", Mode: 0o755},
	})
	if err != nil || len(partial) != 2 || !partial[0].OK() || partial[1].OK() || partial[1].Error == nil {
		t.Fatalf("partial mkdir results=%+v err=%v", partial, err)
	}

	duplicatePath := root + "duplicate/"
	first, err := api.BatchMkdirCtx(ctx, []client.BatchMkdirItem{{Path: duplicatePath, Mode: 0o755}})
	if err != nil || len(first) != 1 || !first[0].OK() || first[0].Created == nil || !*first[0].Created {
		t.Fatalf("first duplicate delivery=%+v err=%v", first, err)
	}
	second, err := api.BatchMkdirCtx(ctx, []client.BatchMkdirItem{{Path: duplicatePath, Mode: 0o755}})
	if err != nil || len(second) != 1 || !second[0].OK() || second[0].Created == nil || *second[0].Created {
		t.Fatalf("second duplicate delivery=%+v err=%v", second, err)
	}

	unknownPath := root + "commit-unknown/"
	dropProxy := newMigrationCommitUnknownProxy(t, base)
	proxyClient := client.New(dropProxy.URL, apiKey)
	if _, err := proxyClient.BatchMkdirCtx(ctx, []client.BatchMkdirItem{{Path: unknownPath, Mode: 0o755}}); err == nil {
		t.Fatal("dropped BatchMkdir response unexpectedly succeeded")
	}
	if stat, err := api.StatCtx(ctx, unknownPath); err != nil || !stat.IsDir || stat.ResourceID == "" {
		t.Fatalf("commit-unknown reobserve stat=%+v err=%v", stat, err)
	}

	systemicPath := root + "systemic-failure/"
	systemicProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "injected systemic failure", http.StatusServiceUnavailable)
	}))
	t.Cleanup(systemicProxy.Close)
	if _, err := client.New(systemicProxy.URL, apiKey).BatchMkdirCtx(ctx, []client.BatchMkdirItem{{Path: systemicPath, Mode: 0o755}}); err == nil {
		t.Fatal("systemic BatchMkdir failure unexpectedly succeeded")
	}
	if _, err := api.StatCtx(ctx, systemicPath); !client.IsNotFound(err) {
		t.Fatalf("systemic failure mutated target: %v", err)
	}

	filePath := root + "file"
	if err := api.WriteCtx(ctx, filePath, []byte("contract")); err != nil {
		t.Fatal(err)
	}
	stat, err := api.StatCtx(ctx, filePath)
	if err != nil || stat.ResourceID == "" || stat.Revision <= 0 {
		t.Fatalf("file stat=%+v err=%v", stat, err)
	}
	revision := stat.Revision
	chmod, err := api.BatchChmodCtx(ctx, []client.BatchChmodItem{{
		Path: filePath, Mode: 0o600, ExpectedResourceID: stat.ResourceID, ExpectedRevision: &revision,
	}})
	if err != nil || len(chmod) != 1 || !chmod[0].OK() || chmod[0].Mode == nil || *chmod[0].Mode != 0o600 {
		t.Fatalf("chmod results=%+v err=%v", chmod, err)
	}

	seen := make(map[string]struct{})
	cursor := ""
	emptyNonFinal := false
	for pages := 0; ; pages++ {
		if pages > 10000 {
			t.Fatal("Manifest cursor chain did not terminate")
		}
		page, err := api.ManifestPageCtx(ctx, root, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) == 0 && !page.Done {
			emptyNonFinal = true
		}
		for _, entry := range page.Entries {
			if _, duplicate := seen[entry.Path]; duplicate {
				t.Fatalf("duplicate Manifest path %q", entry.Path)
			}
			seen[entry.Path] = struct{}{}
		}
		if page.Done {
			break
		}
		cursor = page.NextCursor
	}
	if !emptyNonFinal {
		t.Fatal("Manifest fixture did not exercise an empty non-final page")
	}
	for _, path := range []string{root + "valid/", duplicatePath, filePath} {
		if _, ok := seen[path]; !ok {
			t.Fatalf("Manifest omitted %q: %v", path, seen)
		}
	}
	incompleteProxy := newMigrationIncompleteManifestProxy(t, base)
	incompleteClient := client.New(incompleteProxy.URL, apiKey)
	cursor = ""
	for pages := 0; pages < 10000; pages++ {
		page, err := incompleteClient.ManifestPageCtx(ctx, root, cursor, 1)
		if err != nil {
			if !strings.Contains(err.Error(), `missing required field "metadata_complete"`) {
				t.Fatalf("incomplete Manifest error=%v", err)
			}
			break
		}
		if page.Done {
			t.Fatal("incomplete Manifest proxy reached final page without corrupting an entry")
		}
		cursor = page.NextCursor
	}

	if os.Getenv("DRIVE9_MIGRATION_CONTRACT_SQL") == "1" {
		rows, err := api.SQL(fmt.Sprintf("SELECT COUNT(*) AS n FROM fs_events WHERE path = '%s' AND op = 'mkdir'", duplicatePath))
		if err != nil || len(rows) != 1 || numericJSONValue(rows[0]["n"]) != 1 {
			t.Fatalf("duplicate delivery events=%v err=%v", rows, err)
		}
	}
}

func TestMigrationIncompleteManifestProxyRejectsMetadata(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"entries":[{"path":"/a.txt","type":"regular","metadata_complete":true,"identity_kind":"inode","mode":420,"size":1,"checksum_sha256":"`+checksum+`","revision":1,"resource_id":"inode-a","nlink":1}],"next_cursor":null,"done":true}`)
	}))
	t.Cleanup(upstream.Close)

	proxy := newMigrationIncompleteManifestProxy(t, upstream.URL)
	_, err := client.New(proxy.URL, "").ManifestPageCtx(context.Background(), "/", "", 1)
	if err == nil || !strings.Contains(err.Error(), `missing required field "metadata_complete"`) {
		t.Fatalf("incomplete Manifest error=%v", err)
	}
}

func provisionMigrationContractTenant(t *testing.T, ctx context.Context, base string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/provision", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provision status=%d", resp.StatusCode)
	}
	var result struct {
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || result.APIKey == "" {
		t.Fatalf("provision result=%+v err=%v", result, err)
	}
	return result.APIKey
}

func waitMigrationContractTenantActive(t *testing.T, ctx context.Context, base, apiKey string) {
	t.Helper()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var status struct {
				Status string `json:"status"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&status)
			_ = resp.Body.Close()
			if decodeErr == nil && status.Status == "active" {
				return
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("tenant did not become active: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func numericJSONValue(value any) int64 {
	switch value := value.(type) {
	case float64:
		return int64(value)
	case json.Number:
		result, _ := value.Int64()
		return result
	default:
		return -1
	}
}

func newMigrationIncompleteManifestProxy(t *testing.T, upstream string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward, err := http.NewRequestWithContext(request.Context(), request.Method, upstream+request.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward.Header = request.Header.Clone()
		response, err := http.DefaultClient.Do(forward)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if request.URL.Path == "/v1/migration/manifest" && response.StatusCode < http.StatusMultipleChoices {
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(responseBody, &envelope); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			var entries []map[string]json.RawMessage
			if err := json.Unmarshal(envelope["entries"], &entries); err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			if len(entries) > 0 {
				delete(entries[0], "metadata_complete")
				envelope["entries"], err = json.Marshal(entries)
				if err == nil {
					responseBody, err = json.Marshal(envelope)
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
					return
				}
			}
		}
		for key, values := range response.Header {
			if strings.EqualFold(key, "Content-Length") {
				continue
			}
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(responseBody)
	}))
	t.Cleanup(server.Close)
	return server
}

func newMigrationCommitUnknownProxy(t *testing.T, upstream string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward, err := http.NewRequestWithContext(request.Context(), request.Method, upstream+request.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward.Header = request.Header.Clone()
		response, err := http.DefaultClient.Do(forward)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode >= http.StatusMultipleChoices {
			http.Error(w, "upstream mutation failed", response.StatusCode)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "response hijack unavailable", http.StatusBadGateway)
			return
		}
		connection, _, err := hijacker.Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	t.Cleanup(server.Close)
	return server
}
