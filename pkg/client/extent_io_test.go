package client

import (
	"net/http"
	"testing"
)

func TestExtentHintFromResponse(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusUnprocessableEntity, Header: make(http.Header)}
	resp.Header.Set("X-Dat9-Content-Layout", "extent")
	resp.Header.Set("X-Dat9-Extent-Ino", "42")
	resp.Header.Set("X-Dat9-Extent-Length", "100")
	ino, n, ok := extentHintFromResponse(resp)
	if !ok || ino != 42 || n != 100 {
		t.Fatalf("ino=%d n=%d ok=%v", ino, n, ok)
	}
	resp.StatusCode = http.StatusOK
	if _, _, ok := extentHintFromResponse(resp); ok {
		t.Fatal("200 should not be an extent hint")
	}
}
