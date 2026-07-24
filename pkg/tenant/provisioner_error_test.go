package tenant

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestTiDBCloudAPIErrorCarriesStructuredStatus(t *testing.T) {
	err := error(&TiDBCloudAPIError{
		Operation:    "IAM API key lookup",
		StatusCode:   http.StatusForbidden,
		UpstreamBody: "access denied",
	})
	var apiErr *TiDBCloudAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("errors.As(%T) = false", err)
	}
	if apiErr.Operation != "IAM API key lookup" || apiErr.StatusCode != http.StatusForbidden || apiErr.UpstreamBody != "access denied" {
		t.Fatalf("error = %#v", apiErr)
	}
	if got := err.Error(); !strings.Contains(got, "tidbcloud native IAM API key lookup status 403: access denied") {
		t.Fatalf("Error() = %q", got)
	}
}
