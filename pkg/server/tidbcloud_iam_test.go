package server

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/meta"
)

func TestTiDBCloudOrganizationMatchesRequiresExplicitOrganization(t *testing.T) {
	if !tiDBCloudOrganizationMatches("org-1", "org-1") {
		t.Fatal("matching organizations should be authorized")
	}
	for _, resourceOrganizationID := range []string{"", meta.SharedDBOrgWildcard, "org-2"} {
		if tiDBCloudOrganizationMatches("org-1", resourceOrganizationID) {
			t.Fatalf("resource organization %q should not be authorized", resourceOrganizationID)
		}
	}
}
