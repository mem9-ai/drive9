package git

import (
	"os"
	"testing"

	"github.com/mem9-ai/drive9/pkg/gitcache"
)

// TestMain opts the package into local repositories: these tests materialize
// their fixtures as file:// repositories, which the production repo-URL policy
// rejects so a tenant cannot read a repository off the server's filesystem.
func TestMain(m *testing.M) {
	_ = os.Setenv(gitcache.EnvGitAllowLocalRepos, "1")
	os.Exit(m.Run())
}
