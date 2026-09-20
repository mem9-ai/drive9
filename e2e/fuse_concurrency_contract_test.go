package e2e

import (
	"os"
	"strings"
	"testing"
)

func TestFuseConcurrencyMountsWithExplicitWriteSync(t *testing.T) {
	script, err := os.ReadFile("fuse-concurrency-stress.sh")
	if err != nil {
		t.Fatalf("read fuse concurrency script: %v", err)
	}

	contents := string(script)
	for _, required := range []string{
		"mount_args=(mount --mode=fuse --durability=write-sync)",
		`echo "mount_argv=$mount_argv_text" | tee -a "$MOUNT_LOG"`,
		`drive9 "${mount_args[@]}"`,
	} {
		if !strings.Contains(contents, required) {
			t.Fatalf("fuse concurrency script missing write-sync contract %q", required)
		}
	}
}
