package fuse

import (
	"errors"
	"runtime"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Complements errno_alignment_test.go (#1008) with httpToFuseStatus rows it
// does not assert, and locks the allow-other mount binding that provides the
// kernel-side permission checks. Both were exercised end-to-end by the ext4
// differential harness on EC2 (issue #1006): 80 scenarios × remote/local-only
// branches × fsync/close-sync tiers, mounts passed --allow-other, zero parity
// failures.

func TestHTTPToFuseStatusVariantRows(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"404", &client.StatusError{StatusCode: 404}, gofuse.ENOENT},
		{"409 directory not empty", &client.StatusError{StatusCode: 409, Message: "directory not empty"}, gofuse.Status(syscall.ENOTEMPTY)},
		{"409 string directory not empty", errors.New("rmdir: directory not empty"), gofuse.Status(syscall.ENOTEMPTY)},
		{"401", &client.StatusError{StatusCode: 401}, gofuse.EACCES},
		{"403", &client.StatusError{StatusCode: 403}, gofuse.EACCES},
		{"400", &client.StatusError{StatusCode: 400}, gofuse.Status(syscall.EINVAL)},
		{"other 5xx falls to EIO", &client.StatusError{StatusCode: 501}, gofuse.EIO},
		{"string not found", errors.New("node not found"), gofuse.ENOENT},
		{"string already exists", errors.New("path already exists"), gofuse.Status(syscall.EEXIST)},
		{"net timeout", &netTimeoutError{}, gofuse.Status(syscall.EAGAIN)},
		{"layer rolled back", errors.Join(errLayerRolledBack), gofuse.Status(syscall.ESTALE)},
	}
	for _, c := range cases {
		if got := httpToFuseStatus(c.err); got != c.want {
			t.Errorf("%s: httpToFuseStatus = %v, want %v", c.name, got, c.want)
		}
	}
}

type netTimeoutError struct{}

func (netTimeoutError) Error() string   { return "i/o timeout" }
func (netTimeoutError) Timeout() bool   { return true }
func (netTimeoutError) Temporary() bool { return true }

// TestNewGoFuseMountOptionsDefaultPermissions locks the Linux mount contract:
// kernel-side permission checks ride along with allow-other, which is the
// supported configuration for permission-sensitive workloads (product
// decision on #1006: mounts are expected to pass --allow-other, whose
// default_permissions binding provides ext4-parity EACCES).
func TestNewGoFuseMountOptionsDefaultPermissions(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mount options differ per platform; asserted on linux")
	}
	opts := newGoFuseMountOptions(&MountOptions{AllowOther: true})
	found := false
	for _, o := range opts.Options {
		if o == "default_permissions" {
			found = true
		}
	}
	if !found {
		t.Errorf("AllowOther=true: default_permissions missing from %v", opts.Options)
	}
}
