package httperr

import (
	"errors"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// TestHTTPStatusOfVariantRows locks the variant rows of the
// backend-status → kernel-errno table (revision-conflict 409 → EIO,
// "directory not empty" → ENOTEMPTY, other 409 → EEXIST, quota 507 →
// EDQUOT/EFBIG, 5xx/499 → EAGAIN, 412 → ESTALE, 413 → EFBIG), plus the
// string-matching fallback for non-StatusError errors.
func TestHTTPStatusOfVariantRows(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"404", &client.StatusError{StatusCode: 404}, gofuse.ENOENT},
		{"409 revision conflict", &client.StatusError{StatusCode: 409, Message: "revision conflict on node"}, gofuse.EIO},
		{"409 directory not empty", &client.StatusError{StatusCode: 409, Message: "directory not empty"}, gofuse.Status(syscall.ENOTEMPTY)},
		{"409 default EEXIST", &client.StatusError{StatusCode: 409, Message: "path exists"}, gofuse.Status(syscall.EEXIST)},
		{"401", &client.StatusError{StatusCode: 401}, gofuse.EACCES},
		{"403", &client.StatusError{StatusCode: 403}, gofuse.EACCES},
		{"400", &client.StatusError{StatusCode: 400}, gofuse.Status(syscall.EINVAL)},
		{"412", &client.StatusError{StatusCode: 412}, gofuse.Status(syscall.ESTALE)},
		{"413", &client.StatusError{StatusCode: 413}, gofuse.Status(syscall.EFBIG)},
		{"500", &client.StatusError{StatusCode: 500}, gofuse.Status(syscall.EAGAIN)},
		{"502", &client.StatusError{StatusCode: 502}, gofuse.Status(syscall.EAGAIN)},
		{"503", &client.StatusError{StatusCode: 503}, gofuse.Status(syscall.EAGAIN)},
		{"504", &client.StatusError{StatusCode: 504}, gofuse.Status(syscall.EAGAIN)},
		{"499 client closed", &client.StatusError{StatusCode: 499}, gofuse.Status(syscall.EAGAIN)},
		{"other 5xx falls to EIO", &client.StatusError{StatusCode: 501}, gofuse.EIO},
		{"quota storage", &client.StatusError{StatusCode: 507, Message: "tenant storage quota exceeded: server limit=1 used=2 reserved=0 pending=0 delta=3"}, gofuse.Status(syscall.EDQUOT)},
		{"quota file count", &client.StatusError{StatusCode: 507, Message: "tenant file count quota exceeded: server limit=1 used=2 pending=0 delta=3"}, gofuse.Status(syscall.EDQUOT)},
		{"quota file size", &client.StatusError{StatusCode: 507, Message: "tenant file size quota exceeded: server limit=1 requested=2"}, gofuse.Status(syscall.EFBIG)},
		{"507 unknown message", &client.StatusError{StatusCode: 507, Message: "out of space somehow"}, gofuse.EIO},
		{"canceled", errors.New("context canceled"), gofuse.EIO}, // non-Status errors fall through
	}
	for _, c := range cases {
		if got := HTTPStatusOf(c.err); got != c.want {
			t.Errorf("%s: HTTPStatusOf = %v, want %v", c.name, got, c.want)
		}
	}
}

// String-fallback rows for errors without a typed status.
func TestHTTPStatusOfStringFallback(t *testing.T) {
	cases := []struct {
		msg  string
		want gofuse.Status
	}{
		{"node not found", gofuse.ENOENT},
		{"path already exists", gofuse.Status(syscall.EEXIST)},
	}
	for _, c := range cases {
		if got := HTTPStatusOf(errors.New(c.msg)); got != c.want {
			t.Errorf("%q: HTTPStatusOf = %v, want %v", c.msg, got, c.want)
		}
	}
}
