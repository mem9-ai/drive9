package fuse

import (
	"errors"
	"net/http"
	"regexp"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

var quotaStatusRules = []struct {
	cause   string
	errno   syscall.Errno
	details []*regexp.Regexp
}{
	{
		cause: "tenant storage quota exceeded",
		errno: syscall.EDQUOT,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant storage quota exceeded: server limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ pending=-?[0-9]+ delta=-?[0-9]+$`),
			regexp.MustCompile(`^tenant storage quota exceeded: limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ current_path=-?[0-9]+ requested=-?[0-9]+ delta=-?[0-9]+$`),
			regexp.MustCompile(`^tenant storage quota exceeded: limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ requested=-?[0-9]+ delta=-?[0-9]+$`),
		},
	},
	{
		cause: "tenant file count quota exceeded",
		errno: syscall.EDQUOT,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant file count quota exceeded: server limit=-?[0-9]+ used=-?[0-9]+ pending=-?[0-9]+ delta=-?[0-9]+$`),
		},
	},
	{
		cause: "tenant file size quota exceeded",
		errno: syscall.EFBIG,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant file size quota exceeded: server limit=-?[0-9]+ requested=-?[0-9]+$`),
		},
	},
}

func quotaErrToFuseStatus(err error) (gofuse.Status, bool) {
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusInsufficientStorage {
		return 0, false
	}
	for _, rule := range quotaStatusRules {
		if statusErr.Message == rule.cause {
			return gofuse.Status(rule.errno), true
		}
		for _, details := range rule.details {
			if details.MatchString(statusErr.Message) {
				return gofuse.Status(rule.errno), true
			}
		}
	}
	return 0, false
}
