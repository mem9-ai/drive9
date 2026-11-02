package localize

// qiffang seventeenth round: the DB accounting wrapper participates in
// the UnwrapHandle protocol so FD-named operations resolve the real
// write-plane descriptor underneath it.

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// The wrapper must expose its inner handle: FATTR_FH SetAttr and unlink
// survivor retargeting walk this chain — without it the resolution fell
// back to a per-path lookup and mutated a recreated object through the
// old descriptor.
func TestPortDBAccountingHandleUnwraps(t *testing.T) {
	var _ interface{ UnwrapHandle() fs.Handle } = (*dbAccountingHandle)(nil)
}
