package fuse

import "github.com/mem9-ai/drive9/pkg/client"

// newTestClient is the standard test-only client constructor for fuse
// tests. It pins the small-file threshold to the historical 50KB so
// fake HTTP servers without a /v1/status route still see the legacy
// direct-PUT vs V2-multipart split. Production code negotiates this
// value via /v1/status; the helper exists purely so each test fixture
// doesn't have to either stub out /v1/status or rewrite every server
// handler to expect multipart for small writes.
//
// Tests that specifically exercise the multipart path (force large)
// continue to set c.smallFileThreshold = 1 directly after construction.
func newTestClient(baseURL string) *client.Client {
	c := client.New(baseURL, "")
	c.SetSmallFileThresholdForTests(client.DefaultSmallFileThreshold)
	return c
}
