// Package fs defines the drive9 filesystem composition framework: a
// namespace operation chain, a handle (data-plane) decorator chain, the
// shared Context, lifecycle aggregation, and the event bus. The gofuse
// facade and identity tables stay in pkg/fuse; every policy and mechanism
// composes here as handlers and handle decorators.
package fs
