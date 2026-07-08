package fuse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyCodingAgentProfile_SetsHigherTTLs(t *testing.T) {
	opts := &MountOptions{}
	ApplyCodingAgentProfile(opts)

	require.Equal(t, 30*time.Second, opts.AttrTTL, "AttrTTL")
	require.Equal(t, 30*time.Second, opts.EntryTTL, "EntryTTL")
	require.Equal(t, 30*time.Second, opts.DirTTL, "DirTTL")
	require.Equal(t, 10*time.Second, opts.NegativeEntryTTL, "NegativeEntryTTL")
	require.Equal(t, time.Duration(0), opts.FlushDebounce, "FlushDebounce disabled")
}

func TestApplyCodingAgentProfile_PreservesHigherUserTTLs(t *testing.T) {
	opts := &MountOptions{
		AttrTTL:         60 * time.Second,
		EntryTTL:        60 * time.Second,
		DirTTL:          60 * time.Second,
		NegativeEntryTTL: 30 * time.Second,
	}
	ApplyCodingAgentProfile(opts)

	require.Equal(t, 60*time.Second, opts.AttrTTL, "should preserve user's higher AttrTTL")
	require.Equal(t, 60*time.Second, opts.EntryTTL, "should preserve user's higher EntryTTL")
	require.Equal(t, 60*time.Second, opts.DirTTL, "should preserve user's higher DirTTL")
	require.Equal(t, 30*time.Second, opts.NegativeEntryTTL, "should preserve user's higher NegativeEntryTTL")
}

func TestApplyCodingAgentProfile_OverridesInteractiveDefaults(t *testing.T) {
	// Start with interactive defaults (1s attr/entry, 2s dir).
	opts := &MountOptions{}
	ApplyInteractiveProfile(opts)

	require.Equal(t, 1*time.Second, opts.AttrTTL, "interactive AttrTTL baseline")
	require.Equal(t, 1*time.Second, opts.EntryTTL, "interactive EntryTTL baseline")
	require.Equal(t, 2*time.Second, opts.DirTTL, "interactive DirTTL baseline")

	// Now apply coding-agent on top — it should raise them.
	ApplyCodingAgentProfile(opts)

	require.Equal(t, 30*time.Second, opts.AttrTTL, "coding-agent should raise AttrTTL")
	require.Equal(t, 30*time.Second, opts.EntryTTL, "coding-agent should raise EntryTTL")
	require.Equal(t, 30*time.Second, opts.DirTTL, "coding-agent should raise DirTTL")
	require.Equal(t, 10*time.Second, opts.NegativeEntryTTL, "coding-agent should set NegativeEntryTTL")
}

func TestApplyInteractiveProfile_SetsDefaults(t *testing.T) {
	opts := &MountOptions{}
	ApplyInteractiveProfile(opts)

	require.Equal(t, 1*time.Second, opts.AttrTTL)
	require.Equal(t, 1*time.Second, opts.EntryTTL)
	require.Equal(t, 2*time.Second, opts.DirTTL)
	require.Equal(t, time.Duration(0), opts.FlushDebounce)
}
