package fuse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplyCodingAgentProfile_InheritsInteractiveDefaults(t *testing.T) {
	opts := &MountOptions{}
	ApplyCodingAgentProfile(opts)

	// Coding-agent profile inherits interactive defaults.
	require.Equal(t, 1*time.Second, opts.AttrTTL, "AttrTTL should use interactive default")
	require.Equal(t, 1*time.Second, opts.EntryTTL, "EntryTTL should use interactive default")
	require.Equal(t, 2*time.Second, opts.DirTTL, "DirTTL should use interactive default")
	require.Equal(t, time.Duration(0), opts.FlushDebounce, "FlushDebounce disabled")
}

func TestApplyCodingAgentProfile_PreservesUserTTLs(t *testing.T) {
	opts := &MountOptions{
		AttrTTL:  60 * time.Second,
		EntryTTL: 60 * time.Second,
		DirTTL:   60 * time.Second,
	}
	ApplyCodingAgentProfile(opts)

	require.Equal(t, 60*time.Second, opts.AttrTTL, "should preserve user's AttrTTL")
	require.Equal(t, 60*time.Second, opts.EntryTTL, "should preserve user's EntryTTL")
	require.Equal(t, 60*time.Second, opts.DirTTL, "should preserve user's DirTTL")
}

func TestApplyInteractiveProfile_SetsDefaults(t *testing.T) {
	opts := &MountOptions{}
	ApplyInteractiveProfile(opts)

	require.Equal(t, 1*time.Second, opts.AttrTTL)
	require.Equal(t, 1*time.Second, opts.EntryTTL)
	require.Equal(t, 2*time.Second, opts.DirTTL)
	require.Equal(t, time.Duration(0), opts.FlushDebounce)
}
