//go:build !windows && !darwin

package fuse

func validatePromotionPlatformACL(string) error { return nil }
