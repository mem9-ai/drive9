package main

import (
	"errors"
	"fmt"
	"testing"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
)

func TestFatalPreservesWrappedMountExitCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"permanent", drive9fuse.ExitStartupPermanentErr("authorization denied", errors.New("forbidden")), drive9fuse.ExitStartupPermanent},
		{"transient", drive9fuse.ExitStartupTransientErr("unavailable", errors.New("timeout")), drive9fuse.ExitStartupTransient},
		{"ordinary", errors.New("unclassified failure"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := exitFunc
			t.Cleanup(func() { exitFunc = original })
			var codes []int
			exitFunc = func(code int) { codes = append(codes, code) }
			// Journal selection adds context, and runtime cleanup may join a
			// second error. Neither may turn permanent auth failure into the
			// generic status that the supervisor treats as restartable.
			err := fmt.Errorf("selected mount generation: %w", tc.err)
			err = errors.Join(err, errors.New("cleanup detail"))
			fatal("mount", err)
			if len(codes) != 1 || codes[0] != tc.want {
				t.Fatalf("exit codes = %v, want [%d]", codes, tc.want)
			}
		})
	}
}
