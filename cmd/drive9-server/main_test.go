package main

import "testing"

func TestNormalizeGRPCTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "kubernetes target with port",
			target: "kubernetes:///tidb-mgmt-service.tidb-management-service:10001",
			want:   "dns:///tidb-mgmt-service.tidb-management-service:10001",
		},
		{
			name:   "kubernetes target without port",
			target: "kubernetes:///tidb-mgmt-service.tidb-management-service",
			want:   "dns:///tidb-mgmt-service.tidb-management-service",
		},
		{
			name:   "kubernetes double slash target",
			target: "kubernetes://tidb-mgmt-service.tidb-management-service:10001",
			want:   "dns:///tidb-mgmt-service.tidb-management-service:10001",
		},
		{
			name:   "dns targets unchanged",
			target: "dns:///tidb-mgmt-service.tidb-management-service:10001",
			want:   "dns:///tidb-mgmt-service.tidb-management-service:10001",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeGRPCTarget(tc.target)
			if got != tc.want {
				t.Fatalf("normalizeGRPCTarget() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEnvBool(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		t.Setenv("DRIVE9_TEST_BOOL", "")
		got := envBool("DRIVE9_TEST_BOOL", true)
		if !got {
			t.Fatalf("envBool() = %v, want true", got)
		}
	})

	t.Run("parse true", func(t *testing.T) {
		t.Setenv("DRIVE9_TEST_BOOL", "true")
		got := envBool("DRIVE9_TEST_BOOL", false)
		if !got {
			t.Fatalf("envBool() = %v, want true", got)
		}
	})

	t.Run("invalid uses fallback", func(t *testing.T) {
		t.Setenv("DRIVE9_TEST_BOOL", "not-a-bool")
		got := envBool("DRIVE9_TEST_BOOL", false)
		if got {
			t.Fatalf("envBool() = %v, want false", got)
		}
	})
}
