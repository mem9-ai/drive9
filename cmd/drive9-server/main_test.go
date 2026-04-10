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
