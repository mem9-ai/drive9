package main

import "testing"

func TestHasKubernetesResolverTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []string
		want    bool
	}{
		{
			name:    "kubernetes target with port",
			targets: []string{"kubernetes:///tidb-mgmt-service.tidb-management-service:10001"},
			want:    true,
		},
		{
			name:    "kubernetes target without port",
			targets: []string{"kubernetes:///tidb-mgmt-service.tidb-management-service"},
			want:    true,
		},
		{
			name:    "dns targets only",
			targets: []string{"dns:///tidb-mgmt-service.tidb-management-service:10001"},
			want:    false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := hasKubernetesResolverTarget(tc.targets...)
			if got != tc.want {
				t.Fatalf("hasKubernetesResolverTarget() = %v, want %v", got, tc.want)
			}
		})
	}
}
