package backend

import "testing"

// TestConfigureOptions_MonthlyLLMCostDefault pins the tri-state handling of
// LLMCostBudgetOptions.MaxMonthlyMillicents in configureOptions:
//
//	> 0 — use the explicit value
//	== 0 — apply defaultMaxMonthlyLLMCostMillicents ($10 defense-in-depth cap)
//	< 0 — explicit opt-out (no monthly cap)
//
// The zero-value branch is the defense-in-depth contract: forgetting to
// configure a cap MUST NOT yield unlimited spend. A regression here is how
// a new tenant silently burns through the LLM budget.
func TestConfigureOptions_MonthlyLLMCostDefault(t *testing.T) {
	tests := []struct {
		name       string
		configured int64
		want       int64
	}{
		{
			name:       "unset falls back to global default",
			configured: 0,
			want:       defaultMaxMonthlyLLMCostMillicents,
		},
		{
			name:       "explicit positive wins over default",
			configured: 42_000,
			want:       42_000,
		},
		{
			name:       "explicit negative disables the gate",
			configured: -1,
			want:       0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := &Dat9Backend{}
			b.configureOptions(Options{
				LLMCostBudget: LLMCostBudgetOptions{
					MaxMonthlyMillicents: tc.configured,
				},
			})
			if b.maxMonthlyLLMCostMillicents != tc.want {
				t.Fatalf("maxMonthlyLLMCostMillicents = %d, want %d (configured=%d)",
					b.maxMonthlyLLMCostMillicents, tc.want, tc.configured)
			}
		})
	}
}

// TestDefaultMaxMonthlyLLMCostMillicents_IsTenDollars pins the default at
// exactly $10.00 in millicents. Changing the default is a policy decision
// that should be reviewed, not a silent numeric edit.
func TestDefaultMaxMonthlyLLMCostMillicents_IsTenDollars(t *testing.T) {
	const tenDollarsInMillicents = int64(1_000_000)
	if defaultMaxMonthlyLLMCostMillicents != tenDollarsInMillicents {
		t.Fatalf("defaultMaxMonthlyLLMCostMillicents = %d, want %d ($10.00). "+
			"Changing this default affects every tenant without a per-tenant "+
			"override — update the tenant onboarding docs and this test together.",
			defaultMaxMonthlyLLMCostMillicents, tenDollarsInMillicents)
	}
}
