package gitworktree

import "testing"

// The accepted/refused spelling table for AdminRel and ValidateName. The API and the
// mount layers keep their own cases too: they assert what their endpoint answers, and
// this package's table is what they all resolve through.
func TestAdminRelSpellings(t *testing.T) {
	accepted := []struct{ rel, name, want string }{
		{".git/worktrees/w1", "w1", "worktrees/w1"},
		{"worktrees/w1", "w1", "worktrees/w1"},
		{"worktrees/wt.1", "wt.1", "worktrees/wt.1"},
	}
	for _, tc := range accepted {
		got, err := AdminRel(tc.rel, tc.name)
		if err != nil {
			t.Fatalf("AdminRel(%q, %q) = %v, want success", tc.rel, tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("AdminRel(%q, %q) = %q, want %q", tc.rel, tc.name, got, tc.want)
		}
	}

	refused := []struct{ rel, name string }{
		{".git/hooks", "hooks"},
		{"hooks", "hooks"},
		{".git/config", "config"},
		{".git/objects", "objects"},
		{"worktrees", "w1"},
		{"worktrees/w1/extra", "w1"},
		{"worktrees/../../hooks", "hooks"},
		{"../hooks", "hooks"},
		{"/etc/hooks", "hooks"},
		{".git/worktrees/../hooks", "hooks"},
		{"worktrees/", ""},
		{"worktrees/w1\x00", "w1"},
		{"worktrees/w1\\pre-commit", "w1"},
		{".git/worktrees", ""},
		{"worktrees//w1", "w1"},
		{"worktrees/w1/", "w1"},
		{"worktrees/w1", "w2"},
		{"worktrees/w1", ""},
		{"", "w1"},
	}
	for _, tc := range refused {
		if got, err := AdminRel(tc.rel, tc.name); err == nil {
			t.Fatalf("AdminRel(%q, %q) = %q, want a refusal", tc.rel, tc.name, got)
		}
	}
}

func TestValidateName(t *testing.T) {
	for _, name := range []string{"w1", "wt.1", "repo-wt"} {
		if err := ValidateName(name); err != nil {
			t.Fatalf("ValidateName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "a/b", "a\\b", "w1\x00"} {
		if err := ValidateName(name); err == nil {
			t.Fatalf("ValidateName(%q) accepted", name)
		}
	}
	long := make([]byte, MaxNameBytes+1)
	for i := range long {
		long[i] = 'a'
	}
	if err := ValidateName(string(long)); err == nil {
		t.Fatal("an over-long name was accepted")
	}
}
