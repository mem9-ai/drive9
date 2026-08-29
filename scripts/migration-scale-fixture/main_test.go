package main

import "testing"

func TestFixtureProfileDefaults(t *testing.T) {
	observed, err := fixtureProfile("observed")
	if err != nil {
		t.Fatal(err)
	}
	if observed.Entries != 299853 || observed.Files != 257213 || observed.LogicalBytes != 3328748350 {
		t.Fatalf("observed = %+v", observed)
	}
	full, err := fixtureProfile("full")
	if err != nil {
		t.Fatal(err)
	}
	if full.Entries != 6000000 || full.Files != 5140000 || full.LogicalBytes != 62<<30 {
		t.Fatalf("full = %+v", full)
	}
	if _, err := fixtureProfile("unknown"); err == nil {
		t.Fatal("unknown profile accepted")
	}
}

func TestApplyFixtureOverrides(t *testing.T) {
	config := fixtureConfig{Entries: 10, Files: 6, LogicalBytes: 100}
	got := applyFixtureOverrides(config, 12, -1, 120)
	if got.Entries != 12 || got.Files != 6 || got.LogicalBytes != 120 {
		t.Fatalf("config = %+v", got)
	}
}
