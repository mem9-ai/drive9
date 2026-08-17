package mountstate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteReadClearExitReason(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	want := ExitReason{
		Reason: "serve_abnormal",
		Detail: "test",
		Code:   3,
		PID:    42,
		At:     time.Now().UTC().Truncate(time.Second),
	}
	if err := WriteExitReason(mp, want); err != nil {
		t.Fatal(err)
	}
	got, path, err := ReadExitReason(mp)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("empty path")
	}
	if got.Code != want.Code || got.Reason != want.Reason || got.Detail != want.Detail || got.PID != want.PID {
		t.Fatalf("got %#v want %#v", got, want)
	}
	if err := ClearExitReason(mp); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadExitReason(mp); !os.IsNotExist(err) {
		t.Fatalf("expected not exist after clear, got %v", err)
	}
}

func TestClearStopTokenIfLeavesSuccessor(t *testing.T) {
	mp := t.TempDir()
	oldTS, err := WriteStopTokenReceipt(mp, "ready-timeout")
	if err != nil {
		t.Fatal(err)
	}
	// Successor overwrites with a newer token.
	time.Sleep(2 * time.Millisecond)
	newTS, err := WriteStopTokenReceipt(mp, "umount")
	if err != nil {
		t.Fatal(err)
	}
	if err := ClearStopTokenIf(mp, oldTS); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadStopTokenTime(mp)
	if !ok {
		t.Fatal("successor stop token was cleared")
	}
	if !got.Equal(newTS) {
		t.Fatalf("token ts=%v want successor %v", got, newTS)
	}
}

func TestClearStopTokenIfRemovesMatching(t *testing.T) {
	mp := t.TempDir()
	ts, err := WriteStopTokenReceipt(mp, "ready-timeout")
	if err != nil {
		t.Fatal(err)
	}
	if err := ClearStopTokenIf(mp, ts); err != nil {
		t.Fatal(err)
	}
	if StopTokenPresent(mp) {
		t.Fatal("matching token should be cleared")
	}
}

func TestStopTokenRoundTrip(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	if err := WriteStopToken(mp, "umount"); err != nil {
		t.Fatal(err)
	}
	if !StopTokenPresent(mp) {
		t.Fatal("expected stop token present")
	}
	if err := ClearStopToken(mp); err != nil {
		t.Fatal(err)
	}
	if StopTokenPresent(mp) {
		t.Fatal("expected stop token cleared")
	}
}

func TestSupervisorStateRoundTrip(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	st := SupervisorState{
		PID:        9,
		MountPoint: mp,
		State:      SupervisorStateRunning,
		WorkerPID:  10,
		Args:       []string{"--mode=fuse", mp},
	}
	if err := WriteSupervisorState(mp, st); err != nil {
		t.Fatal(err)
	}
	got, _, err := ReadSupervisorState(mp)
	if err != nil {
		t.Fatal(err)
	}
	if got.PID != 9 || got.WorkerPID != 10 || got.State != SupervisorStateRunning {
		t.Fatalf("got %#v", got)
	}
	if len(got.Args) != 2 {
		t.Fatalf("args=%v", got.Args)
	}
}
