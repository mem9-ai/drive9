package cli

import (
	"strings"
	"testing"
	"time"
)

func TestProfileWriteBackBatchWindow(t *testing.T) {
	tests := []struct {
		name       string
		profile    profileConfig
		policy     fuseWritePolicy
		configured time.Duration
		explicit   bool
		want       time.Duration
	}{
		{
			name:    "builtin coding-agent",
			profile: builtinCodingAgentProfile(),
			policy:  fuseWritePolicyWriteBack,
			want:    defaultCodingAgentWriteBackBatchWindow,
		},
		{
			name:       "explicit zero disables",
			profile:    builtinCodingAgentProfile(),
			policy:     fuseWritePolicyWriteBack,
			explicit:   true,
			configured: 0,
			want:       0,
		},
		{
			name:       "explicit window wins",
			profile:    builtinCodingAgentProfile(),
			policy:     fuseWritePolicyWriteBack,
			explicit:   true,
			configured: 7 * time.Millisecond,
			want:       7 * time.Millisecond,
		},
		{
			name:    "custom same-name profile",
			profile: profileConfig{Name: defaultMountProfile},
			policy:  fuseWritePolicyWriteBack,
			want:    0,
		},
		{
			name:    "interactive profile",
			profile: profileConfig{Name: "interactive", Builtin: true},
			policy:  fuseWritePolicyWriteBack,
			want:    0,
		},
		{
			name:       "strict close policy",
			profile:    builtinCodingAgentProfile(),
			policy:     fuseWritePolicyCloseSync,
			configured: defaultCodingAgentWriteBackBatchWindow,
			want:       0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := profileWriteBackBatchWindow(test.profile, test.policy, test.configured, test.explicit)
			if got != test.want {
				t.Fatalf("profileWriteBackBatchWindow() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestMountCmdAppliesCodingAgentWriteBackBatchWindow(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	stubMountProfileAppendLogProbe(t)
	got := captureProfileMountOptions(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if *got == nil {
		t.Fatal("mountFuse was not called")
	}
	if (*got).WriteBackBatchWindow != defaultCodingAgentWriteBackBatchWindow {
		t.Fatalf("WriteBackBatchWindow = %v, want %v", (*got).WriteBackBatchWindow, defaultCodingAgentWriteBackBatchWindow)
	}
}

func TestMountCmdExplicitZeroDisablesCodingAgentWriteBackBatch(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	stubMountProfileAppendLogProbe(t)
	got := captureProfileMountOptions(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		"--writeback-batch-window=0",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if *got == nil {
		t.Fatal("mountFuse was not called")
	}
	if (*got).WriteBackBatchWindow != 0 {
		t.Fatalf("WriteBackBatchWindow = %v, want disabled", (*got).WriteBackBatchWindow)
	}
}

func TestMountCmdCustomCodingAgentProfileDoesNotInheritWriteBackBatch(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	writeTestProfile(t, "coding-agent", "[local]\n**/custom-cache/**\n")
	got := captureProfileMountOptions(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--profile=coding-agent",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if *got == nil {
		t.Fatal("mountFuse was not called")
	}
	if (*got).WriteBackBatchWindow != 0 {
		t.Fatalf("WriteBackBatchWindow = %v, want no builtin default for custom profile", (*got).WriteBackBatchWindow)
	}
}

func TestMountCmdCloseSyncDoesNotInheritCodingAgentWriteBackBatch(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	stubMountProfileAppendLogProbe(t)
	got := captureProfileMountOptions(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		"--durability=close-sync",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if *got == nil {
		t.Fatal("mountFuse was not called")
	}
	if (*got).WriteBackBatchWindow != 0 {
		t.Fatalf("WriteBackBatchWindow = %v, want disabled for close-sync", (*got).WriteBackBatchWindow)
	}
}

func TestMountCmdFsyncInheritsCodingAgentWriteBackBatch(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	stubMountProfileAppendLogProbe(t)
	got := captureProfileMountOptions(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		"--durability=fsync",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if *got == nil {
		t.Fatal("mountFuse was not called")
	}
	if (*got).WriteBackBatchWindow != defaultCodingAgentWriteBackBatchWindow {
		t.Fatalf("WriteBackBatchWindow = %v, want %v", (*got).WriteBackBatchWindow, defaultCodingAgentWriteBackBatchWindow)
	}
}

func TestMountCmdRejectsExplicitWriteBackBatchForCloseSync(t *testing.T) {
	setupMountProfileAppendLogTest(t)

	err := MountCmd([]string{
		"--foreground",
		"--mode=fuse",
		"--server=https://drive9.example",
		"--api-key=sk-test",
		"--local-root", t.TempDir(),
		"--no-auto-unpack",
		"--durability=close-sync",
		"--writeback-batch-window=20ms",
		t.TempDir(),
	})
	if err == nil {
		t.Fatal("MountCmd should reject an explicit writeback batch window for close-sync")
	}
	if !strings.Contains(err.Error(), "--writeback-batch-window requires --durability auto, interactive, or fsync") {
		t.Fatalf("MountCmd error = %v", err)
	}
}
