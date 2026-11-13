package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func queueProbeData() []byte { return bytes.Repeat([]byte("q"), (8<<20)+37) }

func queueRecovery(ctx context.Context, base string, binding client.ViewBinding) error {
	binary, err := os.Executable()
	if err != nil {
		return errors.New("queue probe executable")
	}
	dir, err := os.MkdirTemp("", "drive9-fixed-queue-")
	if err != nil {
		return errors.New("queue probe directory")
	}
	defer func() { _ = os.RemoveAll(dir) }()
	raw, err := json.Marshal(binding)
	if err != nil {
		return errors.New("queue probe binding")
	}
	for _, phase := range []string{"stage", "recover"} {
		cmd := exec.CommandContext(ctx, binary)
		cmd.Env = append(os.Environ(), "DRIVE9_FIXED_SDK_BASE="+base, "DRIVE9_FIXED_SDK_QUEUE_PHASE="+phase, "DRIVE9_FIXED_SDK_QUEUE_DIR="+dir, "DRIVE9_FIXED_SDK_QUEUE_BINDING="+string(raw))
		// Library diagnostics can contain remote details. The parent reports only
		// which isolated child failed; no raw child stderr crosses that boundary.
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Run(); err != nil {
			return errors.New("queue " + phase + " subprocess")
		}
	}
	return nil
}

// The stage child exits via os.Exit in main, without Stop/Close. The next child
// reconstructs all state from disk, with no inherited queue, fd or generation.
func queuePhase(ctx context.Context, base, phase, dir string, binding client.ViewBinding) error {
	if !localEndpoint(base) || (phase != "stage" && phase != "recover") || !filepath.IsAbs(dir) || binding.LayerID == "" || binding.Validation || binding.Validate() != nil {
		return errors.New("invalid queue probe configuration")
	}
	c, err := client.New(base, "").WithView(binding)
	if err != nil {
		return errors.New("queue binding")
	}
	ledger, err := staging.NewStagingWithQuota(dir, 0, 0)
	if err != nil {
		return errors.New("queue staging")
	}
	if err := ledger.Recover(); err != nil {
		return errors.New("queue journal replay")
	}
	w, err := writeback.New(writeback.Options{Client: c, Staging: ledger, Policy: writeback.PolicyWriteBack, UploadConcurrency: 1})
	if err != nil {
		return errors.New("queue runtime")
	}
	if phase == "recover" {
		records, err := ledger.Journal.PendingFixedUploads()
		if err != nil || len(records) != 1 {
			return errors.New("queue lost recovery request")
		}
		if err := w.Start(ctx); err != nil {
			return errors.New("queue restart replay")
		}
		if err := w.Queue().WaitIdle(ctx); err != nil {
			return errors.New("queue recovery idle")
		}
		if ledger.Pending.HasPending("/queue-file") || ledger.Shadow.Has("/queue-file") {
			return errors.New("queue recovery cleanup")
		}
		records, err = ledger.Journal.PendingFixedUploads()
		if err != nil || len(records) != 0 {
			return errors.New("queue recovery settlement")
		}
		if !readEquals(ctx, c, "/queue-file", queueProbeData()) {
			return errors.New("queue recovered bytes")
		}
		_ = w.Stop(ctx)
		ledger.Close()
		return nil
	}
	if err := w.Start(ctx); err != nil {
		return errors.New("queue initial start")
	}
	data := queueProbeData()
	if err := ledger.Shadow.WriteFull("/queue-file", data, 0); err != nil {
		return errors.New("queue shadow write")
	}
	if err := ledger.Shadow.Sync("/queue-file"); err != nil {
		return errors.New("queue shadow sync")
	}
	generation, err := ledger.Pending.PutWithBaseRev("/queue-file", int64(len(data)), staging.PendingNew, 0)
	if err != nil {
		return errors.New("queue pending publication")
	}
	entry := &writeback.CommitEntry{Path: "/queue-file", Size: int64(len(data)), Kind: staging.PendingNew, ShadowSpill: true, ShadowGen: ledger.Shadow.ActiveGeneration("/queue-file"), PendingIndexGen: generation}
	if err := w.Queue().Enqueue(entry); err != nil {
		return errors.New("queue enqueue")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := w.Queue().WaitIdle(waitCtx); err == nil || !client.IsCommitAttempted(w.Queue().PathTerminalFailure("/queue-file")) {
		return errors.New("queue lost-completion injection")
	}
	records, err := ledger.Journal.PendingFixedUploads()
	if err != nil || len(records) != 1 || !ledger.Pending.HasPending("/queue-file") || !ledger.Shadow.Has("/queue-file") {
		return errors.New("queue lost local recovery")
	}
	// Intentionally no runtime Stop, journal Close, or local settlement here.
	return nil
}
