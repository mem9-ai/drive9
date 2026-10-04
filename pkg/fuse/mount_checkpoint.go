package fuse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

type mountCheckpointFunc func(context.Context, string) (mountcontrol.Checkpoint, error)

type mountCheckpointFilesystem interface {
	Drain(context.Context) mountcontrol.DrainResponse
	mountPointForDrain() string
}

func runMountCheckpoint(
	ctx context.Context,
	fs mountCheckpointFilesystem,
	gate *workspaceMutationGate,
	checkpointID string,
	checkpoint mountCheckpointFunc,
) mountcontrol.DrainResponse {
	startedAt := time.Now().UTC()
	resp := mountcontrol.NewDrainResponse(fs.mountPointForDrain(), startedAt)
	if strings.TrimSpace(checkpointID) == "" {
		resp.Fail("bad_request", "", errors.New("missing checkpoint ID"))
		resp.Finish(time.Now().UTC())
		return resp
	}
	if gate == nil || checkpoint == nil {
		resp.Fail("unsupported", "", errors.New("mount is not a writable LayerFS mount with checkpoint control"))
		resp.Finish(time.Now().UTC())
		return resp
	}

	phaseStart := time.Now()
	release, err := gate.quiesce(ctx)
	quiescePhase := mountcontrol.DrainPhase{
		Name:       "quiesce_mutations",
		DurationMS: time.Since(phaseStart).Milliseconds(),
	}
	if err != nil {
		quiescePhase.Error = err.Error()
		resp.Phases = append(resp.Phases, quiescePhase)
		resp.Fail(drainContextKind(err), "", err)
		resp.Finish(time.Now().UTC())
		return resp
	}
	defer release()

	resp = fs.Drain(ctx)
	resp.StartedAt = startedAt
	resp.Phases = append([]mountcontrol.DrainPhase{quiescePhase}, resp.Phases...)
	if !resp.OK {
		resp.Finish(time.Now().UTC())
		return resp
	}

	phaseStart = time.Now()
	record, err := checkpoint(ctx, checkpointID)
	checkpointPhase := mountcontrol.DrainPhase{
		Name:       "checkpoint_layer",
		DurationMS: time.Since(phaseStart).Milliseconds(),
	}
	if err != nil {
		checkpointPhase.Error = err.Error()
		resp.Phases = append(resp.Phases, checkpointPhase)
		resp.Fail("checkpoint_failed", "", err)
		resp.Finish(time.Now().UTC())
		return resp
	}
	if record.CheckpointID != checkpointID || record.LayerID == "" || record.DurableSeq < 0 {
		err = fmt.Errorf("verified checkpoint does not match the requested identity")
		checkpointPhase.Error = err.Error()
		resp.Phases = append(resp.Phases, checkpointPhase)
		resp.Fail("checkpoint_mismatch", "", err)
		resp.Finish(time.Now().UTC())
		return resp
	}
	resp.Phases = append(resp.Phases, checkpointPhase)
	resp.Checkpoint = &record
	resp.Finish(time.Now().UTC())
	return resp
}
