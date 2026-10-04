package fuse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

type checkpointTestFilesystem struct {
	drain func(context.Context) mountcontrol.DrainResponse
}

func (fs *checkpointTestFilesystem) Drain(ctx context.Context) mountcontrol.DrainResponse {
	return fs.drain(ctx)
}

func (fs *checkpointTestFilesystem) mountPointForDrain() string {
	return "/mnt/drive9"
}

func TestRunMountCheckpointFencesMutationsAroundDrainAndCheckpoint(t *testing.T) {
	fs := newTestDrainFS()
	gate := newWorkspaceMutationGate()
	if !gate.enter(nil) {
		t.Fatal("first mutation did not enter")
	}

	checkpointStarted := make(chan struct{})
	allowCheckpoint := make(chan struct{})
	response := make(chan mountcontrol.DrainResponse, 1)
	go func() {
		response <- runMountCheckpoint(
			context.Background(),
			fs,
			gate,
			"cp-1",
			func(context.Context, string) (mountcontrol.Checkpoint, error) {
				close(checkpointStarted)
				<-allowCheckpoint
				return mountcontrol.Checkpoint{
					CheckpointID: "cp-1",
					LayerID:      "layer-1",
					DurableSeq:   7,
				}, nil
			},
		)
	}()

	select {
	case <-checkpointStarted:
		t.Fatal("checkpoint started before the in-flight mutation completed")
	case <-time.After(50 * time.Millisecond):
	}
	gate.leave()

	select {
	case <-checkpointStarted:
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not start after the in-flight mutation completed")
	}

	secondEntered := make(chan struct{})
	go func() {
		if gate.enter(nil) {
			close(secondEntered)
			gate.leave()
		}
	}()
	select {
	case <-secondEntered:
		t.Fatal("mutation entered while checkpoint callback held the quiesce barrier")
	case <-time.After(50 * time.Millisecond):
	}

	close(allowCheckpoint)
	var resp mountcontrol.DrainResponse
	select {
	case resp = <-response:
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not complete")
	}
	if !resp.OK || resp.Checkpoint == nil {
		t.Fatalf("checkpoint response = %+v", resp)
	}
	if resp.Checkpoint.CheckpointID != "cp-1" || resp.Checkpoint.LayerID != "layer-1" || resp.Checkpoint.DurableSeq != 7 {
		t.Fatalf("checkpoint = %+v", resp.Checkpoint)
	}
	if len(resp.Phases) == 0 || resp.Phases[0].Name != "quiesce_mutations" || resp.Phases[len(resp.Phases)-1].Name != "checkpoint_layer" {
		t.Fatalf("phases = %+v", resp.Phases)
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("mutation did not resume after checkpoint completed")
	}
}

func TestRunMountCheckpointFailureReleasesMutationGate(t *testing.T) {
	gate := newWorkspaceMutationGate()
	resp := runMountCheckpoint(
		context.Background(),
		newTestDrainFS(),
		gate,
		"cp-1",
		func(context.Context, string) (mountcontrol.Checkpoint, error) {
			return mountcontrol.Checkpoint{}, errors.New("checkpoint unavailable")
		},
	)
	if resp.OK || resp.ErrorKind != "checkpoint_failed" || resp.Checkpoint != nil {
		t.Fatalf("checkpoint response = %+v", resp)
	}

	entered := make(chan struct{})
	go func() {
		if gate.enter(nil) {
			close(entered)
			gate.leave()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("checkpoint failure left mutation gate quiesced")
	}
}

func TestRunMountCheckpointDrainFailureSkipsCheckpointAndReleasesGate(t *testing.T) {
	gate := newWorkspaceMutationGate()
	checkpointCalled := false
	fs := &checkpointTestFilesystem{
		drain: func(context.Context) mountcontrol.DrainResponse {
			return mountcontrol.DrainResponse{OK: false, ErrorKind: "pending_work_remaining", Error: "dirty handle remains"}
		},
	}
	resp := runMountCheckpoint(
		context.Background(),
		fs,
		gate,
		"cp-1",
		func(context.Context, string) (mountcontrol.Checkpoint, error) {
			checkpointCalled = true
			return mountcontrol.Checkpoint{}, nil
		},
	)
	if resp.OK || resp.ErrorKind != "pending_work_remaining" || resp.Checkpoint != nil {
		t.Fatalf("checkpoint response = %+v", resp)
	}
	if checkpointCalled {
		t.Fatal("checkpoint callback ran after drain failure")
	}
	if !gate.enter(nil) {
		t.Fatal("drain failure left mutation gate quiesced")
	}
	gate.leave()
}

func TestRunMountCheckpointRejectsUnsupportedMount(t *testing.T) {
	resp := runMountCheckpoint(context.Background(), newTestDrainFS(), nil, "cp-1", nil)
	if resp.OK || resp.ErrorKind != "unsupported" || resp.Checkpoint != nil {
		t.Fatalf("checkpoint response = %+v", resp)
	}
}

func TestRunMountCheckpointDrainsBeforeCreatingCheckpoint(t *testing.T) {
	drained := false
	fs := &checkpointTestFilesystem{
		drain: func(context.Context) mountcontrol.DrainResponse {
			drained = true
			return mountcontrol.DrainResponse{OK: true, MountPoint: "/mnt/drive9"}
		},
	}
	resp := runMountCheckpoint(
		context.Background(),
		fs,
		newWorkspaceMutationGate(),
		"cp-1",
		func(context.Context, string) (mountcontrol.Checkpoint, error) {
			if !drained {
				t.Fatal("checkpoint callback ran before drain")
			}
			return mountcontrol.Checkpoint{CheckpointID: "cp-1", LayerID: "layer-1"}, nil
		},
	)
	if !resp.OK || resp.Checkpoint == nil {
		t.Fatalf("checkpoint response = %+v", resp)
	}
}
