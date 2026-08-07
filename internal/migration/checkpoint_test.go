package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	driveclient "github.com/mem9-ai/drive9/pkg/client"
)

type checkpointFake struct {
	mu             sync.Mutex
	revision       int64
	body           []byte
	failCAS        bool
	writes         int
	dirConflict    bool
	dirIsDir       bool
	failGetAtWrite int
}

func (f *checkpointFake) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "mkdir") {
		if f.dirConflict {
			http.Error(w, "directory exists", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Path != "/v1/fs/.drive9-migration/jobs/vol-001/checkpoint.json" {
		if r.Method == http.MethodHead && f.dirConflict {
			w.Header().Set("X-Dat9-IsDir", strconv.FormatBool(f.dirIsDir))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodHead:
		if f.revision == 0 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(f.revision, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if f.revision == 0 {
			http.NotFound(w, r)
			return
		}
		if f.failGetAtWrite > 0 && f.writes == f.failGetAtWrite {
			f.failGetAtWrite = 0
			http.Error(w, "injected checkpoint read failure", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(f.body)
	case http.MethodPut:
		expected, _ := strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
		if f.failCAS || expected != f.revision {
			http.Error(w, "checkpoint conflict", http.StatusConflict)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.body = append([]byte(nil), body...)
		f.revision++
		f.writes++
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": f.revision})
	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

func newCheckpointFixture(t *testing.T) (*CheckpointStore, *checkpointFake, *Startup) {
	t.Helper()
	fake := &checkpointFake{}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)
	startup := &Startup{
		Config: &Config{Drive9: Drive9Config{Endpoint: server.URL}},
		Job: Job{
			VolumeID: "vol-001", NodeName: "node-a",
			Source: SourceConfig{Type: "ebs", Root: "/ebs/001"},
			Target: TargetConfig{SpaceRef: "space-001", Prefix: "/data"},
		},
		Space: SpaceConfig{CredentialRef: "space-001-key"}, Phase: PhaseSyncing, ConfigHash: "config-hash",
	}
	return NewCheckpointStore(driveclient.New(server.URL, "owner-key")), fake, startup
}

func TestCheckpointRecoverCreatesMinimalRecordAndFreshState(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Record.Revision != 1 || recovery.Record.Checkpoint.HighestPhase != PhaseSyncing || !recovery.WritesAllowed || !recovery.DeepRecoveryRequired || recovery.FenceRecoveryOnly {
		t.Fatalf("recovery=%+v", recovery)
	}
	recovery.State.SetRecoveryComplete(true)
	recovery.State.BeginRound("round", RoundModeDeep, time.Now())
	if err := recovery.State.PublishRound(completeTestRound("round", true)); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.State.Snapshot().LastComplete != nil || restarted.State.Snapshot().RecoveryComplete || restarted.State.Snapshot().Conditions.ReadyForRollout {
		t.Fatalf("restart inherited working state=%+v", restarted.State.Snapshot())
	}

	fake.mu.Lock()
	body := string(fake.body)
	fake.mu.Unlock()
	for _, forbidden := range []string{"round", "token", "revision", "finding", "verification", "upload_id", "secret", "api_key", "repair_mtime_floor"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("checkpoint contains forbidden %q: %s", forbidden, body)
		}
	}
}

func TestCheckpointPhaseAdvanceRejectsRollbackAndIdentityMismatch(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	if _, err := store.Recover(context.Background(), startup); err != nil {
		t.Fatal(err)
	}
	startup.Phase = PhaseDualWriteRepairing
	advanced, err := store.Recover(context.Background(), startup)
	if err != nil || advanced.Record.Checkpoint.HighestPhase != PhaseDualWriteRepairing || advanced.Record.Revision != 2 {
		t.Fatalf("advanced=%+v err=%v", advanced, err)
	}
	startup.Phase = PhaseSyncing
	if _, err := store.Recover(context.Background(), startup); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("rollback error=%v", err)
	}
	startup.Phase = PhaseDualWriteRepairing
	startup.ConfigHash = "changed"
	if _, err := store.Recover(context.Background(), startup); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("identity error=%v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.writes != 2 {
		t.Fatalf("writes=%d, want create+advance only", fake.writes)
	}
}

func TestCheckpointStaleCASFailsClosed(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.failCAS = true
	fake.mu.Unlock()
	next := recovery.Record.Checkpoint
	next.HighestPhase = PhaseDualWriteRepairing
	if _, err := store.Update(context.Background(), recovery.Record, next); !errors.Is(err, ErrCheckpointConflict) {
		t.Fatalf("CAS error=%v", err)
	}
}

func TestCheckpointCASFailureHooksModelBeforeAndAfterCommit(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	next := recovery.Record.Checkpoint
	next.HighestPhase = PhaseDualWriteRepairing
	store.beforeWrite = func(Checkpoint) error { return errors.New("before CAS") }
	if _, err := store.Update(context.Background(), recovery.Record, next); err == nil {
		t.Fatal("before-CAS failure succeeded")
	}
	fake.mu.Lock()
	if fake.writes != 1 {
		t.Fatalf("before-CAS writes=%d", fake.writes)
	}
	fake.mu.Unlock()
	store.beforeWrite = nil
	store.afterWrite = func(Checkpoint) error { return errors.New("after CAS") }
	if _, err := store.Update(context.Background(), recovery.Record, next); err == nil {
		t.Fatal("after-CAS failure succeeded")
	}
	store.afterWrite = nil
	restarted, err := store.Recover(context.Background(), &Startup{
		Config: startup.Config, Job: startup.Job, Space: startup.Space,
		Phase: PhaseDualWriteRepairing, ConfigHash: startup.ConfigHash,
	})
	if err != nil || restarted.Record.Checkpoint.HighestPhase != PhaseDualWriteRepairing {
		t.Fatalf("after-CAS recovery=%+v err=%v", restarted, err)
	}
}

func TestCheckpointFenceIntentForcesRecoveryOnly(t *testing.T) {
	store, _, startup := newCheckpointFixture(t)
	startup.Phase = PhaseDualWriteRepairing
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	next := recovery.Record.Checkpoint
	next.FenceIntent = true
	record, err := store.Update(context.Background(), recovery.Record, next)
	if err != nil {
		t.Fatal(err)
	}
	if record.Revision != 2 {
		t.Fatalf("revision=%d", record.Revision)
	}
	restarted, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.WritesAllowed || restarted.DeepRecoveryRequired || !restarted.FenceRecoveryOnly {
		t.Fatalf("fence recovery=%+v", restarted)
	}
}

func TestCheckpointRejectsForbiddenTransitionAndCorruptPayload(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	next := recovery.Record.Checkpoint
	next.FenceComplete = true
	if _, err := store.Update(context.Background(), recovery.Record, next); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("complete-without-intent error=%v", err)
	}
	fake.mu.Lock()
	fake.body = []byte(`{"version":"v1","job_id":"vol-001","unknown":true}`)
	fake.mu.Unlock()
	if _, err := store.Load(context.Background(), "vol-001"); err == nil {
		t.Fatal("unknown checkpoint field accepted")
	}
}

func TestCheckpointStrictPathDecodeAndShapeValidation(t *testing.T) {
	if _, err := CheckpointPath("../vol-001"); err == nil {
		t.Fatal("unsafe checkpoint path accepted")
	}
	_, _, startup := newCheckpointFixture(t)
	checkpoint := checkpointFromStartup(startup)
	body, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCheckpoint(append(body, []byte(" {}")...)); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("trailing error=%v", err)
	}
	for _, mutate := range []func(*Checkpoint){
		func(value *Checkpoint) { value.Version = "v2" },
		func(value *Checkpoint) { value.ConfigHash = "" },
		func(value *Checkpoint) { value.HighestPhase = "UNKNOWN" },
		func(value *Checkpoint) { value.FenceIntent = true },
		func(value *Checkpoint) { value.HighestPhase, value.FenceIntent = PhaseCutoverReady, true },
		func(value *Checkpoint) { value.FenceComplete = true },
	} {
		invalid := checkpoint
		mutate(&invalid)
		if err := validateCheckpoint(invalid); !errors.Is(err, ErrCheckpointMismatch) {
			t.Fatalf("invalid checkpoint=%+v error=%v", invalid, err)
		}
	}
}

func TestCheckpointLoadRejectsUnstableAndOversizedReads(t *testing.T) {
	_, _, startup := newCheckpointFixture(t)
	checkpoint := checkpointFromStartup(startup)
	body, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		body     []byte
		unstable bool
		want     error
	}{
		{name: "unstable", body: body, unstable: true, want: ErrCheckpointConflict},
		{name: "oversized", body: bytes.Repeat([]byte{'x'}, maxCheckpointBytes+1), want: ErrCheckpointMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var heads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					revision := int64(1)
					if tc.unstable && heads.Add(1) > 1 {
						revision = 2
					}
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
				case http.MethodGet:
					_, _ = w.Write(tc.body)
				}
			}))
			defer server.Close()
			store := NewCheckpointStore(driveclient.New(server.URL, ""))
			if _, err := store.Load(context.Background(), "vol-001"); !errors.Is(err, tc.want) {
				t.Fatalf("load error=%v, want %v", err, tc.want)
			}
		})
	}
}

func TestCheckpointDirectoryConflictMustAlreadyBeDirectory(t *testing.T) {
	store, fake, startup := newCheckpointFixture(t)
	fake.dirConflict = true
	fake.dirIsDir = true
	if _, err := store.Recover(context.Background(), startup); err != nil {
		t.Fatalf("existing directories: %v", err)
	}

	store, fake, startup = newCheckpointFixture(t)
	fake.dirConflict = true
	fake.dirIsDir = false
	if _, err := store.Recover(context.Background(), startup); err == nil {
		t.Fatal("non-directory control collision accepted")
	}
}

func TestCheckpointCompleteFenceIsDurableAndCannotClear(t *testing.T) {
	store, _, startup := newCheckpointFixture(t)
	startup.Phase = PhaseDualWriteRepairing
	recovery, err := store.Recover(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	next := recovery.Record.Checkpoint
	next.FenceIntent = true
	next.FenceComplete = true
	next.HighestPhase = PhaseCutoverReady
	complete, err := store.Update(context.Background(), recovery.Record, next)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Recover(context.Background(), startup)
	if err != nil || restarted.WritesAllowed || restarted.DeepRecoveryRequired || restarted.Record.Checkpoint.HighestPhase != PhaseCutoverReady {
		t.Fatalf("complete recovery=%+v err=%v", restarted, err)
	}
	cleared := complete.Checkpoint
	cleared.FenceIntent = false
	cleared.FenceComplete = false
	if _, err := store.Update(context.Background(), complete, cleared); !errors.Is(err, ErrCheckpointMismatch) {
		t.Fatalf("clear fence error=%v", err)
	}
}
