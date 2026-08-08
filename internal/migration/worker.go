package migration

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	roundInterval  = time.Second
	retryBase      = 100 * time.Millisecond
	maxRetryDelay  = 30 * time.Second
	attentionAfter = 5 * time.Minute
)

// Worker owns the process-local execution state for one Job.
type Worker struct {
	startup        *Startup
	api            *client.Client
	scanner        *Scanner
	inventory      *TargetScanner
	apply          *ApplyEngine
	checkpoint     *CheckpointStore
	recovery       *Recovery
	state          *State
	roundID        atomic.Uint64
	eventID        atomic.Int64
	gracePeriod    time.Duration
	now            func() time.Time
	retryWait      func(context.Context, time.Duration) error
	reporter       *eventReporter
	eventIngest    bool
	controlGate    serialGate
	sourceIdentity sourceMountIdentity
	credentialID   credentialFingerprint
	writesFenced   atomic.Bool
	fenceIntent    atomic.Bool
	fenceComplete  atomic.Bool
}

type workerClientRuntime struct {
	api         *client.Client
	inventory   *TargetScanner
	apply       *ApplyEngine
	checkpoint  *CheckpointStore
	eventIngest bool
}

func NewWorker(ctx context.Context, startup *Startup) (*Worker, error) {
	if startup == nil || startup.Config == nil {
		return nil, errors.New("Worker requires startup configuration")
	}
	scanner, err := NewScanner(startup.Job.Source.Root)
	if err != nil {
		return nil, err
	}
	observedSource, err := observeMountedSource(startup.Job.Source.Root, startup.Job.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("observe mounted source: %w", err)
	}
	if !startup.acceptedSource.present() {
		startup.acceptedSource = observedSource
	} else if !startup.acceptedSource.matches(observedSource) {
		return nil, ErrSourceMountChanged
	}
	w := &Worker{
		startup: startup, scanner: scanner, sourceIdentity: startup.acceptedSource,
		gracePeriod: time.Duration(startup.Config.JobDefaults.Sync.GracePeriod),
	}
	if err := w.refreshClient(ctx); err != nil {
		return nil, err
	}
	w.recovery, err = w.checkpoint.Recover(ctx, startup)
	if err != nil {
		return nil, err
	}
	w.state = w.recovery.State
	w.writesFenced.Store(!w.recovery.WritesAllowed)
	w.fenceIntent.Store(w.recovery.Record.Checkpoint.FenceIntent)
	w.fenceComplete.Store(w.recovery.Record.Checkpoint.FenceComplete)
	if w.recovery.WritesAllowed {
		if err := w.rebuildApply(); err != nil {
			return nil, err
		}
		if w.eventIngest && w.state.Snapshot().Phase == PhaseDualWriteRepairing {
			w.reporter = newEventReporter(w.api, 128)
			w.apply.onCAS = w.reportCAS
		}
	}
	return w, nil
}

func RunWorker(ctx context.Context, startup *Startup) error {
	return RunWorkerAt(ctx, startup, DefaultControlSocket)
}

func RunWorkerAt(ctx context.Context, startup *Startup, socket string) error {
	worker, err := NewWorker(ctx, startup)
	if err != nil {
		return err
	}
	control, err := startControl(ctx, socket, worker)
	if err != nil {
		return err
	}
	defer control.close()
	return worker.Run(ctx)
}

func (w *Worker) refreshClient(ctx context.Context) error {
	if err := w.controlGate.Acquire(ctx); err != nil {
		return err
	}
	defer w.controlGate.Release()
	return w.refreshClientLocked(ctx)
}

func (w *Worker) refreshClientLocked(ctx context.Context) error {
	key, credentialID, err := w.startup.Credential.readStable()
	if err != nil {
		return err
	}
	api := client.New(w.startup.Config.Drive9.Endpoint, key)
	caps, err := api.GetMigrationCapabilities(ctx)
	if err != nil {
		return err
	}
	if missing := missingCapabilities(caps); missing != "" {
		return fmt.Errorf("required capability %s is unavailable", missing)
	}
	inventory, err := NewTargetScanner(api, w.startup.Job.Target.Prefix)
	if err != nil {
		return err
	}
	candidate := workerClientRuntime{
		api: api, inventory: inventory, checkpoint: NewCheckpointStore(api), eventIngest: caps.EventIngest,
	}
	if w.recovery != nil {
		observed, loadErr := candidate.checkpoint.Load(ctx, w.startup.Job.VolumeID)
		if loadErr != nil {
			return fmt.Errorf("validate refreshed credential checkpoint: %w", loadErr)
		}
		active := w.recovery.Record
		if observed.Revision == 0 || observed.Revision != active.Revision || !sameCheckpointIdentity(observed.Checkpoint, active.Checkpoint) || observed.Checkpoint.HighestPhase != active.Checkpoint.HighestPhase || observed.Checkpoint.FenceIntent != active.Checkpoint.FenceIntent || observed.Checkpoint.FenceComplete != active.Checkpoint.FenceComplete {
			return fmt.Errorf("%w: refreshed credential checkpoint identity or control state", ErrCheckpointMismatch)
		}
		if w.state != nil && w.recovery.WritesAllowed {
			candidate.apply, err = w.newApplyEngine(api)
			if err != nil {
				return err
			}
		}
	}
	w.api, w.inventory, w.checkpoint = candidate.api, candidate.inventory, candidate.checkpoint
	w.apply, w.eventIngest = candidate.apply, candidate.eventIngest
	w.credentialID = credentialID
	if w.reporter != nil {
		w.reporter.api.Store(candidate.api)
		if w.apply != nil {
			w.apply.onCAS = nil
			if candidate.eventIngest && w.state.Snapshot().Phase == PhaseDualWriteRepairing {
				w.apply.onCAS = w.reportCAS
			}
		}
	}
	if w.state != nil {
		if w.recovery == nil || w.recovery.WritesAllowed {
			if w.apply == nil {
				return w.rebuildApply()
			}
		}
	}
	return nil
}

func (w *Worker) rebuildApply() error {
	apply, err := w.newApplyEngine(w.api)
	if err != nil {
		return err
	}
	w.apply = apply
	if w.reporter != nil && w.eventIngest && w.state.Snapshot().Phase == PhaseDualWriteRepairing {
		w.apply.onCAS = w.reportCAS
	}
	return nil
}

func (w *Worker) newApplyEngine(api *client.Client) (*ApplyEngine, error) {
	performance := w.startup.Config.JobDefaults.Performance
	apply, err := NewApplyEngine(api, w.scanner, ApplyConfig{
		Prefix: w.startup.Job.Target.Prefix, Phase: w.state.Snapshot().Phase,
		SmallFileThreshold: api.CachedSmallFileThreshold(), SmallWorkers: performance.SmallFileWorkers,
		LargeWorkers: performance.LargeFileWorkers, MaxBytesPerSecond: performance.MaxBytesPerSecond,
	})
	if err != nil {
		return nil, err
	}
	apply.onOperationStart = w.state.beginOperation
	apply.onOperationDone = w.state.endOperation
	return apply, nil
}

func (w *Worker) validateSourceMount() error {
	var (
		observed sourceMountIdentity
		err      error
	)
	if w.startup != nil && w.startup.Job.Source.Root != "" {
		observed, err = observeMountedSource(w.startup.Job.Source.Root, w.startup.Job.VolumeID)
	} else if w.scanner != nil {
		observed, err = observeSourceRoot(w.scanner.root)
	} else {
		err = errors.New("missing source scanner")
	}
	if err == nil && !w.sourceIdentity.present() {
		w.sourceIdentity = observed
		return nil
	}
	if err != nil || !w.sourceIdentity.matches(observed) {
		if w.state != nil {
			w.state.SetAttention(true)
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSourceMountChanged, err)
		}
		return ErrSourceMountChanged
	}
	return nil
}

func (w *Worker) refreshChangedCredentialLocked(ctx context.Context) (bool, error) {
	if w.startup == nil || w.startup.Credential.path == "" {
		return false, nil
	}
	observed, err := w.startup.Credential.fingerprint()
	if err != nil {
		w.state.SetAttention(true)
		return false, err
	}
	if observed == w.credentialID {
		return false, nil
	}
	if err := w.refreshClientLocked(ctx); err != nil {
		w.state.SetAttention(true)
		return false, err
	}
	return true, nil
}

func (w *Worker) validateRemoteCheckpoint(ctx context.Context) error {
	if w.startup == nil || w.recovery == nil || w.checkpoint == nil {
		return nil
	}
	observed, err := w.checkpoint.Load(ctx, w.startup.Job.VolumeID)
	if err != nil {
		return err
	}
	active := w.recovery.Record
	if observed.Revision == 0 || observed.Revision != active.Revision || observed.Checkpoint != active.Checkpoint {
		w.state.SetAttention(true)
		w.writesFenced.Store(true)
		return fmt.Errorf("%w: remote control state changed", ErrCheckpointMismatch)
	}
	return nil
}

func (w *Worker) validateRoundBoundary(ctx context.Context) error {
	if err := w.validateSourceMount(); err != nil {
		return err
	}
	refreshed, err := w.refreshChangedCredentialLocked(ctx)
	if err != nil {
		return err
	}
	if refreshed {
		return nil
	}
	return w.validateRemoteCheckpoint(ctx)
}

func (w *Worker) State() StateSnapshot { return w.state.Snapshot() }

func (w *Worker) DeepRecovery(ctx context.Context) error {
	if err := w.Round(ctx, RoundModeDeep); err != nil {
		return err
	}
	if !w.state.Snapshot().Current.ScanComplete {
		return ErrIncompleteRound
	}
	w.state.SetRecoveryComplete(true)
	if w.state.Snapshot().Phase == PhaseDualWriteRepairing {
		w.state.setRepairMtimeFloor(w.state.Snapshot().Current.StartedAt.Add(-w.gracePeriod))
	}
	return nil
}

func (w *Worker) Round(ctx context.Context, mode RoundMode) error {
	if w.writesFenced.Load() {
		return ErrIllegalAction
	}
	id := fmt.Sprintf("round-%d", w.roundID.Add(1))
	started := w.clock()
	w.state.BeginRound(id, mode, started)
	if err := w.validateRoundBoundary(ctx); err != nil {
		failureClass := "identity"
		if errors.Is(err, ErrSourceMountChanged) {
			failureClass = "scan"
		}
		w.state.FailRound(id, failureClass)
		return err
	}
	if w.state.Snapshot().Phase == PhaseDualWriteRepairing {
		return w.dualRound(ctx, mode, id, started)
	}
	source, target, err := w.scanPair(ctx)
	if err != nil {
		w.state.FailRound(id, "scan")
		return err
	}
	round, err := BuildRound(id, mode, started, source, target)
	if err != nil {
		w.state.FailRound(id, "inventory")
		return err
	}
	w.state.setPendingRepairs(repairFindingCount(round))
	if unsafeRound(round) {
		w.state.SetAttention(true)
		return w.state.PublishRound(round)
	}
	if !round.Converged {
		if w.apply == nil {
			w.state.FailRound(id, "fenced")
			return ErrIllegalAction
		}
		if err := w.validateRoundBoundary(ctx); err != nil {
			w.state.FailRound(id, "identity")
			return err
		}
		if err := w.apply.Apply(ctx, source.Entries, target.Entries); err != nil {
			if errors.Is(err, ErrUnsafeApply) {
				w.state.SetAttention(true)
				return w.state.PublishRound(round)
			}
			w.state.FailRound(id, "apply")
			return err
		}
		source, target, err = w.scanPair(ctx)
		if err != nil {
			w.state.FailRound(id, "reread")
			return err
		}
		round, err = BuildRound(id, mode, started, source, target)
		if err != nil {
			w.state.FailRound(id, "reread")
			return err
		}
		w.state.setPendingRepairs(repairFindingCount(round))
	}
	if round.Converged {
		for path, entry := range round.Source {
			w.state.MarkReconciled(path, entry.Version)
		}
	}
	if err := w.state.PublishRound(round); err != nil {
		return err
	}
	if round.Converged && w.state.Snapshot().Phase == PhaseSyncing {
		w.state.SetInitialCopyComplete(true)
	}
	w.state.SetAttention(false)
	return nil
}

func (w *Worker) scanPair(ctx context.Context) (ScanResult, TargetScan, error) {
	source, err := w.scanSource(ctx)
	if err != nil {
		return source, TargetScan{}, err
	}
	deepPaths := make(map[string]struct{})
	for path, entry := range source.Entries {
		if entry.Kind == EntryRegular {
			deep, readErr := w.scanner.ReadStableEntry(ctx, entry)
			if readErr != nil {
				source.Complete = false
				return source, TargetScan{}, readErr
			}
			entry.ChecksumSHA256 = deep.ChecksumSHA256
			source.Entries[path] = entry
		}
		if entry.Kind != EntryDirectory {
			deepPaths[path] = struct{}{}
		}
	}
	target, err := w.inventory.Scan(ctx, deepPaths)
	return source, target, err
}

func (w *Worker) scanSource(ctx context.Context) (ScanResult, error) {
	source, err := w.scanner.Scan(ctx)
	if err != nil {
		return source, err
	}
	prefix := ""
	if w.inventory != nil {
		prefix = w.inventory.prefix
	} else if w.startup != nil {
		prefix = w.startup.Job.Target.Prefix
	}
	carveOutReservedControlPrefix(&source, prefix)
	return source, nil
}

func carveOutReservedControlPrefix(source *ScanResult, targetPrefix string) {
	if source == nil || targetPrefix != "/" {
		return
	}
	for _, path := range sortedSourcePaths(source.Entries) {
		if path != ControlPrefix && !strings.HasPrefix(path, ControlPrefix+"/") {
			continue
		}
		entry := source.Entries[path]
		delete(source.Entries, path)
		source.EntryCount--
		if entry.Kind == EntryDirectory {
			source.DirectoryCount--
		}
		if entry.Kind == EntryRegular {
			source.LogicalBytes -= entry.Version.Size
		}
		source.Findings = append(source.Findings, Finding{Path: path, Kind: FindingControlPrefix, Severity: SeverityBlocker})
	}
}

func unsafeRound(round Round) bool {
	for _, finding := range round.Findings {
		switch finding.Kind {
		case FindingSourceOnly, FindingTargetOnly, FindingContent, FindingMetadata,
			FindingSparseFile, FindingSymlinkTarget, FindingModeBits:
		default:
			if finding.Severity == SeverityBlocker {
				return true
			}
		}
	}
	return false
}

func repairFindingCount(round Round) int {
	paths := make(map[string]struct{})
	for _, finding := range round.Findings {
		if finding.Severity == SeverityBlocker {
			paths[finding.Path] = struct{}{}
		}
	}
	return len(paths)
}

func (w *Worker) Run(ctx context.Context) error {
	if w.recovery != nil && !w.recovery.WritesAllowed {
		if w.recovery.FenceRecoveryOnly {
			if err := w.controlGate.Acquire(ctx); err != nil {
				return err
			}
			_, err := w.completeFenceLocked(ctx)
			w.controlGate.Release()
			if err != nil {
				return err
			}
		}
		<-ctx.Done()
		return nil
	}
	if w.reporter != nil {
		reporterCtx, cancel := context.WithCancel(ctx)
		w.reporter.start(reporterCtx)
		defer func() { cancel(); w.reporter.wait() }()
	}
	recovered := w.state.Snapshot().RecoveryComplete
	attempt := 0
	var blockedAt time.Time
	for {
		if w.writesFenced.Load() {
			<-ctx.Done()
			return nil
		}
		var err error
		if acquireErr := w.controlGate.Acquire(ctx); acquireErr != nil {
			return nil
		}
		if recovered {
			mode := RoundModeFull
			if w.state.Snapshot().Phase == PhaseDualWriteRepairing {
				mode = RoundModeFast
			}
			err = w.Round(ctx, mode)
		} else {
			err = w.DeepRecovery(ctx)
			recovered = err == nil
		}
		w.controlGate.Release()
		if ctx.Err() != nil {
			return nil
		}
		delay := roundInterval
		if err != nil {
			authRetry := isAuthError(err) && w.startup != nil
			if authRetry {
				if refreshErr := w.refreshClient(ctx); refreshErr != nil {
					w.state.SetAttention(true)
				}
			} else if !retryableWorkerError(err) {
				w.state.SetAttention(true)
				return newWorkerRunError(err)
			}
			if blockedAt.IsZero() {
				blockedAt = w.clock()
			} else if w.clock().Sub(blockedAt) >= attentionAfter {
				w.state.SetAttention(true)
			}
			delay = retryDelay(attempt, maxRetryDelay)
			attempt++
		} else {
			attempt, blockedAt = 0, time.Time{}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

type workerRunError struct {
	class string
	cause error
}

func (e *workerRunError) Error() string { return "migration worker stopped: " + e.class }
func (e *workerRunError) Unwrap() error { return e.cause }

func newWorkerRunError(err error) error {
	result := &workerRunError{class: "non_retryable_operation"}
	var status *client.StatusError
	switch {
	case errors.As(err, &status):
		result.class = fmt.Sprintf("drive9_http_%d", status.StatusCode)
		result.cause = &client.StatusError{StatusCode: status.StatusCode}
	case errors.Is(err, ErrUnsafeApply):
		result.class, result.cause = "unsafe_apply", ErrUnsafeApply
	case errors.Is(err, ErrUnsafeSourcePath):
		result.class, result.cause = "unsafe_source", ErrUnsafeSourcePath
	case errors.Is(err, ErrCheckpointMismatch):
		result.class, result.cause = "checkpoint_mismatch", ErrCheckpointMismatch
	}
	return result
}

func (w *Worker) reportCAS(source SourceEntry, target *client.StatResult, expected int64, started time.Time, err error) {
	if w.reporter == nil || w.startup == nil {
		return
	}
	token := sourceVersionToken(source.Version)
	eventContext, ok := w.state.eventContextForCAS(source.Path, token)
	if !ok || eventContext.Phase != PhaseDualWriteRepairing {
		return
	}
	firstSeen := eventContext.Candidate.FirstSeen
	if firstSeen.IsZero() || w.clock().Before(firstSeen.Add(w.graceWindow())) {
		return
	}
	now := time.Now()
	result, class := "success", "none"
	if errors.Is(err, client.ErrConflict) {
		result, class = "conflict", "cas_conflict"
	} else if err != nil {
		result, class = "failure", classifyRetry(err)
	}
	attempt := w.eventID.Add(1)
	event := client.MigrationEvent{
		EventID: fmt.Sprintf("%s-%d-%d", eventContext.RoundID, attempt, now.UnixNano()), EmittedAt: now.UTC().Format(time.RFC3339Nano),
		Phase: string(eventContext.Phase), RoundID: eventContext.RoundID, CASAttempt: attempt, FirstSeenAt: firstSeen.UTC().Format(time.RFC3339Nano), GraceSeconds: int64(w.graceWindow().Seconds()),
		JobID: w.startup.Job.VolumeID, VolumeID: w.startup.Job.VolumeID, NodeName: w.startup.Job.NodeName, SpaceID: w.startup.Job.Target.SpaceRef,
		SourcePath: source.Path, TargetPath: targetRemotePath(w.startup.Job.Target.Prefix, source.Path, false), SourceVersionToken: token,
		Size: source.Version.Size, Mtime: source.Version.MtimeNS, SourceChecksumSHA256: source.ChecksumSHA256, ExpectedRevision: expected,
		Operation: "update", Result: result, ErrorClass: class, LatencyMS: time.Since(started).Milliseconds(),
	}
	if expected == 0 {
		event.Operation = "create"
	}
	event.PodName, _ = os.Hostname()
	if target != nil {
		event.ResourceID, event.Revision, event.Drive9ChecksumSHA256 = target.ResourceID, target.Revision, target.ChecksumSHA256
	}
	w.reporter.enqueue(event)
}

func (w *Worker) clock() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

func (w *Worker) waitForRetry(ctx context.Context, delay time.Duration) error {
	if w.retryWait != nil {
		return w.retryWait(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryDelay(attempt int, maximum time.Duration) time.Duration {
	delay := retryBase << min(attempt, 20)
	return min(delay, maximum)
}

func isAuthError(err error) bool {
	var status *client.StatusError
	return errors.As(err, &status) && (status.StatusCode == http.StatusUnauthorized || status.StatusCode == http.StatusForbidden)
}

func retryableWorkerError(err error) bool {
	if errors.Is(err, ErrSourceChanged) || errors.Is(err, ErrTargetChanged) || errors.Is(err, ErrApplyRescan) || errors.Is(err, ErrCheckpointConflict) || errors.Is(err, ErrCredentialChanged) || errors.Is(err, fs.ErrNotExist) {
		return true
	}
	var status *client.StatusError
	if errors.As(err, &status) {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= 500
	}
	var network net.Error
	return errors.As(err, &network)
}
