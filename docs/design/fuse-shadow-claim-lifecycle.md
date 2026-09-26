# Follow-up: shadow claim encapsulation

The read-pin policy lives in `shadow_reader.go`; clean adoption and passive
truncate transitions live in `handle_commit.go`. The next bounded refactor is to
encapsulate `ShadowReady`, `ShadowSpill`, `ShadowStageGen`, `ShadowStageSeq` and
`ShadowCommit*` in a handle staging type. Read pins remain separate because reset
and anonymous inode snapshots have independent lifetimes.

The type should own claim capture, generation-checked retirement and clearing,
replacing the remaining guard-plus-clear sequences in `dat9fs.go`. It must not
turn an absent ownership token into path-wide deletion or infer ownership from
sibling lock availability. Keep write-back and pending-index generations tied
to their existing owners.

Acceptance criteria:

- Preserve the existing generation fence, stale writer CAS conflict and passive
  close-sync truncate tests, including queued ownership after handle release.
- Preserve ordinary-commit reader invalidation, explicit reset/unlink snapshot
  lifetime, and newer same-path staging across delayed cleanup.
- Migrate state transitions and their tests together; do not introduce a wrapper
  while leaving independent field writes outside it.

This structural migration is deferred from PR968's read correctness fix because
it spans write, flush, fsync, release and queue ownership transitions. It is not
required for the explicit retirement reasons or the shared reader policy.
