# Drive9 Blackbox Harness

`blackbox/` contains the shared Drive9 blackbox test harness. The harness is
suite-agnostic: FUSE is the first implemented suite, and CLI/API suites can be
added under `blackbox/suites/` without moving the core runner again.

## Layout

```text
blackbox/
  run.py                 generic entrypoint
  harness/               shared runner, target helpers, dependency cache logic
  suites/
    fuse/                FUSE presets, module config, repos, allowlists
      README.md          FUSE suite usage notes
      NOTICE.md          FUSE suite notices for external test suites
      modules/           FUSE module implementations and registry
```

## Commands

```bash
python3 blackbox/run.py --suite fuse --preset smoke
python3 blackbox/run.py --suite fuse --group posix
python3 blackbox/run.py --suite fuse --module drive9.workflow.git_fast_clone
```

Makefile targets are suite-agnostic. `BLACKBOX_SUITE` defaults to `fuse`:

```bash
make blackbox-smoke
make blackbox-daily
make blackbox-module BLACKBOX_MODULE=drive9.workflow.git_fast_clone
```

## Adding A Suite

Add a new suite directory under `blackbox/suites/<suite>/` with at least:

```text
presets.json
modules.json
```

Then add or reuse modules under `blackbox/suites/<suite>/modules/`, register
them in `blackbox/suites/<suite>/modules/registry.py`, and select them from the
suite presets.

Suite-specific capability checks should live in modules or target helpers, not
as global runner prerequisites. This keeps CLI/API modules from being blocked by
FUSE-only requirements.
