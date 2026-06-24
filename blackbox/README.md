# Drive9 Blackbox Harness

`blackbox/` contains the shared Drive9 blackbox test harness. The harness is
suite-agnostic: FUSE is the first implemented suite, and CLI/API suites can be
added under `blackbox/suites/` without moving the core runner again.

## Layout

```text
blackbox/
  run.py                 generic entrypoint
  harness/
    core.py             shared constants, Context, Recorder, Module protocol
    runner.py           BlackboxRunner: module execution, timeout, reporting
    report.py           three-tier report templates (module/suite/overall)
    suite.py            SuiteProvider protocol, suite discovery
    deps.py             DependencyManager: git clone, tool detection
    capabilities.py    host capability detection (OS, tools, features)
  suites/
    fuse/                FUSE module config, repos, allowlists
      README.md          FUSE suite usage notes
      NOTICE.md          FUSE suite notices for external test suites
      provider.py        FUSE lifecycle, prereqs, target/dependency wiring
      target.py          Drive9 CLI/server/FUSE mount provider
      deps.py            FUSE suite dependency preparation
      capabilities.py    macFUSE/FUSE-T/Linux FUSE capability detection
      modules/           FUSE module implementations and registry
      modules.json       module config: groups, per-module settings, compat matrix
```

## Commands

```bash
# Run all modules in a suite
python3 blackbox/run.py --suite fuse --all

# Run a single module or group
python3 blackbox/run.py --suite fuse --module drive9.workflow.git_fast_clone
python3 blackbox/run.py --suite fuse --group posix
python3 blackbox/run.py --suite fuse --category drive9.workflow

# Bootstrap: prepare dependencies into a work-dir, then exit
python3 blackbox/run.py --suite fuse --all --bootstrap --work-dir /tmp/bb

# Reuse the work-dir for a real run
python3 blackbox/run.py --suite fuse --all --work-dir /tmp/bb

# Run all discovered suites and generate an overall report
python3 blackbox/run.py --all-suites --all

# List available modules
python3 blackbox/run.py --suite fuse --list --format json
```

Makefile targets are suite-agnostic. `BLACKBOX_SUITE` defaults to `fuse`:

```bash
make blackbox
make blackbox BLACKBOX_SELECTOR=group:posix
make blackbox BLACKBOX_SELECTOR=category:drive9.workflow
make blackbox BLACKBOX_SELECTOR=module:drive9.workflow.git_fast_clone
make blackbox BLACKBOX_WORK_DIR=/tmp/bb
make blackbox-bootstrap BLACKBOX_SELECTOR=module:community.lock BLACKBOX_WORK_DIR=/tmp/bb
```

## Reports

Blackbox generates three levels of report automatically after each run — no
AI or post-processing required:

1. **Module report** (`artifacts/<module>/report.md` + `report.json`):
   Generated per-module using a template selected by the module's
   `report_profile` (functional / performance / compatibility / customer).
   Modules can override `render_report()` for fully custom output.

2. **Suite report** (`results/<suite>/<session>/report.md` + `suite-report.json`):
   Aggregates all module results with suite goals, profile-grouped summary,
   and metrics highlights. Links to each module report.

3. **Overall report** (`<work-dir>/report.md` + `overall-report.json`):
   Cross-suite summary, generated when running `--all-suites`. For single-suite
   runs, the suite report serves as the top-level report.

## Platform Compatibility

Each module can declare its platform expectation in `modules.json`:

```json
"community.ltp.fs": {
  "compat": { "linux": "run", "darwin": "skip" }
}
```

Values: `"run"` (default), `"skip"` (SKIP with `platform:<os>` classification),
or `"xfail"` (run normally; FAIL→XFAIL, PASS→WARN for unexpected pass).

## Module Timeout

Each module has a wall-clock timeout enforced by the runner via
`ThreadPoolExecutor` + `future.result(timeout=N)`. A timed-out module is
recorded as FAIL with classification `timeout`. Timeout source priority:

1. `BLACKBOX_<SUITE>_<MODULE>_TIMEOUT_S` environment variable
2. `timeout` field in `modules.json` per-module config
3. Module class `timeout` attribute (default 600s)

## Work-Dir Isolation

All writable state (cache, tmp, results, GOCACHE/GOMODCACHE) lives under a
work-dir, keeping the repo tree clean. Use `--work-dir` or `BLACKBOX_WORK_DIR`
to specify; defaults to `blackbox/work/<suite>/<session>/`.

`blackbox` intentionally has no scheduling or profile policy. It only knows how
to run every module in a suite or a caller-selected subset. Scheduling,
frequency, and report publication belong to the caller, for example a GitHub
Actions workflow, cron job, or release gate.

Runs print setup, dependency, module, command, mount, wait, and cleanup progress
to stdout by default. Set `BLACKBOX_QUIET=1` to suppress progress chatter while
keeping the final summary and report path.

## Adding A Suite

Add a new suite directory under `blackbox/suites/<suite>/` with at least:

```text
modules.json
provider.py
```

Then add or reuse modules under `blackbox/suites/<suite>/modules/`, register
them in `blackbox/suites/<suite>/modules/registry.py`, and expose stable module
IDs/categories/groups through `modules.json`. `provider.py` must expose
`create_provider()` or `SuiteProvider` and is responsible for suite config
loading, capability checks, dependency manager creation, target creation,
setup, cleanup, and manifest fields.

Suite-specific capability checks should live in modules or target helpers, not
as global runner prerequisites. This keeps CLI/API modules from being blocked by
FUSE-only requirements.
