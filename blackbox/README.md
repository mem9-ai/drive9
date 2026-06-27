# Drive9 Blackbox Harness

`blackbox/` contains the Drive9 blackbox test framework, organized into three layers:

- **`harness/`** — generic test framework (runner, reporter, module protocol). No drive9 dependencies.
- **`env/`** — drive9 test environment (server startup, CLI build, FUSE mount, dependency management).
- **`suites/`** — test modules organized by group and module name: `suites/<group>/<module>/module.py`.

## Layout

```text
blackbox/
  run.py                 entrypoint
  harness/
    core.py             Context, Recorder, ModuleRecord, constants
    module_base.py      BaseModule class, module_config helper
    runner.py           BlackboxRunner: execution, timeout, reporting
    report.py           report templates
    suite.py            auto-discovery: scan suites/*/*/module.py
    deps.py             DependencyManager: git clone, tool detection
    capabilities.py     host capability detection
  env/
    provider.py         Drive9SuiteProvider: setup/cleanup lifecycle
    target.py           server/CLI/FUSE mount provider
    capabilities.py     FUSE/macFUSE capability detection
    deps.py             Drive9DependencyManager: per-module tool builds
  suites/
    community/          community test modules
      pjdfstest/
        module.py       test logic
        config.json      timeout, compat, labels, module-specific settings
        deps.py          dependency preparation (optional)
        allowlist.json   test allowlist (optional)
      fio/
      ltp/
      ...
    drive9/             drive9 workflow modules
    customer/           customer scenario modules
    juicefs/            JuiceFS-inspired rewrite modules
    git/                git official test modules
```

## Auto-Discovery

Modules are discovered automatically by scanning `suites/<group>/<module>/module.py`.
Each module file must export a class with `id`, `category`, and `run()` attributes.

Module IDs are derived from the class's `id` attribute (not directory path).
A single `module.py` can export multiple test classes (e.g., `ltp/module.py` has
`CommunityLTPFS` + `CommunityLTPSyscalls`).

`--group <name>` selects a directory group: every module whose id starts with
`<group>.`, mirroring the `suites/<group>/` layout. `--label <label>` is an
optional overlay filter (combinable with any selector) that narrows by module
labels.

## Commands

All commands use `python3 blackbox/run.py` directly. The default behavior uses
the `drive9` binary found on PATH and reads server/auth from `~/.drive9/config`
(via the CLI's own credential resolver). No CLI build, no server provisioning.

```bash
# List available modules (no side effects, no work-dir created)
python3 blackbox/run.py --list
python3 blackbox/run.py --list --format json

# Run all modules
python3 blackbox/run.py --all

# Run a single module
python3 blackbox/run.py --module community.pjdfstest

# Run a directory group
python3 blackbox/run.py --group community
python3 blackbox/run.py --group juicefs

# Narrow any selection by label
python3 blackbox/run.py --group community --label performance
python3 blackbox/run.py --all --label functional

# Run by category prefix
python3 blackbox/run.py --category drive9.workflow

# Specify a custom drive9 CLI path
python3 blackbox/run.py --all --bin ./bin/drive9

# Local server mode (user-built server binary, no auto-build)
python3 blackbox/run.py --module community.pjdfstest \
  --server-mode local --bin ./bin/drive9 --local-server ./bin/drive9-server-local

# Bootstrap: prepare dependencies, then exit
python3 blackbox/run.py --all --bootstrap --work-dir /tmp/bb

# Reuse the work-dir for a real run
python3 blackbox/run.py --all --work-dir /tmp/bb

# Prepare dependencies only (no setup/run)
python3 blackbox/run.py --all --deps-only --work-dir /tmp/bb

# Performance runs
python3 blackbox/run.py --module community.fio --runs 3

# Strict prerequisites (fail if FUSE unavailable instead of skipping)
python3 blackbox/run.py --all --strict-prereqs

# Offline mode (no auto-fetch of dependencies)
python3 blackbox/run.py --all --offline
```

## Server Modes

- **`config` (default)**: Uses the `drive9` binary on PATH (or `--bin <path>`)
  and lets the CLI resolve server URL and API key from `~/.drive9/config`. No
  CLI build, no server provisioning, no local server. This is the normal mode
  for running against an existing drive9 deployment.
- **`local`**: Starts a `drive9-server-local` process with a MySQL container
  (or `DRIVE9_LOCAL_DSN`). Requires `--local-server <path>` pointing to a
  pre-built server binary and `--bin <path>` for the CLI. Neither binary is
  built automatically.

## Module Structure

Each module directory contains:

| File | Required | Description |
|------|----------|-------------|
| `__init__.py` | Yes | Python package marker |
| `module.py` | Yes | Test logic: a class with `id`, `category`, `run(ctx) -> dict` |
| `config.json` | No | Module config: `timeout`, `compat`, `labels`, `report_profile`, module-specific settings |
| `deps.py` | No | `ensure_dependencies(ctx)` function for module-specific dependency preparation |
| `*.json` | No | Module-specific data files (allowlists, fixtures, etc.) |

## Adding a Module

1. Create `suites/<group>/<module_name>/` with `__init__.py` and `module.py`
2. Optionally add `config.json` and `deps.py`
3. The module is automatically discovered — no registration needed

```python
# suites/mygroup/mymodule/module.py
from harness.module_base import BaseModule

class MyModule(BaseModule):
    id = "mygroup.mymodule"
    category = "mygroup"
    description = "My test module"
    labels = ("functional",)
    timeout = 300

    def run(self, ctx):
        # ctx.target: server/CLI/mount helpers from env/
        # ctx.deps: dependency manager
        # ctx.capabilities: platform capability dict
        ...
        return {"result": "ok"}
```

## Reports

Blackbox generates two levels of report:

1. **Module report** (`results/<session>/<group>/<module>/report.md` + `report.json`)
2. **Suite report** (`results/<session>/report.md` + `suite-report.json`)

## Platform Compatibility

Each module's `config.json` can declare platform expectations:

```json
{
  "compat": { "linux": "run", "darwin": "skip" }
}
```

Values: `"run"` (default), `"skip"`, or `"xfail"`.

## Module Timeout

Each module has a wall-clock timeout. Priority:

1. `BLACKBOX_<MODULE_ID>_TIMEOUT_S` env var (module ID uppercased, dots → underscores)
2. `timeout` field in `config.json`
3. Module class `timeout` attribute (default 600s)

## Work-Dir Isolation

All writable state (cache, tmp, results, GOCACHE/GOMODCACHE) lives under a
work-dir, keeping the repo tree clean. Use `--work-dir` to specify; defaults to
`blackbox/work/<session>/`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLACKBOX_WORK_DIR` | `blackbox/work/<session>` | Work directory for cache/tmp/results |
| `BLACKBOX_RUNS` | `1` | Performance run count |
| `SERVER_MODE` | `auto` | Server mode: `auto`, `existing`, `local` |
| `OFFLINE` | `false` | Disable auto-fetch of dependencies |
| `STRICT` | `false` | Fail on missing prerequisites instead of skipping |
| `KEEP_ARTIFACTS` | `false` | Keep module artifacts after run |