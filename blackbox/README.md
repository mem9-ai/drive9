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
    groups.json         group definitions (group name → list of module IDs)
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
    ported/             ported JuiceFS modules
    git/                git official test modules
```

## Auto-Discovery

Modules are discovered automatically by scanning `suites/<group>/<module>/module.py`.
Each module file must export a class with `id`, `category`, and `run()` attributes.

Module IDs are derived from the class's `id` attribute (not directory path).
A single `module.py` can export multiple test classes (e.g., `ltp/module.py` has
`CommunityLTPFS` + `CommunityLTPSyscalls`).

Group definitions in `suites/groups.json` map group names to module ID patterns.

## Commands

All commands use `python3 blackbox/run.py` directly. `--drive9-cli` is optional
and defaults to `drive9` found on the system PATH.

```bash
# List available modules (no side effects, no work-dir created)
python3 blackbox/run.py --list
python3 blackbox/run.py --list --format json

# Run all modules
python3 blackbox/run.py --all

# Run a single module
python3 blackbox/run.py --module community.pjdfstest

# Run a group
python3 blackbox/run.py --group perf

# Run by category prefix
python3 blackbox/run.py --category drive9.workflow

# Specify a custom drive9 CLI path
python3 blackbox/run.py --all --drive9-cli ./bin/drive9

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

# Custom server mode
python3 blackbox/run.py --all --server-mode existing
```

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