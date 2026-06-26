# Drive9 Blackbox Harness

`blackbox/` contains the shared Drive9 blackbox test harness. The harness is
split into three layers:

- **`harness/`** — generic test framework (runner, reporter, module protocol).
  No drive9-specific dependencies.
- **`env/`** — drive9 test environment (server startup, CLI build, FUSE mount,
  dependency management, capability detection).
- **`suites/`** — test modules organized by group and module name:
  `suites/<group>/<module>/module.py`.

## Layout

```text
blackbox/
  run.py                 generic entrypoint
  harness/
    core.py             shared constants, Context, Recorder, Module protocol
    module_base.py      BaseModule class, module_config helper
    runner.py           BlackboxRunner: module execution, timeout, reporting
    report.py           three-tier report templates (module/suite/overall)
    suite.py            auto-discovery: scan suites/*/*/module.py
    deps.py             DependencyManager: git clone, tool detection
    capabilities.py    host capability detection (OS, tools, features)
  env/
    provider.py         Drive9SuiteProvider: setup/cleanup lifecycle
    target.py           Drive9 CLI/server/FUSE mount provider
    capabilities.py    macFUSE/FUSE-T/Linux FUSE capability detection
    deps.py             Drive9DependencyManager: per-module tool builds
  suites/
    groups.json         group definitions (group name -> list of module IDs)
    community/          community test modules (pjdfstest, fio, ltp, etc.)
      pjdfstest/
        module.py       test logic
        config.json      timeout, compat, labels, module-specific settings
        deps.py          dependency preparation (optional)
        allowlist.json   test allowlist (optional)
      fio/
      ltp/
      ...
    drive9/             drive9 workflow modules
      git_fast_clone/
        module.py
        config.json
      ...
    customer/           customer scenario modules
    ported/             ported JuiceFS modules
    git/               git official test modules
```

## Auto-Discovery

Modules are discovered automatically by scanning `suites/<group>/<module>/module.py`.
Each module file must export a class with `id`, `category`, and `run()` attributes.

Module IDs are derived from the directory path: `community/pjdfstest` → `community.pjdfstest`.

Group definitions in `groups.json` map group names to module ID patterns.

## Commands

```bash
# Run all modules
python3 blackbox/run.py --all

# Run a single module
python3 blackbox/run.py --module community.pjdfstest

# Run a group
python3 blackbox/run.py --group perf

# Run by category prefix
python3 blackbox/run.py --category drive9.workflow

# List available modules
python3 blackbox/run.py --list --format json

# Bootstrap: prepare dependencies, then exit
python3 blackbox/run.py --all --bootstrap --work-dir /tmp/bb

# Reuse the work-dir for a real run
python3 blackbox/run.py --all --work-dir /tmp/bb
```

Makefile targets:

```bash
make blackbox
make blackbox BLACKBOX_SELECTOR=group:perf
make blackbox BLACKBOX_SELECTOR=module:community.pjdfstest
make blackbox BLACKBOX_WORK_DIR=/tmp/bb
make blackbox-bootstrap BLACKBOX_SELECTOR=module:community.lock BLACKBOX_WORK_DIR=/tmp/bb
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

## Reports

Blackbox generates three levels of report:

1. **Module report** (`results/<session>/<group>/<module>/report.md` + `report.json`)
2. **Suite report** (`results/<session>/report.md` + `suite-report.json`)
3. **Overall report** (cross-suite summary when running `--all-suites`)

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

1. `BLACKBOX_<MODULE>_TIMEOUT_S` environment variable (module ID uppercased, dots → underscores)
2. `timeout` field in `config.json`
3. Module class `timeout` attribute (default 600s)