# Drive9 Blackbox Harness

`blackbox/` contains the Drive9 blackbox test framework, organized into two layers:

- **`harness/`** — generic test framework + drive9 test environment (server startup, CLI build, FUSE mount, dependency management).
- **`suites/`** — test modules organized by group and module name: `suites/<group>/<module>/module.py`.

## Layout

```text
blackbox/
  run.py                 entrypoint
  harness/
    core.py             Context, Recorder, ModuleRecord, constants
    module_base.py      BaseModule class
    runner.py           BlackboxRunner: execution, timeout, reporting
    report.py           report templates
    suite.py            auto-discovery: scan suites/*/*/module.py
    deps.py             DependencyManager: git clone, tool detection
    capabilities.py     host capability detection
    provider.py         Drive9SuiteProvider: setup/cleanup lifecycle
    target.py           server/CLI/FUSE mount provider
  suites/
    community/          community test modules
      pjdfstest/
        module.py       test logic
        deps.py          dependency preparation (optional)
      fio/
      ltp_fs/
      ltp_syscalls/
      ...
    drive9/             drive9 workflow modules
    customer/           customer scenario modules
    juicefs/            JuiceFS-inspired rewrite modules
    git/                git official test modules
```

## Auto-Discovery

Modules are discovered automatically by scanning `suites/<group>/<module>/module.py`.
Module IDs are derived from the directory path: `<group>.<module>`.
A single `module.py` can export multiple test classes (e.g., `ltp/module.py` had
`CommunityLTPFS` + `CommunityLTPSyscalls` — now split into separate `ltp_fs/`
and `ltp_syscalls/` directories).

`--group <name>` selects a directory group: every module whose id starts with
`<group>.`, mirroring the `suites/<group>/` layout. `--label <label>` is an
optional overlay filter (combinable with any selector) that narrows by module
labels.

## Commands

All commands use `python3 blackbox/run.py` directly. The default behavior uses
the `drive9` binary found on PATH and reads server/auth from `~/.drive9/config`
(via the CLI's own credential resolver). No CLI build, no server provisioning.

```bash
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
| `module.py` | Yes | Test logic: a class with `run(ctx) -> dict` |
| `deps.py` | No | `ensure_dependencies(ctx)` function for module-specific dependency preparation |

Module metadata (id, category, labels, timeout, compat) is declared as class
attributes on the module class in `module.py` — there is no external `config.json`
or `modules.json`.

## Adding a Module

1. Create `suites/<group>/<module_name>/` with `__init__.py` and `module.py`
2. Optionally add `deps.py`
3. The module is automatically discovered — no registration needed

```python
# suits/mygroup/mymodule/module.py
from harness.module_base import BaseModule

class MyModule(BaseModule):
    id = "mygroup.mymodule"
    category = "mygroup"
    description = "My test module"
    labels = ("functional",)
    timeout = 300

    def run(self, ctx):
        # ctx.target: server/CLI/mount helpers from harness/
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

Each module can declare platform expectations as a class attribute:

```python
class MyModule(BaseModule):
    compat = {"linux": "run", "darwin": "skip"}
```

Values: `"run"` (default), `"skip"`, or `"xfail"`.

## Module Timeout

Each module has a wall-clock timeout. Priority:

1. `BLACKBOX_<MODULE_ID>_TIMEOUT_S` env var (module ID uppercased, dots → underscores)
2. Module class `timeout` attribute (default 600s)

## Work-Dir Isolation

All writable state (cache, tmp, results, GOCACHE/GOMODCACHE) lives under a
work-dir, keeping the repo tree clean. Use `--work-dir` to specify; defaults to
`blackbox/work/<session>/`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BLACKBOX_WORK_DIR` | `blackbox/work/<session>` | Work directory for cache/tmp/results |
| `BLACKBOX_RUNS` | `1` | Performance run count |
| `SERVER_MODE` | `config` | Server mode: `config` or `local` |
| `OFFLINE` | `false` | Disable auto-fetch of dependencies |
| `STRICT` | `false` | Fail on missing prerequisites instead of skipping |
| `KEEP_ARTIFACTS` | `false` | Keep module artifacts after run |

## CI Workflows

Three GitHub Actions workflows are provided:

- **`blackbox.yml`** — manual trigger only (`workflow_dispatch`). User specifies
  a group (community, juicefs, drive9, git, customer, or all). No push/PR
  triggers.
- **`blackbox-daily.yml`** — runs at 06:00 UTC daily, community group only.
- **`blackbox-weekly.yml`** — runs at 06:00 UTC every Monday, community group only.

All three use `--server-mode local` to start an isolated MySQL container and
`drive9-server-local`, then run `python3 blackbox/run.py` directly.