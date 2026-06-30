# Git Official Modules

This directory holds the `git.official.*` blackbox modules — they run the
upstream [Git](https://github.com/git/git) project's own test suites with their
trash/scratch directories on a Drive9 FUSE mount, validating that Drive9 is a
compatible backend for real-world git workloads.

## Modules

Each subdirectory is one auto-discovered module (`module.py`) with a `deps.py`
that prepares the git source tree. The test scripts to run are declared as class
attributes on the module class (`tests` tuple).

| Module | Category | Description |
|---|---|---|
| `git.official.functional` | functional | Runs selected `t/tNNNN-*.sh` functional tests via `prove`. Trash directories are placed on the FUSE mount. |
| `git.official.perf` | performance | Runs selected `t/perf/pNNNN-*.sh` performance tests via `t/perf/run`. Scratch data is placed on the FUSE mount. |

## Test Scripts

The `tests` class attribute on each module lists the upstream test scripts to run.
Edit the tuple in `module.py` to widen or narrow coverage.

## Selection

```bash
python3 blackbox/run.py --group git
python3 blackbox/run.py --module git.official.functional
python3 blackbox/run.py --group git --label performance
```

## Dependencies

The harness fetches and builds the git source tree from
`https://github.com/git/git.git` (ref configurable via `GIT_TEST_REF`,
default `v2.49.0`). The built tree provides `bin-wrappers/git`,
`t/helper/test-tool`, and the test scripts.

Direct environment overrides (read without prefix):

```bash
GIT_TEST_SOURCE_DIR=/path/to/git          # use a pre-built source tree instead of fetching
GIT_TEST_BUILD_TIMEOUT_S=1800             # build timeout for the fetched tree
```

Tunables read via the harness `env_value` helper (accept a `BLACKBOX_` prefix):

```bash
GIT_TEST_REF=v2.49.0                      # git tag/branch to fetch and build
```

`prove` and `perl` are required system tools; the harness auto-installs them
via `apt-get` on Linux when `AUTO_INSTALL_SYSTEM_DEPS` is enabled.

## Notices / Third-party

The upstream Git test suite is licensed under GPL-2.0-only.
Source: https://github.com/git/git

The harness fetches and builds the git source at runtime; the built tree
(including its test scripts) retains its own license. No git source code is
vendored in this directory.