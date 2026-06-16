# Drive9 FUSE Blackbox Notices

This blackbox framework is Drive9 test code. It integrates or can fetch several
open source filesystem test suites and tools at runtime. Those dependencies
retain their own licenses and notices.

Current FUSE dependency metadata is tracked in
`blackbox/suites/fuse/dependencies.json`.

## JuiceFS-Inspired Tests

Modules under `ported.juicefs.*` are Drive9-owned equivalent rewrites inspired
by generic filesystem behaviors tested in the JuiceFS project, such as random
filesystem operations, random read/write verification, recursive remove, stress,
and cache consistency.

They are not vendored copies of JuiceFS source code. If future work ports or
copies actual JuiceFS test files, that module must preserve the original
copyright header and license notice in the copied file and this NOTICE must be
updated.

JuiceFS source: https://github.com/juicedata/juicefs
JuiceFS license: Apache-2.0

## Community Suites

- pjdfstest: https://github.com/pjd/pjdfstest, BSD-2-Clause
- Linux Test Project: https://github.com/linux-test-project/ltp, GPL family
- Git upstream tests: https://github.com/git/git, GPL-2.0-only
- secfs.test/fsx: https://github.com/billziss-gh/secfs.test, Apache-2.0

fio, mdtest/IOR, vdbench, Python xattr bindings, and platform tools may be
provided by the host environment or installed by CI. Their own distribution
licenses apply.
