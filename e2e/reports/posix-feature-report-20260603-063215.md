# Drive9 POSIX pjdfstest Matrix Report

**Date:** 2026-06-03T06:39:46Z
**Suite:** `posix`
**Base:** `http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com`
**CLI source:** `build`
**Host:** `Linux-6.17.0-1017-aws-x86_64-with-glibc2.39`
**Strict unchecked mode:** `0`
**pjdfstest log:** `/home/ubuntu/drive9/e2e/reports/pjdfstest-20260603-063215.log`

## Summary

| Metric | Count |
|---|---:|
| Total cases | 8789 |
| Passed cases | 3510 |
| Failed cases | 5279 |
| Total files | 237 |
| Passed files | 129 |
| Failed files | 108 |
| Result | FAIL |

## Matrix

### pjdfstest/chflags

- [x] chflags/00.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/01.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/02.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/03.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/04.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/05.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/06.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/07.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/11.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/12.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chflags/13.t - PASS: Tests=1 Passed=1 Failed=0

### pjdfstest/chmod

- [ ] chmod/00.t - FAIL: Tests=119 Passed=48 Failed=71
- [ ] chmod/01.t - FAIL: Tests=17 Passed=5 Failed=12
- [ ] chmod/02.t - FAIL: Tests=5 Passed=4 Failed=1
- [ ] chmod/03.t - FAIL: Tests=5 Passed=1 Failed=4
- [x] chmod/04.t - PASS: Tests=7 Passed=7 Failed=0
- [ ] chmod/05.t - FAIL: Tests=14 Passed=8 Failed=6
- [x] chmod/06.t - PASS: Tests=8 Passed=8 Failed=0
- [ ] chmod/07.t - FAIL: Tests=25 Passed=5 Failed=20
- [x] chmod/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chmod/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chmod/10.t - PASS: Tests=2 Passed=2 Failed=0
- [ ] chmod/11.t - FAIL: Tests=109 Passed=42 Failed=67
- [ ] chmod/12.t - FAIL: Tests=14 Passed=11 Failed=3

### pjdfstest/chown

- [ ] chown/00.t - FAIL: Tests=1280 Passed=327 Failed=953
- [ ] chown/01.t - FAIL: Tests=22 Passed=6 Failed=16
- [ ] chown/02.t - FAIL: Tests=10 Passed=6 Failed=4
- [ ] chown/03.t - FAIL: Tests=10 Passed=2 Failed=8
- [x] chown/04.t - PASS: Tests=9 Passed=9 Failed=0
- [ ] chown/05.t - FAIL: Tests=18 Passed=9 Failed=9
- [x] chown/06.t - PASS: Tests=10 Passed=10 Failed=0
- [ ] chown/07.t - FAIL: Tests=132 Passed=20 Failed=112
- [x] chown/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chown/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] chown/10.t - PASS: Tests=4 Passed=4 Failed=0

### pjdfstest/ftruncate

- [ ] ftruncate/00.t - FAIL: Tests=26 Passed=22 Failed=4
- [x] ftruncate/01.t - PASS: Tests=5 Passed=5 Failed=0
- [ ] ftruncate/02.t - FAIL: Tests=5 Passed=2 Failed=3
- [ ] ftruncate/03.t - FAIL: Tests=5 Passed=1 Failed=4
- [x] ftruncate/04.t - PASS: Tests=4 Passed=4 Failed=0
- [ ] ftruncate/05.t - FAIL: Tests=15 Passed=8 Failed=7
- [x] ftruncate/06.t - PASS: Tests=8 Passed=8 Failed=0
- [x] ftruncate/07.t - PASS: Tests=6 Passed=6 Failed=0
- [x] ftruncate/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] ftruncate/09.t - PASS: Tests=3 Passed=3 Failed=0
- [x] ftruncate/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] ftruncate/11.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] ftruncate/12.t - FAIL: Tests=3 Passed=2 Failed=1
- [x] ftruncate/13.t - PASS: Tests=4 Passed=4 Failed=0
- [x] ftruncate/14.t - PASS: Tests=2 Passed=2 Failed=0

### pjdfstest/granular

- [x] granular/00.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/01.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/02.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/03.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/04.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/05.t - PASS: Tests=1 Passed=1 Failed=0
- [x] granular/06.t - PASS: Tests=1 Passed=1 Failed=0

### pjdfstest/link

- [ ] link/00.t - FAIL: Tests=202 Passed=56 Failed=146
- [ ] link/01.t - FAIL: Tests=32 Passed=8 Failed=24
- [ ] link/02.t - FAIL: Tests=10 Passed=4 Failed=6
- [ ] link/03.t - FAIL: Tests=13 Passed=1 Failed=12
- [x] link/04.t - PASS: Tests=6 Passed=6 Failed=0
- [x] link/05.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] link/06.t - FAIL: Tests=18 Passed=14 Failed=4
- [ ] link/07.t - FAIL: Tests=17 Passed=13 Failed=4
- [x] link/08.t - PASS: Tests=10 Passed=10 Failed=0
- [ ] link/09.t - FAIL: Tests=5 Passed=3 Failed=2
- [ ] link/10.t - FAIL: Tests=23 Passed=11 Failed=12
- [ ] link/11.t - FAIL: Tests=9 Passed=6 Failed=3
- [x] link/12.t - PASS: Tests=1 Passed=1 Failed=0
- [x] link/13.t - PASS: Tests=1 Passed=1 Failed=0
- [x] link/14.t - PASS: Tests=1 Passed=1 Failed=0
- [x] link/15.t - PASS: Tests=1 Passed=1 Failed=0
- [x] link/16.t - PASS: Tests=1 Passed=1 Failed=0
- [x] link/17.t - PASS: Tests=8 Passed=8 Failed=0

### pjdfstest/mkdir

- [ ] mkdir/00.t - FAIL: Tests=36 Passed=25 Failed=11
- [ ] mkdir/01.t - FAIL: Tests=17 Passed=5 Failed=12
- [ ] mkdir/02.t - FAIL: Tests=3 Passed=2 Failed=1
- [ ] mkdir/03.t - FAIL: Tests=3 Passed=1 Failed=2
- [x] mkdir/04.t - PASS: Tests=3 Passed=3 Failed=0
- [ ] mkdir/05.t - FAIL: Tests=12 Passed=8 Failed=4
- [ ] mkdir/06.t - FAIL: Tests=12 Passed=8 Failed=4
- [x] mkdir/07.t - PASS: Tests=6 Passed=6 Failed=0
- [x] mkdir/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] mkdir/09.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] mkdir/10.t - FAIL: Tests=21 Passed=10 Failed=11
- [x] mkdir/11.t - PASS: Tests=1 Passed=1 Failed=0
- [x] mkdir/12.t - PASS: Tests=2 Passed=2 Failed=0

### pjdfstest/mkfifo

- [ ] mkfifo/00.t - FAIL: Tests=36 Passed=5 Failed=31
- [ ] mkfifo/01.t - FAIL: Tests=17 Passed=5 Failed=12
- [ ] mkfifo/02.t - FAIL: Tests=4 Passed=0 Failed=4
- [ ] mkfifo/03.t - FAIL: Tests=4 Passed=1 Failed=3
- [x] mkfifo/04.t - PASS: Tests=3 Passed=3 Failed=0
- [ ] mkfifo/05.t - FAIL: Tests=12 Passed=8 Failed=4
- [ ] mkfifo/06.t - FAIL: Tests=12 Passed=8 Failed=4
- [x] mkfifo/07.t - PASS: Tests=6 Passed=6 Failed=0
- [x] mkfifo/08.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] mkfifo/09.t - FAIL: Tests=21 Passed=9 Failed=12
- [x] mkfifo/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] mkfifo/11.t - PASS: Tests=1 Passed=1 Failed=0
- [x] mkfifo/12.t - PASS: Tests=2 Passed=2 Failed=0

### pjdfstest/mknod

- [ ] mknod/00.t - FAIL: Tests=36 Passed=5 Failed=31
- [ ] mknod/01.t - FAIL: Tests=27 Passed=7 Failed=20
- [ ] mknod/02.t - FAIL: Tests=12 Passed=0 Failed=12
- [ ] mknod/03.t - FAIL: Tests=12 Passed=3 Failed=9
- [x] mknod/04.t - PASS: Tests=3 Passed=3 Failed=0
- [ ] mknod/05.t - FAIL: Tests=12 Passed=8 Failed=4
- [ ] mknod/06.t - FAIL: Tests=12 Passed=8 Failed=4
- [x] mknod/07.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] mknod/08.t - FAIL: Tests=35 Passed=15 Failed=20
- [x] mknod/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] mknod/10.t - PASS: Tests=2 Passed=2 Failed=0
- [ ] mknod/11.t - FAIL: Tests=28 Passed=4 Failed=24

### pjdfstest/open

- [ ] open/00.t - FAIL: Tests=47 Passed=36 Failed=11
- [ ] open/01.t - FAIL: Tests=22 Passed=6 Failed=16
- [ ] open/02.t - FAIL: Tests=4 Passed=3 Failed=1
- [ ] open/03.t - FAIL: Tests=4 Passed=1 Failed=3
- [x] open/04.t - PASS: Tests=4 Passed=4 Failed=0
- [ ] open/05.t - FAIL: Tests=12 Passed=8 Failed=4
- [ ] open/06.t - FAIL: Tests=144 Passed=66 Failed=78
- [ ] open/07.t - FAIL: Tests=25 Passed=12 Failed=13
- [x] open/08.t - PASS: Tests=3 Passed=3 Failed=0
- [x] open/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/11.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/12.t - PASS: Tests=6 Passed=6 Failed=0
- [x] open/13.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/14.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/15.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/16.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] open/17.t - FAIL: Tests=3 Passed=0 Failed=3
- [x] open/18.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/19.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/20.t - PASS: Tests=1 Passed=1 Failed=0
- [x] open/21.t - PASS: Tests=2 Passed=2 Failed=0
- [ ] open/22.t - FAIL: Tests=21 Passed=13 Failed=8
- [x] open/23.t - PASS: Tests=5 Passed=5 Failed=0
- [ ] open/24.t - FAIL: Tests=5 Passed=0 Failed=5
- [x] open/25.t - PASS: Tests=6 Passed=6 Failed=0

### pjdfstest/posix_fallocate

- [x] posix_fallocate/00.t - PASS: Tests=1 Passed=1 Failed=0

### pjdfstest/rename

- [ ] rename/00.t - FAIL: Tests=122 Passed=51 Failed=71
- [ ] rename/01.t - FAIL: Tests=8 Passed=6 Failed=2
- [ ] rename/02.t - FAIL: Tests=6 Passed=4 Failed=2
- [x] rename/03.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] rename/04.t - FAIL: Tests=18 Passed=14 Failed=4
- [ ] rename/05.t - FAIL: Tests=17 Passed=13 Failed=4
- [x] rename/06.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rename/07.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rename/08.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] rename/09.t - FAIL: Tests=2353 Passed=834 Failed=1519
- [ ] rename/10.t - FAIL: Tests=2099 Passed=825 Failed=1274
- [x] rename/11.t - PASS: Tests=10 Passed=10 Failed=0
- [ ] rename/12.t - FAIL: Tests=32 Passed=8 Failed=24
- [ ] rename/13.t - FAIL: Tests=32 Passed=6 Failed=26
- [ ] rename/14.t - FAIL: Tests=32 Passed=16 Failed=16
- [x] rename/15.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rename/16.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rename/17.t - PASS: Tests=8 Passed=8 Failed=0
- [x] rename/18.t - PASS: Tests=6 Passed=6 Failed=0
- [x] rename/19.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] rename/20.t - FAIL: Tests=25 Passed=10 Failed=15
- [ ] rename/21.t - FAIL: Tests=16 Passed=13 Failed=3
- [x] rename/22.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] rename/23.t - FAIL: Tests=42 Passed=6 Failed=36
- [ ] rename/24.t - FAIL: Tests=13 Passed=11 Failed=2

### pjdfstest/rmdir

- [ ] rmdir/00.t - FAIL: Tests=10 Passed=8 Failed=2
- [ ] rmdir/01.t - FAIL: Tests=14 Passed=11 Failed=3
- [ ] rmdir/02.t - FAIL: Tests=4 Passed=3 Failed=1
- [ ] rmdir/03.t - FAIL: Tests=5 Passed=2 Failed=3
- [x] rmdir/04.t - PASS: Tests=4 Passed=4 Failed=0
- [x] rmdir/05.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] rmdir/06.t - FAIL: Tests=23 Passed=5 Failed=18
- [ ] rmdir/07.t - FAIL: Tests=10 Passed=8 Failed=2
- [ ] rmdir/08.t - FAIL: Tests=10 Passed=8 Failed=2
- [x] rmdir/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rmdir/10.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] rmdir/11.t - FAIL: Tests=47 Passed=27 Failed=20
- [x] rmdir/12.t - PASS: Tests=6 Passed=6 Failed=0
- [x] rmdir/13.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rmdir/14.t - PASS: Tests=1 Passed=1 Failed=0
- [x] rmdir/15.t - PASS: Tests=2 Passed=2 Failed=0

### pjdfstest/symlink

- [ ] symlink/00.t - FAIL: Tests=14 Passed=12 Failed=2
- [x] symlink/01.t - PASS: Tests=5 Passed=5 Failed=0
- [ ] symlink/02.t - FAIL: Tests=7 Passed=6 Failed=1
- [ ] symlink/03.t - FAIL: Tests=6 Passed=4 Failed=2
- [x] symlink/04.t - PASS: Tests=3 Passed=3 Failed=0
- [ ] symlink/05.t - FAIL: Tests=12 Passed=8 Failed=4
- [ ] symlink/06.t - FAIL: Tests=12 Passed=8 Failed=4
- [x] symlink/07.t - PASS: Tests=6 Passed=6 Failed=0
- [ ] symlink/08.t - FAIL: Tests=21 Passed=13 Failed=8
- [x] symlink/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] symlink/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] symlink/11.t - PASS: Tests=1 Passed=1 Failed=0
- [x] symlink/12.t - PASS: Tests=6 Passed=6 Failed=0

### pjdfstest/truncate

- [ ] truncate/00.t - FAIL: Tests=21 Passed=11 Failed=10
- [x] truncate/01.t - PASS: Tests=5 Passed=5 Failed=0
- [ ] truncate/02.t - FAIL: Tests=5 Passed=2 Failed=3
- [ ] truncate/03.t - FAIL: Tests=5 Passed=1 Failed=4
- [x] truncate/04.t - PASS: Tests=4 Passed=4 Failed=0
- [ ] truncate/05.t - FAIL: Tests=15 Passed=8 Failed=7
- [x] truncate/06.t - PASS: Tests=8 Passed=8 Failed=0
- [x] truncate/07.t - PASS: Tests=6 Passed=6 Failed=0
- [x] truncate/08.t - PASS: Tests=1 Passed=1 Failed=0
- [x] truncate/09.t - PASS: Tests=3 Passed=3 Failed=0
- [x] truncate/10.t - PASS: Tests=1 Passed=1 Failed=0
- [x] truncate/11.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] truncate/12.t - FAIL: Tests=3 Passed=2 Failed=1
- [x] truncate/13.t - PASS: Tests=4 Passed=4 Failed=0
- [x] truncate/14.t - PASS: Tests=2 Passed=2 Failed=0

### pjdfstest/unlink

- [ ] unlink/00.t - FAIL: Tests=112 Passed=42 Failed=70
- [x] unlink/01.t - PASS: Tests=5 Passed=5 Failed=0
- [ ] unlink/02.t - FAIL: Tests=4 Passed=3 Failed=1
- [ ] unlink/03.t - FAIL: Tests=4 Passed=2 Failed=2
- [x] unlink/04.t - PASS: Tests=4 Passed=4 Failed=0
- [ ] unlink/05.t - FAIL: Tests=10 Passed=8 Failed=2
- [ ] unlink/06.t - FAIL: Tests=10 Passed=8 Failed=2
- [x] unlink/07.t - PASS: Tests=6 Passed=6 Failed=0
- [x] unlink/08.t - PASS: Tests=3 Passed=3 Failed=0
- [x] unlink/09.t - PASS: Tests=1 Passed=1 Failed=0
- [x] unlink/10.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] unlink/11.t - FAIL: Tests=270 Passed=108 Failed=162
- [x] unlink/12.t - PASS: Tests=1 Passed=1 Failed=0
- [x] unlink/13.t - PASS: Tests=2 Passed=2 Failed=0
- [ ] unlink/14.t - FAIL: Tests=7 Passed=5 Failed=2

### pjdfstest/utimensat

- [ ] utimensat/00.t - FAIL: Tests=32 Passed=10 Failed=22
- [x] utimensat/01.t - PASS: Tests=7 Passed=7 Failed=0
- [ ] utimensat/02.t - FAIL: Tests=10 Passed=8 Failed=2
- [x] utimensat/03.t - PASS: Tests=1 Passed=1 Failed=0
- [ ] utimensat/04.t - FAIL: Tests=10 Passed=8 Failed=2
- [ ] utimensat/05.t - FAIL: Tests=16 Passed=12 Failed=4
- [ ] utimensat/06.t - FAIL: Tests=13 Passed=10 Failed=3
- [ ] utimensat/07.t - FAIL: Tests=17 Passed=9 Failed=8
- [ ] utimensat/08.t - FAIL: Tests=9 Passed=7 Failed=2
- [ ] utimensat/09.t - FAIL: Tests=7 Passed=6 Failed=1
