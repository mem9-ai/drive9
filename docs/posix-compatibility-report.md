# Drive9 POSIX Compatibility Report

## Summary

| Suite | Cases | PASS/ALL |
|---|---:|---:|
| pjdfstest | 238 files / 8,798 tests | 8798/8798 |
| LTP filesystem | 18 | 18/18 |
| LTP syscalls | 117 | 117/117 |
| POSIX flock | 1 | 1/1 |
| pyxattr | 4 | 4/4 |
| fsx | 1 | 1/1 |
| fio | 3 | 3/3 |
| mdtest | 1 | 1/1 |
| **Total** | **8,943** | **8943/8943** |

### pjdfstest by directory

| Directory | Files | Tests | PASS/ALL |
|---|---:|---:|---:|
| `chflags/` | 14 | 14 | 14/14 |
| `chmod/` | 13 | 327 | 327/327 |
| `chown/` | 11 | 1,497 | 1497/1497 |
| `ftruncate/` | 15 | 89 | 89/89 |
| `granular/` | 7 | 7 | 7/7 |
| `link/` | 18 | 359 | 359/359 |
| `mkdir/` | 13 | 118 | 118/118 |
| `mkfifo/` | 13 | 120 | 120/120 |
| `mknod/` | 12 | 186 | 186/186 |
| `open/` | 27 | 337 | 337/337 |
| `posix_fallocate/` | 1 | 1 | 1/1 |
| `rename/` | 25 | 4,857 | 4857/4857 |
| `rmdir/` | 16 | 145 | 145/145 |
| `symlink/` | 13 | 95 | 95/95 |
| `truncate/` | 15 | 84 | 84/84 |
| `unlink/` | 15 | 440 | 440/440 |
| `utimensat/` | 10 | 122 | 122/122 |
| **Total** | **238** | **8,798** | **8798/8798** |

### LTP syscalls by family

| Family | Cases | PASS/ALL |
|---|---:|---:|
| `access` | 4 | 4/4 |
| `chmod` | 6 | 6/6 |
| `chown` | 5 | 5/5 |
| `close` | 2 | 2/2 |
| `ftruncate` | 6 | 6/6 |
| `getcwd` | 4 | 4/4 |
| `getdents` | 2 | 2/2 |
| `getxattr` | 4 | 4/4 |
| `link` | 4 | 4/4 |
| `listxattr` | 4 | 4/4 |
| `lseek` | 4 | 4/4 |
| `lstat` | 6 | 6/6 |
| `mkdir` | 5 | 5/5 |
| `open` | 12 | 12/12 |
| `read` | 4 | 4/4 |
| `rename` | 14 | 14/14 |
| `rmdir` | 3 | 3/3 |
| `setxattr` | 3 | 3/3 |
| `stat` | 8 | 8/8 |
| `symlink` | 3 | 3/3 |
| `truncate` | 4 | 4/4 |
| `unlink` | 4 | 4/4 |
| `write` | 6 | 6/6 |
| **Total** | **117** | **117/117** |

## Details

### pjdfstest

| Case | Tests | PASS/ALL |
|---|---:|---:|
| `chflags/00.t` | 1 | 1/1 |
| `chflags/01.t` | 1 | 1/1 |
| `chflags/02.t` | 1 | 1/1 |
| `chflags/03.t` | 1 | 1/1 |
| `chflags/04.t` | 1 | 1/1 |
| `chflags/05.t` | 1 | 1/1 |
| `chflags/06.t` | 1 | 1/1 |
| `chflags/07.t` | 1 | 1/1 |
| `chflags/08.t` | 1 | 1/1 |
| `chflags/09.t` | 1 | 1/1 |
| `chflags/10.t` | 1 | 1/1 |
| `chflags/11.t` | 1 | 1/1 |
| `chflags/12.t` | 1 | 1/1 |
| `chflags/13.t` | 1 | 1/1 |
| `chmod/00.t` | 119 | 119/119 |
| `chmod/01.t` | 17 | 17/17 |
| `chmod/02.t` | 5 | 5/5 |
| `chmod/03.t` | 5 | 5/5 |
| `chmod/04.t` | 7 | 7/7 |
| `chmod/05.t` | 14 | 14/14 |
| `chmod/06.t` | 8 | 8/8 |
| `chmod/07.t` | 25 | 25/25 |
| `chmod/08.t` | 1 | 1/1 |
| `chmod/09.t` | 1 | 1/1 |
| `chmod/10.t` | 2 | 2/2 |
| `chmod/11.t` | 109 | 109/109 |
| `chmod/12.t` | 14 | 14/14 |
| `chown/00.t` | 1280 | 1280/1280 |
| `chown/01.t` | 22 | 22/22 |
| `chown/02.t` | 10 | 10/10 |
| `chown/03.t` | 10 | 10/10 |
| `chown/04.t` | 9 | 9/9 |
| `chown/05.t` | 18 | 18/18 |
| `chown/06.t` | 10 | 10/10 |
| `chown/07.t` | 132 | 132/132 |
| `chown/08.t` | 1 | 1/1 |
| `chown/09.t` | 1 | 1/1 |
| `chown/10.t` | 4 | 4/4 |
| `ftruncate/00.t` | 26 | 26/26 |
| `ftruncate/01.t` | 5 | 5/5 |
| `ftruncate/02.t` | 5 | 5/5 |
| `ftruncate/03.t` | 5 | 5/5 |
| `ftruncate/04.t` | 4 | 4/4 |
| `ftruncate/05.t` | 15 | 15/15 |
| `ftruncate/06.t` | 8 | 8/8 |
| `ftruncate/07.t` | 6 | 6/6 |
| `ftruncate/08.t` | 1 | 1/1 |
| `ftruncate/09.t` | 3 | 3/3 |
| `ftruncate/10.t` | 1 | 1/1 |
| `ftruncate/11.t` | 1 | 1/1 |
| `ftruncate/12.t` | 3 | 3/3 |
| `ftruncate/13.t` | 4 | 4/4 |
| `ftruncate/14.t` | 2 | 2/2 |
| `granular/00.t` | 1 | 1/1 |
| `granular/01.t` | 1 | 1/1 |
| `granular/02.t` | 1 | 1/1 |
| `granular/03.t` | 1 | 1/1 |
| `granular/04.t` | 1 | 1/1 |
| `granular/05.t` | 1 | 1/1 |
| `granular/06.t` | 1 | 1/1 |
| `link/00.t` | 202 | 202/202 |
| `link/01.t` | 32 | 32/32 |
| `link/02.t` | 10 | 10/10 |
| `link/03.t` | 13 | 13/13 |
| `link/04.t` | 6 | 6/6 |
| `link/05.t` | 1 | 1/1 |
| `link/06.t` | 18 | 18/18 |
| `link/07.t` | 17 | 17/17 |
| `link/08.t` | 10 | 10/10 |
| `link/09.t` | 5 | 5/5 |
| `link/10.t` | 23 | 23/23 |
| `link/11.t` | 9 | 9/9 |
| `link/12.t` | 1 | 1/1 |
| `link/13.t` | 1 | 1/1 |
| `link/14.t` | 1 | 1/1 |
| `link/15.t` | 1 | 1/1 |
| `link/16.t` | 1 | 1/1 |
| `link/17.t` | 8 | 8/8 |
| `mkdir/00.t` | 36 | 36/36 |
| `mkdir/01.t` | 17 | 17/17 |
| `mkdir/02.t` | 3 | 3/3 |
| `mkdir/03.t` | 3 | 3/3 |
| `mkdir/04.t` | 3 | 3/3 |
| `mkdir/05.t` | 12 | 12/12 |
| `mkdir/06.t` | 12 | 12/12 |
| `mkdir/07.t` | 6 | 6/6 |
| `mkdir/08.t` | 1 | 1/1 |
| `mkdir/09.t` | 1 | 1/1 |
| `mkdir/10.t` | 21 | 21/21 |
| `mkdir/11.t` | 1 | 1/1 |
| `mkdir/12.t` | 2 | 2/2 |
| `mkfifo/00.t` | 36 | 36/36 |
| `mkfifo/01.t` | 17 | 17/17 |
| `mkfifo/02.t` | 4 | 4/4 |
| `mkfifo/03.t` | 4 | 4/4 |
| `mkfifo/04.t` | 3 | 3/3 |
| `mkfifo/05.t` | 12 | 12/12 |
| `mkfifo/06.t` | 12 | 12/12 |
| `mkfifo/07.t` | 6 | 6/6 |
| `mkfifo/08.t` | 1 | 1/1 |
| `mkfifo/09.t` | 21 | 21/21 |
| `mkfifo/10.t` | 1 | 1/1 |
| `mkfifo/11.t` | 1 | 1/1 |
| `mkfifo/12.t` | 2 | 2/2 |
| `mknod/00.t` | 36 | 36/36 |
| `mknod/01.t` | 27 | 27/27 |
| `mknod/02.t` | 12 | 12/12 |
| `mknod/03.t` | 12 | 12/12 |
| `mknod/04.t` | 3 | 3/3 |
| `mknod/05.t` | 12 | 12/12 |
| `mknod/06.t` | 12 | 12/12 |
| `mknod/07.t` | 6 | 6/6 |
| `mknod/08.t` | 35 | 35/35 |
| `mknod/09.t` | 1 | 1/1 |
| `mknod/10.t` | 2 | 2/2 |
| `mknod/11.t` | 28 | 28/28 |
| `open/00.t` | 47 | 47/47 |
| `open/01.t` | 22 | 22/22 |
| `open/02.t` | 4 | 4/4 |
| `open/03.t` | 4 | 4/4 |
| `open/04.t` | 4 | 4/4 |
| `open/05.t` | 12 | 12/12 |
| `open/06.t` | 144 | 144/144 |
| `open/07.t` | 25 | 25/25 |
| `open/08.t` | 3 | 3/3 |
| `open/09.t` | 1 | 1/1 |
| `open/10.t` | 1 | 1/1 |
| `open/11.t` | 1 | 1/1 |
| `open/12.t` | 6 | 6/6 |
| `open/13.t` | 1 | 1/1 |
| `open/14.t` | 1 | 1/1 |
| `open/15.t` | 1 | 1/1 |
| `open/16.t` | 6 | 6/6 |
| `open/17.t` | 3 | 3/3 |
| `open/18.t` | 1 | 1/1 |
| `open/19.t` | 1 | 1/1 |
| `open/20.t` | 1 | 1/1 |
| `open/21.t` | 2 | 2/2 |
| `open/22.t` | 21 | 21/21 |
| `open/23.t` | 5 | 5/5 |
| `open/24.t` | 5 | 5/5 |
| `open/25.t` | 6 | 6/6 |
| `open/26.t` | 9 | 9/9 |
| `posix_fallocate/00.t` | 1 | 1/1 |
| `rename/00.t` | 122 | 122/122 |
| `rename/01.t` | 8 | 8/8 |
| `rename/02.t` | 6 | 6/6 |
| `rename/03.t` | 6 | 6/6 |
| `rename/04.t` | 18 | 18/18 |
| `rename/05.t` | 17 | 17/17 |
| `rename/06.t` | 1 | 1/1 |
| `rename/07.t` | 1 | 1/1 |
| `rename/08.t` | 1 | 1/1 |
| `rename/09.t` | 2353 | 2353/2353 |
| `rename/10.t` | 2099 | 2099/2099 |
| `rename/11.t` | 10 | 10/10 |
| `rename/12.t` | 32 | 32/32 |
| `rename/13.t` | 32 | 32/32 |
| `rename/14.t` | 32 | 32/32 |
| `rename/15.t` | 1 | 1/1 |
| `rename/16.t` | 1 | 1/1 |
| `rename/17.t` | 8 | 8/8 |
| `rename/18.t` | 6 | 6/6 |
| `rename/19.t` | 6 | 6/6 |
| `rename/20.t` | 25 | 25/25 |
| `rename/21.t` | 16 | 16/16 |
| `rename/22.t` | 1 | 1/1 |
| `rename/23.t` | 42 | 42/42 |
| `rename/24.t` | 13 | 13/13 |
| `rmdir/00.t` | 10 | 10/10 |
| `rmdir/01.t` | 14 | 14/14 |
| `rmdir/02.t` | 4 | 4/4 |
| `rmdir/03.t` | 5 | 5/5 |
| `rmdir/04.t` | 4 | 4/4 |
| `rmdir/05.t` | 6 | 6/6 |
| `rmdir/06.t` | 23 | 23/23 |
| `rmdir/07.t` | 10 | 10/10 |
| `rmdir/08.t` | 10 | 10/10 |
| `rmdir/09.t` | 1 | 1/1 |
| `rmdir/10.t` | 1 | 1/1 |
| `rmdir/11.t` | 47 | 47/47 |
| `rmdir/12.t` | 6 | 6/6 |
| `rmdir/13.t` | 1 | 1/1 |
| `rmdir/14.t` | 1 | 1/1 |
| `rmdir/15.t` | 2 | 2/2 |
| `symlink/00.t` | 14 | 14/14 |
| `symlink/01.t` | 5 | 5/5 |
| `symlink/02.t` | 7 | 7/7 |
| `symlink/03.t` | 6 | 6/6 |
| `symlink/04.t` | 3 | 3/3 |
| `symlink/05.t` | 12 | 12/12 |
| `symlink/06.t` | 12 | 12/12 |
| `symlink/07.t` | 6 | 6/6 |
| `symlink/08.t` | 21 | 21/21 |
| `symlink/09.t` | 1 | 1/1 |
| `symlink/10.t` | 1 | 1/1 |
| `symlink/11.t` | 1 | 1/1 |
| `symlink/12.t` | 6 | 6/6 |
| `truncate/00.t` | 21 | 21/21 |
| `truncate/01.t` | 5 | 5/5 |
| `truncate/02.t` | 5 | 5/5 |
| `truncate/03.t` | 5 | 5/5 |
| `truncate/04.t` | 4 | 4/4 |
| `truncate/05.t` | 15 | 15/15 |
| `truncate/06.t` | 8 | 8/8 |
| `truncate/07.t` | 6 | 6/6 |
| `truncate/08.t` | 1 | 1/1 |
| `truncate/09.t` | 3 | 3/3 |
| `truncate/10.t` | 1 | 1/1 |
| `truncate/11.t` | 1 | 1/1 |
| `truncate/12.t` | 3 | 3/3 |
| `truncate/13.t` | 4 | 4/4 |
| `truncate/14.t` | 2 | 2/2 |
| `unlink/00.t` | 112 | 112/112 |
| `unlink/01.t` | 5 | 5/5 |
| `unlink/02.t` | 4 | 4/4 |
| `unlink/03.t` | 4 | 4/4 |
| `unlink/04.t` | 4 | 4/4 |
| `unlink/05.t` | 10 | 10/10 |
| `unlink/06.t` | 10 | 10/10 |
| `unlink/07.t` | 6 | 6/6 |
| `unlink/08.t` | 3 | 3/3 |
| `unlink/09.t` | 1 | 1/1 |
| `unlink/10.t` | 1 | 1/1 |
| `unlink/11.t` | 270 | 270/270 |
| `unlink/12.t` | 1 | 1/1 |
| `unlink/13.t` | 2 | 2/2 |
| `unlink/14.t` | 7 | 7/7 |
| `utimensat/00.t` | 32 | 32/32 |
| `utimensat/01.t` | 7 | 7/7 |
| `utimensat/02.t` | 10 | 10/10 |
| `utimensat/03.t` | 1 | 1/1 |
| `utimensat/04.t` | 10 | 10/10 |
| `utimensat/05.t` | 16 | 16/16 |
| `utimensat/06.t` | 13 | 13/13 |
| `utimensat/07.t` | 17 | 17/17 |
| `utimensat/08.t` | 9 | 9/9 |
| `utimensat/09.t` | 7 | 7/7 |
| **Total** | **8,798** | **8798/8798** |

### LTP filesystem

| Case | PASS/ALL |
|---|---:|
| `fs_inod01` | 1/1 |
| `openfile01` | 1/1 |
| `inode01` | 1/1 |
| `stream01` | 1/1 |
| `stream02` | 1/1 |
| `stream03` | 1/1 |
| `stream04` | 1/1 |
| `stream05` | 1/1 |
| `ftest01` | 1/1 |
| `ftest02` | 1/1 |
| `ftest03` | 1/1 |
| `ftest05` | 1/1 |
| `ftest06` | 1/1 |
| `ftest07` | 1/1 |
| `lftest01` | 1/1 |
| `writetest01` | 1/1 |
| `fs_di` | 1/1 |
| `fs_racer` | 1/1 |
| **Total** | **18/18** |

### LTP syscalls

| Case | PASS/ALL |
|---|---:|
| `access01` | 1/1 |
| `access02` | 1/1 |
| `access03` | 1/1 |
| `access04` | 1/1 |
| `chmod01` | 1/1 |
| `chmod03` | 1/1 |
| `chmod05` | 1/1 |
| `chmod06` | 1/1 |
| `chmod07` | 1/1 |
| `chmod08` | 1/1 |
| `chown01` | 1/1 |
| `chown02` | 1/1 |
| `chown03` | 1/1 |
| `chown04` | 1/1 |
| `chown05` | 1/1 |
| `close01` | 1/1 |
| `close02` | 1/1 |
| `ftruncate01` | 1/1 |
| `ftruncate01_64` | 1/1 |
| `ftruncate03` | 1/1 |
| `ftruncate03_64` | 1/1 |
| `ftruncate04` | 1/1 |
| `ftruncate04_64` | 1/1 |
| `getcwd01` | 1/1 |
| `getcwd02` | 1/1 |
| `getcwd03` | 1/1 |
| `getcwd04` | 1/1 |
| `getdents01` | 1/1 |
| `getdents02` | 1/1 |
| `getxattr01` | 1/1 |
| `getxattr02` | 1/1 |
| `getxattr03` | 1/1 |
| `getxattr04` | 1/1 |
| `link02` | 1/1 |
| `link04` | 1/1 |
| `link05` | 1/1 |
| `link08` | 1/1 |
| `listxattr01` | 1/1 |
| `listxattr02` | 1/1 |
| `listxattr03` | 1/1 |
| `listxattr04` | 1/1 |
| `lseek01` | 1/1 |
| `lseek02` | 1/1 |
| `lseek07` | 1/1 |
| `lseek11` | 1/1 |
| `lstat01` | 1/1 |
| `lstat01_64` | 1/1 |
| `lstat02` | 1/1 |
| `lstat02_64` | 1/1 |
| `lstat03` | 1/1 |
| `lstat03_64` | 1/1 |
| `mkdir02` | 1/1 |
| `mkdir03` | 1/1 |
| `mkdir04` | 1/1 |
| `mkdir05` | 1/1 |
| `mkdir09` | 1/1 |
| `open01` | 1/1 |
| `open02` | 1/1 |
| `open03` | 1/1 |
| `open04` | 1/1 |
| `open06` | 1/1 |
| `open07` | 1/1 |
| `open08` | 1/1 |
| `open10` | 1/1 |
| `open11` | 1/1 |
| `open12` | 1/1 |
| `open13` | 1/1 |
| `open14` | 1/1 |
| `read01` | 1/1 |
| `read02` | 1/1 |
| `read03` | 1/1 |
| `read04` | 1/1 |
| `rename01` | 1/1 |
| `rename03` | 1/1 |
| `rename04` | 1/1 |
| `rename05` | 1/1 |
| `rename06` | 1/1 |
| `rename07` | 1/1 |
| `rename08` | 1/1 |
| `rename09` | 1/1 |
| `rename10` | 1/1 |
| `rename11` | 1/1 |
| `rename12` | 1/1 |
| `rename13` | 1/1 |
| `rename14` | 1/1 |
| `rename15` | 1/1 |
| `rmdir01` | 1/1 |
| `rmdir02` | 1/1 |
| `rmdir03` | 1/1 |
| `setxattr01` | 1/1 |
| `setxattr02` | 1/1 |
| `setxattr03` | 1/1 |
| `stat01` | 1/1 |
| `stat01_64` | 1/1 |
| `stat02` | 1/1 |
| `stat02_64` | 1/1 |
| `stat03` | 1/1 |
| `stat03_64` | 1/1 |
| `stat04` | 1/1 |
| `stat04_64` | 1/1 |
| `symlink02` | 1/1 |
| `symlink03` | 1/1 |
| `symlink04` | 1/1 |
| `truncate02` | 1/1 |
| `truncate02_64` | 1/1 |
| `truncate03` | 1/1 |
| `truncate03_64` | 1/1 |
| `unlink05` | 1/1 |
| `unlink07` | 1/1 |
| `unlink08` | 1/1 |
| `unlink10` | 1/1 |
| `write01` | 1/1 |
| `write02` | 1/1 |
| `write03` | 1/1 |
| `write04` | 1/1 |
| `write05` | 1/1 |
| `write06` | 1/1 |
| **Total** | **117/117** |

### POSIX flock

| Case | PASS/ALL |
|---|---:|
| Exclusive `fcntl.flock` (`LOCK_EX` + `LOCK_NB`) granted; second exclusive lock on another fd is blocked | 1/1 |

### pyxattr

| Case | PASS/ALL |
|---|---:|
| `os.setxattr` `user.drive9.blackbox` = `value` | 1/1 |
| `os.getxattr` returns `value` | 1/1 |
| `os.listxattr` includes `user.drive9.blackbox` | 1/1 |
| `os.removexattr` removes the attribute | 1/1 |
| **Total** | **4/4** |

### fsx

| Case | PASS/ALL |
|---|---:|
| Randomized file operations (`-N 5000`) | 1/1 |

### fio

| Case | PASS/ALL |
|---|---:|
| `seq_write` (`--rw=write --bs=1m`) | 1/1 |
| `seq_read` (`--rw=read --bs=1m`) | 1/1 |
| `rand_rw` (`--rw=randrw --bs=4k`) | 1/1 |
| **Total** | **3/3** |

### mdtest

| Case | PASS/ALL |
|---|---:|
| create / stat / remove (`-n 1000 -u -L -F`) | 1/1 |

