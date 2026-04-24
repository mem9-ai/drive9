# Server-Mode Quota E2E Test Report

**Date:** 2026-04-22  
**Script:** `quota-server-e2e.sh`  
**Result:** PASS (17/17)

---

## Environment

| Component | Details |
|-----------|---------|
| Meta DB | MySQL 8.0 (`127.0.0.1:13306`) |
| Tenant DB | TiDB v8.5.6 (`127.0.0.1:14000`) |
| Server | `drive9-server-local` @ `127.0.0.1:19009` |
| S3 | Local directory `/tmp/drive9-quota-e2e-s3` |
| Quota Source | `server` (central) |
| Quota Limits | 1 MiB storage, 10 MiB max upload, 2 media LLM files |

---

## Test Results

### Test 1: Inline write 20 KiB (within quota)
- **Expected:** HTTP 200, file accepted
- **Actual:** HTTP 200
- **Status:** PASS

### Test 2: Inline large-file PUT 2 MiB (exceeds 1 MiB quota)
- **Expected:** HTTP 507 (Insufficient Storage)
- **Actual:** HTTP 507
- **Status:** PASS

### Test 3: Verify central quota counters after inline writes
- **Expected:** `storage_bytes=20480`, `reserved_bytes=0`, `media_file_count=0`
- **Actual:** `storage_bytes=20480`, `reserved_bytes=0`, `media_file_count=0`
- **Status:** PASS (3 assertions)

### Test 4: Overwrite with smaller file (negative delta)
- **Expected:** HTTP 200, `storage_bytes` reduced to 1024
- **Actual:** HTTP 200, `storage_bytes=1024`
- **Status:** PASS (2 assertions)

### Test 5: Upload image file increments media_file_count
- **Expected:** HTTP 200, `media_file_count=1`
- **Actual:** HTTP 200, `media_file_count=1`
- **Status:** PASS (2 assertions)

### Test 6: Initiate upload 512 KiB (within quota)
- **Expected:** HTTP 202, `reserved_bytes=524288`
- **Actual:** HTTP 202, `reserved_bytes=524288`
- **Status:** PASS (2 assertions)

### Test 7: Abort upload releases reserved bytes
- **Expected:** HTTP 200, `reserved_bytes=0`
- **Actual:** HTTP 200, `reserved_bytes=0`
- **Status:** PASS (2 assertions)

### Test 8: Initiate upload 2 MiB (exceeds quota)
- **Expected:** HTTP 507
- **Actual:** HTTP 507
- **Status:** PASS

### Test 9: Verify mutation log has recorded operations
- **Expected:** mutation log has entries
- **Actual:** 3 entries in `quota_mutation_log`
- **Status:** PASS

### Test 10: Backfill-quota CLI produces correct counters
- **Expected:** `storage_bytes=1044`, `media_file_count=1` (matches tenant DB)
- **Actual:** `storage_bytes=1044`, `media_file_count=1`
- **Status:** PASS (2 assertions)

---

## Summary

| Metric | Value |
|--------|-------|
| Total assertions | 17 |
| Passed | 17 |
| Failed | 0 |

All server-mode quota enforcement features validated successfully:
- Inline write quota checks (small and large files)
- Central `tenant_quota_usage` counter accuracy
- Media file counting (`media_file_count`)
- Negative-delta handling on overwrites
- Multipart upload reservation saga (reserve / abort release)
- Quota mutation log persistence
- Backfill CLI consistency with tenant DB
