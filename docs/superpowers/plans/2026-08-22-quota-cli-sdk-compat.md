# Quota CLI and SDK Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with verification checkpoints.

**Goal:** Update the Drive9 Go SDK and CLI for the merged media/video LLM quota contract while centralizing quota request serialization and text rendering.

**Architecture:** Keep `pkg/client/quota.go` as the public quota model. Add the new limit/usage fields there, use one private client payload mapper for both quota endpoints, and use one CLI table formatter for admin list/get and quota set. Preserve existing exported fields, methods, endpoint paths, credentials, and server-owned spending-limit semantics.

**Tech Stack:** Go 1.25.1, `encoding/json`, existing client HTTP helpers, CLI parser/output helpers, Go tests, `gofmt`, golangci-lint.

## Global Constraints

- Do not modify server behavior, quota enforcement, free-organization limits, or extract-config APIs.
- Preserve `tidbcloud_spending_limit` input/output for dedicated tenants and do not reject it for shared tenants.
- Use pointer request fields so omitted LLM limits differ from explicit zero.
- Keep existing exported method signatures and JSON field names.
- Use `apply_patch` for source edits and run `gofmt` on changed Go files.

---

### Task 1: Extend and centralize the SDK quota model

**Files:** `pkg/client/quota.go`, `pkg/client/admin_tenants.go`, `pkg/client/quota_test.go`

**Interfaces:** `QuotaConfig` produces both LLM limits; `QuotaUsage` produces both extraction counters; `QuotaSetRequest` consumes optional `*int64` LLM limits; a private payload mapper produces the complete JSON field set for both quota endpoints.

- [ ] Add response-decoding assertions for `max_media_llm_files`, `max_video_llm_files`, `media_file_count`, and `video_file_count`.
- [ ] Add request tests for non-zero LLM values, explicit zero serialization, and omission when the pointer is nil.
- [ ] Add the two response fields to `QuotaConfig`, the two usage fields to `QuotaUsage`, and the two pointer fields to `QuotaSetRequest`.
- [ ] Replace the anonymous admin quota request body with one private payload type/helper beside the quota model. Preserve admin URL/credential placement and deprecated `/v1/quota` tenant-id behavior.
- [ ] Run `gofmt -w pkg/client/quota.go pkg/client/admin_tenants.go pkg/client/quota_test.go`.
- [ ] Run `go test ./pkg/client -run 'Test(GetQuota|SetQuota|AdminSetTenantQuota)' -count=1` and commit as `feat: support llm quota fields in go client`.

### Task 2: Centralize CLI quota parsing and rendering

**Files:** `cmd/drive9/cli/quota.go`, `cmd/drive9/cli/admin.go`, `cmd/drive9/visual_help.go`, `cmd/drive9/cli/quota_test.go`, `cmd/drive9/cli/admin_test.go`

**Interfaces:** `quotaSet` consumes `--max-media-llm-files` and `--max-video-llm-files` and produces `client.QuotaSetRequest` pointers; the shared formatter produces the same quota columns for admin list/get and quota set output.

- [ ] Add CLI tests for both flags, including explicit `0`, request forwarding, new output headers/cells, and help text.
- [ ] Parse both flags with `parseNonNegativeQuotaInt64Flag`, include them in the required-field check, and forward them to the SDK request.
- [ ] Extend the existing quota helpers with LLM limit/usage cells and refactor admin tenant tables plus quota response output to one column order and row writer. Preserve `-`, `unlimited`, byte formatting, and absent spending-limit output.
- [ ] Update regular and visual help with both flags and the server-side extract-config prerequisite.
- [ ] Run `gofmt -w cmd/drive9/cli/quota.go cmd/drive9/cli/admin.go cmd/drive9/visual_help.go cmd/drive9/cli/quota_test.go cmd/drive9/cli/admin_test.go`.
- [ ] Run `go test ./cmd/drive9/cli -run 'Test(Quota|Admin)' -count=1` and commit as `feat: expose llm quota controls in cli`.

### Task 3: Update documentation and verify delivery

**Files:** `docs/guides/quota.md`

- [ ] Document both LLM limits and both extraction usage counters in field tables, CLI examples, and JSON examples.
- [ ] State that matching tenant-specific extract config is required for LLM quota mutation, while shared spending-limit input is accepted and ignored and dedicated tenants retain it.
- [ ] Search quota-specific inventories in `cmd/drive9`, `docs`, `examples`, and `pkg/client`; update only stale user-facing field lists.
- [ ] Run `go test ./pkg/client -count=1`, `go test ./cmd/drive9/cli -count=1`, and `go test ./examples/go-sdk-cookbook -count=1`.
- [ ] Run `gofmt -l` on all changed Go files, `git diff --check`, and `make lint`.
- [ ] Commit documentation as `docs: describe llm quota cli contract`.
- [ ] Push `feat/quota-cli-sdk-compat`, create the PR, then inspect its head SHA, checks, review state, and changed-file list.
