---
title: pkg/server - drive9 HTTP server
---

## Overview

HTTP server for `/v1/fs/*`, upload (V1/V2), SSE events, auth, background workers. Central type: `Server`. ~13 non-test Go files, ~10K lines.

## File Map

| File | Responsibility |
|---|---|
| `server.go` (3534 lines) | Config, Server struct, NewWithConfig, ServeHTTP, handleBusiness dispatch, all FS handlers, V1/V2 upload, provision, healthz, status, SQL, grep/find |
| `auth.go` | tenantAuthMiddleware, TenantScope, capabilityAuthMiddleware, ScopeFromContext |
| `fs_authorization.go` | FSScope, FSOp, authorizeFS, scoped-path matching |
| `instrumentation.go` | observe middleware (trace ID, metrics, logging), serverMetrics, /metrics |
| `sse.go`, `eventbus.go` | SSE streaming, EventBus ring-buffer pub/sub |
| `tokens.go` | Scoped token issue/revoke |
| `slock.go` | SSO OAuth login flow |
| `journal.go` | Journal CRUD + search handlers |
| `vault.go` | Secret vault management + capability-token read |
| `fork.go` | Tenant fork/branch provisioning |
| `semantic_worker.go` | Background semantic task polling (lease-based, per-tenant) |
| `object_gc_worker.go` | Periodic S3 orphan blob cleanup |

## Route Registration (3-layer dispatch)

1. **Mux prefix matching** — `http.ServeMux` routes `/v1/fs/`, `/v1/uploads`, `/v2/uploads/` etc. to `handleBusiness`. Unauthenticated routes (provision, healthz, metrics) registered directly on mux.
2. **handleBusiness switch** — dispatches on `r.URL.Path`: batch-stat, `/v1/fs/*` → handleFS, `/v1/uploads/initiate`, `/v2/uploads/*`, etc.
3. **handleFS method+query dispatch** — `GET ?stat` → handleStat, `GET ?list` → handleList, `POST ?copy` → handleCopy. Query modifiers are presence-based (`?list` ≡ `?list=1`).

## Auth Middleware

`tenantAuthMiddleware`: extract Bearer token → hash → resolve by hash in meta store → verify JWT (sig, tenant, version) → check tenant status → load FSScopes → pool.Acquire tenant backend → inject TenantScope into context. FS-scoped tokens additionally go through `authorizeFS(w, r, FSOpXxx, path)` per-operation. Scoped token admission gate (`isScopedBusinessRequestAllowed`) blocks non-FS endpoints.

## Handler Pattern

`func (s *Server) handleXxx(w http.ResponseWriter, r *http.Request, path string)`. Call `authorizeFS(w, r, FSOpXxx, path)` at top. Extract backend via `backendFromRequest(r)`. Write errors via `errJSON(w, code, msg)`. Write JSON responses with `encoding/json` + `Content-Type: application/json`.

## Background Workers

- **semanticWorkerManager** — lease-based task claiming, per-tenant concurrency control.
- **objectGCWorker** — periodic S3 orphan cleanup.
- **Pattern**: `Start(ctx)` → goroutine with ticker + `ctx.Done()` select → `Stop()` cancels + `wg.Wait()`.

## Conventions

- **Scoped tokens MUST NEVER reach chmod** — double-guarded in auth admission gate and handler body.
- **Upload V1 vs V2**: V1 presigns all parts upfront; V2 presigns on demand/batch (used by FUSE streaming write).
- **SSE self-filtering**: Events carry `actorID` so FUSE clients can skip their own mutations.

## Key Types

`Server`, `Config`, `TenantScope`, `SemanticWorkerOptions`, `FSScope`, `FSOp`, `EventBus`, `ChangeEvent`, `TenantStatusResponse`, `SlockOAuthClient`
