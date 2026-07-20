# Drive9 Grafana Dashboards

This directory uses a usage-first layout plus focused incident drill-downs.

## Breaking metric migration

This dashboard set expects the `drive9_*` Prometheus namespace. The previous `dat9_*` metric names are intentionally not dual-emitted by Drive9 after this metrics contract rewrite. Existing external dashboards, alerts, and recording rules must be migrated from `dat9_*` to `drive9_*` at the same time as this server rollout.

`drive9_tenant_count` also changes its label contract. The old synthetic series such as `drive9_tenant_count{state="total_non_deleted"}` and `drive9_tenant_count{state="active"}` are not emitted. Use `drive9_tenant_count{status="<real status>"}` instead, where status is one of `pending`, `provisioning`, `active`, `failed`, `suspended`, `deleting`, or `deleted`. Dashboards or alerts that need "non-deleted" totals should aggregate the real statuses they want to include instead of depending on a synthetic metric state.

## 1. Usage dashboard

- `drive9-tenant-usage-dashboard.json`: first-stop dashboard for tenant-level product usage: active tenants, request frequency, in-flight requests, logical file reads/writes, HTTP transport bytes, storage/media quota state, latency, and non-OK rates by TiDB Cloud org/tenant/surface/action. Use this to answer `who is using Drive9, how much, and through which workflows?`

Tenant usage metrics intentionally allow `tenant_id` and `tidbcloud_org_id` as Prometheus labels, but keep high-cardinality values out of labels: no path, file ID, upload ID, API key ID, raw URL, user agent, or trace ID. When a tenant has no TiDB Cloud org binding, or a hot path cannot safely resolve the binding without extra work, `tidbcloud_org_id` is reported as `guest`.

## Tenant metric contract

- `drive9_tenant_count`: tenant count by real tenant `status`.
- `drive9_tenant_pool_bindings`: tenant-pool binding count by `pool_id`, `tidbcloud_org_id`, and binding `status=free|used`. The binding label is `status`, not `pool_status`.
- `drive9_tenant_requests_total`: request count by `tenant_id`, `tidbcloud_org_id`, `surface`, `action`, `result`, and `status_class`.
- `drive9_tenant_request_duration_seconds`: request latency histogram by `surface` and `status_class`.
- `drive9_tenant_inflight_requests`: current in-flight request gauge by `tenant_id`, `tidbcloud_org_id`, `surface`, and `action`.
- `drive9_tenant_http_bytes_total`: HTTP transport bytes by `tenant_id`, `tidbcloud_org_id`, `surface`, and `direction=request|response`.
- `drive9_tenant_file_bytes_total`: logical file bytes by `tenant_id`, `tidbcloud_org_id`, `surface`, `action`, and `direction=read|write`.
- `drive9_tenant_storage_bytes`: opportunistically published quota storage gauge by `tenant_id`, `tidbcloud_org_id`, and `state=confirmed|reserved|limit`.
- `drive9_tenant_media_files`: opportunistically published quota media-file gauge by `tenant_id`, `tidbcloud_org_id`, and `state=confirmed|limit`.

## 2. Service overview dashboards

- `drive9-service-health-dashboard.json`: first-stop dashboard for service incidents, module availability, HTTP health, service error ratio, and coarse DB pressure.
- `drive9-async-fuse-dashboard.json`: first-stop dashboard for delayed background work and mount-path incidents, especially semantic worker backlog, image/audio extraction runtime, audio extract failures, and FUSE remote behavior.

## 3. Request-path drill-down dashboards

- `drive9-api-surface-dashboard.json`: non-FS and non-upload API request-path analysis for control, vault, SQL, events, and auxiliary HTTP surfaces.
- `drive9-filesystem-path-dashboard.json`: filesystem-path analysis that owns `/v1/fs/*`, correlates HTTP traffic with `fs_*` backend work, and now includes explicit WebDAV behavior.
- `drive9-upload-path-dashboard.json`: upload-path analysis that owns upload flows and correlates upload HTTP traffic, backend upload operations, and upload-related events.
- `drive9-upload-s3-path-dashboard.json`: upload plus raw object-store analysis for incidents that have narrowed from upload symptoms into `s3client` throughput, errors, or latency.

## 4. Data-store drill-down dashboards

- `drive9-metadata-db-dashboard.json`: metadata/control-plane DB analysis that owns `role="meta"`.
- `drive9-tenant-data-db-dashboard.json`: tenant data-path DB analysis that owns `role="user"`.

## Operating rule

Start from `Drive9 Tenant Usage` for product/user questions and from `Drive9 Service Health` for incidents. Only move into request-path or DB drill-down dashboards after the shape is clear.

Dashboard links are embedded in the overview and drill-down dashboards so the normal workflow is clickable inside Grafana rather than document-only.

We intentionally do not keep a single all-in-one observability dashboard. The split is:

- Tenant usage answers `which tenants are active, what operations are they doing, and how much are they reading/writing?`
- Service overview dashboards answer `is the system healthy and where is the blast radius?`
- Request-path dashboards answer `which API or business path is slow or failing?`
- Data-store dashboards answer `is the database the limiting factor?`
