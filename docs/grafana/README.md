# Drive9 Grafana dashboards

Drive9 maintains exactly three core Grafana dashboards. Import the three JSON
files into the same Grafana folder and select the Prometheus datasource when
prompted.

## Drive9 Server Overview

Import `drive9-server-overview-dashboard.json` for server-wide incident triage.
It has only the Prometheus datasource variable and does not require an
organization or tenant selection.

The rows cover:

- **HTTP Service**: total request rate, P95 latency with P99 on the route-level
  HTTP overview, status mix, in-flight requests, and request-body read latency.
- **Server Runtime and Background Work**: module availability, mutation queue
  pressure, tenant outbox age, notification coalescing, FUSE remote work,
  service failures, and S3 client behavior.
- **Meta DB - Control Plane**: only `role="meta"`, including availability,
  operation outcomes and latency, connections, waits, and close reasons.
- **TiDB Cloud Control Plane and Tenant Lifecycle**: RBAC cache, TiDB Cloud
  OpenAPI, pool metadata resume waits, and fleet-wide lifecycle outcomes.

Tenant usage, quota, SSE, and tenant database details do not belong in Server
Overview. They are shown in Organization Overview or Tenant Overview at the
scope where their labels are valid.

## Drive9 Organization Overview

Import `drive9-organization-overview-dashboard.json` to inspect one TiDB Cloud
organization. It has a query-backed, single-select **TiDB Cloud Org ID**
dropdown populated from `drive9_tenant_pool_bindings`; it is not a textbox and
it excludes the synthetic `guest` organization.

An organization has one logical tenant pool, so there is no pool variable. The
dashboard is organized as:

- **Organization Summary**: current used tenants, net tenant change, free
  reserve slots, reserve depletion, total storage usage, storage usage ratio,
  request rate, 5xx ratio, and compact organization-wide trends.
- **Pool Capacity and Supply**: free tenant bindings, configured reserve target,
  metadata resume waits, and shared physical pool counts by lifecycle status.
- **Organization Database Resources**: organization-scoped dedicated
  `role="user"` handles and shared `role="shared"` / `role="shared_schema"`
  handles. Tenant DB operation latency is not shown because its histogram does
  not carry organization labels.
- **Organization Usage and Request Experience**: organization aggregates for
  in-flight work, logical file I/O, SSE, and `fs_events` rows, plus a bounded
  top-20 tenant request view.
- **Quota, Storage and Content**: organization totals plus bounded top-20 tenant
  comparisons for storage, media files, video files, and published limits.
- **Failures and Recovery**: aggregate tenant-attributed failures and SSE reset
  reasons, bounded top-20 failure/backlog views, shared-pool cache state, and
  organization-attributed lifecycle outcomes.

The tenant dashboard link carries the selected organization into Tenant
Overview. Enter the exact tenant ID after opening the tenant dashboard.

Organization Overview does not return unbounded series grouped by
`tenant_id`. Tenant-ranked panels use `topk(20)`; all other panels aggregate by
low-cardinality dimensions such as `surface`, `direction`, `reason`, `state`,
or `operation`. Exact tenant detail belongs in Tenant Overview.

## Drive9 Tenant Overview

Import `drive9-tenant-overview-dashboard.json` for one exact tenant. Select the
organization first, then replace the `select-a-tenant` placeholder in the
**Tenant ID** textbox with the exact tenant ID. Panels intentionally stay empty
while the placeholder is present. The textbox does not run a tenant-inventory
query, so it remains usable when a large tenant series set makes a query-backed
dropdown slow or incomplete. It is an exact ID input, not a regular-expression
filter.

Every tenant-scoped PromQL expression uses both
`tidbcloud_org_id="$org"` and `tenant_id="$tenant"`. Its rows cover:

- **Tenant Summary**: request health, in-flight work, storage, SSE, and dedicated
  DB connection state at a glance.
- **Requests and File I/O**: request surfaces/actions, logical read/write bytes,
  and per-surface in-flight work.
- **Storage, Quota and Content**: confirmed/reserved/limit storage and media or
  video file counts.
- **Events and SSE**: connections, replay/reset latency, delivered events,
  reset reasons, and `fs_events` row count.
- **Dedicated Tenant Database**: exact `role="user"` pool registration,
  connections, waits, and closes.
- **Failures and Recovery**: tenant-attributed operation failures, quota-related
  failures, mutation replay state, and attributable lifecycle outcomes.

`native_shared` tenants use an organization-scoped physical database pool and
do not emit an exact tenant-specific DB pool. Their **Dedicated Tenant
Database** row therefore shows no data; the physical shared-pool state remains
in Organization Overview.

## Metric semantics used by the dashboards

### Organization tenant count and change

`drive9_tenant_pool_bindings` is a gauge with labels `pool_id`,
`tidbcloud_org_id`, and `status`. Current used tenants for an organization are:

The dashboards intentionally do not display `drive9_tenant_count`. That metric
has only a server-global `status` label, so it cannot answer an organization
question. It is also written by the current leader without being cleared when
leadership is lost, so multi-replica `sum` or `max` aggregation can preserve an
overlapping or stale snapshot. Organization Overview therefore uses
`drive9_tenant_pool_bindings` for organization-scoped tenant counts.

```promql
sum(drive9_tenant_pool_bindings{tidbcloud_org_id="$org",status="used"})
```

The dashboard calculates **Net Used Tenant Change** with `delta()` over the
selected Grafana time range. This is a gauge delta, not a creation counter. A
negative result may represent tenant release, deletion, or reconciliation.

### Pool reserve depletion

`drive9_tenant_pool_capacity` has labels `pool_id`, `organization_id`, and
`state`. `state="size"` is the configured target number of free reserve slots;
it is not the maximum number of tenants the organization may own. This gauge is
an event-driven snapshot refreshed by pool admin/replenish activity rather than
the minute-by-minute binding-count observer, so it can be temporarily absent
after a server restart until pool activity refreshes it.

The old `used bindings / size` interpretation could produce values such as
969% because it divided the total used tenant count by a reserve target. The
dashboards instead use:

```promql
clamp_max(
  clamp_min(
    1
    - sum(drive9_tenant_pool_capacity{organization_id="$org",state="free"})
      / clamp_min(
          sum(drive9_tenant_pool_capacity{organization_id="$org",state="size"}),
          1
        ),
    0
  ),
  1
)
and
(
  sum(drive9_tenant_pool_capacity{organization_id="$org",state="size"}) > 0
)
```

This measures depletion of the configured reserve, stays within 0-100%, and
shows no data when no positive reserve target exists.

### Scope limitations

- `drive9_tenant_request_duration_seconds` intentionally has no organization or
  tenant labels, so Organization and Tenant Overview do not pretend to provide
  scoped request latency. `drive9_tenant_requests_total` always carries
  `tidbcloud_org_id`; data-plane requests and control-plane 4xx/5xx responses
  additionally carry `tenant_id`. Organization request panels therefore cover
  all organization-attributed requests, while Tenant Overview intentionally
  excludes successful control-plane requests that have no tenant label.
- `drive9_db_operations_total` and
  `drive9_db_operation_duration_seconds` have role/operation/result labels but
  no organization or tenant identity. Exact tenant DB connection statistics
  come from the DB pool metrics; tenant-specific DB operation latency is not
  available.
- Quota gauges are opportunistic snapshots. Missing limit or usage series stay
  as no data rather than being converted into a false zero.

## Validation performed for this dashboard set

The dashboard files use Grafana schema version 41. Validation is currently a
manual review procedure, not a repository script or CI target. Run the following
baseline commands from the repository root:

```bash
jq empty docs/grafana/*.json

expected="$(printf '%s\n' \
  docs/grafana/drive9-organization-overview-dashboard.json \
  docs/grafana/drive9-server-overview-dashboard.json \
  docs/grafana/drive9-tenant-overview-dashboard.json)"
actual="$(rg --files docs/grafana -g '*.json' | sort)"
test "$actual" = "$expected"

for dashboard in docs/grafana/*.json; do
  jq -e '
    ([.panels[].id] | length) == ([.panels[].id] | unique | length)
    and all(.panels[];
      .gridPos.x >= 0 and .gridPos.y >= 0
      and .gridPos.w > 0 and .gridPos.h > 0
      and (.gridPos.x + .gridPos.w <= 24))
  ' "$dashboard"
done

rg -o --no-filename 'drive9_[A-Za-z0-9_]+' docs/grafana/*.json | sort -u
rg -n 'drive9_[A-Za-z0-9_]+' pkg/metrics
git diff --check -- docs/grafana
```

The two `rg` outputs are compared manually to confirm that referenced Prometheus
metrics exist in `pkg/metrics`; histogram `_bucket`, `_count`, and `_sum` series
come from the declared histogram base name. Reviewers also inspect panel grids
for overlap, resolve every dashboard variable reference, verify selector labels
against the metric declarations, and confirm that only the three supported JSON
files remain. Do not assume these checks run automatically.

These static checks do not prove that a deployed Prometheus currently has
samples for every metric. After import, select the datasource and a time range
that includes recent Drive9 activity.
