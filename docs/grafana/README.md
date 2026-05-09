# Drive9 Grafana Dashboards

This directory uses a three-bucket layout instead of a single mega dashboard.

## 1. Overview dashboards

- `drive9-service-health-dashboard.json`: first-stop dashboard for service incidents, module availability, HTTP health, service error ratio, and coarse DB pressure.
- `drive9-async-fuse-dashboard.json`: first-stop dashboard for delayed background work and mount-path incidents, especially semantic worker backlog, image/audio extraction runtime, audio extract failures, and FUSE remote behavior.

## 2. Request-path drill-down dashboards

- `drive9-api-surface-dashboard.json`: non-FS and non-upload API request-path analysis for control, vault, SQL, events, and auxiliary HTTP surfaces.
- `drive9-filesystem-path-dashboard.json`: filesystem-path analysis that owns `/v1/fs/*`, correlates HTTP traffic with `fs_*` backend work, and now includes explicit WebDAV behavior.
- `drive9-upload-path-dashboard.json`: upload-path analysis that owns upload flows and correlates upload HTTP traffic, backend upload operations, and upload-related events.
- `drive9-upload-s3-path-dashboard.json`: upload plus raw object-store analysis for incidents that have narrowed from upload symptoms into `s3client` throughput, errors, or latency.

## 3. Data-store drill-down dashboards

- `drive9-metadata-db-dashboard.json`: metadata/control-plane DB analysis that owns `role="meta"`.
- `drive9-tenant-data-db-dashboard.json`: tenant data-path DB analysis that owns `role="user"`.

## Operating rule

Start from an overview dashboard. Only move into drill-down dashboards after the incident shape is clear.

Dashboard links are embedded in the overview and drill-down dashboards so the normal workflow is clickable inside Grafana rather than document-only.

We intentionally do not keep a single all-in-one observability dashboard anymore. The split is:

- Overview dashboards answer `is the system healthy and where is the blast radius?`
- Request-path dashboards answer `which API or business path is slow or failing?`
- Data-store dashboards answer `is the database the limiting factor?`