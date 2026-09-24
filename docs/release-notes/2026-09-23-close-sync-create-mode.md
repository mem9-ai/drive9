# Close-sync inline create and mode

New nonempty inline files with deferred permissions can commit content and mode
in one remote transaction. This optimization requires the authenticated status
response to advertise `storage_capabilities.batch_write_mode_v1: true`.

The server implementation and its contract tests are maintained in
[tidbcloud/fs#189](https://github.com/tidbcloud/fs/pull/189). Deploy that server
change before expecting the performance improvement. Releasing this client
first is safe: absent, false or failed capability negotiation retains the
ordinary PUT-then-chmod path. The historical server source in `mem9-ai/drive9`
does not advertise this capability. Existing mounts cache status and may need
remounting after the server upgrade. A failed or malformed initial status fetch
also leaves the optimization disabled until status is successfully negotiated,
normally by remounting.

The advertised contract includes atomic content+mode create with create-only
CAS, owner-only mode authorization, per-item 403/revision 0 before any mutation,
and preserved committed revisions on post-commit errors. Scoped mounts still
commit authorized content and report a denied chmod. With perf enabled,
`close_sync_mode_forbidden_fallback` counts the extra request when a combined
create is rejected before mutation.

The background commit queue uses the same capability gate for batch mode fields.
Without support, it batches content only and applies pending permissions through
the ordinary chmod endpoint; a denied chmod remains an error and retains staging.
For active tenants, servers advertise this capability from the serving backend's
inline support, including DB-only db9 deployments. Non-active tenants use the provider default; capability support does not imply readiness. Multipart/S3 uploads and the durability contract are unchanged.
