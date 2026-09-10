package main

// telemetryKnownFlagNames is every flag the CLI defines, in the normalized form
// telemetry reports (no leading dashes, lower case, no inline value).
//
// Flag names are the only argv-derived data telemetry sends, so this is a closed
// allowlist: a dash-prefixed token that is not one of these names is user input
// (a mistyped or unknown flag, or a value the command would reject) and must
// never be reported. Dropping an unknown name can only lose analytics, never
// leak a value.
//
// TestTelemetryKnownFlagNamesCoverEveryDefinedFlag re-derives this set from the
// source with go/ast and fails when a new flag is missing here.
var telemetryKnownFlagNames = map[string]struct{}{
	"access-key-id": {}, "account-id": {}, "actor": {}, "adopt": {},
	"adopt-worker-creation": {}, "adopt-worker-pid": {}, "after": {}, "agent": {},
	"alert-file": {}, "alert-webhook": {}, "allow": {}, "allow-other": {},
	"api-base": {}, "api-key": {}, "api-key-file": {}, "append": {},
	"append-log": {}, "attr-ttl": {}, "auth": {}, "b": {},
	"base-url": {}, "blobless": {}, "bucket": {}, "cache-dir": {},
	"cache-size": {}, "cascade": {}, "check": {}, "checkpoint": {},
	"clear-description": {}, "clear-tags": {}, "cloud-provider": {}, "color": {},
	"commit-queue-max-pending": {}, "credential-kind": {}, "cursor": {}, "d": {},
	"debug": {}, "description": {}, "detach": {}, "details": {},
	"dir-cache-max-entries": {}, "dir-ttl": {}, "direct-mount-strict": {}, "disk-read-cache-free-ratio": {},
	"disk-read-cache-size-mb": {}, "durability": {}, "enabled": {}, "endpoint": {},
	"entries": {}, "entry-ttl": {}, "env": {}, "exclude": {},
	"external-id": {}, "f": {}, "fast": {}, "file": {},
	"flat": {}, "flush-debounce": {}, "follow": {}, "force": {},
	"force-path-style": {}, "foreground": {}, "format": {}, "from": {},
	"from-file": {}, "fuse-sync-read": {}, "gvisor-compat": {}, "h": {},
	"health-failures": {}, "health-interval": {}, "health-timeout": {}, "help": {},
	"hydrate": {}, "id": {}, "idempotency-key": {}, "include": {},
	"include-quota": {}, "install": {}, "jobs": {}, "json": {},
	"json-array": {}, "k": {}, "kind": {}, "l": {},
	"label": {}, "layer": {}, "legacy-dir-stat-fallback": {}, "length": {},
	"limit": {}, "local-only": {}, "local-root": {}, "local-root-meta": {},
	"log": {}, "long": {}, "lookup-retry-count": {}, "lookup-retry-timeout": {},
	"m": {}, "manifest-url": {}, "max-file-count": {}, "max-file-size": {},
	"max-media-llm-files": {}, "max-restarts": {}, "max-session-ttl": {}, "max-storage-size": {},
	"max-video-llm-files": {}, "media-type": {}, "meta": {}, "mode": {},
	"model": {}, "mount": {}, "mount-kind": {}, "mountpoint": {},
	"name": {}, "namespace-id": {}, "newer": {}, "no-auto-pack": {},
	"no-auto-unpack": {}, "no-force-path-style": {}, "no-pager": {}, "no-replace": {},
	"no-supervise": {}, "o": {}, "offset": {}, "older": {},
	"output": {}, "pack": {}, "pack-path": {}, "pack-paths-json": {},
	"page": {}, "page-size": {}, "parallel-read-block-size-mb": {}, "parallel-read-concurrency": {},
	"perf-addr": {}, "perf-cpu-duration": {}, "perf-cpu-interval": {}, "perf-dir": {},
	"perf-heap-interval": {}, "perf-interval": {}, "perf-max-profile-files": {}, "perf-max-sample-files": {},
	"perf-max-samples": {}, "perm": {}, "plain": {}, "pool-size": {},
	"prefix": {}, "print": {}, "profile": {}, "prompt": {},
	"protocol": {}, "q": {}, "query": {}, "r": {},
	"read-cache-max-file-mb": {}, "read-cache-ttl": {}, "read-concurrency": {}, "read-only": {},
	"readdir-prefetch": {}, "readdir-prefetch-max-bytes": {}, "readdir-prefetch-max-file-bytes": {}, "readdir-prefetch-max-files": {},
	"readdir-prefetch-timeout": {}, "recursive": {}, "region": {}, "region-code": {},
	"remote-only": {}, "remote-root": {}, "reset": {}, "restart": {},
	"restart-backoff-max": {}, "restart-window": {}, "resume": {}, "reveal": {},
	"role-arn": {}, "s": {}, "sanitized-args-json": {}, "scheme": {},
	"scoped": {}, "secret": {}, "secret-access-key": {}, "server": {},
	"since": {}, "size": {}, "source": {}, "status": {},
	"stdout": {}, "stop-timeout": {}, "sts-endpoint": {}, "subject": {},
	"supervise-foreground": {}, "supervised": {}, "t": {}, "tag": {},
	"tenant-id": {}, "tidbcloud-private-key": {}, "tidbcloud-public-key": {}, "tidbcloud-spending-limit": {},
	"timeout": {}, "title": {}, "token-only": {}, "tree": {},
	"trust-process-local-events": {}, "ttl": {}, "type": {}, "unpack": {},
	"until": {}, "upload-concurrency": {}, "v": {}, "version": {},
	"wait": {}, "write-cache-free-ratio": {}, "write-cache-size-mb": {}, "writeback-batch-max-bytes": {},
	"writeback-batch-max-files": {}, "writeback-batch-window": {}, "y": {}, "yes": {},
}
