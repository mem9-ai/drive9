---
title: Drive9 Space Workload Benchmark
---

`drive9-space-bench` provisions or reuses Drive9 spaces and continuously sends
verified write/read traffic, with optional deletes, to every selected space
through the HTTP API. It does not invoke the `drive9` executable or mount FUSE.

## Configuration file

The command optionally reads:

```text
~/.drive9/bench/config.json
```

The file is not created or rewritten automatically. Create it as a secret file
with exact mode `0600`:

```json
{
  "server": "https://drive9.example.com",
  "tidbcloud_public_key": "your-public-key",
  "tidbcloud_private_key": "your-private-key",
  "spaces": 500,
  "tidbcloud_spending_limit": 10000
}
```

```bash
mkdir -p ~/.drive9/bench
chmod 700 ~/.drive9/bench
chmod 600 ~/.drive9/bench/config.json
go run ./cmd/drive9-space-bench
```

Use `--config PATH` to select another file. A missing default file is ignored;
a missing explicitly selected file is an error. Unknown fields, malformed JSON,
non-regular files, and modes other than `0600` are rejected before any HTTP
request.

The built-in space count remains 500. A command-line value overrides both the
file and the default:

```bash
go run ./cmd/drive9-space-bench --spaces 800
```

Configuration precedence is:

```text
CLI flags > non-empty environment variables > config file > built-in defaults
```

The file intentionally supports only `server`, TiDB Cloud AK/SK, `spaces`, and
`tidbcloud_spending_limit`. Other workload and output controls remain flags.

## Environment variables and flags

Server and TiDB Cloud AK/SK can alternatively be provided through environment
variables:

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_PUBLIC_KEY=your-public-key
export DRIVE9_PRIVATE_KEY=your-private-key

go run ./cmd/drive9-space-bench \
  --spaces 500 \
  --tidbcloud-spending-limit 10000 \
  --out /tmp/drive9-space-bench.json
```

Flags override both the environment and config file:

```bash
go run ./cmd/drive9-space-bench \
  --server https://drive9.example.com \
  --tidbcloud-public-key "$TIDBCLOUD_AK" \
  --tidbcloud-private-key "$TIDBCLOUD_SK" \
  --spaces 500 \
  --tidbcloud-spending-limit 10000
```

The config file or environment variables are preferable to credential flags on
shared hosts because command-line arguments may be visible in process listings.
AK/SK are sent only when new spaces must be provisioned. When stored in the
user-managed config file, they are never copied to the generated space file,
report, or console.

## Reusing spaces

Space tenant IDs and API keys are stored by default in:

```text
~/.drive9/bench/spaces.json
```

Use `--spaces-file` to override the path. The file uses mode `0600`; its
automatically created directory uses mode `0700`. Treat the file as a secret.

On later runs, existing entries are reused. If the file contains fewer entries
than `--spaces`, the command provisions only the numeric shortfall. Existing
entries are never replaced automatically when readiness or authentication
fails, preventing transient failures from unexpectedly creating more spaces.
AK/SK are therefore optional when the file already contains the requested
number of spaces.

The server URL in the file must match `--server` or `DRIVE9_SERVER`. One space
file must not be used concurrently by multiple benchmark processes.

### Using a 15,000-Space selection from drive9-create-bench

`drive9-create-bench --sample-out` writes the same
`drive9-space-bench-spaces/v1` schema used by `--spaces-file`, so no conversion
is required:

```bash
drive9-space-bench \
  --server https://drive9.example.com \
  --spaces-file ~/.drive9/bench/spaces-15k.json \
  --spaces 15000 \
  --workers-per-space 1 \
  --space-start-rps 20 \
  --io-rps 2000 \
  --file-size 4096 \
  --report-interval 30s \
  --out ~/.drive9/bench/space-workload-report.json
```

This activates 20 new Spaces per second, starts one worker for each activated
Space, and caps the combined PUT, GET, and DELETE rate at 2,000 HTTP operations
per second. TiDB Cloud AK/SK are not needed when the selected file already
contains all 15,000 Spaces. One machine is sufficient when the goal is
continuous traffic rather than server saturation; adjust `--io-rps` to match
the load generator's CPU, network, and file descriptor capacity.

## Workload

After every selected Space reports `active`, Spaces enter the workload at
`--space-start-rps`. The first Space starts immediately, and all configured
workers for each later Space start together when it receives an activation
slot. The default value `0` starts all Spaces immediately, preserving the
original behavior. A worker rotates through a bounded file set and repeats:

1. Generate a deterministic payload.
2. `PUT /v1/fs/...`.
3. `GET /v1/fs/...`.
4. Verify the returned bytes exactly.
5. When `--delete-every N` is enabled, delete the verified file after every N
   successful write/read cycles.

The default run continues until `SIGINT` or `SIGTERM`. Useful controls include:

1. `--duration`: stop after a fixed workload duration.
2. `--workers-per-space`: increase per-space concurrency.
3. `--files-per-worker`: size of each worker's rotating file set.
4. `--file-size`: payload bytes, up to 1 MiB.
5. `--io-rps`: global filesystem request limit; zero is unlimited.
6. `--space-start-rps`: maximum new Spaces activated per second; zero starts
   all immediately.
7. `--delete-every`: deterministic DELETE cadence; zero disables DELETE.
8. `--report-interval`: periodic console progress interval.
9. `--provision-concurrency` and `--provision-rps`: space creation controls.

For example, `--delete-every 10` adds one DELETE after every ten successful
PUT/GET cycles. DELETE accounts for about 4.8% of total filesystem requests in
the no-error case. `--space-start-rps 20` takes about 8 minutes 20 seconds to
activate 10,000 Spaces. The command never deletes a Drive9 Space.

## Results

Periodic console output reports active and total Spaces, successful and failed
operations, verification errors, and aggregate operations per second. An
active Space has been enabled by the client; it does not mean the server
backend is confirmed warm. On exit, the command also prints separate write,
read, and DELETE latency summaries and histograms.

Progress remains on stdout. When write, read, or DELETE failures occur, stderr
receives at most one summary per operation per `--report-interval`. Each summary
includes new and cumulative error counts plus the latest tenant, worker, remote
path, and sanitized error message. Errors accumulated after the last interval
are flushed during shutdown. AK/SK and Space API keys are always redacted.

For a long-running process, keep progress and failures separate:

```bash
drive9-space-bench \
  >> ~/.drive9/bench/drive9-space-bench.stdout.log \
  2>> ~/.drive9/bench/drive9-space-bench.stderr.log
```

The JSON report defaults to
`~/.drive9/bench/latest-report.json` and can be changed with `--out`. Histogram
storage uses fixed buckets, so memory usage remains bounded during an unlimited
run. Reports contain aggregate error counts but not retained error samples, AK,
SK, or Space API keys.

Run `go run ./cmd/drive9-space-bench --help` for every flag and default.
