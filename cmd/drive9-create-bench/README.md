---
title: Drive9 Create Benchmark
---

`drive9-create-bench` measures Drive9 tenant provisioning through
`POST /v1/provision` and, by default, polls `GET /v1/status` until each tenant
becomes active.

## Configuration

The command optionally reads the same secret config file as
`drive9-space-bench`:

```text
~/.drive9/bench/config.json
```

For example:

```json
{
  "server": "https://drive9.example.com",
  "tidbcloud_public_key": "your-public-key",
  "tidbcloud_private_key": "your-private-key"
}
```

The file must be a regular file with exact mode `0600`. A missing default file
is ignored; a missing explicitly selected `--config PATH` is an error.
Configuration precedence is:

```text
CLI flags > non-empty environment variables > config file > built-in defaults
```

The shared config's `spaces` and `tidbcloud_spending_limit` fields are accepted
but intentionally do not change a create benchmark. Use `--total` and
`--tidbcloud-spending-limit` explicitly.

Configuration inputs are:

1. `--config`: optional alternate config file.
2. `--server` or `DRIVE9_SERVER`: Drive9 server URL.
3. `DRIVE9_PUBLIC_KEY` and `DRIVE9_PRIVATE_KEY`: optional TiDB Cloud
   credentials; either set both or neither.
4. `--out`: required JSON report path.
5. `--total`, `--concurrency`, and `--rps`: workload size and rate.
6. `--wait-ready`, `--poll-interval`, and `--timeout`: readiness behavior.
7. `--tidbcloud-spending-limit`: optional limit when credentials are supplied.
8. `--inventory`: optional append-only JSONL credential inventory.
9. `--sample-size`, `--sample-seed`, and `--sample-out`: deterministic active
   Space selection for `drive9-space-bench`.
10. `--report-interval`: periodic aggregate progress logging.

## Usage

Anonymous provisioning:

```bash
go run ./cmd/drive9-create-bench \
  --server http://127.0.0.1:8080 \
  --total 10 \
  --concurrency 2 \
  --rps 1 \
  --out /tmp/drive9-create-bench.json
```

TiDB Cloud provisioning:

```bash
export DRIVE9_SERVER=https://drive9.example.com
export DRIVE9_PUBLIC_KEY=your-public-key
export DRIVE9_PRIVATE_KEY=your-private-key

go run ./cmd/drive9-create-bench \
  --total 10 \
  --concurrency 2 \
  --rps 1 \
  --tidbcloud-spending-limit 10 \
  --out /tmp/drive9-create-bench.json
```

Run `go run ./cmd/drive9-create-bench --help` for all flags. The benchmark creates
real tenants and deliberately does not delete them automatically.

## Creating 500,000 Spaces and selecting 15,000

Create the secret output directory first:

```bash
mkdir -p ~/.drive9/bench
chmod 700 ~/.drive9/bench
```

With server and credentials stored in the default config file, run:

```bash
drive9-create-bench \
  --total 500000 \
  --concurrency 100 \
  --rps 100 \
  --wait-ready=true \
  --tidbcloud-spending-limit 10000 \
  --inventory ~/.drive9/bench/spaces-500k.jsonl \
  --sample-size 15000 \
  --sample-seed drive9-500k-v1 \
  --sample-out ~/.drive9/bench/spaces-15k.json \
  --report-interval 30s \
  --out ~/.drive9/bench/drive9-create-bench-report.json
```

Tune `--concurrency` and `--rps` to the server's accepted provisioning rate.
Concurrency limits simultaneous provisioning/readiness flows; `--rps` limits
only `POST /v1/provision`.

The inventory is one JSON object per line and contains every accepted
`tenant_id` and Space API key. The selected JSON contains exactly 15,000
deterministically chosen active Spaces and is directly consumable by
`drive9-space-bench`.

Both credential files are created with mode `0600` and their parent directory
must use mode `0700`. Existing inventory or selected output files are refused
instead of overwritten. There is no crash-resume mode, so use new output paths
for a new run. Each accepted record is flushed to the inventory immediately for
live readers. The file is durably synced every 100 records and once more during
graceful shutdown. The inventory `created_at` field is the client-side time
immediately after the provision response was accepted.

## Sampling across multiple inventories

Stopping and restarting a benchmark creates a separate inventory for each run.
Use the `sample` subcommand to select Spaces across all of them:

```bash
drive9-create-bench sample \
  --inventory ~/.drive9/bench/drive9-create-runs/run-1/spaces.jsonl \
  --inventory ~/.drive9/bench/drive9-create-runs/run-2/spaces.jsonl \
  --sample-size 15000 \
  --sample-seed drive9-500k-v1 \
  --out ~/.drive9/bench/spaces-15k.json
```

`--inventory` is repeatable. The command streams every mode-`0600` inventory,
requires the same schema and Drive9 server in all inputs, and selects only active
Spaces. Exact duplicate tenant credentials are counted once; conflicting
credentials for the same tenant are rejected. The selection is deterministic
for a given seed and input set, regardless of input order.

The output uses the `drive9-space-bench-spaces/v1` schema and can be passed
directly to `drive9-space-bench --spaces-file`. Existing output files are never
overwritten. The sample command does not read the benchmark config, contact the
Drive9 server, or require TiDB Cloud credentials. For a final population-wide
sample, run it after all input inventories have stopped growing.

## Output

Stdout receives timestamped statistics every `--report-interval`. Each interval
reports window and cumulative success/failure counts plus exact provision and
ready P50/P90/P95/P99 latency. The latency window resets after each report so
server performance before and after a deployment can be compared directly.
Provision window latency includes every completed provision request, including
failed requests; ready latency includes successfully measured ready flows.

The final console summary includes separate provision and ready latency
histograms when those samples exist. The report contains fixed histogram
buckets, aggregate latency values, and at most 100 failure samples; it does not
retain raw latency samples or per-Space successes, so memory and report size
remain bounded for 500,000 Spaces.

Stderr records every failed provision or status HTTP attempt with a timestamp,
operation, tenant index, tenant ID when available, attempt number, HTTP status,
request latency, and sanitized error. Retried status failures remain logged
even if the Space later becomes active. Every tenant that ultimately fails also
gets a timestamped terminal error line with its statuses and measured
provision/ready durations.

The JSON report and console never contain TiDB Cloud credentials or Space API
keys. The inventory and selected snapshot intentionally contain Space API keys
and must be treated as secrets.
