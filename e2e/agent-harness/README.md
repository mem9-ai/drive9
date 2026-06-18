---
title: Drive9 Agent Harness
---

## Commands

1. Build:

   ```bash
   go build ./e2e/agent-harness/cmd/drive9-agent-harness
   ```

2. Preflight:

   ```bash
   drive9-agent-harness preflight --api-key "$DRIVE9_API_KEY"
   ```

3. Smoke:

   ```bash
   drive9-agent-harness run --suite smoke --api-key "$DRIVE9_API_KEY"
   ```

4. Targeted regression:

   ```bash
   drive9-agent-harness run --suite regression --case path-edge-strict,git-lock-strict,doctor-no-allow-other --api-key "$DRIVE9_API_KEY"
   ```

5. Stress:

   ```bash
   drive9-agent-harness run --suite stress --api-key "$DRIVE9_API_KEY"
   ```

6. Fault injection:

   ```bash
   drive9-agent-harness run --suite fault --allow-fault --api-key "$DRIVE9_API_KEY"
   ```

7. Regenerate report:

   ```bash
   drive9-agent-harness report --run-dir /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ
   ```

8. Generate a performance report:

   ```bash
   drive9-agent-harness report --run-dir /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ --format customer-perf
   ```

9. Verify EC2/EBS metadata in the performance report:

   ```bash
   jq '.instance_type,.storage_type,.storage_size,.storage_iops,.storage_throughput,.storage_encrypted' \
     /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ/perf/environment.json
   ```

   On EC2, the harness reads instance metadata from IMDS during `run`. If AWS CLI credentials are available, it also calls `ec2 describe-instances` and `ec2 describe-volumes` to fill root EBS type, size, IOPS, throughput, and encryption. For explicit credentials or cross-account runs, set `DRIVE9_PERF_AWS_PROFILE` and optionally `DRIVE9_PERF_AWS_REGION` before `run`.

10. Publish a performance report bundle:

   ```bash
   drive9-agent-harness publish-perf --run-dir /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ --workspace-root :/performance-reports
   ```

11. Garbage collect generated local mountpoints and remote roots:

   ```bash
   drive9-agent-harness gc --run-dir /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ --confirm-delete
   ```

12. Collect server evidence:

   ```bash
   drive9-agent-harness collect-server-evidence --run-dir /tmp/drive9-agent-test-YYYYMMDDTHHMMSSZ --approve-external --kube-context prod-dat9-eks-ap-southeast-1
   ```

## Phase 1 Contract

1. Each case gets a generated remote root under `--remote-root-base`.
2. The harness creates the remote root with `drive9 fs mkdir` before mounting.
3. The mount source is exactly the generated case root.
4. Known-bug cases keep correct-behavior oracles. Failing product oracles are non-gating and passing product oracles become fixed candidates.
5. Remote-root deletion is deferred. Local mountpoints are removed only for `cleanup: always` after verified unmount.

## Extended Harness

1. `stress.yaml` covers sequential `fio`, small-file storm, and parallel writer workloads.
2. `fault.yaml` covers open-file unmount and kill-during-write recovery classification.
3. `gc` refuses to delete without `--confirm-delete` and checks run gating when `--successful-only` is set.
4. `collect-server-evidence` refuses external reads without `--approve-external`.
5. `report --format customer-perf` renders a self-named Markdown report under `perf/`, for example `perf/drive9-performance-test-report.md`.
6. `publish-perf` uploads the report bundle and updates a Drive9 workspace index.
