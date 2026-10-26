# drive9-cpp

A standalone C++17 client for the existing drive9 / TiDB File System HTTP API.
It has no Go runtime, FUSE daemon, database dependency, or DuckDB dependency.

```sh
cmake -S clients/drive9-cpp -B build/drive9-cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/drive9-cpp --parallel
ctest --test-dir build/drive9-cpp --output-on-failure
```

Requires CMake 3.18+, a C++17 compiler, OpenSSL development libraries and threads.
On macOS, pass `-DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3` when necessary.
The build downloads checksum-pinned cpp-httplib 0.53.1 (the upstream HTTP library
also used by DuckDB), nlohmann/json 3.11.3 and utf8proc 2.10.0. Their license
notices remain in the downloaded sources. Nothing executes downloaded build
scripts except utf8proc's pinned CMake project. Consumers link `drive9::cpp`.

```cpp
#include <drive9/client.hpp>

auto client = std::make_shared<drive9::Client>(
    drive9::ClientOptions{"https://fs.example.com", token});
drive9::ExtentFileSystem fs(client);
drive9::OpenOptions options;
options.write = true;
options.create = true;
auto handle = fs.Open("/application.data", options);
fs.WriteAt(handle, data, length, 0); // acknowledged durable publication
fs.Sync(handle);
fs.Close(handle);
```

Set `OpenOptions::lock` to `Shared` or `Exclusive` for a whole-file advisory
record lock. These are the server's existing locks and interoperate with other
clients. Locks require cooperation from every application accessing a file.

## Capability audit

No new endpoint, schema, lock table or database-specific protocol is introduced.

| Client capability | Existing public API |
| --- | --- |
| Read / replace a classic file; Range and conditions | `GET` / `PUT /v1/fs/{path}` |
| Other classic operations, including patch plans | `Client::Request` / `JsonRequest` using the documented HTTP method and body |
| Lookup, attributes, create, directory listing, remove, rename | `POST /v1/extent/meta`, operation header |
| Session identity, heartbeat, expiry and stale cleanup | `nextSession`, `new_session`, `refresh_session`, `find_stale_sessions`, `clean_stale_session` |
| Transactional stale-writer rejection | `caller_sid` and the server's advertised `session_guard` capability |
| Shared/exclusive whole-file record locks | Existing `setlk`, portable `R` / `W` / `U` wire flags |
| Open-unlink and replaced-open-file lifetimes | Existing `open_ref` / `delete_sustained` on `jfs_sustained` |
| Random reads, sparse holes, overlapping writes, truncate | Existing inode and 64 MiB chunk slice maps |
| Immutable object bytes | Existing `POST /v1/data-credential`, then direct object GET/PUT |

`Client::ExtentMeta` exposes raw protocol operations without automatic retries.
The higher-level `ExtentFileSystem` binds one allocated session to every later
RPC. Inodes and session IDs remain exact 64-bit integers. It checks its confirmed
lease with a monotonic deadline before and after I/O. A lost/malformed metadata
acknowledgement, failed renewal or ambiguous write permanently fails that
filesystem instance; callers must reopen and recover rather than replay writes.
Short heartbeats require a server advertising transactional session guards.
Legacy servers remain usable with the default 12-second heartbeat, subject to
their weaker stale-writer protection.

Writes upload immutable 4 MiB blocks, then publish the existing slice metadata.
Inodes and slice IDs use the existing batched `incr_counter` reservations:
the returned value is the exclusive upper bound of the reserved range. This
allows C++, Go and other extent clients to alternate writes without reusing
an immutable object's ID. The client supplies its reserved inode to `mknod`.
`WriteAt` returns only after both are acknowledged; `Sync` waits behind preceding
writes. There is no background writeback. Shutdown waits for active reads and
writes, stops heartbeats, expires its own session and invokes normal server
cleanup. The server remains responsible for compaction and object reclamation.

S3 uses SigV4 and prefix-scoped credentials. GCS uses the existing scoped OAuth
token and JSON API endpoint. The same-host `file` mock is for development. Azure
is explicitly rejected, matching the prior public Go extent runtime's support
boundary. Unsupported compression/encryption formats are rejected before opening
files; existing format settings are never overwritten. Credential renewal is
bound to the original tenant, namespace, bucket, endpoint and encryption policy.
Control-plane bearer tokens are never attached to object requests. Redirects
are disabled and TLS peer verification is enabled. A checked patch to the pinned
HTTP header disables its implicit TLS reconnect/replay for mutation methods;
a close-notify-after-application test verifies one application, even when the
response is lost. Explicit private HTTP endpoints remain supported; use HTTPS
for bearer tokens across untrusted networks.

Omitted cloud endpoints resolve as in the existing SDK: GCS uses its standard
JSON API root, and a plain/global S3 endpoint follows the minted signing region.
Custom, dual-stack, FIPS and VPC endpoints are preserved. S3 partition DNS data
is generated from the checksum-pinned AWS SDK v2 v1.41.5 model, without linking
that SDK or running Go. Regenerate it with `tools/update_aws_partitions.py`;
its Apache license is retained under `third_party/`. Dotted S3 buckets use path
addressing over TLS. GCS bearer tokens require HTTPS except on loopback.

Concurrent positional reads share immutable blocks through a bounded 64 MiB
LRU cache and coalesce simultaneous downloads. Slice overlays are decoded in
`O(n log n)` time. Slice maps expire after one second; missing objects trigger one
mapping refresh for server compaction. Acquiring a file lock invalidates cached
maps and reloads authoritative attributes before returning ownership. File length
also follows this TTL, including unlocked append/truncate readers. Non-regular
files are rejected rather than treating symlinks as sparse regular data.

Empty extent directories are projected in the existing server mknod transaction.
Directory rename preserves descendants in both the extent and `/v1/fs` views;
preexisting classic directories are mirrored using that same mknod operation.
These are generic namespace repairs, with no extra endpoint, table or lease.
Stale-session sweeping is best-effort maintenance and does not revoke a healthy
client when an unrelated expired session cannot be cleaned.

## Validation

Standalone tests exercise HTTP deadline and pool behavior, redirect rejection,
mutation acknowledgement loss without replay, sparse dirty-buffer reads,
overlapping slice/hole decoding, exact large session IDs, shared open references,
failed uploads, stale-session publication, credential renewal/binding rejection,
parallel cache misses and close/read ordering. The SDK CI runs these tests on
Linux with address/undefined-behavior sanitizers and on macOS. Full native
database, process-crash and RustFS interoperability results are tracked by the
consumer integration PR; these unit fixtures are not a production object-store
fault certification.
