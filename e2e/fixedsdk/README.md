# Paired fixed-view SDK probe

The server repository's `TestFixedUploadPairedSDK` starts an isolated HTTP fixture,
installs real quota rows and invokes this executable. The fixture uses the normal
handlers and local object transport with TiKV-backed metadata. No live tenant,
production credential or local database startup is required by this command.

Build with `go build -o /absolute/path/fixed-sdk-probe ./e2e/fixedsdk`, then set
`DRIVE9_FIXED_SDK_PROBE` to that executable in the isolated server test job.
The server test sets the loopback endpoint itself. CI must pin both repository
commits and build the executable before running that test.

The probe covers a three-part seekable upload, cross-part reading, conditional
replacement, stale-revision refusal, child/main isolation, initiation replay,
cancellation, seal and validation. It also runs partial and sequential writes
through the actual writeback/core chain with preload disabled; fixed views
currently upload the complete file for either edit. The paired fixture changes
the first writeback completion response to 503 after the normal server handler
has succeeded. The probe requires that failure, then retries Flush with the
original completion and verifies stored bytes and isolation. The probe then launches a queue child process, writes a three-part shadow image alongside an older cache image,
and requires another injected 503 after the server commits it. That child exits
without stopping writeback or closing staging. A fresh process performs the
mount-order WAL recovery and runtime Start, replays the original request, checks
its bytes, and verifies local cleanup. The paired fixture requires identical
operation, upload, body and returned revision, with no intervening initiation.
This exercises process exit after a known failure boundary, not arbitrary
power-loss durability. It does not cover kernel
FUSE, same-mount switching or final merge.
`go test -race ./e2e/fixedsdk` only checks the probe's endpoint and failure handling;
it is not the paired integration run.

The writeback offline regressions also cover retrying a lost extent completion
response on the same open handle. They keep its original upload/operation and
part list, then retire only the confirmed buffer generation. The same-handle recovery value
is in memory; the separate queue subprocess case above verifies the persistent
request used by the asynchronous runtime.

The queue restart also verifies that the superseded cache is removed before it
can be uploaded as another owner. Both background tracks must drain, and the
queue shadow, pending entry, cache entry, and completion WAL must all be settled.

The same real writeback/core chain also opens the recovered extent file, unlinks
its last name, recreates that name, and checks the old descriptor's full bytes,
EOF, and anonymous write/flush isolation from the replacement. This exercises
the userspace handle path and does not replace kernel FUSE acceptance.
