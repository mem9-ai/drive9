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
currently upload the complete file for either edit. It does not cover kernel
FUSE, same-mount switching or final merge.
`go test -race ./e2e/fixedsdk` only checks the probe's endpoint and failure handling;
it is not the paired integration run.
