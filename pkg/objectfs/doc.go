// Package objectfs implements s3/cos/tos/oss access and FUSE mounts.
//
// fs commands open the bucket root so object keys stay relative to the
// bucket. Mount opens the URI prefix so the VFS root is that prefix.
//
// Dat9FS / VaultFS stay in pkg/fuse. This package only translates FUSE
// lookup/readdir/read/write into rclone VFS calls.
//
// Object mounts use the same go-fuse replace as Dat9FS
// (github.com/mornyx/go-fuse/v2 in go.mod) so inode/nlookup behavior
// matches the rest of the CLI. Pin that replace; do not drop it.
package objectfs
