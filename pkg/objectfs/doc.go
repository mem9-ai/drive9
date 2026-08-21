// Package objectfs implements s3/cos/tos/oss access and FUSE mounts.
//
// fs commands open the bucket root so object keys stay relative to the
// bucket. Mount opens the URI prefix so the VFS root is that prefix.
//
// Dat9FS / VaultFS stay in pkg/fuse. This package only translates FUSE
// lookup/readdir/read/write into rclone VFS calls.
package objectfs
