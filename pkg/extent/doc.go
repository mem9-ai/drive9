// Package extent wires JuiceFS vfs.VFS and object storage to drive9's HTTP
// meta engine for content_layout=extent files. Dat9FS stays the kernel
// frontend and calls VFS for Open/SetAttr/Write/Read/Flush/Release.
package extent
