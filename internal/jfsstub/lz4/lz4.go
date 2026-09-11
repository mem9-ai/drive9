// Package lz4 is a CGO-free stand-in for github.com/hungys/go-lz4 used by JuiceFS.
package lz4

import "github.com/pierrec/lz4/v4"

func CompressBound(size int) int {
	return lz4.CompressBlockBound(size)
}

func CompressDefault(src, dst []byte) (int, error) {
	return lz4.CompressBlock(src, dst, nil)
}

func DecompressSafe(src, dst []byte) (int, error) {
	return lz4.UncompressBlock(src, dst)
}
