// Package zstd is a CGO-free stand-in for github.com/DataDog/zstd used by JuiceFS.
package zstd

import "github.com/klauspost/compress/zstd"

func CompressBound(srcLen int) int {
	if srcLen <= 0 {
		return 16
	}
	return srcLen + srcLen/255 + 16
}

func CompressLevel(dst, src []byte, level int) ([]byte, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	if dst == nil {
		dst = []byte{}
	}
	return enc.EncodeAll(src, dst[:0]), nil
}

func Decompress(dst, src []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	if dst == nil {
		dst = []byte{}
	}
	return dec.DecodeAll(src, dst[:0])
}
