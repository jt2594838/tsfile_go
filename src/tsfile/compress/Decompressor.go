package compress

import (
	"tsfile/common/constant"
)

type Decompressor interface {
	GetDecompressedLength(data []byte) (int, error)
	Decompress(compressed []byte) ([]byte, error)
}

func GetDecompressor(name constant.CompressionType) Decompressor {
	var decompressor Decompressor
	switch {
	case name == constant.UNCOMPRESSED:
		decompressor = new(NoDecompressor)
	case name == constant.SNAPPY:
		decompressor = new(SnappyDecompressor)
	default:
		panic("Decompressor not found")
	}

	return decompressor
}
