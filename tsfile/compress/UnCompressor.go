package compress

import (
	"tsfile/common/constant"
)

type UnCompressor interface {
	GetUncompressedLength(data []byte) (int, error)
	UnCompress(compressed []byte) ([]byte, error)
	//Uncompress(compressed []byte, offset int, lengh int, output []byte, outOffset int) int
	//Uncompress(ByteBuffer []byte, ByteBuffer uncompressed) int
	GetCodecName() constant.CompressionType
}

func GetUnCompressor(name constant.CompressionType) UnCompressor {
	var uncompressor UnCompressor
	switch {
	case name == constant.UNCOMPRESSED:
		uncompressor = new(NoUnCompressor)
	case name == constant.SNAPPY:
		uncompressor = new(SnappyUnCompressor)
	default:
		panic("UnCompressor not found")
	}

	return uncompressor
}
