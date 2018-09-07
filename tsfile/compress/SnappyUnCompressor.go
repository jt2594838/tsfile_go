package compress

import (
	"tsfile/common/constant"

	"github.com/golang/snappy"
)

type SnappyUnCompressor struct{}

func (n *SnappyUnCompressor) GetUncompressedLength(data []byte) (int, error) {
	return snappy.DecodedLen(data)
}

func (n *SnappyUnCompressor) UnCompress(compressed []byte) ([]byte, error) {
	return snappy.Decode(nil, compressed)
}

func (n *SnappyUnCompressor) GetCodecName() constant.CompressionType {
	return constant.SNAPPY
}
