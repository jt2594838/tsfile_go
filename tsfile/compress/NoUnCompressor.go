package compress

import (
	"tsfile/common/constant"
)

type NoUnCompressor struct{}

func (n *NoUnCompressor) GetUncompressedLength(data []byte) (int, error) {
	return len(data), nil
}

func (n *NoUnCompressor) UnCompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}

func (n *NoUnCompressor) GetCodecName() constant.CompressionType {
	return constant.UNCOMPRESSED
}
