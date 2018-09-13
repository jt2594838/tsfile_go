package compress

import (
	"github.com/golang/snappy"
)

type SnappyDecompressor struct{}

func (n *SnappyDecompressor) GetDecompressedLength(data []byte) (int, error) {
	return snappy.DecodedLen(data)
}

func (n *SnappyDecompressor) Decompress(compressed []byte) ([]byte, error) {
	return snappy.Decode(nil, compressed)
}
