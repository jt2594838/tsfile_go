package compress

type NoDecompressor struct{}

func (n *NoDecompressor) GetDecompressedLength(data []byte) (int, error) {
	return len(data), nil
}

func (n *NoDecompressor) Decompress(compressed []byte) ([]byte, error) {
	return compressed, nil
}
