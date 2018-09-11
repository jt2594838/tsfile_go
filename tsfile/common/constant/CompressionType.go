package constant

type CompressionType int16

const (
	UNCOMPRESSED CompressionType = 0
	SNAPPY       CompressionType = 1
	GZIP         CompressionType = 2
	LZO          CompressionType = 3
	SDT          CompressionType = 4
	PAA          CompressionType = 5
	PLA          CompressionType = 6
)
