package conf

const (
	MAGIC_STRING string = "TsFilev0.8.0"

	// RLE configuration
	/**
	 * Default bit width of RLE encoding is 8
	 */
	RLE_MIN_REPEATED_NUM   int = 8
	RLE_MAX_REPEATED_NUM   int = 0x7FFF
	RLE_MAX_BIT_PACKED_NUM int = 63
)
