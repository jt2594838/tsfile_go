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

	// Gorilla encoding configuration
	FLOAT_LENGTH               int = 32
	FLAOT_LEADING_ZERO_LENGTH  int = 5
	FLOAT_VALUE_LENGTH         int = 6
	DOUBLE_LENGTH              int = 64
	DOUBLE_LEADING_ZERO_LENGTH int = 6
	DOUBLE_VALUE_LENGTH        int = 7
)
