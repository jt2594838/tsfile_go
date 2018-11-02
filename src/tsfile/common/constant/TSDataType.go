package constant

type TSDataType int8

const (
	BOOLEAN TSDataType = 0
	INT32   TSDataType = 1
	INT64   TSDataType = 2
	FLOAT   TSDataType = 3
	DOUBLE  TSDataType = 4
	TEXT    TSDataType = 5
	INVALID TSDataType = -1
)

const (
	BOOLEAN_LEN int = 1
	SHORT_LEN   int = 2
	INT_LEN     int = 4
	LONG_LEN    int = 8
	FLOAT_LEN   int = 4
	DOUBLE_LEN  int = 8
)
