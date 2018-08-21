package enums

//	"bufio"
//"log"
//"os"
//"tsfile/common/utils"

type TSDataType int16

const (
	BOOLEAN TSDataType = 0
	INT32   TSDataType = 1
	INT64   TSDataType = 2
	FLOAT   TSDataType = 3
	DOUBLE  TSDataType = 4
	TEXT    TSDataType = 5
)
