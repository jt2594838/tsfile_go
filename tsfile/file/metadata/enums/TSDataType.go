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

//func (dt TSDataType) Deserialize(i int16) TSDataType {
//	switch i {
//	case 0:
//		return BOOLEAN
//	case 1:
//		return INT32
//	case 2:
//		return INT64
//	case 3:
//		return FLOAT
//	case 4:
//		return DOUBLE
//	case 5:
//		return TEXT
//	default:
//		return TEXT
//	}
//}

//func (dt TSDataType) Serialize() int16 {
//	switch dt {
//	case BOOLEAN:
//		return 0
//	case INT32:
//		return 1
//	case INT64:
//		return 2
//	case FLOAT:
//		return 3
//	case DOUBLE:
//		return 4
//	case TEXT:
//		return 5
//	default:
//		return -1
//	}
//}

func (dt TSDataType) GetSerializedSize() int {
	return 2
}
