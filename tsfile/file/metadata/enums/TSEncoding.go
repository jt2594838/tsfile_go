package enums

//	"bufio"
//"log"
//"os"
//"tsfile/common/utils"

type TSEncoding int16

const (
	PLAIN            TSEncoding = 0
	PLAIN_DICTIONARY TSEncoding = 1
	RLE              TSEncoding = 2
	DIFF             TSEncoding = 3
	TS_2DIFF         TSEncoding = 4
	BITMAP           TSEncoding = 5
	GORILLA          TSEncoding = 6
)

//func (en TSEncoding) Deserialize(i int16) TSEncoding {
//	switch i {
//	case 0:
//		return PLAIN
//	case 1:
//		return PLAIN_DICTIONARY
//	case 2:
//		return RLE
//	case 3:
//		return DIFF
//	case 4:
//		return TS_2DIFF
//	case 5:
//		return BITMAP
//	case 6:
//		return GORILLA
//	default:
//		return PLAIN
//	}
//}

//func (en TSEncoding) Serialize() int16 {
//	switch en {
//	case PLAIN:
//		return 0
//	case PLAIN_DICTIONARY:
//		return 1
//	case RLE:
//		return 2
//	case DIFF:
//		return 3
//	case TS_2DIFF:
//		return 4
//	case BITMAP:
//		return 5
//	case GORILLA:
//		return 6
//	default:
//		return 0
//	}
//}

func (en TSEncoding) GetSerializedSize() int {
	return 2
}
