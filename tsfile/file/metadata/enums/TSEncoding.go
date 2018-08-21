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
