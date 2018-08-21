package enums

import (
	//	"bufio"
	//"log"
	//"os"
	//"tsfile/common/utils"
	"strings"
)

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

//func (ct CompressionType) Deserialize(i int16) CompressionType {
//	switch i {
//	case 0:
//		return UNCOMPRESSED
//	case 1:
//		return SNAPPY
//	case 2:
//		return GZIP
//	case 3:
//		return LZO
//	case 4:
//		return SDT
//	case 5:
//		return PAA
//	default:
//		return PLA
//	}
//}

//func (ct CompressionType) Serialize() int16 {
//	switch ct {
//	case UNCOMPRESSED:
//		return 0
//	case SNAPPY:
//		return 1
//	case GZIP:
//		return 2
//	case LZO:
//		return 3
//	case SDT:
//		return 4
//	case PAA:
//		return 5
//	case PLA:
//		return 6
//	default:
//		return 0
//	}
//}

func (ct CompressionType) GetSerializedSize() int {
	return 2
}

func (ct CompressionType) FindByShortName(name string) CompressionType {
	if name == "" {
		return UNCOMPRESSED
	}
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "UNCOMPRESSED":
		return UNCOMPRESSED
	case "SNAPPY":
		return SNAPPY
	case "GZIP":
		return GZIP
	case "LZO":
		return LZO
	case "SDT":
		return SDT
	case "PAA":
		return PAA
	case "PLA":
		return PLA
	default:
		panic("CompressionTypeNotSupportedException")
	}
}

func (ct CompressionType) GetExtension() string {
	switch ct {
	case UNCOMPRESSED:
		return ""
	case SNAPPY:
		return ".snappy"
	case GZIP:
		return ".gz"
	case LZO:
		return ".lzo"
	case SDT:
		return ".sdt"
	case PAA:
		return ".paa"
	case PLA:
		return ".pla"
	default:
		return ""
	}
}
