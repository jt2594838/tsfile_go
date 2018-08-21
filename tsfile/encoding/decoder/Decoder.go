package decoder

import (
	"os"
	"tsfile/encoding/common"
	"tsfile/file/metadata/enums"
)

type Decoder interface {
	HasNext(reader *os.File) bool
	ReadBool(reader *os.File) bool
	ReadShort(reader *os.File) int16
	ReadInt(reader *os.File) int
	ReadLong(reader *os.File) int64
	ReadFloat(reader *os.File) float32
	ReadDouble(reader *os.File) float64
	//	ReadBinary(reader *os.File) interface{}
	//	ReadBigDecimal(reader *os.File) interface{}
}

func GetDecoderByType(encoding enums.TSEncoding, dataType enums.TSDataType) Decoder {
	// PLA and DFT encoding are not supported in current version
	var decoder Decoder
	switch {
	case encoding == enums.PLAIN:
		decoder = &PlainDecoder{EndianType: common.LITTLE_ENDIAN}
		//	case (enums.TSEncoding.RLE && dataType == TSDataType.BOOLEAN):
		//      	 	decode = new IntRleDecoder(EndianType.LITTLE_ENDIAN)
	default:
		panic("Decoder not found")
	}

	return decoder
}
