package decoder

import (
	_ "bytes"
	"math"
	_ "os"
	"strconv"
	"tsfile/common/constant"
)

const (
	RLE        = 0
	BIT_PACKED = 1
)

type Decoder interface {
	Init(data []byte)
	HasNext() bool
	ReadBool() bool
	ReadShort() int16
	ReadInt() int32
	ReadLong() int64
	ReadFloat() float32
	ReadDouble() float64
	ReadString() string
	//	ReadBigDecimal(reader *bytes.Reader) interface{}
}

func GetDecoderByType(encoding constant.TSEncoding, dataType constant.TSDataType) Decoder {
	// PLA and DFT encoding are not supported in current version
	var decoder Decoder

	switch {
	case encoding == constant.PLAIN:
		decoder = &PlainDecoder{endianType: constant.LITTLE_ENDIAN}
	case (encoding == constant.RLE && dataType == constant.BOOLEAN):
		decoder = &IntRleDecoder{endianType: constant.LITTLE_ENDIAN}
	case (encoding == constant.RLE && dataType == constant.INT32):
		decoder = &IntRleDecoder{endianType: constant.LITTLE_ENDIAN}
	case (encoding == constant.RLE && dataType == constant.INT64):
		decoder = &LongRleDecoder{endianType: constant.LITTLE_ENDIAN}
	case (encoding == constant.TS_2DIFF && dataType == constant.INT32):
		decoder = new(IntDeltaDecoder)
	case (encoding == constant.TS_2DIFF && dataType == constant.INT64):
		decoder = new(LongDeltaDecoder)
	case ((encoding == constant.RLE || encoding == constant.TS_2DIFF) && (dataType == constant.FLOAT || dataType == constant.DOUBLE)):
		decoder = &FloatDecoder{encoding: encoding, dataType: dataType}
	case (encoding == constant.GORILLA && dataType == constant.FLOAT):
		decoder = new(SinglePrecisionDecoder)
	case (encoding == constant.GORILLA && dataType == constant.DOUBLE):
		decoder = new(DoublePrecisionDecoder)
	default:
		panic("Decoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return decoder
}

func ceil(v int) int {
	return int(math.Ceil(float64(v) / 8.0))
}
