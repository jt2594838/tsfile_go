package decoder

import (
	_ "bytes"
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
	ReadValue() interface{}
}

func CreateDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) Decoder {
	// PLA and DFT encoding are not supported in current version
	var decoder Decoder

	switch {
	case encoding == constant.PLAIN:
		decoder = &PlainDecoder{dataType: dataType}
	case (encoding == constant.RLE && dataType == constant.BOOLEAN):
		decoder = &IntRleDecoder{dataType: dataType}
	case (encoding == constant.RLE && dataType == constant.INT32):
		decoder = &IntRleDecoder{dataType: dataType}
	case (encoding == constant.RLE && dataType == constant.INT64):
		decoder = &LongRleDecoder{dataType: dataType}
	case (encoding == constant.RLE && dataType == constant.FLOAT):
		decoder = &FloatDecoder{encoding: encoding, dataType: dataType}
	case (encoding == constant.RLE && dataType == constant.DOUBLE):
		decoder = &DoubleDecoder{encoding: encoding, dataType: dataType}
	case (encoding == constant.TS_2DIFF && dataType == constant.INT32):
		decoder = &IntDeltaDecoder{dataType: dataType}
	case (encoding == constant.TS_2DIFF && dataType == constant.INT64):
		decoder = &LongDeltaDecoder{dataType: dataType}
	case (encoding == constant.TS_2DIFF && dataType == constant.FLOAT):
		decoder = &FloatDecoder{encoding: encoding, dataType: dataType}
	case (encoding == constant.TS_2DIFF && dataType == constant.DOUBLE):
		decoder = &DoubleDecoder{encoding: encoding, dataType: dataType}
	case (encoding == constant.GORILLA && dataType == constant.FLOAT):
		decoder = &SinglePrecisionDecoder{dataType: dataType}
	case (encoding == constant.GORILLA && dataType == constant.DOUBLE):
		decoder = &DoublePrecisionDecoder{dataType: dataType}
	default:
		panic("Decoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return decoder
}
