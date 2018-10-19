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
	Next() interface{}
}

func CreateDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) Decoder {
	// PLA and DFT encoding are not supported in current version
	var decoder Decoder

	switch {
	case encoding == constant.PLAIN:
		decoder = &PlainDecoder{dataType: dataType}
	case encoding == constant.RLE:
		if dataType == constant.BOOLEAN {
			decoder = NewIntRleDecoder(dataType)
		} else if dataType == constant.INT32 {
			decoder = NewIntRleDecoder(dataType)
		} else if dataType == constant.INT64 {
			decoder = NewLongRleDecoder(dataType)
		} else if dataType == constant.FLOAT {
			decoder = NewFloatDecoder(encoding, dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoubleDecoder(encoding, dataType)
		}
	case encoding == constant.TS_2DIFF:
		if dataType == constant.INT32 {
			decoder = NewIntDeltaDecoder(dataType)
		} else if dataType == constant.INT64 {
			decoder = NewLongDeltaDecoder(dataType)
		} else if dataType == constant.FLOAT {
			decoder = NewFloatDecoder(encoding, dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoubleDecoder(encoding, dataType)
		}
	case encoding == constant.GORILLA:
		if dataType == constant.FLOAT {
			decoder = NewSinglePrecisionDecoder(dataType)
		} else if dataType == constant.DOUBLE {
			decoder = NewDoublePrecisionDecoder(dataType)
		}
	default:
		panic("Decoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return decoder
}
