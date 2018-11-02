package encoder

import (
	"bytes"
	"strconv"
	"tsfile/common/conf"
	"tsfile/common/constant"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-9-28 下午5:55
 * @Description:
 */

type Encoder interface {
	Encode(value interface{}, buffer *bytes.Buffer)
	Flush(buffer *bytes.Buffer)
	GetOneItemMaxSize() int
	GetMaxByteSize() int64
}

func GetEncoder(et int16, tdt int16) Encoder {
	encoding := constant.TSEncoding(et)
	dataType := constant.TSDataType(tdt)

	var encoder Encoder
	switch {
	case encoding == constant.PLAIN:
		encoder, _ = NewPlainEncoder(dataType)
	case encoding == constant.RLE:
		if dataType == constant.INT32 {
			encoder = NewRleEncoder(constant.INT32)
		} else if dataType == constant.INT64 {
			encoder = NewRleEncoder(constant.INT64)
		} else if dataType == constant.FLOAT || dataType == constant.DOUBLE {
			encoder = NewFloatEncoder(encoding, int32(conf.FloatPrecision), dataType)
		}
	case encoding == constant.TS_2DIFF:
		if dataType == constant.INT32 {
			encoder = NewIntDeltaEncoder(constant.INT32)
		} else if dataType == constant.INT64 {
			encoder = NewLongDeltaEncoder(constant.INT32)
		} else if dataType == constant.DOUBLE {
			encoder = NewDoubleDeltaEncoder(encoding, conf.FloatPrecision, dataType)
			//encoder = NewFloatEncoder(encoding, conf.FloatPrecision, dataType)
		} else if dataType == constant.FLOAT {
			encoder = NewFloatDeltaEncoder(encoding, conf.FloatPrecision, dataType)
			//encoder = NewFloatEncoder(encoding, conf.FloatPrecision, dataType)
		}
	case encoding == constant.GORILLA:
		if dataType == constant.FLOAT {
			encoder = NewSinglePrecisionEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			encoder = NewDoublePrecisionEncoder(dataType)
		}

	default:
		panic("Encoder not found, encoding:" + strconv.Itoa(int(encoding)) + ", dataType:" + strconv.Itoa(int(dataType)))
	}

	return encoder
}
