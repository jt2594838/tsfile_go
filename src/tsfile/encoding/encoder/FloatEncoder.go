package encoder

import (
	"bytes"
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	baseDecoder             Encoder
	maxPointNumber          int
	maxPointNumberSavedFlag bool
	maxPointValue           float64
}

func (d *FloatEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	value := v.(float32)

	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(int32(d.maxPointNumber), buffer)
		d.maxPointNumberSavedFlag = true
	}

	if d.dataType == constant.FLOAT {
		valueInt := int32(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseDecoder.Encode(valueInt, buffer)
	} else if d.dataType == constant.DOUBLE {
		valueLong := int64(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseDecoder.Encode(valueLong, buffer)
	}
}

func (d *FloatEncoder) Flush(buffer *bytes.Buffer) {
	d.baseDecoder.Flush(buffer)
}

func (d *FloatEncoder) GetMaxByteSize() int64 {
	return d.baseDecoder.GetMaxByteSize()
}

func (d *FloatEncoder) GetOneItemMaxSize() int {
	return d.baseDecoder.GetOneItemMaxSize()
}

func NewFloatEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *FloatEncoder {
	d := &FloatEncoder{dataType: dataType}

	if encoding == constant.RLE {
		if dataType == constant.FLOAT {
			//d.baseDecoder = NewIntRleEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			//d.baseDecoder = NewLongRleEncoder(dataType)
		} else {
			panic("data type is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else if encoding == constant.TS_2DIFF {
		if dataType == constant.FLOAT {
			d.baseDecoder = NewIntDeltaEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			d.baseDecoder = NewLongDeltaEncoder(dataType)
		} else {
			panic("data type is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else {
		panic("encoding is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
	}

	d.maxPointNumber = maxPointNumber
	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(maxPointNumber)
	}

	d.maxPointNumberSavedFlag = false

	return d
}
