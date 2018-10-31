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

	baseEncoder             Encoder
	maxPointNumber          int
	maxPointNumberSavedFlag bool
	maxPointValue           float64
}

func (d *FloatEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(int32(d.maxPointNumber), buffer)
		d.maxPointNumberSavedFlag = true
	}

	if d.dataType == constant.FLOAT {
		value := v.(float32)
		valueInt := int32(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseEncoder.Encode(valueInt, buffer)
	} else if d.dataType == constant.DOUBLE {
		value := v.(float64)
		valueLong := int64(utils.Round(float64(value)*d.maxPointValue, 0))
		d.baseEncoder.Encode(valueLong, buffer)
	} else {
		panic("invalid data type in FloatEncoder")
	}
}

func (d *FloatEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *FloatEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *FloatEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}

func NewFloatEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *FloatEncoder {
	d := &FloatEncoder{dataType: dataType}

	if encoding == constant.RLE {
		if dataType == constant.FLOAT {
			d.baseEncoder = NewRleEncoder(constant.INT32)
		} else if dataType == constant.DOUBLE {
			d.baseEncoder = NewRleEncoder(constant.INT64)
		} else {
			panic("data type is not supported by FloatEncoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else if encoding == constant.TS_2DIFF {
		if dataType == constant.FLOAT {
			d.baseEncoder = NewIntDeltaEncoder(dataType)
		} else if dataType == constant.DOUBLE {
			d.baseEncoder = NewLongDeltaEncoder(dataType)
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
