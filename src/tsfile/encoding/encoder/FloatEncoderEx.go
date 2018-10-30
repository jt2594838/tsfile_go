package encoder

import (
	"bytes"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	maxPointNumber          int32
	maxPointNumberSavedFlag bool
	maxPointValue           float64

	baseEncoder *IntDeltaEncoder
}

func NewFloatDeltaEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *FloatDeltaEncoder {
	d := &FloatDeltaEncoder{
		dataType:                dataType,
		maxPointNumber:          int32(maxPointNumber),
		maxPointNumberSavedFlag: false,
		maxPointValue:           0,
	}
	d.baseEncoder = NewIntDeltaEncoder(dataType)

	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(maxPointNumber)
	}
	return d
}

func (d *FloatDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(d.maxPointNumber, buffer)
		d.maxPointNumberSavedFlag = true
	}
	value := (int32)(math.Round(float64(v.(float32)) * d.maxPointValue))
	d.baseEncoder.Encode(value, buffer)
}

func (d *FloatDeltaEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *FloatDeltaEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *FloatDeltaEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}

type DoubleDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	maxPointNumber          int32
	maxPointNumberSavedFlag bool
	maxPointValue           float64

	baseEncoder *LongDeltaEncoder
}

func NewDoubleDeltaEncoder(encoding constant.TSEncoding, maxPointNumber int, dataType constant.TSDataType) *DoubleDeltaEncoder {
	d := &DoubleDeltaEncoder{
		dataType:                dataType,
		maxPointNumber:          int32(maxPointNumber),
		maxPointNumberSavedFlag: false,
		maxPointValue:           0,
	}
	d.baseEncoder = NewLongDeltaEncoder(dataType)

	if d.maxPointNumber <= 0 {
		d.maxPointNumber = 0
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow10(maxPointNumber)
	}
	return d
}

func (d *DoubleDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if !d.maxPointNumberSavedFlag {
		utils.WriteUnsignedVarInt(d.maxPointNumber, buffer)
		d.maxPointNumberSavedFlag = true
	}
	//value := (int64)(math.Round(v.(float64) * d.maxPointValue))
	d.baseEncoder.Encode((int64)(math.Round(v.(float64)*d.maxPointValue)), buffer)
}

func (d *DoubleDeltaEncoder) Flush(buffer *bytes.Buffer) {
	d.baseEncoder.Flush(buffer)
}

func (d *DoubleDeltaEncoder) GetMaxByteSize() int64 {
	return d.baseEncoder.GetMaxByteSize()
}

func (d *DoubleDeltaEncoder) GetOneItemMaxSize() int {
	return d.baseEncoder.GetOneItemMaxSize()
}
