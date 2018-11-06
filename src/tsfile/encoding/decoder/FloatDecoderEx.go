package decoder

import (
	_ "bytes"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDeltaDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	base          *IntDeltaDecoder
	maxPointValue float64
}

func (d *FloatDeltaDecoder) Init(data []byte) {
	d.base = &IntDeltaDecoder{dataType: d.dataType}
	d.reader = utils.NewBytesReader(data)
	maxPointNumber := d.reader.ReadUnsignedVarInt()
	if maxPointNumber <= 0 {
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow(10.0, float64(maxPointNumber))
	}
	d.base.Init(d.reader.Remaining())
}

func (d *FloatDeltaDecoder) HasNext() bool {
	if d.base == nil {
		return false
	}
	return d.base.HasNext()
}

func (d *FloatDeltaDecoder) NextInt64() int64 {
	return 0
}

func (d *FloatDeltaDecoder) Next() interface{} {
	return float32(float64(d.base.NextEx()) / d.maxPointValue)
}

type DoubleDeltaDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	base          *LongDeltaDecoder
	maxPointValue float64
}

func (d *DoubleDeltaDecoder) Init(data []byte) {
	d.base = &LongDeltaDecoder{dataType: d.dataType}
	d.reader = utils.NewBytesReader(data)
	maxPointNumber := d.reader.ReadUnsignedVarInt()
	if maxPointNumber <= 0 {
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow(10.0, float64(maxPointNumber))
	}
	d.base.Init(d.reader.Remaining())
}

func (d *DoubleDeltaDecoder) HasNext() bool {
	if d.base == nil {
		return false
	}
	return d.base.HasNext()
}

func (d *DoubleDeltaDecoder) NextInt64() int64 {
	return 0
}

func (d *DoubleDeltaDecoder) Next() interface{} {
	return float64(d.base.NextEx()) / d.maxPointValue
}

func NewDoubleDeltaDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) *DoubleDeltaDecoder {
	return &DoubleDeltaDecoder{encoding: encoding, dataType: dataType}
}

func NewFloatDeltaDecoder(encoding constant.TSEncoding, dataType constant.TSDataType) *FloatDeltaDecoder {
	return &FloatDeltaDecoder{encoding: encoding, dataType: dataType}
}
