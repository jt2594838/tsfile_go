package decoder

import (
	_ "bytes"
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	baseDecoder   Decoder
	maxPointValue float64
}

func (d *FloatDecoder) Init(data []byte) {
	if d.encoding == constant.RLE {
		d.baseDecoder = &IntRleDecoder{dataType: d.dataType}
	} else if d.encoding == constant.TS_2DIFF {
		d.baseDecoder = &IntDeltaDecoder{dataType: d.dataType}
	} else {
		panic("data type is not supported by FloatDecoder: " + strconv.Itoa(int(d.dataType)))
	}

	d.reader = utils.NewBytesReader(data)

	maxPointNumber := d.reader.ReadUnsignedVarInt()
	if maxPointNumber <= 0 {
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow(10.0, float64(maxPointNumber))
	}

	d.baseDecoder.Init(d.reader.Remaining())
}

func (d *FloatDecoder) HasNext() bool {
	if d.baseDecoder == nil {
		return false
	}
	return d.baseDecoder.HasNext()
}

func (d *FloatDecoder) ReadValue() interface{} {
	value := d.baseDecoder.ReadValue().(int32)
	result := float64(value) / d.maxPointValue

	return float32(result)
}
