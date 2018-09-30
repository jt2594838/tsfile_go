package decoder

import (
	_ "bytes"
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type DoubleDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	baseDecoder   Decoder
	maxPointValue float64
}

func (d *DoubleDecoder) Init(data []byte) {
	if d.encoding == constant.RLE {
		d.baseDecoder = &LongRleDecoder{dataType: d.dataType}
	} else if d.encoding == constant.TS_2DIFF {
		d.baseDecoder = &LongDeltaDecoder{dataType: d.dataType}
	} else {
		panic("encoding is not supported by DoubleDecoder: " + strconv.Itoa(int(d.encoding)))
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

func (d *DoubleDecoder) HasNext() bool {
	if d.baseDecoder == nil {
		return false
	}
	return d.baseDecoder.HasNext()
}

func (d *DoubleDecoder) Next() interface{} {
	value := d.baseDecoder.Next().(int64)
	result := float64(value) / d.maxPointValue

	return result
}
