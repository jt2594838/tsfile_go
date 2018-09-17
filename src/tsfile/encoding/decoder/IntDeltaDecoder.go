package decoder

import (
	_ "bytes"
	_ "encoding/binary"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

// This package is a decoder for decoding the byte array that encoded by DeltaBinaryDecoder just supports integer and long values.
// 0-3 bits int32 存储数值个数
// 4-7 bits int32 存储单个数值宽度
// 8-11 bits int32 存储最小值，作为所有数值的基数
// 12-15 bits int32 存储第一个值
// 15 bit 之后存储数值
type IntDeltaDecoder struct {
	dataType constant.TSDataType
	reader   *utils.BytesReader

	//value count
	count int
	//width per value
	width int
	//value index for reading
	index int

	baseValue     int32
	firstValue    int32
	previousValue int32
	decodedValues []int32
}

func (d *IntDeltaDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *IntDeltaDecoder) HasNext() bool {
	return (d.index < d.count) || (d.reader.Len() > 0)
}

func (d *IntDeltaDecoder) ReadValue() interface{} {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return int32(result)
	}
}

func (d *IntDeltaDecoder) loadPack() int32 {
	d.count = int(d.reader.ReadInt())
	d.width = int(d.reader.ReadInt())
	d.baseValue = d.reader.ReadInt()
	d.firstValue = d.reader.ReadInt()

	d.index = 0

	//how many bytes data takes after encoding
	encodingLength := int(math.Ceil(float64(d.count*d.width) / 8.0))
	valueBuffer := d.reader.ReadSlice(encodingLength)

	//allocateDataArray
	d.decodedValues = make([]int32, d.count)

	d.previousValue = d.firstValue
	for i := 0; i < d.count; i++ {
		p := d.width * i
		//v := int32(binary.BigEndian.Uint32(valueBuffer[p:p + d.width]))
		v := utils.BytesToInt(valueBuffer, p, d.width)
		d.decodedValues[i] = d.previousValue + d.baseValue + v

		d.previousValue = d.decodedValues[i]
	}

	return d.firstValue
}
