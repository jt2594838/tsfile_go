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

	count int32
	width int32
	index int32

	baseValue     int32
	firstValue    int32
	decodedValues []int32
}

func (d *IntDeltaDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *IntDeltaDecoder) HasNext() bool {
	return (d.index < d.count) || (d.reader.Len() > 0)
}

func (d *IntDeltaDecoder) Next() interface{} {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return int32(result)
	}
}

func (d *IntDeltaDecoder) NextInt64() int64 {
	return 0
}

func (d *IntDeltaDecoder) NextEx() int32 {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return int32(result)
	}
}

func (d *IntDeltaDecoder) loadPack() int32 {
	d.count = int32(d.reader.ReadInt())
	d.width = int32(d.reader.ReadInt())
	d.baseValue = d.reader.ReadInt()
	d.firstValue = d.reader.ReadInt()

	d.index = 0

	//how many bytes data takes after encoding
	encodingLength := int32(math.Ceil(float64(d.count*d.width) / 8.0))
	valueBuffer := d.reader.ReadSlice(encodingLength)

	previousValue := d.firstValue
	d.decodedValues = make([]int32, d.count)

	var value int32

	var iCount int32
	var width int32 = d.width
	var offset int32 = width - 1
	var index int32 = 0
	var i int32
	for iCount = 0; iCount < d.count; iCount++ {
		////pos = width * iCount
		//value = utils.BytesToInt(valueBuffer, pos, width)
		value = 0
		//offset = pos + width - 1
		index = offset
		for i = 0; i < width; i++ {
			//index := offset - i
			//value = setIntN(value, i, getByteN(data[index/8], index))

			if (valueBuffer[index/8] & (1 << uint32(7-index&7))) != 0 {
				value = (value | (1 << uint32(i&0x1f)))
			} else {
				value = (value & ^(1 << uint32(i&0x1f)))
			}
			index--
		}

		d.decodedValues[iCount] = previousValue + d.baseValue + value
		previousValue = d.decodedValues[iCount]

		offset += width
	}

	return d.firstValue
}

func NewIntDeltaDecoder(dataType constant.TSDataType) *IntDeltaDecoder {
	return &IntDeltaDecoder{dataType: dataType}
}
