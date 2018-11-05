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
// 8-15 bits int64 存储最小值，作为所有数值的基数
// 16-23 bits int64 存储第一个值
// 24 bit 之后存储数值
type LongDeltaDecoder struct {
	dataType constant.TSDataType
	reader   *utils.BytesReader

	count int32
	width int32
	index int32

	baseValue     int64
	firstValue    int64
	decodedValues []int64
}

func (d *LongDeltaDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *LongDeltaDecoder) HasNext() bool {
	return (d.index < d.count) || (d.reader.Len() > 0)
}

func (d *LongDeltaDecoder) Next() interface{} {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return result
	}
}

func (d *LongDeltaDecoder) NextEx() int64 {
	if d.index == d.count {
		return d.loadPack()
	} else {
		result := d.decodedValues[d.index]
		d.index++

		return result
	}
}

func (d *LongDeltaDecoder) loadPack() int64 {
	d.count = int32(d.reader.ReadInt())
	d.width = int32(d.reader.ReadInt())
	d.baseValue = d.reader.ReadLong()
	d.firstValue = d.reader.ReadLong()

	d.index = 0

	//how many bytes data takes after encoding
	encodingLength := int(math.Ceil(float64(d.count*d.width) / 8.0))
	valueBuffer := d.reader.ReadSlice(encodingLength)

	previousValue := d.firstValue
	d.decodedValues = make([]int64, d.count)
	var width int32 = d.width
	var value int64
	var offset int32 = width - 1
	var index int32 = 0
	var i int32 = 0
	var iCount int32
	for iCount = 0; iCount < d.count; iCount++ {
		////pos = width * iCount
		//value = utils.BytesToLong(valueBuffer, pos, width)
		value = 0
		index = offset
		//offset := pos + width - 1
		for i = 0; i < width; i++ {
			//index := offset - i
			//value = setLongN(value, i, getByteN(data[index/8], index))
			if (valueBuffer[index/8] & (1 << uint32(7-index&7))) != 0 {
				value = (value | (1 << uint32(i&0x3f)))
			} else {
				value = (value & ^(1 << uint32(i&0x3f)))
			}
			index--
		}

		d.decodedValues[iCount] = previousValue + d.baseValue + value
		previousValue = d.decodedValues[iCount]
		offset += width
	}

	return d.firstValue
}

func NewLongDeltaDecoder(dataType constant.TSDataType) *LongDeltaDecoder {
	return &LongDeltaDecoder{dataType: dataType}
}
