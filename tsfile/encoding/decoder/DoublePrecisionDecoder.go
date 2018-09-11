package decoder

import (
	_ "bytes"
	"math"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type DoublePrecisionDecoder struct {
	endianType constant.EndianType
	reader     *utils.BytesReader
	preValue   int64

	base GorillaDecoder
}

func (d *DoublePrecisionDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *DoublePrecisionDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *DoublePrecisionDecoder) ReadDouble() float64 {
	if !d.base.flag {
		d.base.flag = true

		ch := d.reader.ReadSlice(8)
		var res int64 = 0
		for i := 0; i < 8; i++ {
			res += int64(ch[i] << uint(i*8))
		}
		d.preValue = res

		d.base.leadingZeroNum = d.base.numberOfLeadingZerosLong(d.preValue)
		d.base.tailingZeroNum = d.base.numberOfTrailingZerosLong(d.preValue)
		tmp := math.Float64frombits(uint64(d.preValue))
		d.base.fillBuffer(d.reader)
		d.getNextValue()

		return tmp
	} else {
		tmp := math.Float64frombits(uint64(d.preValue))
		d.getNextValue()

		return tmp
	}
}

func (d *DoublePrecisionDecoder) getNextValue() {
	d.base.nextFlag1 = d.base.readBit(d.reader)
	// case: '0'
	if !d.base.nextFlag1 {
		return
	}
	d.base.nextFlag2 = d.base.readBit(d.reader)

	if !d.base.nextFlag2 {
		// case: '10'
		var tmp int64 = 0
		for i := 0; i < conf.DOUBLE_LENGTH-int(d.base.leadingZeroNum+d.base.tailingZeroNum); i++ {
			var bit int64
			if d.base.readBit(d.reader) {
				bit = 1
			} else {
				bit = 0
			}
			tmp |= bit << uint64(conf.DOUBLE_LENGTH-1-int(d.base.leadingZeroNum)-i)
		}
		tmp ^= d.preValue
		d.preValue = tmp
	} else {
		// case: '11'
		leadingZeroNumTmp := int(d.base.readIntFromStream(d.reader, conf.DOUBLE_LEADING_ZERO_LENGTH))
		lenTmp := int(d.base.readIntFromStream(d.reader, conf.DOUBLE_VALUE_LENGTH))
		var tmp int64 = d.base.readLongFromStream(d.reader, lenTmp)
		tmp <<= uint(conf.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	d.base.leadingZeroNum = d.base.numberOfLeadingZerosLong(d.preValue)
	d.base.tailingZeroNum = d.base.numberOfTrailingZerosLong(d.preValue)
}

func (d *DoublePrecisionDecoder) ReadBool() bool {
	panic("ReadBoolean not supported by DoublePrecisionDecoder")
}

func (d *DoublePrecisionDecoder) ReadShort() int16 {
	panic("ReadShort not supported by DoublePrecisionDecoder")
}

func (d *DoublePrecisionDecoder) ReadInt() int32 {
	panic("ReadInt not supported by DoublePrecisionDecoder")
}

func (d *DoublePrecisionDecoder) ReadLong() int64 {
	panic("ReadLong not supported by DoublePrecisionDecoder")
}

func (d *DoublePrecisionDecoder) ReadFloat() float32 {
	panic("ReadFloat not supported by DoublePrecisionDecoder")
}

func (d *DoublePrecisionDecoder) ReadString() string {
	panic("ReadString not supported by DoublePrecisionDecoder")
}
