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
	dataType   constant.TSDataType
	reader     *utils.BytesReader
	flag       bool
	preValue   int64

	base GorillaDecoder
}

func (d *DoublePrecisionDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *DoublePrecisionDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *DoublePrecisionDecoder) NextInt64() int64 {
	return 0
}

func (d *DoublePrecisionDecoder) Next() interface{} {
	if !d.flag {
		base := &(d.base)
		reader := d.reader
		d.flag = true

		ch := reader.ReadSlice(8)
		var res int64 = 0
		for i := 0; i < 8; i++ {
			res += int64(ch[i]) << uint(i*8)
		}
		d.preValue = res

		base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
		base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		tmp := math.Float64frombits(uint64(d.preValue))
		base.fillBuffer(reader)
		d.getNextValue()

		return tmp
	} else {
		tmp := math.Float64frombits(uint64(d.preValue))
		d.getNextValue()

		return tmp
	}
}

func (d *DoublePrecisionDecoder) getNextValue() {
	// case: '0'
	base := &(d.base)
	reader := d.reader
	if !base.readBit(reader) {
		return
	}
	if !base.readBit(reader) {
		// case: '10'
		var tmp int64 = 0
		l := conf.DOUBLE_LENGTH - (base.leadingZeroNum + d.base.tailingZeroNum)
		t := conf.DOUBLE_LENGTH - (base.leadingZeroNum + 1)
		for i := int32(0); i < l; i++ {
			var bit int64
			if base.readBit(reader) {
				bit = 1
			} else {
				bit = 0
			}
			tmp |= int64(bit << uint(t-i))
		}
		tmp ^= d.preValue
		d.preValue = tmp
	} else {
		// case: '11'
		leadingZeroNumTmp := base.readIntFromStream(d.reader, conf.DOUBLE_LEADING_ZERO_LENGTH)
		lenTmp := base.readIntFromStream(d.reader, conf.DOUBLE_VALUE_LENGTH)
		var tmp int64 = base.readLongFromStream(d.reader, lenTmp)
		tmp <<= uint(conf.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
	base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
}

func NewDoublePrecisionDecoder(dataType constant.TSDataType) *DoublePrecisionDecoder {
	return &DoublePrecisionDecoder{dataType: dataType}
}
