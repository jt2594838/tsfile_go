package decoder

import (
	_ "bytes"
	"math"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type DoublePrecisionDecoder struct {
	endianType         constant.EndianType
	dataType           constant.TSDataType
	reader             *utils.BytesReader
	flag               bool
	preValue           uint64
	leadingZeroNum     uint32
	tailingZeroNum     uint32
	buffer             byte
	numberLeftInBuffer uint32
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
		reader := d.reader
		d.flag = true

		ch := reader.ReadSlice(8)

		res := uint64(ch[0]) + (uint64(ch[1]) << 8) + (uint64(ch[2]) << 16) + (uint64(ch[3]) << 24) +
			(uint64(ch[4]) << 32) + (uint64(ch[5]) << 40) + (uint64(ch[6]) << 48) + (uint64(ch[7]) << 56)
		d.preValue = res

		d.leadingZeroNum = NumberOfLeadingZerosLong(d.preValue)
		d.tailingZeroNum = NumberOfTrailingZerosLong(d.preValue)
		//tmp := math.Float64frombits(res)
		d.buffer = reader.ReadByte()
		d.numberLeftInBuffer = 8
		d.getNextValue()

		tmp := math.Float64frombits(res)
		return tmp
	} else {
		tmp := math.Float64frombits(d.preValue)

		d.getNextValue()

		return tmp
	}
}

func (d *DoublePrecisionDecoder) getNextValue() {
	// case: '0'
	reader := d.reader
	if d.numberLeftInBuffer == 0 {
		d.buffer = reader.ReadByte()
		d.numberLeftInBuffer = 8
	}
	d.numberLeftInBuffer--
	if ((d.buffer >> d.numberLeftInBuffer) & 1) != 1 {
		return
	}
	if d.numberLeftInBuffer == 0 {
		d.buffer = reader.ReadByte()
		d.numberLeftInBuffer = 8
	}
	d.numberLeftInBuffer--
	if ((d.buffer >> d.numberLeftInBuffer) & 1) != 1 {
		// case: '10'
		var tmp uint64 = 0
		l := conf.DOUBLE_LENGTH - int32(d.leadingZeroNum+d.tailingZeroNum)
		t := uint32(conf.DOUBLE_LENGTH) - (d.leadingZeroNum + 1)

		bRead := d.buffer
		numberLeftInBuffer := d.numberLeftInBuffer
		//fmt.Printf("len:l=%d leadingZeroNum=%d tailingZeroNum:%d\n", l, d.leadingZeroNum, d.tailingZeroNum)
		var iMoved uint32 = uint32(t)
		for i := int32(0); i < l; i++ {
			if numberLeftInBuffer == 0 {
				bRead = reader.ReadByte()
				numberLeftInBuffer = 8
			}
			numberLeftInBuffer--
			if ((bRead >> uint32(numberLeftInBuffer)) & 1) == 1 {
				//if base.readBit(reader) {
				tmp |= 1 << iMoved
			}
			iMoved--
		}
		d.buffer = bRead
		d.numberLeftInBuffer = numberLeftInBuffer
		tmp ^= d.preValue
		d.preValue = tmp
	} else {
		// case: '11'
		var tmp uint64 = 0
		var iMoved uint32
		var i uint32
		var iForLen uint32
		bRead := d.buffer
		numberLeftInBuffer := d.numberLeftInBuffer

		//leadingZeroNumTmp := base.readIntFromStream(reader, conf.DOUBLE_LEADING_ZERO_LENGTH)
		var leadingZeroNumTmp uint32 = 0
		iMoved = uint32(conf.DOUBLE_LEADING_ZERO_LENGTH - 1)
		iForLen = uint32(conf.DOUBLE_LEADING_ZERO_LENGTH)
		for i = uint32(0); i < iForLen; i++ {
			if numberLeftInBuffer == 0 {
				bRead = reader.ReadByte()
				numberLeftInBuffer = 8
			}
			numberLeftInBuffer--
			if ((bRead >> numberLeftInBuffer) & 1) == 1 {
				leadingZeroNumTmp |= 1 << iMoved
			}
			iMoved--
		}

		//lenTmp := base.readIntFromStream(reader, conf.DOUBLE_VALUE_LENGTH)
		var lenTmp uint32 = 0
		iMoved = uint32(conf.DOUBLE_VALUE_LENGTH - 1)
		iForLen = uint32(conf.DOUBLE_VALUE_LENGTH)
		for i = uint32(0); i < iForLen; i++ {
			if numberLeftInBuffer == 0 {
				bRead = reader.ReadByte()
				numberLeftInBuffer = 8
			}
			numberLeftInBuffer--
			if ((bRead >> numberLeftInBuffer) & 1) == 1 {
				lenTmp |= 1 << iMoved
			}
			iMoved--
		}

		//var tmp int64 = base.readLongFromStream(reader, lenTmp)
		iMoved = uint32(lenTmp - 1)
		iForLen = uint32(lenTmp)
		for i = uint32(0); i < iForLen; i++ {
			if numberLeftInBuffer == 0 {
				bRead = reader.ReadByte()
				numberLeftInBuffer = 8
			}
			numberLeftInBuffer--
			if ((bRead >> numberLeftInBuffer) & 1) == 1 {
				tmp |= 1 << iMoved
			}
			iMoved--
		}
		d.buffer = bRead
		d.numberLeftInBuffer = numberLeftInBuffer

		tmp <<= (uint32(conf.DOUBLE_LENGTH) - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	d.leadingZeroNum = NumberOfLeadingZerosLong(d.preValue)
	d.tailingZeroNum = NumberOfTrailingZerosLong(d.preValue)
}

func NumberOfLeadingZerosLong(i uint64) uint32 {
	if i == 0 {
		return 64
	}

	var n uint32 = 1
	var x uint32 = uint32(i >> 32)

	if x == 0 {
		n += 32
		x = uint32(i)
	}
	if uint32(x)>>16 == 0 {
		n += 16
		x <<= 16
	}
	if uint32(x)>>24 == 0 {
		n += 8
		x <<= 8
	}
	if uint32(x)>>28 == 0 {
		n += 4
		x <<= 4
	}
	if uint32(x)>>30 == 0 {
		n += 2
		x <<= 2
	}
	n -= uint32(uint32(x) >> 31)

	return n
}

func NumberOfTrailingZerosLong(i uint64) uint32 {
	if i == 0 {
		return 64
	}

	var x, y uint32
	var n uint32 = 63
	y = uint32(i)

	if y != 0 {
		n = n - 32
		x = y
	} else {
		x = (uint32)(uint64(i) >> 32)
	}
	y = x << 16
	if y != 0 {
		n = n - 16
		x = y
	}
	y = x << 8
	if y != 0 {
		n = n - 8
		x = y
	}
	y = x << 4
	if y != 0 {
		n = n - 4
		x = y
	}
	y = x << 2
	if y != 0 {
		n = n - 2
		x = y
	}

	return n - uint32(uint32(x<<1)>>31)
}

func NewDoublePrecisionDecoder(dataType constant.TSDataType) *DoublePrecisionDecoder {
	return &DoublePrecisionDecoder{dataType: dataType}
}
