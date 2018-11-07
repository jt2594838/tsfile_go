package decoder

import (
	_ "bytes"
	"math"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type SinglePrecisionDecoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType
	reader     *utils.BytesReader
	flag       bool
	preValue   uint32

	leadingZeroNum     uint32
	tailingZeroNum     uint32
	buffer             byte
	numberLeftInBuffer uint32
}

func (d *SinglePrecisionDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *SinglePrecisionDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *SinglePrecisionDecoder) NextInt64() int64 {
	return 0
}

func (d *SinglePrecisionDecoder) Next() interface{} {
	if !d.flag {
		d.flag = true
		reader := d.reader

		ch := reader.ReadSlice(4)
		d.preValue = uint32(ch[0]) + uint32(ch[1])<<8 + uint32(ch[2])<<16 + uint32(ch[3])<<24
		d.leadingZeroNum = NumberOfLeadingZeros(d.preValue)
		d.tailingZeroNum = NumberOfTrailingZeros(d.preValue)
		tmp := math.Float32frombits(uint32(d.preValue))
		//d.base.fillBuffer(d.reader)
		d.buffer = reader.ReadByte()
		d.numberLeftInBuffer = 8
		d.getNextValue()

		return tmp
	} else {
		tmp := math.Float32frombits(uint32(d.preValue))
		d.getNextValue()

		return tmp
	}
}

func (d *SinglePrecisionDecoder) getNextValue() {
	reader := d.reader
	// case: '0'
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
		var tmp uint32 = 0
		iLen := conf.FLOAT_LENGTH - int32(d.leadingZeroNum+d.tailingZeroNum)
		iMoved := uint32(conf.FLOAT_LENGTH) - 1 - (d.leadingZeroNum)

		bRead := d.buffer
		numberLeftInBuffer := d.numberLeftInBuffer
		for i := int32(0); i < iLen; i++ {
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
		bRead := d.buffer
		numberLeftInBuffer := d.numberLeftInBuffer

		var iMoved uint32
		var i uint32
		var iForLen uint32

		//leadingZeroNumTmp := d.base.readIntFromStream(d.reader, conf.FLAOT_LEADING_ZERO_LENGTH)
		var leadingZeroNumTmp uint32 = 0
		iMoved = uint32(conf.FLAOT_LEADING_ZERO_LENGTH - 1)
		iForLen = uint32(conf.FLAOT_LEADING_ZERO_LENGTH)
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

		//lenTmp := d.base.readIntFromStream(d.reader, conf.FLOAT_VALUE_LENGTH)
		var lenTmp uint32 = 0
		iMoved = uint32(conf.FLOAT_VALUE_LENGTH - 1)
		iForLen = uint32(conf.FLOAT_VALUE_LENGTH)
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
		//var tmp int32 = d.base.readIntFromStream(d.reader, lenTmp)
		var tmp uint32 = 0
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

		tmp <<= (uint32(conf.FLOAT_LENGTH) - leadingZeroNumTmp - lenTmp)
		tmp ^= d.preValue
		d.preValue = tmp
	}
	d.leadingZeroNum = NumberOfLeadingZeros(d.preValue)
	d.tailingZeroNum = NumberOfTrailingZeros(d.preValue)
}

func NumberOfLeadingZeros(i uint32) uint32 {
	if i == 0 {
		return 32
	}

	var n uint32 = 1
	if uint32(i)>>16 == 0 {
		n += 16
		i <<= 16
	}
	if uint32(i)>>24 == 0 {
		n += 8
		i <<= 8
	}
	if uint32(i)>>28 == 0 {
		n += 4
		i <<= 4
	}
	if uint32(i)>>30 == 0 {
		n += 2
		i <<= 2
	}
	n -= uint32(uint32(i) >> 31)

	return n
}

func NumberOfTrailingZeros(i uint32) uint32 {
	if i == 0 {
		return 32
	}

	var y uint32
	var n uint32 = 31
	y = i << 16
	if y != 0 {
		n = n - 16
		i = y
	}
	y = i << 8
	if y != 0 {
		n = n - 8
		i = y
	}
	y = i << 4
	if y != 0 {
		n = n - 4
		i = y
	}
	y = i << 2
	if y != 0 {
		n = n - 2
		i = y
	}

	return n - uint32(uint32(i<<1)>>31)
}

func NewSinglePrecisionDecoder(dataType constant.TSDataType) *SinglePrecisionDecoder {
	return &SinglePrecisionDecoder{dataType: dataType}
}
