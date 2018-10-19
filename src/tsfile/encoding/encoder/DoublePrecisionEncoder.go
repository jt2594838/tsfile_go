package encoder

import (
	"bytes"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/common/conf"
	"tsfile/common/log"
)

type DoublePrecisionEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	base     GorillaEncoder
	preValue int64
}

func (d *DoublePrecisionEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if (!d.base.flag) {
		// case: write first 8 byte value without any encoding
		d.base.flag = true
		d.preValue = int64(math.Float64bits(v.(float64)))
		d.base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
		d.base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		var bufferLittle []byte
		bufferLittle = utils.Int64ToByte(d.preValue, 1)
		buffer.Write(bufferLittle)
	} else {
		var nextValue int64;
		var tmp int64;
		nextValue = int64(math.Float64bits(v.(float64)))
		tmp = nextValue ^ d.preValue
		if (tmp == 0) {
			// case: write '0'
			d.base.writeBit(false, buffer)
		} else {
			var leadingZeroNumTmp int32
			var tailingZeroNumTmp int32
			leadingZeroNumTmp = utils.NumberOfLeadingZerosLong(tmp)
			tailingZeroNumTmp = utils.NumberOfTrailingZerosLong(tmp)
			if (leadingZeroNumTmp >= d.base.leadingZeroNum && tailingZeroNumTmp >= d.base.tailingZeroNum) {
				// case: write '10' and effective bits without first leadingZeroNum '0' and last tailingZeroNum '0'
				d.base.writeBit(true, buffer)
				d.base.writeBit(false, buffer)
				d.writeBits(tmp, buffer, int32(conf.DOUBLE_LENGTH)-1-d.base.leadingZeroNum, d.base.tailingZeroNum)
			} else {
				// case: write '11', leading zero num of value, effective bits len and effective bit value
				d.base.writeBit(true, buffer);
				d.base.writeBit(true, buffer);
				d.writeBits(int64(leadingZeroNumTmp), buffer, int32(conf.DOUBLE_LEADING_ZERO_LENGTH)-1, 0)
				d.writeBits(int64(int32(conf.DOUBLE_LENGTH)-leadingZeroNumTmp-tailingZeroNumTmp), buffer, int32(conf.DOUBLE_VALUE_LENGTH)-1, 0)
				d.writeBits(tmp, buffer, int32(conf.DOUBLE_LENGTH)-1-leadingZeroNumTmp, tailingZeroNumTmp)

			}
			d.preValue = nextValue
			d.base.leadingZeroNum = utils.NumberOfLeadingZerosLong(d.preValue)
			d.base.tailingZeroNum = utils.NumberOfTrailingZerosLong(d.preValue)
		}

	}
}

func (d *DoublePrecisionEncoder) Flush(buffer *bytes.Buffer) {
	d.Encode(math.Float64frombits(0x7ff8000000000000), buffer)
	d.base.CleanBuffer(buffer)
	d.base.Reset()
}

func (d *DoublePrecisionEncoder) GetMaxByteSize() int64 {
	// max(first 4 byte, case '11' bit + 5bit + 6bit + 32bit = 45bit) + NaN(case '11' bit + 5bit + 6bit + 32bit = 45bit) = 90bit
	return 20;
}

func (d *DoublePrecisionEncoder) GetOneItemMaxSize() int {
	// case '11'
	// 2bit + 5bit + 6bit + 32bit = 45bit
	return 10;
}

func (d *DoublePrecisionEncoder) writeBits(num int64, buffer *bytes.Buffer, start int32, end int32) {
	var bit int64 = 0;
	var i int32 = 0
	for i = start; i >= end; i-- {
		bit = num & ( 1 << uint32(i))
		d.base.writeLongBit(bit, buffer)
	}
}

func NewDoublePrecisionEncoder(dataType constant.TSDataType) (*DoublePrecisionEncoder) {
	log.Info("double using Gorilla")
	d := &DoublePrecisionEncoder{dataType: dataType}
	d.base.flag = false
	return d
}
