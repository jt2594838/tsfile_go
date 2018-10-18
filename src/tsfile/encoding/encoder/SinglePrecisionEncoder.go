package encoder

import (
	"bytes"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"github.com/go_sample/src/tsfile/common/tsFileConf"
)

type SinglePrecisionEncoder struct {
	encoding constant.TSEncoding
	dataType   constant.TSDataType

	base GorillaEncoder
	preValue int32
}

func (d *SinglePrecisionEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	if (!d.base.flag){
		d.base.flag = true
		d.preValue = int32(math.Float32bits(v.(float32)))
		d.base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
		d.base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
		buffer.Write(utils.Int32ToByte(d.preValue,1))

		//buffer.WriteByte(byte((d.preValue >> 0) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 8) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 16) & 0xFF))
		//buffer.WriteByte(byte((d.preValue >> 24) & 0xFF))
	}else {
		var nextValue int32;
		var tmp int32;
		nextValue = int32(math.Float32bits(v.(float32)))
		tmp = nextValue ^ d.preValue
		if (tmp == 0){
                    d.base.writeBit(false,buffer)
		}else {
                    var leadingZeroNumTmp int32
		    var tailingZeroNumTmp  int32
		    leadingZeroNumTmp = utils.NumberOfLeadingZeros(tmp)
		    tailingZeroNumTmp = utils.NumberOfTrailingZeros(tmp)
		    if (leadingZeroNumTmp >= d.base.leadingZeroNum && tailingZeroNumTmp >= tailingZeroNumTmp){
			    // case: write '10' and effective bits without first leadingZeroNum '0' and last tailingZeroNum '0'
			    d.base.writeBit(true,buffer)
			    d.base.writeBit(false,buffer)
			    d.writeBits(tmp, buffer, tsFileConf.FLOAT_LENGTH  -1 - d.base.leadingZeroNum, d.base.tailingZeroNum)
		    }else{
			    // case: write '11', leading zero num of value, effective bits len and effective bit value
			    d.base.writeBit(true, buffer)
			    d.base.writeBit(true, buffer)
			    d.writeBits(leadingZeroNumTmp, buffer, tsFileConf.FLAOT_LEADING_ZERO_LENGTH - 1, 0)
			    d.writeBits(tsFileConf.FLOAT_LENGTH - leadingZeroNumTmp - tailingZeroNumTmp, buffer, tsFileConf.FLOAT_VALUE_LENGTH - 1, 0)
			    d.writeBits(tmp, buffer, tsFileConf.FLOAT_LENGTH - 1 - leadingZeroNumTmp, tailingZeroNumTmp)
		    }
			d.preValue = nextValue
			d.base.leadingZeroNum = utils.NumberOfLeadingZeros(d.preValue)
			d.base.tailingZeroNum = utils.NumberOfTrailingZeros(d.preValue)
		}
	}
}

func (d *SinglePrecisionEncoder) Flush(buffer *bytes.Buffer) {
	d.Encode(math.Float32frombits(0x11), buffer)
	d.base.CleanBuffer(buffer)
	d.base.Reset()
}

func (d *SinglePrecisionEncoder) GetMaxByteSize() int64 {
	// max(first 4 byte, case '11' bit + 5bit + 6bit + 32bit = 45bit) + NaN(case '11' bit + 5bit + 6bit + 32bit = 45bit) = 90bit
	return 12;
}

func (d *SinglePrecisionEncoder) GetOneItemMaxSize() int {
	// case '11'
	// 2bit + 5bit + 6bit + 32bit = 45bit
	return 6;
}

func (d *SinglePrecisionEncoder) writeBits(num int32, buffer *bytes.Buffer, start int32, end int32){
	var bit int32 = 0
	var i int32 = 0
	for i = start; i >= end; i--{
		bit = num & ( 1 << uint32(i))
		d.base.writeIntBit(bit, buffer)
	}
}


func NewSinglePrecisionEncoder(dataType constant.TSDataType) (*SinglePrecisionEncoder) {
	d := &SinglePrecisionEncoder{dataType:dataType}
	d.base.flag = false
	return d
}