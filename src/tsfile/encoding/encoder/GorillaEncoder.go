package encoder

import (
	"bytes"
	"tsfile/common/constant"
)

type GorillaEncoder struct {
	encoding constant.TSEncoding
	dataType   constant.TSDataType

	//baseDecoder   Encoder
	flag bool
	leadingZeroNum int32
	tailingZeroNum int32
	buffer byte
	numberLeftInBuffer uint32
}

func (d *GorillaEncoder) writeBit(b bool, buffer *bytes.Buffer){
	d.buffer <<= 1
	if (b) {
		d.buffer |= 1
	}
	d.numberLeftInBuffer++
	if (d.numberLeftInBuffer == 8){
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) writeIntBit(i int32, buffer *bytes.Buffer){
	if (i == 0){
		d.writeBit(false,buffer)
	}else {
		d.writeBit(true, buffer)
	}
}

func (d *GorillaEncoder) writeLongBit(i int64, buffer *bytes.Buffer){
	if (i == 0){
		d.writeBit(false, buffer)
	}else{
		d.writeBit(true, buffer)
	}
}

func (d *GorillaEncoder) Reset(){
	d.flag = false
	d.numberLeftInBuffer = 0
	d.buffer = 0
}

func (d *GorillaEncoder) CleanBuffer(buffer *bytes.Buffer){
	if (d.numberLeftInBuffer == 0){
		return
	}
	if (d.numberLeftInBuffer > 0 ){
		d.buffer <<= (8 - d.numberLeftInBuffer)
	}
	buffer.WriteByte(d.buffer)
	d.numberLeftInBuffer = 0
	d.buffer = 0;
}


/*func NewGorillaEncoder() (*GorillaEncoder) {
	d := &GorillaEncoder{encoding:constant.GORILLA}
	return d
}*/
/*if encoding == constant.RLE {
	if dataType == constant.FLOAT {
		//d.baseDecoder = NewIntRleEncoder(dataType)
	} else if dataType == constant.DOUBLE {
		//d.baseDecoder = NewLongRleEncoder(dataType)
	} else {
		panic("data type is not supported by GorillaEncoder: " + strconv.Itoa(int(d.dataType)))
	}
} else if encoding == constant.TS_2DIFF {
	if dataType == constant.FLOAT {
		d.baseDecoder = NewIntDeltaEncoder(dataType)
	} else if dataType == constant.DOUBLE {
		d.baseDecoder = NewLongDeltaEncoder(dataType)
	} else {
		panic("data type is not supported by GorillaEncoder: " + strconv.Itoa(int(d.dataType)))
	}
} else {
	panic("encoding is not supported by GorillaEncoder: " + strconv.Itoa(int(d.dataType)))
}

d.maxPointNumber = maxPointNumber
if d.maxPointNumber <= 0 {
	d.maxPointNumber = 0
	d.maxPointValue = 1
} else {
	d.maxPointValue = math.Pow10(maxPointNumber)
}

d.maxPointNumberSavedFlag = false
*/
