package encoder

import (
	"bytes"
	"tsfile/common/constant"
)

type GorillaEncoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	//baseDecoder   Encoder
	flag               bool
	leadingZeroNum     int32
	tailingZeroNum     int32
	buffer             byte
	numberLeftInBuffer uint32
}

func (d *GorillaEncoder) writeBit(b bool, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if b {
		d.buffer |= 1
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) writeIntBit(i int32, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if i != 0 {
		d.buffer |= 1
		//d.writeBit(true, buffer)
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) writeLongBit(i int64, buffer *bytes.Buffer) {
	d.buffer <<= 1
	if i != 0 {
		d.buffer |= 1
		//d.writeBit(true, buffer)
	}
	d.numberLeftInBuffer++
	if d.numberLeftInBuffer == 8 {
		d.CleanBuffer(buffer)
	}
}

func (d *GorillaEncoder) Reset() {
	d.flag = false
	d.numberLeftInBuffer = 0
	d.buffer = 0
}

func (d *GorillaEncoder) CleanBuffer(buffer *bytes.Buffer) {
	if d.numberLeftInBuffer == 0 {
		return
	}
	if d.numberLeftInBuffer > 0 {
		d.buffer <<= (8 - d.numberLeftInBuffer)
	}
	buffer.WriteByte(d.buffer)
	d.numberLeftInBuffer = 0
	d.buffer = 0
}
