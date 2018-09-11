package decoder

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type PlainDecoder struct {
	endianType constant.EndianType
	reader     *utils.BytesReader
}

func (d *PlainDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *PlainDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *PlainDecoder) ReadBool() bool {
	return d.reader.ReadBool()
}

func (d *PlainDecoder) ReadShort() int16 {
	return d.reader.ReadShort()
}

func (d *PlainDecoder) ReadInt() int32 {
	return d.reader.ReadInt()
}

func (d *PlainDecoder) ReadLong() int64 {
	return d.reader.ReadLong()
}

func (d *PlainDecoder) ReadFloat() float32 {
	return d.reader.ReadFloat()
}

func (d *PlainDecoder) ReadDouble() float64 {
	return d.reader.ReadDouble()
}

func (d *PlainDecoder) ReadString() string {
	return d.reader.ReadString()
}
