package decoder

import (
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type PlainDecoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType
	reader     *utils.BytesReader
}

func (d *PlainDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
}

func (d *PlainDecoder) HasNext() bool {
	return d.reader.Len() > 0
}

func (d *PlainDecoder) ReadValue() interface{} {
	switch {
	case d.dataType == constant.BOOLEAN:
		return d.reader.ReadBool()
	case d.dataType == constant.INT32:
		return d.reader.ReadInt()
	case d.dataType == constant.INT64:
		return d.reader.ReadLong()
	case d.dataType == constant.FLOAT:
		return d.reader.ReadFloat()
	case d.dataType == constant.DOUBLE:
		return d.reader.ReadDouble()
	case d.dataType == constant.TEXT:
		return d.reader.ReadString()
	default:
		panic("ReadValue not supported: " + strconv.Itoa(int(d.dataType)))
	}
}
