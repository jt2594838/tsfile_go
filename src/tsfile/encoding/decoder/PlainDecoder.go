package decoder

import (
	"encoding/binary"
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

func (d *PlainDecoder) Next() interface{} {
	switch {
	case d.dataType == constant.BOOLEAN:
		return d.reader.ReadBool()
	case d.dataType == constant.INT32:
		result := d.reader.ReadSlice(4)
		return int32(binary.LittleEndian.Uint32(result))
	case d.dataType == constant.INT64:
		result := d.reader.ReadSlice(8)
		return int64(binary.LittleEndian.Uint64(result))
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
