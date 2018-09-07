package decoder

import (
	_ "bytes"
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
	//	buf := make([]byte, 1)
	//	reader.Read(buf)

	//	return (buf[0] == 0)
	return d.reader.ReadBool()
}

func (d *PlainDecoder) ReadShort() int16 {
	//	buf := make([]byte, 2)
	//	reader.Read(buf)

	//	if d.EndianType == constant.LITTLE_ENDIAN {
	//		return int16(binary.LittleEndian.Uint16(buf))
	//		//return int16((buf[1] << 8) + buf[0])
	//	} else {
	//		log.Println("tsfile-encoding PlainEncoder: current version does not support short value decoding")
	//	}

	//	return -1
	return d.reader.ReadShort()
}

func (d *PlainDecoder) ReadInt() int {
	//	buf := make([]byte, 4)
	//	reader.Read(buf)

	//	if d.EndianType == constant.LITTLE_ENDIAN {
	//		return int(binary.LittleEndian.Uint32(buf))
	//		//return int(buf[0]) + int(buf[1]<<8) + int(buf[2]<<16) + int(buf[3]<<24)
	//	} else {
	//		log.Println("tsfile-encoding PlainEncoder: current version does not support int value encoding")
	//	}

	//	return -1
	return d.reader.ReadInt()
}

func (d *PlainDecoder) ReadLong() int64 {
	//	buf := make([]byte, 8)
	//	reader.Read(buf)

	//	var res int64 = 0
	//	var i uint8 = 0
	//	for i = 0; i < 8; i++ {
	//		res += int64(buf[i] << (i * 8))
	//	}

	//	return res
	return d.reader.ReadLong()
}

func (d *PlainDecoder) ReadFloat() float32 {
	//	buf := make([]byte, 4)
	//	reader.Read(buf)
	//	bits := binary.LittleEndian.Uint32(buf)
	//	result := math.Float32frombits(bits)

	//	return result
	return d.reader.ReadFloat()
}

func (d *PlainDecoder) ReadDouble() float64 {
	//	buf := make([]byte, 8)
	//	reader.Read(buf)
	//	bits := binary.LittleEndian.Uint64(buf)
	//	result := math.Float64frombits(bits)

	//	return result
	return d.reader.ReadDouble()
}

func (d *PlainDecoder) ReadString() string {
	//	length := d.ReadInt(reader)
	//	buf := make([]byte, length)

	//	reader.Read(buf)

	//	return buf
	return d.reader.ReadString()
}
