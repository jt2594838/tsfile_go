package decoder

import (
	"encoding/binary"
	"log"
	"math"
	"os"
	"tsfile/encoding/common"
)

type PlainDecoder struct {
	EndianType common.EndianType
}

func (d *PlainDecoder) HasNext(reader *os.File) bool {
	//	reader.pos < reader.size

	return false
}

func (d *PlainDecoder) ReadBool(reader *os.File) bool {
	buf := make([]byte, 1)
	reader.Read(buf)

	return (buf[0] == 0)
}

func (d *PlainDecoder) ReadShort(reader *os.File) int16 {
	buf := make([]byte, 2)
	reader.Read(buf)

	if d.EndianType == common.LITTLE_ENDIAN {
		return int16((buf[1] << 8) + buf[0])
	} else {
		log.Println("tsfile-encoding PlainEncoder: current version does not support short value decoding")
	}

	return -1
}

func (d *PlainDecoder) ReadInt(reader *os.File) int {
	buf := make([]byte, 4)
	reader.Read(buf)

	if d.EndianType == common.LITTLE_ENDIAN {
		return int(buf[0]) + int(buf[1]<<8) + int(buf[2]<<16) + int(buf[3]<<24)
	} else {
		log.Println("tsfile-encoding PlainEncoder: current version does not support int value encoding")
	}

	return -1
}

func (d *PlainDecoder) ReadLong(reader *os.File) int64 {
	buf := make([]byte, 8)
	reader.Read(buf)

	var res int64 = 0
	var i uint8 = 0
	for i = 0; i < 8; i++ {
		res += int64(buf[i] << (i * 8))
	}

	return res
}

///	for ix := 0; ix < 8; ix++ {
//		num <<= 8
//		num |= int64(bytes[ix] & 0xff)
//	}

func (d *PlainDecoder) ReadFloat(reader *os.File) float32 {
	buf := make([]byte, 4)
	reader.Read(buf)
	bits := binary.LittleEndian.Uint32(buf)
	result := math.Float32frombits(bits)

	return result
}

func (d *PlainDecoder) ReadDouble(reader *os.File) float64 {
	buf := make([]byte, 8)
	reader.Read(buf)
	bits := binary.LittleEndian.Uint64(buf)
	result := math.Float64frombits(bits)

	return result
}
