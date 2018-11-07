package decoder

import (
	_ "bytes"
	_ "tsfile/common/constant"
	"tsfile/common/utils"
)

type GorillaDecoder struct {
	leadingZeroNum, tailingZeroNum int32
	buffer                         byte
	numberLeftInBuffer             uint32
}

func (g *GorillaDecoder) readBit(reader *utils.BytesReader) bool {
	if g.numberLeftInBuffer == 0 {
		g.fillBuffer(reader)
	}

	g.numberLeftInBuffer--
	return ((g.buffer >> uint32(g.numberLeftInBuffer)) & 1) == 1
}

func (g *GorillaDecoder) fillBuffer(reader *utils.BytesReader) {
	g.buffer = reader.ReadByte()
	g.numberLeftInBuffer = 8
}

func (g *GorillaDecoder) readIntFromStream(reader *utils.BytesReader, len int32) int32 {
	var num int32 = 0
	var iTemp uint32 = uint32(len - 1)
	for i := int32(0); i < len; i++ {
		if g.readBit(reader) {
			num |= 1 << iTemp
		}
		iTemp--
	}
	return num
}

func (g *GorillaDecoder) readLongFromStream(reader *utils.BytesReader, len int32) int64 {
	var num int64 = 0
	var iTemp uint32 = uint32(len - 1)
	for i := int32(0); i < len; i++ {
		if g.readBit(reader) {
			num |= 1 << iTemp
		}
		iTemp--
	}
	return num
}
