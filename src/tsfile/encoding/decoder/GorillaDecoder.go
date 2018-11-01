package decoder

import (
	_ "bytes"
	_ "tsfile/common/constant"
	"tsfile/common/utils"
)

type GorillaDecoder struct {
	leadingZeroNum, tailingZeroNum int32
	buffer                         int32
	numberLeftInBuffer             int32
}

func (g *GorillaDecoder) readBit(reader *utils.BytesReader) bool {
	if g.numberLeftInBuffer == 0 {
		g.fillBuffer(reader)
	}

	g.numberLeftInBuffer--
	return ((g.buffer >> uint32(g.numberLeftInBuffer)) & 1) == 1
}

func (g *GorillaDecoder) fillBuffer(reader *utils.BytesReader) {
	g.buffer = reader.Read()
	g.numberLeftInBuffer = 8
}

func (g *GorillaDecoder) readIntFromStream(reader *utils.BytesReader, len int) int32 {
	var num int32 = 0
	for i := 0; i < len; i++ {
		var bit int32
		if g.readBit(reader) {
			bit = 1
		} else {
			bit = 0
		}
		num |= bit << uint(len-1-i)
	}
	return num
}

func (g *GorillaDecoder) readLongFromStream(reader *utils.BytesReader, len int) int64 {
	var num int64 = 0
	for i := 0; i < len; i++ {
		var bit int64
		if g.readBit(reader) {
			bit = 1
		} else {
			bit = 0
		}
		num |= bit << uint(len-1-i)
	}
	return num
}
