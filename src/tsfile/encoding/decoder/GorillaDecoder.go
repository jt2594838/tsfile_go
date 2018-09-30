package decoder

import (
	_ "bytes"
	_ "tsfile/common/constant"
	"tsfile/common/utils"
)

type GorillaDecoder struct {
	leadingZeroNum, tailingZeroNum int32
	buffer                         int32
	numberLeftInBuffer             int
	nextFlag1                      bool
	nextFlag2                      bool
}

func (g *GorillaDecoder) readBit(reader *utils.BytesReader) bool {
	if g.numberLeftInBuffer == 0 {
		g.fillBuffer(reader)
	}

	g.numberLeftInBuffer--
	return ((g.buffer >> uint(g.numberLeftInBuffer)) & 1) == 1
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

func (g *GorillaDecoder) numberOfLeadingZeros(i int32) int32 {
	if i == 0 {
		return 32
	}

	var n int32 = 1
	if i>>16 == 0 {
		n += 16
		i <<= 16
	}
	if i>>24 == 0 {
		n += 8
		i <<= 8
	}
	if i>>28 == 0 {
		n += 4
		i <<= 4
	}
	if i>>30 == 0 {
		n += 2
		i <<= 2
	}
	n -= int32(uint32(i) >> 31)

	return n
}

func (g *GorillaDecoder) numberOfTrailingZeros(i int32) int32 {
	if i == 0 {
		return 32
	}

	var y int32
	var n int32 = 31
	y = i << 16
	if y != 0 {
		n = n - 16
		i = y
	}
	y = i << 8
	if y != 0 {
		n = n - 8
		i = y
	}
	y = i << 4
	if y != 0 {
		n = n - 4
		i = y
	}
	y = i << 2
	if y != 0 {
		n = n - 2
		i = y
	}

	return n - int32(uint32(i<<1)>>31)
}

func (g *GorillaDecoder) numberOfLeadingZerosLong(i int64) int32 {
	if i == 0 {
		return 64
	}

	var n int32 = 1
	var x int32 = int32(uint64(i) >> 32)

	if x == 0 {
		n += 32
		x = int32(i)
	}
	if uint32(x)>>16 == 0 {
		n += 16
		x <<= 16
	}
	if uint32(x)>>24 == 0 {
		n += 8
		x <<= 8
	}
	if uint32(x)>>28 == 0 {
		n += 4
		x <<= 4
	}
	if uint32(x)>>30 == 0 {
		n += 2
		x <<= 2
	}
	n -= int32(uint32(x) >> 31)

	return n
}

func (g *GorillaDecoder) numberOfTrailingZerosLong(i int64) int32 {
	if i == 0 {
		return 64
	}

	var x, y int32
	var n int32 = 63
	y = int32(i)

	if y != 0 {
		n = n - 32
		x = y
	} else {
		x = (int32)(uint64(i) >> 32)
	}
	y = x << 16
	if y != 0 {
		n = n - 16
		x = y
	}
	y = x << 8
	if y != 0 {
		n = n - 8
		x = y
	}
	y = x << 4
	if y != 0 {
		n = n - 4
		x = y
	}
	y = x << 2
	if y != 0 {
		n = n - 2
		x = y
	}

	return n - int32(uint32(x<<1)>>31)
}
