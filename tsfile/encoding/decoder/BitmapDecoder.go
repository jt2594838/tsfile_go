package decoder

import (
	_ "bytes"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type BitmapDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader *utils.BytesReader

	// how many bytes for all encoded data
	length int
	// number of encoded data
	number int
	// number of data left for reading in current buffer
	currentCount int
	// decoder reads all bitmap index from byteCache and save in
	buffer map[int32][]byte
}

func (d *BitmapDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)

	d.length = 0
	d.number = 0
	d.currentCount = 0
}

func (d *BitmapDecoder) ReadInt() int32 {
	if d.currentCount == 0 {
		// reset
		d.length = 0
		d.number = 0
		d.buffer = make(map[int32][]byte)

		// getLengthAndNumber
		d.length = int(d.reader.ReadUnsignedVarInt())
		d.number = int(d.reader.ReadUnsignedVarInt())

		d.readPackage()
	}

	var result int32 = 0
	index := (d.number - d.currentCount) / 8
	offset := 7 - ((d.number - d.currentCount) % 8)
	for k, v := range d.buffer {
		if v[index]&(1<<uint(offset)) != 0 {
			result = k
			break
		}
	}

	d.currentCount--

	return result
}

func (d *BitmapDecoder) readPackage() {
	packageReader := utils.NewBytesReader(d.reader.ReadSlice(int(d.length)))

	len := (d.number + 7) / 8
	for packageReader.Len() > 0 {
		value := packageReader.ReadUnsignedVarInt()
		data := packageReader.ReadBytes(len)

		d.buffer[value] = data
	}

	d.currentCount = d.number
}

func (d *BitmapDecoder) ReadBool() bool {
	panic("ReadBool not supported by BitmapDecoder")
}
func (d *BitmapDecoder) ReadShort() int16 {
	panic("ReadShort not supported by BitmapDecoder")
}
func (d *BitmapDecoder) ReadLong() int64 {
	panic("ReadLong not supported by BitmapDecoder")
}
func (d *BitmapDecoder) ReadFloat() float32 {
	panic("ReadFloat not supported by BitmapDecoder")
}
func (d *BitmapDecoder) ReadDouble() float64 {
	panic("ReadDouble not supported by BitmapDecoder")
}
func (d *BitmapDecoder) ReadString() string {
	panic("ReadString not supported by BitmapDecoder")
}
