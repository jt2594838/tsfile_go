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
	length int32
	// number of encoded data
	number int32
	// number of data left for reading in current buffer
	currentCount int32
	// decoder reads all bitmap index from byteCache and save in
	buffer map[int32][]byte
}

func (d *BitmapDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)

	d.length = 0
	d.number = 0
	d.currentCount = 0
}

func (d *BitmapDecoder) NextInt64() int64 {
	return 0
}

func (d *BitmapDecoder) Next() interface{} {
	if d.currentCount == 0 {
		// reset
		d.length = 0
		d.number = 0
		d.buffer = make(map[int32][]byte)

		// getLengthAndNumber
		d.length = d.reader.ReadUnsignedVarInt()
		d.number = d.reader.ReadUnsignedVarInt()

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
	packageReader := utils.NewBytesReader(d.reader.ReadSlice(d.length))

	len := int32((d.number + 7) / 8)
	for packageReader.Len() > 0 {
		value := packageReader.ReadUnsignedVarInt()
		data := packageReader.ReadBytes(len)

		d.buffer[value] = data
	}

	d.currentCount = d.number
}
