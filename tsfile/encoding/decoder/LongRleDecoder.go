package decoder

import (
	_ "bytes"
	_ "log"
	_ "strconv"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/encoding/bitpacking"
)

// rle-bit-packing-hybrid: <length> <bitwidth> <encoded-data>
// 		length := length of the <bitwidth> <encoded-data> in bytes stored as 4 bytes little endian
// 		bitwidth := bitwidth for all encoded data in <encoded-data>
// 		encoded-data := <bit-packed-run> | <rle-run>
// 			bit-packed-run := <bit-packed-header> <lastBitPackedNum> <bit-packed-values>
// 				bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
// 				lastBitPackedNum := the number of useful value in last bit-pack may be less than 8, so lastBitPackedNum indicates how many values are useful
// 				bit-packed-values :=  bit packed
// 			rle-run := <rle-header> <repeated-value>
// 				rle-header := varint-encode( (number of times repeated) << 1)
// 				repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)

type LongRleDecoder struct {
	endianType constant.EndianType

	reader *utils.BytesReader
	packer *bitpacking.LongPacker

	packageReader *utils.BytesReader
	// how many bytes for all encoded data
	length int
	// bit width for bit-packing and rle to decode
	bitWidth int
	// number of data left for reading in current buffer
	currentCount int
	// mode to indicate current encoding type
	mode int
	// number of bit-packing group in which is saved in header
	bitPackingNum int

	currentValue  int64
	decodedValues []int64

	isReadingBegan bool
}

func (d *LongRleDecoder) Init(data []byte) {
	d.reader = utils.NewBytesReader(data)
	d.currentCount = 0
	d.currentValue = 0
	d.isReadingBegan = false
}

func (d *LongRleDecoder) HasNext() bool {
	if d.currentCount > 0 || d.reader.Len() > 0 || d.packageReader.Len() > 0 {
		return true
	}
	return false
}

func (d *LongRleDecoder) ReadBool() bool {
	return (d.ReadLong() == 0)
}

func (d *LongRleDecoder) ReadLong() int64 {
	if !d.isReadingBegan {
		// read length and bit width of current package before we decode number
		d.length = int(d.reader.ReadUnsignedVarInt())

		d.packageReader = utils.NewBytesReader(d.reader.ReadSlice(d.length))
		d.bitWidth = int(d.packageReader.Read())

		d.packer = &bitpacking.LongPacker{BitWidth: d.bitWidth}

		d.isReadingBegan = true
	}

	if d.currentCount == 0 {
		d.readPackage()
	}

	d.currentCount--

	var result int64 = 0
	switch d.mode {
	case RLE:
		result = d.currentValue
		break
	case BIT_PACKED:
		result = d.decodedValues[d.bitPackingNum-d.currentCount-1]
		break
	default:
		panic("tsfile-encoding LongRleDecoder: not a valid mode")
	}

	if d.packageReader.Len() <= 0 {
		d.isReadingBegan = false
	}

	return result
}

func (d *LongRleDecoder) readPackage() {
	header := int(d.packageReader.ReadUnsignedVarInt())
	if (header & 1) == 0 {
		d.mode = RLE
	} else {
		d.mode = BIT_PACKED
	}

	switch d.mode {
	case RLE:
		d.currentCount = header >> 1
		d.currentValue = d.readLongLittleEndianPaddedOnBitWidth(d.packageReader, d.bitWidth)

	case BIT_PACKED:
		bitPackedGroupCount := header >> 1
		// in last bit-packing group, there may be some useless value, lastBitPackedNum indicates how many values is useful
		lastBitPackedNum := int(d.packageReader.Read())
		if bitPackedGroupCount > 0 {
			d.currentCount = (bitPackedGroupCount-1)*conf.RLE_MIN_REPEATED_NUM + lastBitPackedNum
			d.bitPackingNum = d.currentCount
		} else {
			panic("tsfile-encoding LongRleDecoder: bitPackedGroupCount smaller than 1")
		}

		d.readBitPackingBuffer(bitPackedGroupCount, lastBitPackedNum, d.bitWidth)
	default:
		panic("tsfile-encoding LongRleDecoder: unknown encoding mode")
	}
}

// unpack all values from packageReader into decodedValues
func (d *LongRleDecoder) readBitPackingBuffer(bitPackedGroupCount int, lastBitPackedNum int, bitWidth int) {
	bytesToRead := bitPackedGroupCount * bitWidth
	if bytesToRead > d.packageReader.Len() {
		bytesToRead = d.packageReader.Len()
	}
	bytes := d.packageReader.ReadSlice(int(bytesToRead))

	d.decodedValues = make([]int64, bitPackedGroupCount*(conf.RLE_MIN_REPEATED_NUM))
	d.packer.UnpackAllValues(bytes, bytesToRead, d.decodedValues)
}

func (r *LongRleDecoder) readLongLittleEndianPaddedOnBitWidth(reader *utils.BytesReader, bitWidth int) int64 {
	paddedByteNum := (bitWidth + 7) / 8
	if paddedByteNum > 8 {
		panic("ReadLongLittleEndianPaddedOnBitWidth(): encountered value that requires more than 4 bytes")
	}

	var result int64 = 0
	for i := 0; i < paddedByteNum; i++ {
		ch := reader.Read()
		result <<= 8
		result |= int64(ch & 0xff)
	}
	return result
}

func (d *LongRleDecoder) ReadShort() int16 {
	panic("ReadShort not supported by LongRleDecoder")
}

func (d *LongRleDecoder) ReadInt() int32 {
	panic("ReadLong not supported by LongRleDecoder")
}

func (d *LongRleDecoder) ReadFloat() float32 {
	panic("ReadFloat not supported by LongRleDecoder")
}

func (d *LongRleDecoder) ReadDouble() float64 {
	panic("ReadDouble not supported by LongRleDecoder")
}

func (d *LongRleDecoder) ReadString() string {
	panic("ReadString not supported by LongRleDecoder")
}
