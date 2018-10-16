package encoder

import (
	"bytes"
	"math"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type LongDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	blockSize int32
	width     int32
	index     int32

	baseValue     int64
	firstValue    int64
	previousValue int64
	encodedValues []int64
}

func (d *LongDeltaEncoder) Init() {
	d.blockSize = BLOCK_DEFAULT_SIZE
	d.index = -1
	d.firstValue = 0
	d.previousValue = 0
	d.baseValue = math.MaxInt64
	d.encodedValues = make([]int64, d.blockSize)
}

func (d *LongDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	value := v.(int64)

	if d.index == -1 {
		d.index++
		d.firstValue = value
		d.previousValue = d.firstValue
		return
	}

	// calculate delta
	delta := value - d.previousValue
	if delta < d.baseValue {
		d.baseValue = delta
	}
	d.encodedValues[d.index] = delta
	d.index++

	d.previousValue = value
	if d.index == d.blockSize {
		d.Flush(buffer)
	}
}

func (d *LongDeltaEncoder) Flush(buffer *bytes.Buffer) {
	if d.index != -1 {
		// since we store the min delta, the deltas will be converted to be the difference to min delta and all positive
		for i := 0; i < int(d.index); i++ {
			d.encodedValues[i] = d.encodedValues[i] - d.baseValue
		}

		w := int32(0)
		for i := int32(0); i < d.index; i++ {
			valueWidth := int32(32) - utils.NumberOfLeadingZerosLong(d.encodedValues[i])
			if valueWidth > w {
				w = valueWidth
			}
		}

		d.width = w

		//write header
		buffer.Write(utils.Int32ToByte(d.index, int16(constant.BIG_ENDIAN)))
		buffer.Write(utils.Int32ToByte(d.width, int16(constant.BIG_ENDIAN)))
		buffer.Write(utils.Int64ToByte(d.baseValue, int16(constant.BIG_ENDIAN)))
		buffer.Write(utils.Int64ToByte(d.firstValue, int16(constant.BIG_ENDIAN)))

		//write data with min width
		if encodingLength := int(math.Ceil(float64(d.index * d.width) / 8.0)); encodingLength > 0 {
			encodingBlockBuffer := make([]byte, encodingLength)
			for i := int32(0); i < d.index; i++ {
				utils.LongToBytes(d.encodedValues[i], encodingBlockBuffer, int(d.width*i), int(d.width))
			}

			buffer.Write(encodingBlockBuffer)
		}

		d.reset()
	}
}

func (d *LongDeltaEncoder) GetMaxByteSize() int64 {
	return int64(24 + d.index*8)
}

func (d *LongDeltaEncoder) GetOneItemMaxSize() int {
	return 8
}

func (d *LongDeltaEncoder) reset() {
	d.blockSize = BLOCK_DEFAULT_SIZE
	d.index = -1
	d.firstValue = 0
	d.previousValue = 0
	d.baseValue = math.MaxInt64
	d.encodedValues = make([]int64, d.blockSize)
}

func NewLongDeltaEncoder(dataType constant.TSDataType) (*LongDeltaEncoder) {
	d := &LongDeltaEncoder{dataType:dataType}
	d.reset()

	return d
}