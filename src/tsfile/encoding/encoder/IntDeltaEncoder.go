package encoder

import (
	"bytes"
	"encoding/binary"
	_ "fmt"
	"math"
	_ "time"
	"tsfile/common/constant"
	_ "tsfile/common/logcost"
	"tsfile/common/utils"
)

const BLOCK_DEFAULT_SIZE = 128

type IntDeltaEncoder struct {
	endianType constant.EndianType
	dataType   constant.TSDataType

	blockSize int32
	width     int32
	index     int32

	baseValue     int32
	firstValue    int32
	previousValue int32
	encodedValues []int32
}

func (d *IntDeltaEncoder) Encode(v interface{}, buffer *bytes.Buffer) {
	value := v.(int32)

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

func (d *IntDeltaEncoder) Flush(buffer *bytes.Buffer) {
	if d.index != -1 {
		// since we store the min delta, the deltas will be converted to be the difference to min delta and all positive
		for i := 0; i < int(d.index); i++ {
			d.encodedValues[i] = d.encodedValues[i] - d.baseValue
			//d.encodedValues[i] -= d.baseValue
		}

		w := int32(0)
		for i := int32(0); i < d.index; i++ {
			valueWidth := int32(32) - utils.NumberOfLeadingZeros(d.encodedValues[i])
			if valueWidth > w {
				w = valueWidth
			}
		}

		d.width = w

		//write header
		binary.Write(buffer, binary.BigEndian, d.index)
		binary.Write(buffer, binary.BigEndian, d.width)
		binary.Write(buffer, binary.BigEndian, d.baseValue)
		binary.Write(buffer, binary.BigEndian, d.firstValue)
		//buffer.Write(utils.Int32ToByte(d.index, int16(constant.BIG_ENDIAN)))
		//buffer.Write(utils.Int32ToByte(d.width, int16(constant.BIG_ENDIAN)))
		//buffer.Write(utils.Int32ToByte(d.baseValue, int16(constant.BIG_ENDIAN)))
		//buffer.Write(utils.Int32ToByte(d.firstValue, int16(constant.BIG_ENDIAN)))

		//write data with min width
		if encodingLength := int(math.Ceil(float64(d.index*d.width) / 8.0)); encodingLength > 0 {
			//encodingBlockBuffer := make([]byte, encodingLength)
			//s1 := make([]byte, 0)
			//buf := bytes.NewBuffer(s1)
			//tsCurWrite1 := time.Now()
			for i := int32(0); i < d.index; i++ {
				//utils.IntToBytes(d.encodedValues[i], encodingBlockBuffer, int(d.width*i), int(d.width))
				//fmt.Printf("value:%d pos:%d  width:%d d.index=%d\n", d.encodedValues[i], d.width*i, d.width, d.index)
				binary.Write(buffer, binary.BigEndian, d.encodedValues[i])
			}
			//fmt.Println(encodingBlockBuffer)
			//fmt.Println(buf.Bytes())
			//logcost.CostWriteTimesTest6 += int64(time.Since(tsCurWrite1))
			//buffer.Write(encodingBlockBuffer)
		}

		d.reset()
	}
}

func (d *IntDeltaEncoder) GetMaxByteSize() int64 {
	return int64(24 + d.index*4)
}

func (d *IntDeltaEncoder) GetOneItemMaxSize() int {
	return 4
}

func (d *IntDeltaEncoder) reset() {
	d.blockSize = BLOCK_DEFAULT_SIZE
	d.index = -1
	d.firstValue = 0
	d.previousValue = 0
	d.baseValue = math.MaxInt32
	d.encodedValues = make([]int32, d.blockSize)
}

func NewIntDeltaEncoder(dataType constant.TSDataType) *IntDeltaEncoder {
	d := &IntDeltaEncoder{dataType: dataType}
	d.reset()

	return d
}
