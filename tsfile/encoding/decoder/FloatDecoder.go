package decoder

import (
	_ "bytes"
	"log"
	"math"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type FloatDecoder struct {
	encoding constant.TSEncoding
	dataType constant.TSDataType

	reader        *utils.BytesReader
	decoder       Decoder
	maxPointValue float64
}

func (d *FloatDecoder) Init(data []byte) {
	if d.encoding == constant.RLE {
		if d.dataType == constant.FLOAT {
			d.decoder = &IntRleDecoder{endianType: constant.LITTLE_ENDIAN}
			//log.Println("tsfile-encoding FloatDecoder: init decoder using int-rle and float")
		} else if d.dataType == constant.DOUBLE {
			panic("encoding is not supported by FloatDecoder: RLE + DOUBLE")
			//d.decoder = &LongRleDecoder{endianType: EndianType.LITTLE_ENDIAN}
			//log.Println("tsfile-encoding FloatDecoder: init decoder using long-rle and double")
		} else {
			panic("data type is not supported by FloatDecoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else if d.encoding == constant.TS_2DIFF {
		if d.dataType == constant.FLOAT {
			panic("encoding is not supported by FloatDecoder: TS_2DIFF + FLOAT")
			//d.decoder = &IntDeltaDecoder
			//log.Println("tsfile-encoding FloatDecoder: init decoder using int-delta and float")
		} else if d.dataType == constant.DOUBLE {
			d.decoder = new(LongDeltaDecoder)
			log.Println("tsfile-encoding FloatDecoder: init decoder using long-delta and double")
		} else {
			panic("data type is not supported by FloatDecoder: " + strconv.Itoa(int(d.dataType)))
		}
	} else {
		panic("encoding is not supported by FloatDecoder: " + strconv.Itoa(int(d.encoding)))
	}

	d.reader = utils.NewBytesReader(data)

	maxPointNumber := d.reader.ReadUnsignedVarInt()
	if maxPointNumber <= 0 {
		d.maxPointValue = 1
	} else {
		d.maxPointValue = math.Pow(10.0, float64(maxPointNumber))
	}

	d.decoder.Init(d.reader.Remaining())
}

func (d *FloatDecoder) HasNext() bool {
	if d.decoder == nil {
		return false
	}
	return d.decoder.HasNext()
}

func (d *FloatDecoder) ReadFloat() float32 {
	value := d.decoder.ReadInt()
	result := float64(value) / d.maxPointValue

	return float32(result)
}

func (d *FloatDecoder) ReadDouble() float64 {
	value := d.decoder.ReadLong()
	result := float64(value) / d.maxPointValue

	return result
}

func (d *FloatDecoder) ReadString() string {
	panic("ReadString not supported by FloatDecoder")
}

func (d *FloatDecoder) ReadBool() bool {
	panic("ReadBoolean not supported by FloatDecoder")
}

func (d *FloatDecoder) ReadShort() int16 {
	panic("ReadShort not supported by FloatDecoder")
}

func (d *FloatDecoder) ReadInt() int {
	panic("ReadInt not supported by FloatDecoder")
}

func (d *FloatDecoder) ReadLong() int64 {
	panic("ReadLong not supported by FloatDecoder")
}
