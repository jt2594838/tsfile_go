package tsFileWriter

/**
 * @Package Name: valueWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-31 下午4:51
 * @Description:
 */

import (
	"bytes"
	"tsfile/common/conf"
	"tsfile/common/utils"
	"tsfile/encoding/encoder"
	"tsfile/timeseries/write/sensorDescriptor"
	"tsfile/common/log"
)

type ValueWriter struct {
	// time
	timeEncoder  interface{}
	valueEncoder interface{}
	timeBuf      *bytes.Buffer
	valueBuf     *bytes.Buffer
	desc         *sensorDescriptor.SensorDescriptor
	//buf := bytes.NewBuffer([]byte{})
}

func (v *ValueWriter) GetCurrentMemSize() int {
	var sizeT int64 = 0
	var sizeV int64 = 0
	if encT, ok := v.timeEncoder.(encoder.Encoder); ok {
		sizeT = encT.GetMaxByteSize()
	}
	if encV, ok := v.valueEncoder.(encoder.Encoder); ok {
		sizeV = encV.GetMaxByteSize()
	}
	return v.timeBuf.Len() + v.valueBuf.Len() + int(sizeT) + int(sizeV)
}

func (v *ValueWriter) PrepareEndWriteOnePage() {
	if encT, ok := v.timeEncoder.(encoder.Encoder); ok {
		encT.Flush(v.timeBuf)
	}
	if encV, ok := v.valueEncoder.(encoder.Encoder); ok {
		encV.Flush(v.valueBuf)
	}
	return
}

func (v *ValueWriter) GetByteBuffer() *bytes.Buffer {
	v.PrepareEndWriteOnePage()
	timeSize := v.timeBuf.Len()
	encodeBuffer := bytes.NewBuffer([]byte{})

	// write timeBuf size
	utils.WriteUnsignedVarInt(int32(timeSize), encodeBuffer)

	//声明一个空的slice,容量为timebuf的长度
	timeSlice := make([]byte, timeSize)
	//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
	v.timeBuf.Read(timeSlice)
	encodeBuffer.Write(timeSlice)

	//声明一个空的value slice,容量为valuebuf的长度
	valueSlice := make([]byte, v.valueBuf.Len())
	//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
	v.valueBuf.Read(valueSlice)
	encodeBuffer.Write(valueSlice)

	return encodeBuffer
}

// write with encoder
func (v *ValueWriter) Write(t int64, tdt int16, value interface{}, valueCount int) {

	if encT, ok := v.timeEncoder.(encoder.Encoder); ok {
		//if valueCount == 0 {
		//	encT.Encode(t, v.timeBuf)
		//	encT.Encode(t, v.timeBuf)
		//	encT.Encode(t, v.timeBuf)
		//}
		//if v.desc.GetTimeCount() == conf.DeltaBlockSize {
		//	encT.Encode(t, v.timeBuf)
		//	encT.Encode(t, v.timeBuf)
		//	encT.Encode(t, v.timeBuf)
		//
		//	v.desc.SetTimeCount(0)
		//}
		encT.Encode(t, v.timeBuf)
	}

	if encV, ok := v.valueEncoder.(encoder.Encoder); ok {

		switch tdt {
		case 0:
			// bool
			if data, ok := value.(bool); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 1:
			//int32
			if data, ok := value.(int32); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 2:
			//int64
			if data, ok := value.(int64); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 3:
			//float
			if data, ok := value.(float32); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 4:
			//double
			if data, ok := value.(float64); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 5:
			//text
			if data, ok := value.(string); ok {
				// encode
				encV.Encode(data, v.valueBuf)
			}
		case 6:
			//fixed_len_byte_array
		case 7:
			//enums
		case 8:
			//bigdecimal
		default:
			// int32
		}
	}
	log.Info("askdfjalskdfffffffffffffffffffffffffffffffff")
	return
}

// write without encoder
func (v *ValueWriter) WriteWithoutEnc(t int64, tdt int16, value interface{}, valueCount int) {
	var timeByteData []byte
	var valueByteData []byte
	switch tdt {
	case 0:
		// bool
		if data, ok := value.(bool); ok {
			// encode
			valueByteData = utils.BoolToByte(data, 1)
		}
	case 1:
		//int32
		if data, ok := value.(int32); ok {
			valueByteData = utils.Int32ToByte(data, 1)
		}
	case 2:
		//int64
		if data, ok := value.(int64); ok {
			valueByteData = utils.Int64ToByte(data, 1)
		}

	case 3:
		//float
		if data, ok := value.(float32); ok {
			valueByteData = utils.Float32ToByte(data, 1)
		}
	case 4:
		//double , float64 in golang as double in c
		if data, ok := value.(float64); ok {
			valueByteData = utils.Float64ToByte(data, 1)
		}
	case 5:
		//text
		if data, ok := value.(string); ok {
			valueByteData = []byte(data)
		}
	case 6:
		//fixed_len_byte_array
	case 7:
		//enums
	case 8:
		//bigdecimal
	default:
		// int32
	}
	// write time to byteBuffer
	timeByteData = utils.Int64ToByte(t, 1)

	// write to byteBuffer
	if valueCount == 0 {
		aa := []byte{24}
		v.timeBuf.Write(aa)
		//s.timeBuf.Write(utils.BoolToByte(true))
		//v.timeBuf.Write(timeByteData)
		//v.timeBuf.Write(timeByteData)
		//v.timeBuf.Write(timeByteData)
		//s.desc.SetTimeCount(encodeCount + 1)
	}
	v.timeBuf.Write(timeByteData)
	if v.desc.GetTimeCount() == conf.DeltaBlockSize {
		v.timeBuf.Write(timeByteData)
		v.timeBuf.Write(timeByteData)
		v.timeBuf.Write(timeByteData)
		v.desc.SetTimeCount(0)
	}
	// log.Info("s.timeBuf size: %d", s.timeBuf.Len())
	// write value to byteBuffer
	v.valueBuf.Write(valueByteData)
	// log.Info("s.valueBuf size: %d", s.valueBuf.Len())
	return
}

func (v *ValueWriter) Reset() {
	v.timeBuf.Reset()
	v.valueBuf.Reset()
	return
}

func NewValueWriter(d *sensorDescriptor.SensorDescriptor) (*ValueWriter, error) {
	//tEnc := encoder.GetEncoder(d.GetTsEncoding(), int16(constant.INT64))
	//vEnc := encoder.GetEncoder(d.GetTsEncoding(), d.GetTsDataType())
	tEnc := d.GetTimeEncoder()
	vEnc := d.GetValueEncoder()

	return &ValueWriter{
		//sensorId:sId,
		timeBuf:      bytes.NewBuffer([]byte{}),
		valueBuf:     bytes.NewBuffer([]byte{}),
		desc:         d,
		timeEncoder:  tEnc,
		valueEncoder: vEnc,
	}, nil
}
