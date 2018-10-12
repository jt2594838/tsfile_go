package encoder

import (
	"tsfile/common/log"
	"bytes"
	"tsfile/common/utils"
	"tsfile/common/constant"
	"tsfile/common/conf"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-10-10 下午2:12
 * @Description:
 */

type PlainEncoder struct {
	tsDataType   int16
	endianType   int16
	encodeEndian int16
	valueCount   int
}

func (p *PlainEncoder) Encode(value interface{}, buffer *bytes.Buffer) () {
	log.Info("enter PlainEncoder!!")
	switch {
	case p.tsDataType == int16(constant.BOOLEAN):
		if data, ok := value.(bool); ok {
			p.EncBool(data, buffer)
		}
	case p.tsDataType == int16(constant.INT32):
		if data, ok := value.(int32); ok {
			p.EncInt32(data, buffer)
		}
	case p.tsDataType == int16(constant.INT64):
		if data, ok := value.(int64); ok {
			if p.valueCount == -1 {
				aa := []byte{24}
				buffer.Write(aa)
				p.EncInt64(data, buffer)
				p.EncInt64(data, buffer)
				p.EncInt64(data, buffer)
				p.valueCount += 1
			}
			if p.valueCount == conf.DeltaBlockSize {
				p.EncInt64(data, buffer)
				p.EncInt64(data, buffer)
				p.EncInt64(data, buffer)
				p.valueCount = 0
			} else {
				p.valueCount += 1
			}

		}
	case p.tsDataType == int16(constant.FLOAT):
		if data, ok := value.(float32); ok {
			p.EncFloat32(data, buffer)
		}
	case p.tsDataType == int16(constant.DOUBLE):
		if data, ok := value.(float64); ok {
			p.EncFloat64(data, buffer)
		}
	case p.tsDataType == int16(constant.TEXT):
		if data, ok := value.([]byte); ok {
			p.EncBinary(data, buffer)
		}
	default:
		log.Error("invalid input encode type: %d", p.tsDataType)
	}
	return
}

func (p *PlainEncoder) EncBool(value bool, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input bool value: %d", value)
	buffer.Write(utils.BoolToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncShort(value int16, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int16 value: %d", value)
	buffer.Write(utils.Int16ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncInt32(value int32, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int32 value: %d", value)
	buffer.Write(utils.Int32ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncInt64(value int64, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int64 value: %d", value)
	buffer.Write(utils.Int64ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncFloat32(value float32, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input float32 value: %d", value)
	buffer.Write(utils.Float32ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncFloat64(value float64, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input float64 value: %d", value)
	buffer.Write(utils.Float64ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncBinary(value []byte, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input binary value: %d", value)
	buffer.Write(value)
	return
}

func (p *PlainEncoder) Flush(buffer *bytes.Buffer) () {
	return
}

func (p *PlainEncoder) GetMaxByteSize() (int64) {
	return 0
}

func (p *PlainEncoder) GetOneItemMaxSize() (int) {
	switch p.tsDataType {
	case int16(constant.BOOLEAN):
		return 1
	case int16(constant.INT32):
		return 4
	case int16(constant.INT64):
		return 8
	case int16(constant.FLOAT):
		return 4
	case int16(constant.DOUBLE):
		return 8
	case int16(constant.TEXT):
		return 4 + conf.BYTE_SIZE_PER_CHAR*conf.MaxStringLength
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", p.tsDataType)

	}
	return 0
}

func NewPlainEncoder(tdt int16, endianType int16) (*PlainEncoder, error) {
	return &PlainEncoder{
		tsDataType:   tdt,
		endianType:   endianType,
		encodeEndian: 1,
		valueCount:   -1,
	}, nil
}
