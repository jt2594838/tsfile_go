package encoder

import (
	"bytes"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/common/utils"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-10-10 下午2:12
 * @Description:
 */

type PlainEncoder struct {
	tsDataType   constant.TSDataType
	encodeEndian int16
	valueCount   int
}

func (p *PlainEncoder) Encode(value interface{}, buffer *bytes.Buffer) {
	switch {
	case p.tsDataType == constant.BOOLEAN:
		if data, ok := value.(bool); ok {
			p.EncBool(data, buffer)
		}
	case p.tsDataType == constant.INT32:
		if data, ok := value.(int32); ok {
			p.EncInt32(data, buffer)
		}
	case p.tsDataType == constant.INT64:
		if data, ok := value.(int64); ok {
			p.EncInt64(data, buffer)
		}
	case p.tsDataType == constant.FLOAT:
		if data, ok := value.(float32); ok {
			p.EncFloat32(data, buffer)
		}
	case p.tsDataType == constant.DOUBLE:
		if data, ok := value.(float64); ok {
			p.EncFloat64(data, buffer)
		}
	case p.tsDataType == constant.TEXT:
		if data, ok := value.(string); ok {
			p.EncBinary([]byte(data), buffer)
		}
	default:
		log.Error("invalid input encode type: %d", p.tsDataType)
	}
	return
}

func (p *PlainEncoder) EncBool(value bool, buffer *bytes.Buffer) {
	buffer.Write(utils.BoolToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncShort(value int16, buffer *bytes.Buffer) {
	buffer.Write(utils.Int16ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncInt32(value int32, buffer *bytes.Buffer) {
	buffer.Write(utils.Int32ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncInt64(value int64, buffer *bytes.Buffer) {
	buffer.Write(utils.Int64ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncFloat32(value float32, buffer *bytes.Buffer) {
	buffer.Write(utils.Float32ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncFloat64(value float64, buffer *bytes.Buffer) {
	buffer.Write(utils.Float64ToByte(value, p.encodeEndian))
	return
}

func (p *PlainEncoder) EncBinary(value []byte, buffer *bytes.Buffer) {
	p.EncInt32(int32(len(value)), buffer)
	buffer.Write(value)
	return
}

func (p *PlainEncoder) Flush(buffer *bytes.Buffer) {
	return
}

func (p *PlainEncoder) GetMaxByteSize() int64 {
	return 0
}

func (p *PlainEncoder) GetOneItemMaxSize() int {
	switch p.tsDataType {
	case constant.BOOLEAN:
		return 1
	case constant.INT32:
		return 4
	case constant.INT64:
		return 8
	case constant.FLOAT:
		return 4
	case constant.DOUBLE:
		return 8
	case constant.TEXT:
		return 4 + conf.BYTE_SIZE_PER_CHAR*conf.MaxStringLength
	default:
		log.Error("invalid input dataType in plainEncoder. tsDataType: %d", p.tsDataType)

	}
	return 0
}

func NewPlainEncoder(dataType constant.TSDataType) (*PlainEncoder, error) {
	return &PlainEncoder{
		tsDataType:   dataType,
		encodeEndian: 1,
		valueCount:   -1,
	}, nil
}
