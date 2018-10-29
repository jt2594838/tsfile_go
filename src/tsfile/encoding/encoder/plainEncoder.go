package encoder

import (
	"bytes"
	"encoding/binary"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	_ "tsfile/common/utils"
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
	switch p.tsDataType {
	case constant.BOOLEAN, constant.INT32, constant.INT64, constant.FLOAT, constant.DOUBLE:
		if p.encodeEndian == 0 {
			_ = binary.Write(buffer, binary.BigEndian, value)
		} else {
			_ = binary.Write(buffer, binary.LittleEndian, value)
		}
	case constant.TEXT:
		if data, ok := value.(string); ok {
			if p.encodeEndian == 0 {
				_ = binary.Write(buffer, binary.BigEndian, int32(len(data)))
			} else {
				_ = binary.Write(buffer, binary.LittleEndian, int32(len(data)))
			}
			buffer.Write([]byte(data))
		}
	default:
		log.Error("invalid input encode type: %d", p.tsDataType)
	}
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
