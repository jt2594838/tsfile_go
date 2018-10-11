package encoder

import (
	"tsfile/common/log"
	"bytes"
	"tsfile/common/utils"
	"tsfile/common/constant"
)

/**
 * @Package Name: encoder
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-10-10 下午2:12
 * @Description:
 */

type PlainEncoder struct {
	tsDataType		int16
}

func (i *PlainEncoder) Encode (value interface{}, buffer *bytes.Buffer) () {
	log.Info("enter PlainEncoder!!")
	switch {
	case i.tsDataType == int16(constant.BOOLEAN):
		if data, ok := value.(bool); ok {
			i.EncBool(data, buffer)
		}
	case i.tsDataType == int16(constant.INT32):
		if data, ok := value.(int32); ok {
			i.EncInt32(data, buffer)
		}
	case i.tsDataType == int16(constant.FLOAT):
		if data, ok := value.(float32); ok {
			i.EncFloat32(data, buffer)
		}
	}
	return
}

func (i *PlainEncoder) EncBool (value bool, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input bool value: %d", value)
	buffer.Write(utils.BoolToByte(value))
	return
}

func (i *PlainEncoder) EncShort (value int16, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int16 value: %d", value)
	buffer.Write(utils.Int16ToByte(value))
	return
}

func (i *PlainEncoder) EncInt32 (value int32, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int32 value: %d", value)
	buffer.Write(utils.Int32ToByte(value))
	return
}

func (i *PlainEncoder) EncInt64 (value int64, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input int64 value: %d", value)
	buffer.Write(utils.Int64ToByte(value))
	return
}

func (i *PlainEncoder) EncFloat32 (value float32, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input float32 value: %d", value)
	buffer.Write(utils.Float32ToByte(value))
	return
}

func (i *PlainEncoder) EncBinary (value []byte, buffer *bytes.Buffer) () {
	log.Info("final enc ok! input float32 value: %d", value)
	buffer.Write(value)
	return
}

func NewPlainEncoder(tdt int16) (*PlainEncoder, error) {
	return &PlainEncoder{
		tsDataType:tdt,
	},nil
}