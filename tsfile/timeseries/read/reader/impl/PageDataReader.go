package impl

import (
	_ "bytes"
	_ "encoding/binary"
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read/datatype"
)

type PageDataReader struct {
	DataType     constant.TSDataType
	ValueDecoder decoder.Decoder
	TimeDecoder  decoder.Decoder

	hasOneCachedTimeValuePair bool
	cachedTimeValuePair       datatype.TimeValuePair
}

func (r *PageDataReader) Read(data []byte) {
	reader := utils.NewBytesReader(data)
	timeInputStreamLength := int(reader.ReadUnsignedVarInt())
	pos := reader.Pos()

	r.TimeDecoder.Init(data[pos : timeInputStreamLength+pos])
	r.ValueDecoder.Init(data[timeInputStreamLength+pos:])

	r.hasOneCachedTimeValuePair = false
}

func (r *PageDataReader) HasNext() bool {
	if r.hasOneCachedTimeValuePair {
		return true
	}

	if r.TimeDecoder.HasNext() && r.ValueDecoder.HasNext() {
		r.cacheOneTimeValuePair()
		r.hasOneCachedTimeValuePair = true
		return true
	}

	return false
}

func (r *PageDataReader) Next() datatype.TimeValuePair {
	if r.HasNext() {
		r.hasOneCachedTimeValuePair = false
		return r.cachedTimeValuePair
	} else {
		panic("No more TimeValuePair in current page")
	}
}

func (r *PageDataReader) SkipCurrentTimeValuePair() {
	r.Next()
}

func (r *PageDataReader) Close() {
}

func (r *PageDataReader) cacheOneTimeValuePair() {
	timestamp := r.TimeDecoder.ReadLong()
	value := r.readOneValue()

	r.cachedTimeValuePair = datatype.TimeValuePair{Timestamp: timestamp, Value: value}
}

func (r *PageDataReader) readOneValue() interface{} {
	switch {
	case r.DataType == constant.BOOLEAN:
		return r.ValueDecoder.ReadBool()
	case r.DataType == constant.INT32:
		return r.ValueDecoder.ReadInt()
	case r.DataType == constant.INT64:
		return r.ValueDecoder.ReadLong()
	case r.DataType == constant.FLOAT:
		return r.ValueDecoder.ReadFloat()
	case r.DataType == constant.DOUBLE:
		return r.ValueDecoder.ReadDouble()
	case r.DataType == constant.TEXT:
		return r.ValueDecoder.ReadString()
	default:
		panic("Unsupported data type :" + strconv.Itoa(int(r.DataType)))
	}
}
