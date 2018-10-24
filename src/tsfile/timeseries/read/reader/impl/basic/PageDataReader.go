package basic

import (
	_ "bytes"
	_ "encoding/binary"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read/datatype"
)

type PageDataReader struct {
	DataType     constant.TSDataType
	ValueDecoder decoder.Decoder
	TimeDecoder  decoder.Decoder
}

func (r *PageDataReader) Read(data []byte) {
	reader := utils.NewBytesReader(data)
	timeInputStreamLength := int(reader.ReadUnsignedVarInt())
	pos := reader.Pos()

	r.TimeDecoder.Init(data[pos : timeInputStreamLength+pos])
	r.ValueDecoder.Init(data[timeInputStreamLength+pos:])
}

func (r *PageDataReader) HasNext() bool {
	return r.TimeDecoder.HasNext() && r.ValueDecoder.HasNext()
}

func (r *PageDataReader) Next() (*datatype.TimeValuePair, error) {
	// TODO: catch errors
	return &datatype.TimeValuePair{Timestamp: r.TimeDecoder.Next().(int64), Value: r.ValueDecoder.Next()}, nil
}

func (r *PageDataReader) Skip() {
	r.Next()
}

func (r *PageDataReader) Close() {
}
