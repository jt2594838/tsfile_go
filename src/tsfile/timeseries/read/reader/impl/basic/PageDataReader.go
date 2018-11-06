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
	timeInputStreamLength := reader.ReadUnsignedVarInt()
	pos := reader.Pos()

	r.TimeDecoder.Init(data[pos : timeInputStreamLength+pos])
	r.ValueDecoder.Init(data[timeInputStreamLength+pos:])
}

func (r *PageDataReader) HasNext() bool {
	return r.TimeDecoder.HasNext() && r.ValueDecoder.HasNext()
}

func (r *PageDataReader) Next2(pair *datatype.TimeValuePair) error {
	pair.Timestamp = r.TimeDecoder.NextInt64()
	pair.Value = r.ValueDecoder.Next()
	return nil
	//return &datatype.TimeValuePair{Timestamp: r.TimeDecoder.Next().(int64), Value: r.ValueDecoder.Next()}, nil
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

func NewPageDataReader(dataType constant.TSDataType,
	valueDecoder decoder.Decoder,
	timeDecoder decoder.Decoder) *PageDataReader {
	return &PageDataReader{DataType: dataType,
		ValueDecoder: valueDecoder,
		TimeDecoder:  timeDecoder}
}
