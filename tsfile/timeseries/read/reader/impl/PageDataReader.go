package impl

import (
	//	"tsfile/common/utils"
	"tsfile/encoding/decoder"
	"tsfile/file/metadata/enums"
	"tsfile/timeseries/read/datatype"
)

type PageDataReader struct {
	DataType     enums.TSDataType
	ValueDecoder decoder.Decoder
	TimeDecoder  decoder.Decoder

	hasOneCachedTimeValuePair bool
	cachedTimeValuePair       datatype.TimeValuePair
	//InputStream               timestampInputStream //FIXME change to bytebuffer
	//InputStream               valueInputStream     //FIXME change to bytebuffer
}

func (r *PageDataReader) HasNext() bool {
	if r.hasOneCachedTimeValuePair {
		return true
	}

	return false
}

func (r *PageDataReader) Next() datatype.TimeValuePair {
	panic("to be implemented")
}

func (r *PageDataReader) SkipCurrentTimeValuePair() {
}

func (r *PageDataReader) Close() {
}

//func (r *PageDataReader) splitDataToTimeStampAndValue([]byte pageData) {
//    timeInputStreamLength := utils.ReadUnsignedVarInt(pageData);
//    ByteBuffer timeDataBuffer= pageData.slice();
//    timeDataBuffer.limit(timeInputStreamLength);
//    timestampInputStream= new ByteBufferBackedInputStream(timeDataBuffer);

//    ByteBuffer valueDataBuffer= pageData.slice();
//    valueDataBuffer.position(timeInputStreamLength);
//    valueInputStream = new ByteBufferBackedInputStream(valueDataBuffer);
//}
