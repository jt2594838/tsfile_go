package header

import (
	_ "bufio"
	_ "log"
	_ "os"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type ChunkHeader struct {
	sensor          string
	dataSize        int
	dataType        constant.TSDataType
	compressionType constant.CompressionType
	encodingType    constant.TSEncoding
	numberOfPages   int
	serializedSize  int
}

func (h *ChunkHeader) DeserializeFrom(reader *utils.FileReader) {
	h.sensor = reader.ReadString()
	h.dataSize = reader.ReadInt()
	h.dataType = constant.TSDataType(reader.ReadShort())
	h.numberOfPages = reader.ReadInt()
	h.compressionType = constant.CompressionType(reader.ReadShort())
	h.encodingType = constant.TSEncoding(reader.ReadShort())

	h.serializedSize = constant.INT_LEN + len(h.sensor) + constant.INT_LEN + constant.SHORT_LEN + constant.INT_LEN + constant.SHORT_LEN + constant.SHORT_LEN
}

func (h *ChunkHeader) GetSensor() string {
	return h.sensor
}

func (h *ChunkHeader) GetDataSize() int {
	return h.dataSize
}

func (h *ChunkHeader) GetDataType() constant.TSDataType {
	return h.dataType
}

func (h *ChunkHeader) GetCompressionType() constant.CompressionType {
	return h.compressionType
}

func (h *ChunkHeader) GetEncodingType() constant.TSEncoding {
	return h.encodingType
}

func (h *ChunkHeader) GetNumberOfPages() int {
	return h.numberOfPages
}

func (h *ChunkHeader) GetSerializedSize() int {
	return h.serializedSize
}
