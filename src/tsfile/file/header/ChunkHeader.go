package header

import (
	_ "bufio"
	_ "log"
	_ "os"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"bytes"
)

type ChunkHeader struct {
	sensor           string
	dataSize         int
	dataType         constant.TSDataType
	compressionType  constant.CompressionType
	encodingType     constant.TSEncoding
	numberOfPages    int
	maxTombstoneTime int64
	serializedSize   int
}

func (h *ChunkHeader) Deserialize(reader *utils.FileReader) {
	h.sensor = reader.ReadString()
	h.dataSize = int(reader.ReadInt())
	h.dataType = constant.TSDataType(reader.ReadShort())
	h.numberOfPages = int(reader.ReadInt())
	h.compressionType = constant.CompressionType(reader.ReadShort())
	h.encodingType = constant.TSEncoding(reader.ReadShort())
	h.maxTombstoneTime = reader.ReadLong()

	h.serializedSize = (constant.INT_LEN + len(h.sensor) + constant.INT_LEN + constant.SHORT_LEN + constant.INT_LEN + constant.SHORT_LEN + constant.SHORT_LEN + constant.LONG_LEN)
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

func (h *ChunkHeader) GetMaxTombstoneTime() int64 {
	return h.maxTombstoneTime
}

func (h *ChunkHeader) GetSerializedSize() int {
	return h.serializedSize
}

func (c *ChunkHeader) ChunkHeaderToMemory(buffer *bytes.Buffer)(int32){
	// write chunk header to buffer
	buffer.Write(utils.Int32ToByte(int32(len(c.sensor)), 0))
	buffer.Write([]byte(c.sensor))
	buffer.Write(utils.Int32ToByte(int32(c.dataSize), 0))
	buffer.Write(utils.Int16ToByte(int16(c.dataType), 0))
	buffer.Write(utils.Int32ToByte(int32(c.numberOfPages), 0))
	buffer.Write(utils.Int16ToByte(int16(c.compressionType), 0))
	buffer.Write(utils.Int16ToByte(int16(c.encodingType), 0))
	buffer.Write(utils.Int64ToByte(c.maxTombstoneTime, 0))
	return int32(c.serializedSize)
}

func (c *ChunkHeader) SetMaxTombstoneTime (mtt int64) () {
	c.maxTombstoneTime = mtt
}

func GetChunkSerializedSize (sensorId string) (int) {
	return 3 * 4 + 3 * 2 + len(sensorId) + 8
}

func NewChunkHeader(sId string, pbs int, tdt int16, ct int16, et int16, nop int, mtt int64) (*ChunkHeader, error) {
	ss := 3 * 4 + 3 * 2 + len(sId) + 8
	return &ChunkHeader{
		sensor:sId,
		dataSize:pbs,
		dataType:constant.TSDataType(tdt),
		compressionType:constant.CompressionType(ct),
		encodingType:constant.TSEncoding(et),
		numberOfPages:nop,
		serializedSize:ss,
		maxTombstoneTime:mtt,
	},nil
}