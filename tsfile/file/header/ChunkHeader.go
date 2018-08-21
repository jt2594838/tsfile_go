package header

import (
	//	"bufio"
	//"log"
	"os"
	"tsfile/common/utils"
	"tsfile/file/metadata/enums"
)

type ChunkHeader struct {
	MeasurementID   string
	DataSize        int
	DataType        enums.TSDataType
	CompressionType enums.CompressionType
	EncodingType    enums.TSEncoding
	NumberOfPages   int
	SerializedSize  int
}

func (f *ChunkHeader) DeserializeFrom(reader *os.File) {
	f.MeasurementID = utils.ReadString(reader)
	f.DataSize = utils.ReadInt(reader)
	f.DataType = enums.TSDataType(utils.ReadShort(reader))
	f.NumberOfPages = utils.ReadInt(reader)
	f.CompressionType = enums.CompressionType(utils.ReadShort(reader))
	f.EncodingType = enums.TSEncoding(utils.ReadShort(reader))

	f.SerializedSize = utils.INT_LEN + len(f.MeasurementID) + utils.LONG_LEN + utils.SHORT_LEN + utils.INT_LEN + utils.SHORT_LEN + utils.SHORT_LEN
}
