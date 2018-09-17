package metadata

import (
	"container/list"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type RowGroupMetaData struct {
	device                        string
	totalByteSize                 int64
	fileOffsetOfCorrespondingData int64
	serializedSize                int
	timeSeriesChunkMetaDataList   *list.List
}

func (f *RowGroupMetaData) Deserialize(reader *utils.BytesReader) {
	f.device = reader.ReadString()
	f.totalByteSize = reader.ReadLong()
	f.fileOffsetOfCorrespondingData = reader.ReadLong()
	size := int(reader.ReadInt())

	f.serializedSize = constant.INT_LEN + len(f.device) + constant.LONG_LEN + constant.INT_LEN

	f.timeSeriesChunkMetaDataList = list.New()
	for i := 0; i < size; i++ {
		chunkMetaData := new(ChunkMetaData)
		chunkMetaData.Deserialize(reader)

		f.timeSeriesChunkMetaDataList.PushBack(chunkMetaData)
		f.serializedSize += chunkMetaData.GetSerializedSize()
	}
}

func (f *RowGroupMetaData) GetSerializedSize() int {
	return f.serializedSize
}
