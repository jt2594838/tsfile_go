package metadata

import (
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type ChunkMetaData struct {
	sensor                        string
	fileOffsetOfCorrespondingData int64
	numOfPoints                   int64
	totalByteSizeOfPagesOnDisk    int64
	startTime                     int64
	endTime                       int64
	valuesStatistics              *Digest
}

func (c *ChunkMetaData) TotalByteSizeOfPagesOnDisk() int64 {
	return c.totalByteSizeOfPagesOnDisk
}

func (c *ChunkMetaData) FileOffsetOfCorrespondingData() int64 {
	return c.fileOffsetOfCorrespondingData
}

func (f *ChunkMetaData) Deserialize(reader *utils.BytesReader) {
	f.sensor = reader.ReadString()
	f.fileOffsetOfCorrespondingData = reader.ReadLong()
	f.numOfPoints = reader.ReadLong()
	f.totalByteSizeOfPagesOnDisk = reader.ReadLong()
	f.startTime = reader.ReadLong()
	f.endTime = reader.ReadLong()

	digest := new(Digest)
	digest.Deserialize(reader)

	f.valuesStatistics = digest
}

func (f *ChunkMetaData) GetSerializedSize() int {
	size_statistics := 4
	if f.valuesStatistics != nil {
		size_statistics = f.valuesStatistics.GetSerializedSize()
	}

	return constant.INT_LEN + len(f.sensor) + 5*constant.LONG_LEN + size_statistics
}
