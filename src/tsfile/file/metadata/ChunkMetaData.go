package metadata

import (
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"bytes"
)

type ChunkMetaData struct {
	sensor                        string
	fileOffsetOfCorrespondingData int64
	numOfPoints                   int64
	totalByteSizeOfPagesOnDisk    int64
	startTime                     int64
	endTime                       int64
	valuesStatistics              *TsDigest
}

func (c *ChunkMetaData) Sensor() string {
	return c.sensor
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

	digest := new(TsDigest)
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

func (t *ChunkMetaData) SetDigest (tsDigest *TsDigest) () {
	t.valuesStatistics = tsDigest
}

func (t *ChunkMetaData) GetStartTime () (int64) {
	return t.startTime
}

func (t *ChunkMetaData) GetEndTime () (int64) {
	return t.endTime
}

func (t *ChunkMetaData) SetTotalByteSizeOfPagesOnDisk (size int64) () {
	t.totalByteSizeOfPagesOnDisk = size
}

func (t *ChunkMetaData) SetNumOfPoints (num int64) () {
	t.numOfPoints = num
}

func (t *ChunkMetaData) SerializeTo (buf *bytes.Buffer) (int) {
	var byteLen int

	n1, _ := buf.Write(utils.Int32ToByte(int32(len(t.sensor)), 0))
	byteLen += n1
	n2, _ := buf.Write([]byte(t.sensor))
	byteLen += n2

	n3, _ := buf.Write(utils.Int64ToByte(t.fileOffsetOfCorrespondingData, 0))
	byteLen += n3
	n4, _ := buf.Write(utils.Int64ToByte(t.numOfPoints, 0))
	byteLen += n4
	n5, _ := buf.Write(utils.Int64ToByte(t.totalByteSizeOfPagesOnDisk, 0))
	byteLen += n5
	n6, _ := buf.Write(utils.Int64ToByte(t.startTime, 0))
	byteLen += n6
	n7, _ := buf.Write(utils.Int64ToByte(t.endTime, 0))
	byteLen += n7

	if t.valuesStatistics.sizeOfList <= 0 {
		byteLen += t.valuesStatistics.GetNullDigestSize()
	} else {
		// tsDigest serializeTo
		byteLen += t.valuesStatistics.serializeTo(buf)
	}

	return byteLen
}

func NewTimeSeriesChunkMetaData(sid string, fOffset int64, sTime int64, eTime int64) (*ChunkMetaData, error) {
	return &ChunkMetaData{
		sensor:sid,
		fileOffsetOfCorrespondingData:fOffset,
		startTime:sTime,
		endTime:eTime,
		totalByteSizeOfPagesOnDisk:0,
		numOfPoints:0,
	},nil
}
