package metadata

import (
	_ "encoding/binary"
	_ "log"
	"tsfile/common/utils"
)

type FileMetaData struct {
	currentVersion                   int
	createdBy                        string
	firstTimeSeriesMetadataOffset    int64 //相对于file metadata开头位置 的offset
	lastTimeSeriesMetadataOffset     int64 //相对于file metadata开头位置 的offset
	firstTsDeltaObjectMetadataOffset int64 //相对于file metadata开头位置 的offset
	lastTsDeltaObjectMetadataOffset  int64 //相对于file metadata开头位置 的offset

	deviceMap             map[string]*DeviceMetaData
	timeSeriesMetadataMap map[string]*TimeSeriesMetaData
}

func (f *FileMetaData) TimeSeriesMetadataMap() map[string]*TimeSeriesMetaData {
	return f.timeSeriesMetadataMap
}

func (f *FileMetaData) DeviceMap() map[string]*DeviceMetaData {
	return f.deviceMap
}

func (f *FileMetaData) Deserialize(metadata []byte) {
	reader := utils.NewBytesReader(metadata)

	f.deviceMap = make(map[string]*DeviceMetaData)
	if size := int(reader.ReadInt()); size > 0 {
		for i := 0; i < size; i++ {
			key := reader.ReadString()

			value := new(DeviceMetaData)
			value.Deserialize(reader)

			f.deviceMap[key] = value
		}
	}

	f.timeSeriesMetadataMap = make(map[string]*TimeSeriesMetaData)
	if size := int(reader.ReadInt()); size > 0 {
		for i := 0; i < size; i++ {
			value := new(TimeSeriesMetaData)
			value.Deserialize(reader)

			f.timeSeriesMetadataMap[value.GetSensor()] = value
		}
	}

	f.currentVersion = int(reader.ReadInt())
	if reader.ReadBool() {
		f.createdBy = reader.ReadString()
	}
	f.firstTimeSeriesMetadataOffset = reader.ReadLong()
	f.lastTimeSeriesMetadataOffset = reader.ReadLong()
	f.firstTsDeltaObjectMetadataOffset = reader.ReadLong()
	f.lastTsDeltaObjectMetadataOffset = reader.ReadLong()
}

func (f *FileMetaData) GetCurrentVersion() int {
	return f.currentVersion
}
