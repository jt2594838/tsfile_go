package metadata

import (
	_ "encoding/binary"
	_ "log"
	"tsfile/common/utils"
	"bytes"
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

func (t *FileMetaData) SerializeTo (buf *bytes.Buffer) (int) {
	var byteLen int
	if t.deviceMap == nil {
		n, _ := buf.Write(utils.Int32ToByte(0))
		byteLen += n
	} else {
		n := len(t.deviceMap)
		d1, _ := buf.Write(utils.Int32ToByte(int32(n)))
		byteLen += d1

		for k, v := range t.deviceMap {
			// write string tsDeviceMetaData key
			d2, _ := buf.Write(utils.Int32ToByte(int32(len(k))))
			byteLen += d2
			d3, _ := buf.Write([]byte(k))
			byteLen += d3
			// tsDeviceMetaData SerializeTo
			byteLen += v.SerializeTo(buf)
			// log.Info("v: %s", v)
		}
	}
	if t.timeSeriesMetadataMap == nil {
		e1, _ := buf.Write(utils.Int32ToByte(0))
		byteLen += e1
	} else {
		e2, _ := buf.Write(utils.Int32ToByte(int32(len(t.timeSeriesMetadataMap))))
		byteLen += e2
		for _, vv := range t.timeSeriesMetadataMap {
			// timeSeriesMetaData SerializeTo
			byteLen += vv.Serialize(buf)
			// log.Info("vv: %s", vv)
		}
	}
	f1, _ := buf.Write(utils.Int32ToByte(int32(t.currentVersion)))
	byteLen += f1
	if t.createdBy == "" {
		// write flag for t.createBy
		f2, _ := buf.Write(utils.BoolToByte(false))
		byteLen += f2
	} else {
		// write flag for t.createBy
		f3, _ := buf.Write(utils.BoolToByte(true))
		byteLen += f3
		// write string t.createBy
		f4, _ := buf.Write(utils.Int32ToByte(int32(len(t.createdBy))))
		byteLen += f4
		f5, _ := buf.Write([]byte(t.createdBy))
		byteLen += f5
	}

	off1, _ := buf.Write(utils.Int64ToByte(t.firstTimeSeriesMetadataOffset))
	byteLen += off1
	off2, _ := buf.Write(utils.Int64ToByte(t.lastTimeSeriesMetadataOffset))
	byteLen += off2
	off3, _ := buf.Write(utils.Int64ToByte(t.firstTsDeltaObjectMetadataOffset))
	byteLen += off3
	off4, _ := buf.Write(utils.Int64ToByte(t.lastTsDeltaObjectMetadataOffset))
	byteLen += off4

	return byteLen
}



func NewTsFileMetaData(tdmd map[string]*DeviceMetaData, tss map[string]*TimeSeriesMetaData, version int) (*FileMetaData, error) {

	return &FileMetaData{
		deviceMap:tdmd,
		timeSeriesMetadataMap:tss,
		currentVersion:version,
		createdBy:"",
	},nil
}
