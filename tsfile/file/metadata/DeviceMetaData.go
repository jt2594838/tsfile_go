package metadata

import (
	"container/list"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type DeviceMetaData struct {
	startTime            int64
	endTime              int64
	serializedSize       int
	rowGroupMetadataList *list.List
}

func (f *DeviceMetaData) Deserialize(reader *utils.BytesReader) {
	f.startTime = reader.ReadLong()
	f.endTime = reader.ReadLong()

	size := int(reader.ReadInt())
	if size > 0 {
		f.rowGroupMetadataList = list.New()
		for i := 0; i < size; i++ {
			rowGroupMetaData := new(RowGroupMetaData)
			rowGroupMetaData.Deserialize(reader)

			f.rowGroupMetadataList.PushBack(rowGroupMetaData)
		}
	}
}

func (f *DeviceMetaData) GetSerializedSize() int {
	f.serializedSize = 2*constant.LONG_LEN + constant.INT_LEN
	if f.rowGroupMetadataList != nil {
		// iterate list
		for e := f.rowGroupMetadataList.Front(); e != nil; e = e.Next() {
			f.serializedSize += e.Value.(*RowGroupMetaData).GetSerializedSize()
		}
	}

	return f.serializedSize
}
