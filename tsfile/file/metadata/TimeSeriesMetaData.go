package metadata

import (
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type TimeSeriesMetaData struct {
	sensor   string
	dataType constant.TSDataType
}

func (f *TimeSeriesMetaData) DeserializeFrom(reader *utils.BytesReader) {
	if reader.ReadBool() {
		f.sensor = reader.ReadString()
	}

	if reader.ReadBool() {
		f.dataType = constant.TSDataType(reader.ReadShort())
	}
}

func (f *TimeSeriesMetaData) GetSensor() string {
	return f.sensor
}
