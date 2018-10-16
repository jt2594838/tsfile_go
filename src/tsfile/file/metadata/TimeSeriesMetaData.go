package metadata

import (
	"bytes"
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type TimeSeriesMetaData struct {
	sensor   string
	dataType constant.TSDataType
}

func (t *TimeSeriesMetaData) DataType() constant.TSDataType {
	return t.dataType
}

func (f *TimeSeriesMetaData) Deserialize(reader *utils.BytesReader) {
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

func (t *TimeSeriesMetaData) Serialize(buf *bytes.Buffer) int {
	var byteLen int
	if t.sensor == "" {
		n1, _ := buf.Write(utils.BoolToByte(false, 0))
		byteLen += n1
	} else {
		n2, _ := buf.Write(utils.BoolToByte(true, 0))
		byteLen += n2

		n3, _ := buf.Write(utils.Int32ToByte(int32(len(t.sensor)), 0))
		byteLen += n3
		n4, _ := buf.Write([]byte(t.sensor))
		byteLen += n4
	}

	if t.dataType >= 0 && t.dataType <= 9 { // not empty
		n5, _ := buf.Write(utils.BoolToByte(true, 0))
		byteLen += n5

		n6, _ := buf.Write(utils.Int16ToByte(int16(t.dataType), 0))
		byteLen += n6
	} else {
		n7, _ := buf.Write(utils.BoolToByte(false, 0))
		byteLen += n7
	}

	return byteLen
}

func NewTimeSeriesMetaData(sid string, tdt int16) (*TimeSeriesMetaData, error) {

	return &TimeSeriesMetaData{
		sensor:   sid,
		dataType: constant.TSDataType(tdt),
	}, nil
}
