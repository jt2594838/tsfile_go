package statistics

import (
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Statistics interface {
	DeserializeFrom(reader *utils.FileReader)
	GetSerializedSize() int
}

func DeserializeFrom(reader *utils.FileReader, dataType constant.TSDataType) Statistics {
	var statistics Statistics

	switch dataType {
	case constant.BOOLEAN:
		statistics = new(Boolean)
	case constant.INT32:
		statistics = new(Integer)
	case constant.INT64:
		statistics = new(Long)
	case constant.FLOAT:
		statistics = new(Float)
	case constant.DOUBLE:
		statistics = new(Double)
	case constant.TEXT:
		statistics = new(Binary)
	default:
		panic("Statistics unknown dataType: " + strconv.Itoa(int(dataType)))
	}

	statistics.DeserializeFrom(reader)

	return statistics
}
