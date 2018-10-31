package tsFileWriter

import (
	"tsfile/common/constant"
)

/**
 * @Package Name: dataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午3:19
 * @Description:
 */

type LongDataPoint struct {
	sensorId   string
	tsDataType int16
	value      int64
}

func NewLongOld(sId string, tdt constant.TSDataType, val int64) (*DataPoint, error) {
	return &DataPoint{
		sensorId:   sId,
		tsDataType: tdt,
		value:      val,
	}, nil
}

func NewLong(sId string, tdt constant.TSDataType, val int64) (*DataPoint, error) {
	f := getDataPoint()
	f.sensorId = sId
	f.tsDataType = tdt
	f.value = val
	return f, nil
}
