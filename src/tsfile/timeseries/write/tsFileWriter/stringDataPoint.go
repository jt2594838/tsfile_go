package tsFileWriter

import (
	"tsfile/common/constant"
)

/**
 * @Package Name: StringDataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午3:19
 * @Description:
 */

type StringDataPoint struct {
	sensorId   string
	tsDataType int16
	value      string
}

func NewStringOld(sId string, tdt constant.TSDataType, val string) (*DataPoint, error) {
	return &DataPoint{
		sensorId:   sId,
		tsDataType: tdt,
		value:      val,
	}, nil
}

func NewString(sId string, tdt constant.TSDataType, val string) (*DataPoint, error) {
	f := getDataPoint()
	f.sensorId = sId
	f.tsDataType = tdt
	f.value = val
	return f, nil
}
