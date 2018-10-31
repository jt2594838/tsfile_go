package tsFileWriter

import (
	"tsfile/common/constant"
)

/**
 * @Package Name: DoubleDataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午3:19
 * @Description:
 */

type DoubleDataPoint struct {
	sensorId   string
	tsDataType int16
	value      float64
}

func NewDoubleOld(sId string, tdt constant.TSDataType, val float64) (*DataPoint, error) {
	return &DataPoint{
		sensorId:   sId,
		tsDataType: tdt,
		value:      val,
	}, nil
}

func NewDouble(sId string, tdt constant.TSDataType, val float64) (*DataPoint, error) {
	f := getDataPoint()
	f.sensorId = sId
	f.tsDataType = tdt
	f.value = val
	return f, nil
}
