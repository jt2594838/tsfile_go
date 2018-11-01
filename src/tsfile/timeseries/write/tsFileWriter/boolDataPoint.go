package tsFileWriter

import (
	"tsfile/common/constant"
)

/**
 * @Package Name: BoolDataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-27 下午3:19
 * @Description:
 */

type BoolDataPoint struct {
	sensorId   string
	tsDataType int16
	value      bool
}

func NewBoolOld(sId string, tdt constant.TSDataType, val bool) (*DataPoint, error) {
	return &DataPoint{
		sensorId: sId,
		//tsDataType: tdt,
		value: val,
	}, nil
}

func NewBool(sId string, tdt constant.TSDataType, val bool) (*DataPoint, error) {
	f := getDataPoint()
	f.sensorId = sId
	//f.tsDataType = tdt
	f.value = val
	return f, nil
}
