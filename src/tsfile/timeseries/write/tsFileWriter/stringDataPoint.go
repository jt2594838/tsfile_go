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

func NewString(sId string, tdt constant.TSDataType, val string) (*DataPoint, error) {
	return &DataPoint{
		sensorId:   sId,
		tsDataType: int16(tdt),
		value:      val,
	}, nil
}
