package tsFileWriter

/**
 * @Package Name: dataPoint
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午4:27
 * @Description:
 */

import (
	"sync"
	_ "tsfile/common/constant"
	"tsfile/common/log"
)

type DataPointOperate interface {
	write()
}

type DataPoint struct {
	sensorId string
	//tsDataType constant.TSDataType
	value interface{}
}

func (d *DataPoint) GetSensorId() string {
	return d.sensorId
}

func (d *DataPoint) Write(t int64, sw *SeriesWriter) bool {
	if sw.GetTsDeviceId() == "" {
		log.Info("give seriesWriter is null, do nothing and return.")
		return false
	}
	sw.Write(t, d)
	return true
}

func (d *DataPoint) SetValue(sId string, val interface{}) {
	d.sensorId = sId
	d.value = val
}

var dataPointMutex sync.Mutex
var dataPointArrBuf []DataPoint //= make([]FloatDataPoint, 100)
var dataPointArrBufCount int = 0

func getDataPoint() *DataPoint {
	dataPointMutex.Lock()
	if dataPointArrBufCount == 0 {
		dataPointArrBufCount = 200
		dataPointArrBuf = make([]DataPoint, dataPointArrBufCount)
	}
	dataPointArrBufCount--
	f := &(dataPointArrBuf[dataPointArrBufCount])
	dataPointMutex.Unlock()
	return f
}

//func New(sId string, tdt int, te int) (*DataPoint, error) {
//	return &DataPoint{
//		sensorId:sId,
//		tsDataType:tdt,
//		tsEncoding:te,
//	},nil
//}
