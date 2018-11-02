package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/common/logcost"
	"tsfile/timeseries/write/sensorDescriptor"
	"tsfile/timeseries/write/tsFileWriter"
)

type MyTsRecord struct {
	ts       int64
	i32Value int32
	i64Value int64
	f32Value float32
	f64Value float64
	strValue string
}

var CostTimeTs int64 = 0
var CostTimeTsClose int64 = 0
var CostTimeTsOpen int64 = 0

func writeTsFile(fileTsName string, fileInName string,
	strDeviceID string, strSensorID string,
	iType constant.TSDataType, iEncode constant.TSEncoding, iMaxSize int) time.Duration {

	defer func() {
		if err := recover(); err != nil {
			log.Info("Error: ", err)
		}
	}()

	CostTimeTs = 0
	CostTimeTsClose = 0
	CostTimeTsOpen = 0
	dataSlice := make([]*MyTsRecord, 0)
	f, err := os.Open(fileInName)
	if err != nil {
		return time.Duration(CostTimeTs)
	}
	buf := bufio.NewReader(f)
	buf.ReadByte()
	buf.ReadByte()
	buf.ReadByte()

	//var iCost int64 = 0
	var tfWriter *tsFileWriter.TsFileWriter
	var tfwErr error
	if _, err := os.Stat(fileTsName); !os.IsNotExist(err) {
		os.Remove(fileTsName)
	}

	tsCur := time.Now()
	tfWriter, tfwErr = tsFileWriter.NewTsFileWriter(fileTsName)
	if tfwErr != nil {
		log.Info("init tsFileWriter error = %s", tfwErr)
	}
	if iType == constant.INT32 {
		sd1, sdErr := sensorDescriptor.New(strSensorID, constant.INT32, iEncode) //constant.RLE
		if sdErr != nil {
			log.Info("init sensorDescriptor error = %s", sdErr)
		}
		tfWriter.AddSensor(sd1)
	} else if iType == constant.INT64 {
		sd1, sdErr := sensorDescriptor.New(strSensorID, constant.INT64, iEncode) //constant.RLE
		if sdErr != nil {
			log.Info("init sensorDescriptor error = %s", sdErr)
		}
		tfWriter.AddSensor(sd1)
	} else if iType == constant.FLOAT {
		sd1, sdErr := sensorDescriptor.New(strSensorID, constant.FLOAT, iEncode) //constant.RLE
		if sdErr != nil {
			log.Info("init sensorDescriptor error = %s", sdErr)
		}
		tfWriter.AddSensor(sd1)
	} else if iType == constant.DOUBLE {
		sd1, sdErr := sensorDescriptor.New(strSensorID, constant.DOUBLE, iEncode) //constant.RLE
		if sdErr != nil {
			log.Info("init sensorDescriptor error = %s", sdErr)
		}
		tfWriter.AddSensor(sd1)
	} else if iType == constant.TEXT {
		sd1, sdErr := sensorDescriptor.New(strSensorID, constant.TEXT, iEncode) //constant.RLE
		if sdErr != nil {
			log.Info("init sensorDescriptor error = %s", sdErr)
		}
		tfWriter.AddSensor(sd1)
	}
	CostTimeTsOpen = time.Since(tsCur).Nanoseconds()
	CostTimeTs += CostTimeTsOpen

	var _ts time.Time
	var _i32Value int64
	var _i64Value int64
	var _f64Value float64
	var _f32Value float64
	var _strValue string
	for {
		line, err := buf.ReadString('\n')
		s := strings.Split(line, ";")
		if len(s) < 2 {
			if err != nil {
				if err == io.EOF {
					break
				}
				break
			}
			continue
		}
		line = s[1]
		line = strings.TrimSpace(line)
		_ts, err = time.Parse("2006-01-02 15:04:05", s[0])
		//var strTsValue string = _ts.Format("2006-01-02 15:04:05")
		//log.Info("wangcan time %s %s", strTsValue, line)
		_i64Value = 0
		_f64Value = 0
		_f32Value = 0
		_i32Value = 0
		_strValue = ""
		if iType == constant.INT32 {
			_i32Value, _ = strconv.ParseInt(line, 10, 32)
		} else if iType == constant.INT64 {
			_i64Value, _ = strconv.ParseInt(line, 10, 64)
		} else if iType == constant.FLOAT {
			_f32Value, _ = strconv.ParseFloat(line, 32)
		} else if iType == constant.DOUBLE {
			_f64Value, _ = strconv.ParseFloat(line, 64)
		} else if iType == constant.TEXT {
			_strValue = line
		}

		trValue := &MyTsRecord{
			ts:       (_ts.UnixNano() / 1000000) - 28800000,
			i32Value: int32(_i32Value),
			i64Value: _i64Value,
			f64Value: _f64Value,
			f32Value: float32(_f32Value),
			strValue: _strValue,
		}
		dataSlice = append(dataSlice, trValue)

		if len(dataSlice) >= iMaxSize {
			writeBufferToTsFile(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
			dataSlice = make([]*MyTsRecord, 0)
		}

		if err != nil {
			if err == io.EOF {
				if len(dataSlice) > 0 {
					writeBufferToTsFile(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
				}
				tsCur = time.Now()
				tfWriter.Close()
				CostTimeTsClose = time.Since(tsCur).Nanoseconds()
				CostTimeTs += CostTimeTsClose
				return time.Duration(CostTimeTs)
			}
			tsCur = time.Now()
			tfWriter.Close()
			CostTimeTsClose = time.Since(tsCur).Nanoseconds()
			CostTimeTs += CostTimeTsClose
			return time.Duration(CostTimeTs)
		}
	}
	if len(dataSlice) > 0 {
		writeBufferToTsFile(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
		tsCur = time.Now()
		tfWriter.Close()
		CostTimeTsClose = time.Since(tsCur).Nanoseconds()
		CostTimeTs += CostTimeTsClose
	}
	return time.Duration(CostTimeTs)
}

func writeBufferToTsFile(tfWriter *tsFileWriter.TsFileWriter, dpslice []*MyTsRecord,
	strDeviceID string, strSensorID string, iType constant.TSDataType) {
	var tsCurNew time.Time
	var fdp *tsFileWriter.DataPoint
	tsCurNew = time.Now()
	fdp, _ = tsFileWriter.NewDataPoint()
	dataSlice := make([]*tsFileWriter.DataPoint, 1)
	dataSlice[0] = fdp
	tr1, trErr := tsFileWriter.NewTsRecordUseTimestamp(0, "")
	if trErr != nil {
		log.Info("init tsRecord error.")
	}
	tr1.SetDataPointSli(dataSlice)
	CostTimeTs += time.Since(tsCurNew).Nanoseconds()
	for _, dp := range dpslice {
		tsCurNew = time.Now()
		tr1.SetTimestampDeviceID(dp.ts, strDeviceID)
		switch iType {
		case constant.INT32:
			fdp.SetValue(strSensorID, dp.i32Value)
		case constant.INT64:
			fdp.SetValue(strSensorID, dp.i64Value)
		case constant.FLOAT:
			fdp.SetValue(strSensorID, dp.f32Value)
		case constant.DOUBLE:
			fdp.SetValue(strSensorID, dp.f64Value)
		case constant.TEXT:
			fdp.SetValue(strSensorID, dp.strValue)
		}
		tfWriter.Write(tr1)
		CostTimeTs += time.Since(tsCurNew).Nanoseconds()
	}
	//return int64(time.Since(tsCur))
}

func logoutput(tsFile string, inputFile string, tag string, iCostTime time.Duration, bReadTsFile bool, bMoreInfo bool) {
	if bMoreInfo {
		logcost.CostWriteTimesTest1 = CostTimeTsOpen
		logcost.CostWriteTimesTest2 = CostTimeTsClose
		fmt.Printf("%s %s %s cost time %d = %fms \ntotal=%d \ntest1=%d \ntest2=%d \ntest3=%d \ntest4=%d \ntest5=%d \ntest6=%d\n",
			inputFile, tag, tsFile, iCostTime.Nanoseconds(),
			iCostTime.Seconds()*1000, iCostTime.Nanoseconds(),
			logcost.CostWriteTimesTest1, logcost.CostWriteTimesTest2, logcost.CostWriteTimesTest3,
			logcost.CostWriteTimesTest4, logcost.CostWriteTimesTest5, logcost.CostWriteTimesTest6)
	} else {
		fmt.Printf("%s %s %s cost time %d = %fms \n", inputFile, tag, tsFile,
			iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}
	if bReadTsFile {
		TestRead(tsFile)
	}
	logcost.CostWriteTimesTest1 = 0
	logcost.CostWriteTimesTest2 = 0
	logcost.CostWriteTimesTest3 = 0
	logcost.CostWriteTimesTest4 = 0
	logcost.CostWriteTimesTest5 = 0
	logcost.CostWriteTimesTest6 = 0
}

func TestWriteTsFilePerf(debug int, debugErr int, bReadTs bool, bMoreInfo bool) {
	var DebugErr int = debugErr //RLE 调试
	var DebugI int = debug      //0调试所有
	var iCacheCount int = 10000
	//TS_2DIFF 1,2,3,4
	//PLAIN TEXT 5
	//RLE  6,7,8,9
	//GORILLA 10,11
	//PLAIN 12,13,14,15

	var iCostTime time.Duration = 0
	if DebugI == 0 || DebugI == 1 {
		iCostTime = writeTsFile("goout/output1.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.TS_2DIFF, iCacheCount)
		logoutput("goout/output1.ts", "datain/output1.txt", "INT32 TS_2DIFF",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 2 {
		iCostTime = writeTsFile("goout/output2.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.TS_2DIFF, iCacheCount)
		logoutput("goout/output2.ts", "datain/output2.txt", "INT64 TS_2DIFF",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 3 {
		iCostTime = writeTsFile("goout/output3.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.TS_2DIFF, iCacheCount)
		logoutput("goout/output3.ts", "datain/output3.txt", "Float TS_2DIFF",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 4 {
		iCostTime = writeTsFile("goout/output4.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.TS_2DIFF, iCacheCount)
		logoutput("goout/output4.ts", "datain/output4.txt", "DOUBLE TS_2DIFF",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 5 {
		iCostTime = writeTsFile("goout/output5.ts", "datain/output5.txt", "device_1", "sensor_5",
			constant.TEXT, constant.PLAIN, iCacheCount)
		logoutput("goout/output5.ts", "datain/output5.txt", "TEXT PLAIN",
			iCostTime, bReadTs, bMoreInfo)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 6) {
		iCostTime = writeTsFile("goout/output6.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.RLE, iCacheCount)
		logoutput("goout/output6.ts", "datain/output1.txt", "INT32 RLE",
			iCostTime, bReadTs, bMoreInfo)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 7) {
		iCostTime = writeTsFile("goout/output7.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.RLE, iCacheCount)
		logoutput("goout/output7.ts", "datain/output2.txt", "INT64 RLE",
			iCostTime, bReadTs, bMoreInfo)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 8) {
		iCostTime = writeTsFile("goout/output8.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.RLE, iCacheCount)
		logoutput("goout/output8.ts", "datain/output3.txt", "Float RLE",
			iCostTime, bReadTs, bMoreInfo)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 9) {
		iCostTime = writeTsFile("goout/output9.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.RLE, iCacheCount)
		logoutput("goout/output9.ts", "datain/output4.txt", "DOUBLE RLE",
			iCostTime, bReadTs, bMoreInfo)
	}
	if DebugI == 0 || DebugI == 10 {
		iCostTime = writeTsFile("goout/output10.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.GORILLA, iCacheCount)
		logoutput("goout/output10.ts", "datain/output3.txt", "Float GORILLA",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 11 {
		iCostTime = writeTsFile("goout/output11.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.GORILLA, iCacheCount)
		logoutput("goout/output11.ts", "datain/output4.txt", "DOUBLE GORILLA",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 12 {
		iCostTime = writeTsFile("goout/output12.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.PLAIN, iCacheCount)
		logoutput("goout/output12.ts", "datain/output1.txt", "INT32 PLAIN",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 13 {
		iCostTime = writeTsFile("goout/output13.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.PLAIN, iCacheCount)
		logoutput("goout/output13.ts", "datain/output2.txt", "INT64 PLAIN",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 14 {
		iCostTime = writeTsFile("goout/output14.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.PLAIN, iCacheCount)
		logoutput("goout/output14.ts", "datain/output3.txt", "Float PLAIN",
			iCostTime, bReadTs, bMoreInfo)
	}

	if DebugI == 0 || DebugI == 15 {
		iCostTime = writeTsFile("goout/output15.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.PLAIN, iCacheCount)
		logoutput("goout/output15.ts", "datain/output4.txt", "DOUBLE PLAIN",
			iCostTime, bReadTs, bMoreInfo)
	}
}

//func main() {
//	TestGenFilePerf()
//}
