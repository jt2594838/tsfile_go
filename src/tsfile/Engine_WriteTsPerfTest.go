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
	ts       time.Time
	i64Value int64
	f64Value float64
	strValue string
}

func ReadFileToTSFile(fileName string, tfWriter *tsFileWriter.TsFileWriter,
	strDeviceID string, strSensorID string,
	iType constant.TSDataType, iEncode constant.TSEncoding, iMaxSize int,
	handler func(*tsFileWriter.TsFileWriter, []*MyTsRecord, string, string, constant.TSDataType) int64) (int64, error) {

	var iCostTime int64 = 0
	//dataSlice := make([]*tsFileWriter.DataPoint, 0)
	dataSlice := make([]*MyTsRecord, 0)
	f, err := os.Open(fileName)
	if err != nil {
		return iCostTime, err
	}
	buf := bufio.NewReader(f)
	buf.ReadByte()
	buf.ReadByte()
	buf.ReadByte()

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

	var _ts time.Time
	var _i64Value int64
	var _f64Value float64
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
		_strValue = ""
		if iType == constant.INT32 {
			_i64Value, _ = strconv.ParseInt(line, 10, 32)
		} else if iType == constant.INT64 {
			_i64Value, _ = strconv.ParseInt(line, 10, 64)
		} else if iType == constant.FLOAT {
			_f64Value, _ = strconv.ParseFloat(line, 32)
		} else if iType == constant.DOUBLE {
			_f64Value, _ = strconv.ParseFloat(line, 64)
		} else if iType == constant.TEXT {
			_strValue = line
		}

		trValue := &MyTsRecord{
			ts:       _ts,
			i64Value: _i64Value,
			f64Value: _f64Value,
			strValue: _strValue,
		}
		dataSlice = append(dataSlice, trValue)

		if len(dataSlice) >= iMaxSize {
			iCostTime += handler(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
			//dataSlice = make([]*tsFileWriter.DataPoint, 0)
			dataSlice = make([]*MyTsRecord, 0)
		}

		if err != nil {
			if err == io.EOF {
				if len(dataSlice) > 0 {
					iCostTime += handler(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
				}
				return iCostTime, nil
			}
			return iCostTime, err
		}
	}
	if len(dataSlice) > 0 {
		iCostTime += handler(tfWriter, dataSlice, strDeviceID, strSensorID, iType)
	}
	return iCostTime, nil
}

func writeBufferToTsFile(tfWriter *tsFileWriter.TsFileWriter, dpslice []*MyTsRecord,
	strDeviceID string, strSensorID string, iType constant.TSDataType) int64 {
	//var tsCur, tsCurNew, tsCurWrite time.Time
	tsCur := time.Now()
	for _, dp := range dpslice {
		//tsCurNew = time.Now()
		tr1, trErr := tsFileWriter.NewTsRecord(dp.ts, strDeviceID)
		if trErr != nil {
			log.Info("init tsRecord error.")
		}
		if iType == constant.INT32 {
			fdp, fDpErr := tsFileWriter.NewInt(strSensorID, constant.INT32, int32(dp.i64Value))
			if fDpErr == nil {
				tr1.AddTuple(fdp)
			}
		} else if iType == constant.INT64 {
			fdp, fDpErr := tsFileWriter.NewLong(strSensorID, constant.INT64, dp.i64Value)
			if fDpErr == nil {
				tr1.AddTuple(fdp)
			}
		} else if iType == constant.FLOAT {
			fdp, fDpErr := tsFileWriter.NewFloat(strSensorID, constant.FLOAT, float32(dp.f64Value))
			if fDpErr == nil {
				tr1.AddTuple(fdp)
			}
		} else if iType == constant.DOUBLE {
			fdp, fDpErr := tsFileWriter.NewDouble(strSensorID, constant.DOUBLE, dp.f64Value)
			if fDpErr == nil {
				tr1.AddTuple(fdp)
			}
		} else if iType == constant.TEXT {
			fdp, fDpErr := tsFileWriter.NewString(strSensorID, constant.TEXT, dp.strValue)
			if fDpErr == nil {
				tr1.AddTuple(fdp)
			}
		}
		//logcost.CostNewTimes += int64(time.Since(tsCurNew))
		//tsCurWrite := time.Now()
		tfWriter.Write(*tr1)
		//logcost.CostWriteTimes += int64(time.Since(tsCurWrite))
	}
	return int64(time.Since(tsCur))
}

func writeTsFile(fileName string, fileInFile string, strDeviceID string, strSensorID string,
	iType constant.TSDataType, iEncode constant.TSEncoding, iCachSize int) time.Duration {

	defer func() {
		if err := recover(); err != nil {
			log.Info("Error: ", err)
		}
	}()
	var iCost int64 = 0
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		os.Remove(fileName)
	}

	tfWriter, tfwErr := tsFileWriter.NewTsFileWriter(fileName)
	if tfwErr != nil {
		log.Info("init tsFileWriter error = %s", tfwErr)
	}

	iCost, _ = ReadFileToTSFile(fileInFile, tfWriter, strDeviceID, strSensorID,
		iType, iEncode, iCachSize, writeBufferToTsFile)

	tfWriter.Close()
	return time.Duration(iCost)
}

func TestWriteTsFilePerf(debug int, debugErr int) {
	var DebugErr int = debugErr //RLE 调试
	var DebugI int = debug      //0调试所有
	//TS_2DIFF 1,2,3,4
	//PLAIN TEXT 5
	//RLE  6,7,8,9
	//GORILLA 10,11
	//PLAIN 12,13,14,15

	var iCostTime time.Duration = 0
	if DebugI == 0 || DebugI == 1 {
		iCostTime = writeTsFile("goout/output1.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.TS_2DIFF, 10000)
		log.Info("datain/output1.txt INT32 TS_2DIFF cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 2 {
		iCostTime = writeTsFile("goout/output2.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.TS_2DIFF, 10000)
		log.Info("datain/output1.txt INT64 TS_2DIFF cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 3 {
		iCostTime = writeTsFile("goout/output3.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.TS_2DIFF, 10000)
		log.Info("datain/output3.txt Float TS_2DIFF cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 4 {
		iCostTime = writeTsFile("goout/output4.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.TS_2DIFF, 10000)
		log.Info("datain/output4.txt DOUBLE TS_2DIFF cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 5 {
		iCostTime = writeTsFile("goout/output5.ts", "datain/output5.txt", "device_1", "sensor_5",
			constant.TEXT, constant.PLAIN, 10000)
		log.Info("output1.txt TEXT PLAIN cost time %d = %fs ", iCostTime.Nanoseconds(), iCostTime.Seconds())
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 6) {

		iCostTime = writeTsFile("goout/output6.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.RLE, 10000)
		log.Info("datain/output1.txt INT32 RLE cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 7) {
		iCostTime = writeTsFile("goout/output7.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.RLE, 10000)
		log.Info("datain/output1.txt INT64 RLE cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 8) {
		iCostTime = writeTsFile("goout/output8.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.RLE, 10000)
		log.Info("datain/output3.txt Float RLE cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}
	if DebugErr != 0 && (DebugI == 0 || DebugI == 9) {
		iCostTime = writeTsFile("goout/output9.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.RLE, 10000)
		log.Info("datain/output4.txt DOUBLE RLE cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}
	if DebugI == 0 || DebugI == 10 {
		iCostTime = writeTsFile("goout/output10.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.GORILLA, 10000)
		log.Info("datain/output3.txt Float GORILLA cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 11 {
		iCostTime = writeTsFile("goout/output11.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.GORILLA, 10000)
		log.Info("datain/output4.txt DOUBLE GORILLA cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 12 {
		iCostTime = writeTsFile("goout/output12.ts", "datain/output1.txt", "device_1", "sensor_1",
			constant.INT32, constant.PLAIN, 10000)
		log.Info("datain/output1.txt INT32 PLAIN cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 13 {
		iCostTime = writeTsFile("goout/output13.ts", "datain/output2.txt", "device_1", "sensor_2",
			constant.INT64, constant.PLAIN, 10000)
		log.Info("datain/output1.txt INT64 PLAIN cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 14 {
		iCostTime = writeTsFile("goout/output14.ts", "datain/output3.txt", "device_1", "sensor_3",
			constant.FLOAT, constant.PLAIN, 10000)
		log.Info("datain/output3.txt Float PLAIN cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI == 0 || DebugI == 15 {
		iCostTime = writeTsFile("goout/output15.ts", "datain/output4.txt", "device_1", "sensor_4",
			constant.DOUBLE, constant.PLAIN, 10000)
		log.Info("datain/output4.txt DOUBLE PLAIN cost time %d = %fms ", iCostTime.Nanoseconds(), iCostTime.Seconds()*1000)
	}

	if DebugI != 0 {
		fmt.Printf("output1.txt cost time %d = %fms iNewCost:%d write:%d total=%d \ntotal=%d \ntest1=%d \ntest2=%d \ntest3=%d \ntest4=%d \ntest5=%d \ntest6=%d\n",
			iCostTime.Nanoseconds(), iCostTime.Seconds()*1000,
			logcost.CostNewTimes, logcost.CostWriteTimes, logcost.CostNewTimes+logcost.CostWriteTimes,
			iCostTime.Nanoseconds(),
			logcost.CostWriteTimesTest1, logcost.CostWriteTimesTest2, logcost.CostWriteTimesTest3,
			logcost.CostWriteTimesTest4, logcost.CostWriteTimesTest5, logcost.CostWriteTimesTest6)
	}
}

//func main() {
//	TestGenFilePerf()
//}
