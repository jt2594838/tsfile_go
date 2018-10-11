// tsfile project main.go
package main

import (
	"time"
	"tsfile/timeseries/write/tsFileWriter"
	"tsfile/common/log"
	"tsfile/timeseries/write/sensorDescriptor"
	"tsfile/common/constant"
)

const (
	fileName = "test.ts"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Info("Error: ", err)
		}
	}()
//////////////////////////////////////////////////
	// init tsFileWriter
	tfWriter, tfwErr := tsFileWriter.NewTsFileWriter(fileName)
	if tfwErr != nil {
		log.Info("init tsFileWriter error = %s", tfwErr)
	}

	// init sensorDescriptor
	sd, sdErr := sensorDescriptor.New("sensor_1", constant.FLOAT, constant.PLAIN)
	if sdErr != nil {
		log.Info("init sensorDescriptor error = %s", sdErr)
	}
	sd2, sdErr2 := sensorDescriptor.New("sensor_2", constant.INT32, constant.PLAIN)
	if sdErr2 != nil {
		log.Info("init sensorDescriptor error = %s", sdErr2)
	}

	// add sensorDescriptor to tfFileWriter
	tfWriter.AddSensor(sd)
	tfWriter.AddSensor(sd2)

	// create a tsRecord
	ts := time.Now()
	//timestamp := strconv.FormatInt(ts.UTC().UnixNano(), 10)
	//fmt.Println(timestamp)
	log.Info("init tsRecord device_1.")
	tr, trErr := tsFileWriter.NewTsRecord(ts, "device_1")
	if trErr != nil {
		log.Info("init tsRecord error.")
	}

	// create two data points
	fdp, fDpErr := tsFileWriter.NewFloat("sensor_1", constant.FLOAT,1.2)
	if fDpErr != nil {
		log.Info("init float data point error.")
	}
	idp, iDpErr := tsFileWriter.NewInt("sensor_2", constant.INT32,20)
	if iDpErr != nil {
		log.Info("init int data point error.")
	}

	// add data points to ts record
	tr.AddTuple(fdp)
	tr.AddTuple(idp)


	// write tsRecord to file
	tfWriter.Write(*tr)


	log.Info("init tsRecord device_1_2")



	tr1, trErr1 := tsFileWriter.NewTsRecord(ts, "device_1")
	if trErr1 != nil {
		log.Info("init tsRecord error.")
	}

	// create two data points
	fdp1, fDpErr1 := tsFileWriter.NewFloat("sensor_1", constant.FLOAT,90.0)
	if fDpErr1 != nil {
		log.Info("init float data point error.")
	}
	idp1, iDpErr1 := tsFileWriter.NewInt("sensor_2", constant.INT32,20)
	if iDpErr1 != nil {
		log.Info("init int data point error.")
	}


	// add data points to ts record
	tr1.AddTuple(idp1)
	tr1.AddTuple(fdp1)

	// write tsRecord to file
	tfWriter.Write(*tr1)
	//tfWriter.Write([]byte("&TsFileData&"))


	log.Info("init tsRecord device_2.")

	ts3 := time.Now()
	tr2, trErr2 := tsFileWriter.NewTsRecord(ts3, "lidong_2")
	if trErr2 != nil {
		log.Info("init tsRecord error.")
	}

	// create two data points
	fdp2, fDpErr2 := tsFileWriter.NewFloat("sensor_1", constant.FLOAT,1.2)
	if fDpErr2 != nil {
		log.Info("init float data point error.")
	}
	idp2, iDpErr2 := tsFileWriter.NewInt("sensor_2", constant.INT32,20)
	if iDpErr2 != nil {
		log.Info("init int data point error.")
	}


	// add data points to ts record
	tr2.AddTuple(fdp2)
	tr2.AddTuple(idp2)


	// write tsRecord to file
	tfWriter.Write(*tr2)



	// close file descriptor
	tfWriter.Close()
///////////////////////////////////////////////////
	log.Info("Program exit.")
}
