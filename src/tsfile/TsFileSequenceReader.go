package main

import (
	"fmt"
	"log"
	"strconv"
	_ "testing"
	"time"
	"tsfile/common/constant"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/datatype"

	"tsfile/timeseries/read/reader/impl/basic"
)

func TestRead(strPath string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error:", err)
		}
	}()

	//file := "goout/output1.ts"
	f := new(read.TsFileSequenceReader)
	f.Open(strPath)
	defer f.Close()

	headerString := f.ReadHeadMagic()
	log.Println("Header string: " + headerString)

	tailerString := f.ReadTailMagic()
	log.Println("Tail string: " + tailerString)

	fileMetadata := f.ReadFileMetadata()
	log.Println("File version: " + strconv.Itoa(fileMetadata.GetCurrentVersion()))

	for f.HasNextRowGroup() {
		groupHeader := f.ReadRowGroupHeader()
		log.Println("row group: " + groupHeader.GetDevice() + ", chunk number: " + strconv.Itoa(int(groupHeader.GetNumberOfChunks())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
		for i := 0; i < int(groupHeader.GetNumberOfChunks()); i++ {
			chunkHeader := f.ReadChunkHeader()
			log.Println("  chunk: " + chunkHeader.GetSensor() + ", page number: " + strconv.Itoa(chunkHeader.GetNumberOfPages()) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
			defaultTimeDecoder := decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)
			valueDecoder := decoder.CreateDecoder(chunkHeader.GetEncodingType(), chunkHeader.GetDataType())
			for j := 0; j < chunkHeader.GetNumberOfPages(); j++ {
				pageHeader := f.ReadPageHeader(chunkHeader.GetDataType())
				log.Println("    page dps: " + strconv.Itoa(int(pageHeader.GetNumberOfValues())) + ", page data size: " + strconv.Itoa(int(pageHeader.GetCompressedSize())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))

				pageData := f.ReadPage(pageHeader, chunkHeader.GetCompressionType())
				reader1 := &basic.PageDataReader{DataType: chunkHeader.GetDataType(), ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
				reader1.Read(pageData)
				for reader1.HasNext() {
					pair, _ := reader1.Next()
					log.Println("      (time,value): " + strconv.FormatInt(pair.Timestamp, 10) + ", " + fmt.Sprintf("%v", pair.Value))
				}
			}
		}
	}

}

func TestReadFile(ts *ReadTestResult, bDebugValue bool) time.Duration {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error:", err)
		}
	}()

	tsCurNew := time.Now()
	f := new(read.TsFileSequenceReader)
	f.Open(ts.StrTsFile)
	defer f.Close()

	_ = f.ReadHeadMagic()
	//log.Println("Header string: " + headerString)

	_ = f.ReadTailMagic()
	//log.Println("Tail string: " + tailerString)

	_ = f.ReadFileMetadata()
	//log.Println("File version: " + strconv.Itoa(fileMetadata.GetCurrentVersion()))

	var pair *datatype.TimeValuePair = &datatype.TimeValuePair{}
	//var curTime time.Time
	for f.HasNextRowGroup() {
		//curTime = time.Now()
		groupHeader := f.ReadRowGroupHeader()
		//ts.CostTimeTest1 += time.Since(curTime).Nanoseconds()

		//log.Println("row group: " + groupHeader.GetDevice() + ", chunk number: " + strconv.Itoa(int(groupHeader.GetNumberOfChunks())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
		for i := 0; i < int(groupHeader.GetNumberOfChunks()); i++ {
			//curTime = time.Now()
			chunkHeader := f.ReadChunkHeader()
			//log.Println("  chunk: " + chunkHeader.GetSensor() + ", page number: " + strconv.Itoa(chunkHeader.GetNumberOfPages()) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
			defaultTimeDecoder := decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)
			valueDecoder := decoder.CreateDecoder(chunkHeader.GetEncodingType(), chunkHeader.GetDataType())
			//ts.CostTimeTest2 += time.Since(curTime).Nanoseconds()
			for j := 0; j < chunkHeader.GetNumberOfPages(); j++ {
				//curTime = time.Now()
				pageHeader := f.ReadPageHeader(chunkHeader.GetDataType())
				//log.Println("    page dps: " + strconv.Itoa(int(pageHeader.GetNumberOfValues())) + ", page data size: " + strconv.Itoa(int(pageHeader.GetCompressedSize())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))

				pageData := f.ReadPage(pageHeader, chunkHeader.GetCompressionType())
				reader1 := &basic.PageDataReader{DataType: chunkHeader.GetDataType(), ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
				reader1.Read(pageData)
				//ts.CostTimeTest3 += time.Since(curTime).Nanoseconds()
				for reader1.HasNext() {
					//curTime = time.Now()
					reader1.Next2(pair)
					//pair, _ := reader1.Next()
					if bDebugValue {
						fmt.Printf("%d %v\n", pair.Timestamp, pair.Value)
					}
					//ts.CostTimeTest4 += time.Since(curTime).Nanoseconds()
					//log.Println("      (time,value): " + strconv.FormatInt(pair.Timestamp, 10) + ", " + fmt.Sprintf("%v", pair.Value))
				}
			}
		}
	}
	costTime := time.Since(tsCurNew)
	return costTime
	//fmt.Printf("%s %s cost time %d = %fms\n", strPath, strTag,
	//	costTime.Nanoseconds(), costTime.Seconds()*1000)
}

type ReadTestResult struct {
	StrTsFile     string
	StrTag        string
	CostTime      time.Duration
	CostTimeTest1 int64
	CostTimeTest2 int64
	CostTimeTest3 int64
	CostTimeTest4 int64
	CostTimeTest5 int64
	CostTimeTest6 int64
}

func TestReadEx(strDir string, bDebugMoreInfo bool, bDebugValue bool) {
	var t *ReadTestResult
	var arrResult []*ReadTestResult = make([]*ReadTestResult, 15)

	arrResult[0] = &ReadTestResult{StrTsFile: strDir + "output1.ts", StrTag: "TS_2DIFF   int32", CostTime: 0}
	arrResult[1] = &ReadTestResult{StrTsFile: strDir + "output2.ts", StrTag: "TS_2DIFF   int64", CostTime: 0}
	arrResult[2] = &ReadTestResult{StrTsFile: strDir + "output3.ts", StrTag: "TS_2DIFF float32", CostTime: 0}
	arrResult[3] = &ReadTestResult{StrTsFile: strDir + "output4.ts", StrTag: "TS_2DIFF float64", CostTime: 0}
	arrResult[4] = &ReadTestResult{StrTsFile: strDir + "output5.ts", StrTag: "PLAIN       Text", CostTime: 0}
	arrResult[5] = &ReadTestResult{StrTsFile: strDir + "output6.ts", StrTag: "RLE        int32", CostTime: 0}
	arrResult[6] = &ReadTestResult{StrTsFile: strDir + "output7.ts", StrTag: "RLE        int64", CostTime: 0}
	arrResult[7] = &ReadTestResult{StrTsFile: strDir + "output8.ts", StrTag: "RLE      float32", CostTime: 0}
	arrResult[8] = &ReadTestResult{StrTsFile: strDir + "output9.ts", StrTag: "RLE      float64", CostTime: 0}
	arrResult[9] = &ReadTestResult{StrTsFile: strDir + "output10.ts", StrTag: "GORILLA float32", CostTime: 0}
	arrResult[10] = &ReadTestResult{StrTsFile: strDir + "output11.ts", StrTag: "GORILLA float64", CostTime: 0}
	arrResult[11] = &ReadTestResult{StrTsFile: strDir + "output12.ts", StrTag: "PLAIN     int32", CostTime: 0}
	arrResult[12] = &ReadTestResult{StrTsFile: strDir + "output13.ts", StrTag: "PLAIN     int64", CostTime: 0}
	arrResult[13] = &ReadTestResult{StrTsFile: strDir + "output14.ts", StrTag: "PLAIN   float32", CostTime: 0}
	arrResult[14] = &ReadTestResult{StrTsFile: strDir + "output15.ts", StrTag: "PLAIN   float64", CostTime: 0}

	var iMax int32 = 1
	for i := int32(0); i < iMax; i++ {
		for _, t = range arrResult {
			t.CostTime += TestReadFile(t, bDebugValue)
		}
	}
	for _, t = range arrResult {
		t.CostTime = time.Duration(t.CostTime.Nanoseconds() / int64(iMax))
		if bDebugMoreInfo {
			fmt.Printf("%s %s cost time %d = %fms\ntotal:%d\ntest1:%d\ntest2:%d\ntest3:%d\ntest4:%d\ntest5:%d\ntest6:%d\n", t.StrTsFile, t.StrTag,
				t.CostTime.Nanoseconds(), t.CostTime.Seconds()*1000,
				t.CostTime.Nanoseconds(), t.CostTimeTest1,
				t.CostTimeTest2, t.CostTimeTest3, t.CostTimeTest4, t.CostTimeTest5, t.CostTimeTest6)
		} else {
			fmt.Printf("%s %s cost time %d = %fms\n", t.StrTsFile, t.StrTag,
				t.CostTime.Nanoseconds(), t.CostTime.Seconds()*1000)
		}

	}
}
