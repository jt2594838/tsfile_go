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

func TestReadFile(strPath string, strTag string, bDebugValue bool) time.Duration {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error:", err)
		}
	}()

	tsCurNew := time.Now()
	f := new(read.TsFileSequenceReader)
	f.Open(strPath)
	defer f.Close()

	_ = f.ReadHeadMagic()
	//log.Println("Header string: " + headerString)

	_ = f.ReadTailMagic()
	//log.Println("Tail string: " + tailerString)

	_ = f.ReadFileMetadata()
	//log.Println("File version: " + strconv.Itoa(fileMetadata.GetCurrentVersion()))

	for f.HasNextRowGroup() {
		groupHeader := f.ReadRowGroupHeader()
		//log.Println("row group: " + groupHeader.GetDevice() + ", chunk number: " + strconv.Itoa(int(groupHeader.GetNumberOfChunks())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
		for i := 0; i < int(groupHeader.GetNumberOfChunks()); i++ {
			chunkHeader := f.ReadChunkHeader()
			//log.Println("  chunk: " + chunkHeader.GetSensor() + ", page number: " + strconv.Itoa(chunkHeader.GetNumberOfPages()) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))
			defaultTimeDecoder := decoder.CreateDecoder(constant.TS_2DIFF, constant.INT64)
			valueDecoder := decoder.CreateDecoder(chunkHeader.GetEncodingType(), chunkHeader.GetDataType())
			for j := 0; j < chunkHeader.GetNumberOfPages(); j++ {
				pageHeader := f.ReadPageHeader(chunkHeader.GetDataType())
				//log.Println("    page dps: " + strconv.Itoa(int(pageHeader.GetNumberOfValues())) + ", page data size: " + strconv.Itoa(int(pageHeader.GetCompressedSize())) + ", end posistion: " + strconv.FormatInt(f.Pos(), 10))

				pageData := f.ReadPage(pageHeader, chunkHeader.GetCompressionType())
				reader1 := &basic.PageDataReader{DataType: chunkHeader.GetDataType(), ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
				reader1.Read(pageData)
				for reader1.HasNext() {
					pair, _ := reader1.Next()
					if bDebugValue {
						fmt.Printf("%d %v\n", pair.Timestamp, pair.Value)
					}
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
	StrTsFile string
	StrTag    string
	CostTime  time.Duration
}

func TestReadEx(strDir string, bDebugValue bool) {
	var t *ReadTestResult
	var arrResult []*ReadTestResult = make([]*ReadTestResult, 15)

	arrResult[0] = &ReadTestResult{StrTsFile: strDir + "output1.ts", StrTag: "TS_2DIFF   int32", CostTime: 0}
	arrResult[1] = &ReadTestResult{strDir + "output2.ts", "TS_2DIFF   int64", 0}
	arrResult[2] = &ReadTestResult{strDir + "output3.ts", "TS_2DIFF float32", 0}
	arrResult[3] = &ReadTestResult{strDir + "output4.ts", "TS_2DIFF float64", 0}
	arrResult[4] = &ReadTestResult{strDir + "output5.ts", "PLAIN       Text", 0}
	arrResult[5] = &ReadTestResult{strDir + "output6.ts", "RLE        int32", 0}
	arrResult[6] = &ReadTestResult{strDir + "output7.ts", "RLE        int64", 0}
	arrResult[7] = &ReadTestResult{strDir + "output8.ts", "RLE      float32", 0}
	arrResult[8] = &ReadTestResult{strDir + "output9.ts", "RLE      float64", 0}
	arrResult[9] = &ReadTestResult{strDir + "output10.ts", "GORILLA float32", 0}
	arrResult[10] = &ReadTestResult{strDir + "output11.ts", "GORILLA float64", 0}
	arrResult[11] = &ReadTestResult{strDir + "output12.ts", "PLAIN     int32", 0}
	arrResult[12] = &ReadTestResult{strDir + "output13.ts", "PLAIN     int64", 0}
	arrResult[13] = &ReadTestResult{strDir + "output14.ts", "PLAIN   float32", 0}
	arrResult[14] = &ReadTestResult{strDir + "output15.ts", "PLAIN   float64", 0}

	var iMax int32 = 1
	for i := int32(0); i < iMax; i++ {
		for _, t = range arrResult {
			t.CostTime += TestReadFile(t.StrTsFile, t.StrTag, bDebugValue)
		}
	}
	for _, t = range arrResult {
		t.CostTime = time.Duration(t.CostTime.Nanoseconds() / int64(iMax))
		fmt.Printf("%s %s cost time %d = %fms\n", t.StrTsFile, t.StrTag,
			t.CostTime.Nanoseconds(), t.CostTime.Seconds()*1000)
	}
	/*arrResult[0] += TestReadFile(strDir+"output1.ts", "TS_2DIFF   int32", bDebugValue)
	arrResult[1] += TestReadFile(strDir+"output2.ts", "TS_2DIFF   int64", bDebugValue)
	arrResult[2] += TestReadFile(strDir+"output3.ts", "TS_2DIFF float32", bDebugValue)
	arrResult[3] += TestReadFile(strDir+"output4.ts", "TS_2DIFF float64", bDebugValue)
	arrResult[4] += TestReadFile(strDir+"output5.ts", "PLAIN       Text", bDebugValue)
	arrResult[5] += TestReadFile(strDir+"output6.ts", "RLE        int32", bDebugValue)
	arrResult[6] += TestReadFile(strDir+"output7.ts", "RLE        int64", bDebugValue)
	arrResult[7] += TestReadFile(strDir+"output8.ts", "RLE      float32", bDebugValue)
	arrResult[8] += TestReadFile(strDir+"output9.ts", "RLE      float64", bDebugValue)
	arrResult[9] += TestReadFile(strDir+"output10.ts", "GORILLA float32", bDebugValue)
	arrResult[10] += TestReadFile(strDir+"output11.ts", "GORILLA float64", bDebugValue)
	arrResult[11] += TestReadFile(strDir+"output12.ts", "PLAIN     int32", bDebugValue)
	arrResult[12] += TestReadFile(strDir+"output13.ts", "PLAIN     int64", bDebugValue)
	arrResult[13] += TestReadFile(strDir+"output14.ts", "PLAIN   float32", bDebugValue)
	arrResult[14] += TestReadFile(strDir+"output15.ts", "PLAIN   float64", bDebugValue)*/
}
