// tsfile project main.go
package main

import (
	"fmt"
	"log"
	"strconv"
	"tsfile/common/constant"
	"tsfile/encoding/decoder"
	"tsfile/timeseries/read"
	"tsfile/timeseries/read/reader/impl"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Error: ", err)
		}
	}()

	f := new(read.TsFileSequenceReader)
	f.Open("D:/E2E/iot/iotdb/tsfile-hxd_thanos/test.ts")
	defer f.Close()

	headerString := f.ReadHeadMagic()
	log.Println("Header string: " + headerString)

	tailerString := f.ReadTailMagic()
	log.Println("Tail string: " + tailerString)

	fileMetadata := f.ReadFileMetadata()
	log.Println("File version: " + strconv.Itoa(fileMetadata.GetCurrentVersion()))

	for f.HasNextRowGroup() {
		log.Println("")

		groupHeader := f.ReadRowGroupHeader()
		log.Println("row group:" + groupHeader.GetDevice())
		log.Println("chunk number: " + strconv.Itoa(groupHeader.GetNumberOfChunks()))

		for i := 0; i < groupHeader.GetNumberOfChunks(); i++ {
			chunkHeader := f.ReadChunkHeader()
			log.Println("chunk: " + chunkHeader.GetSensor())
			log.Println("page number: " + strconv.Itoa(chunkHeader.GetNumberOfPages()))
			defaultTimeDecoder := decoder.GetDecoderByType(constant.TS_2DIFF, constant.INT64)
			valueDecoder := decoder.GetDecoderByType(chunkHeader.GetEncodingType(), chunkHeader.GetDataType())
			for j := 0; j < chunkHeader.GetNumberOfPages(); j++ {
				pageHeader := f.ReadPageHeader(chunkHeader.GetDataType())
				log.Println("points in the page: " + strconv.Itoa(pageHeader.GetNumberOfValues()))
				log.Println("page data size: " + strconv.Itoa(pageHeader.GetCompressedSize()))

				pageData := f.ReadPage(pageHeader, chunkHeader.GetCompressionType())

				reader1 := &impl.PageDataReader{DataType: chunkHeader.GetDataType(), ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
				reader1.Read(pageData)
				for reader1.HasNext() {
					pair := reader1.Next()
					log.Println("time, value: " + strconv.FormatInt(pair.Timestamp, 10) + ", " + fmt.Sprintf("%v", pair.Value))
				}
			}
		}
	}
}
