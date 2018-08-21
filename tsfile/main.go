// tsfile project main.go
package main

import (
	"log"
	"strconv"
	_ "tsfile/encoding/decoder"
	_ "tsfile/file/metadata/enums"
	"tsfile/timeseries/read"
	_ "tsfile/timeseries/read/reader/impl"
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

	_ = f.ReadFileMetadata()
	//log.Println("File version: " + strconv.Itoa(metadata.CurrentVersion))

	groupHeader := f.ReadRowGroupHeader()
	//for f.HasNextRowGroup() {
		log.Println("row group:" + groupHeader.DeltaObjectID)
		log.Println("chunk number: " + strconv.Itoa(groupHeader.NumberOfChunks))
		//log.Println(f.pos)

		//for i := 0; i < groupHeader.NumberOfChunks; i++ {
			chunkHeader := f.ReadChunkHeader()
			log.Println("chunk: " + chunkHeader.MeasurementID)
			log.Println("page number: " + strconv.Itoa(chunkHeader.NumberOfPages))
			//	defaultTimeDecoder := decoder.GetDecoderByType(enums.TS_2DIFF, enums.INT64)
			//	valueDecoder := decoder.GetDecoderByType(chunkHeader.EncodingType, chunkHeader.DataType)
			//for j := 0; j < chunkHeader.NumberOfPages; j++ {
				pageHeader := f.ReadPageHeader()
				log.Println("points in the page: " + strconv.Itoa(pageHeader.NumberOfValues))
				log.Println("page data size: " + strconv.Itoa(pageHeader.CompressedSize))
				//pageData := f.ReadPage(pageHeader, chunkHeader.CompressionType)
				//reader1 := &impl.PageDataReader{DataType: chunkHeader.DataType, ValueDecoder: valueDecoder, TimeDecoder: defaultTimeDecoder}
			//}
		//}
	//}
}
