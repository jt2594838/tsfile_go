package tsFileWriter

/**
 * @Package Name: seriesWriter
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-28 下午8:28
 * @Description:
 */

import (
	"encoding/binary"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/log"
	"tsfile/file/header"
	"tsfile/file/metadata/statistics"
	"tsfile/timeseries/write/sensorDescriptor"
)

type SeriesWriter struct {
	deviceId          string
	dataSeriesWriters map[string]SeriesWriter

	desc       *sensorDescriptor.SensorDescriptor
	tsDataType int16
	pageWriter *PageWriter
	/* page size threshold 	*/
	psThres             int
	pageCountUpperBound int
	/* value writer to encode data*/
	valueWriter ValueWriter
	/* value count on a page. It will be reset agter calling */
	valueCount                 int
	valueCountForNextSizeCheck int
	/*statistics on a page. It will be reset after calling */
	pageStatistics             statistics.Statistics
	seriesStatistics           statistics.Statistics
	time                       int64
	minTimestamp               int64
	sensorDescriptor           sensorDescriptor.SensorDescriptor
	minimumRecordCountForCheck int
	numOfPages                 int
}

func (s *SeriesWriter) GetTsDataType() int16 {
	return s.tsDataType
}

func (s *SeriesWriter) GetTsDeviceId() string {
	return s.deviceId
}

func (s *SeriesWriter) GetNumOfPages() int {
	return s.numOfPages
}

func (s *SeriesWriter) GetCurrentChunkSize(sId string) int {
	//return int64(tfiw.chunkHeader.GetChunkSerializedSize()) + s.pageWriter.GetCurrentDataSize()
	chunkHeaderSize := header.GetChunkSerializedSize(sId)
	size := chunkHeaderSize + s.pageWriter.GetCurrentDataSize()
	return size
}

func (s *SeriesWriter) Write(t int64, data *DataPoint) bool {
	s.time = t
	//s.valueCount = s.valueCount + 1

	vw := &(s.valueWriter)
	vw.timeEncoder.Encode(t, vw.timeBuf)
	switch s.tsDataType {
	case 0, 1, 2, 3, 4, 5:
		vw.valueEncoder.Encode(data.value, vw.valueBuf)
	default:
	}
	//s.valueWriter.Write(t, s.tsDataType, data, s.valueCount)
	//logcost.CostWriteTimesTest5 += int64(time.Since(tsCurNew))
	s.valueCount = s.valueCount + 1
	// statistics ignore here, if necessary, Statistics.java
	s.pageStatistics.UpdateStats(data.value)

	if s.minTimestamp == -1 {
		s.minTimestamp = t
	}
	// check page size and write page data to buffer
	s.checkPageSizeAndMayOpenNewpage()
	return true
}

func (s *SeriesWriter) WriteToFileWriter(tsFileIoWriter *TsFileIoWriter) {
	// write all pages in the same chunk to file
	s.pageWriter.WriteAllPagesOfSeriesToTsFile(tsFileIoWriter, s.seriesStatistics, s.numOfPages)
	// reset pageWriter
	s.pageWriter.Reset()
	// reset series_statistics
	s.seriesStatistics = statistics.GetStatsByType(s.tsDataType)
}

func (s *SeriesWriter) checkPageSizeAndMayOpenNewpage() {
	if s.valueCount == conf.MaxNumberOfPointsInPage {
		//log.Info("current line count reaches the upper bound, write page %s", s.sensorDescriptor)
		// write data to buffer
		s.WritePage()
	} else if s.valueCount >= s.valueCountForNextSizeCheck {
		currentColumnSize := s.valueWriter.GetCurrentMemSize()
		if currentColumnSize > s.psThres {
			// write data to buffer
			s.WritePage()
		} //else {
		//	log.Info("not enough size to write disk now.")
		//}
		// int * 1.0 / int 为float， 再乘以valueCount，得到下次检查的count
		s.valueCountForNextSizeCheck = s.psThres * 1.0 / currentColumnSize * s.valueCount
	}
}

func (s *SeriesWriter) PreFlush() {
	if s.valueCount > 0 {
		s.WritePage()
	}
}

func (s *SeriesWriter) EstimateMaxSeriesMemSize() int64 {
	valueMemSize := s.valueWriter.timeBuf.Len() + s.valueWriter.valueBuf.Len()
	pageMemSize := s.pageWriter.EstimateMaxPageMemSize()
	return int64(valueMemSize + pageMemSize)
}

func (s *SeriesWriter) WritePage() {
	pageWriter := s.pageWriter
	//pageWriter.WritePageHeaderAndDataIntoBuff(s.valueWriter.GetByteBuffer(),
	//	s.valueCount, s.pageStatistics, s.time, s.minTimestamp)
	dataBuffer := s.valueWriter.GetByteBuffer()
	valueCount := s.valueCount
	//sts statistics.Statistics
	//maxTimestamp int64, minTimestamp int64
	if pageWriter.desc.GetCompresstionType() == int16(constant.UNCOMPRESSED) {
		//this uncompressedSize should be calculate from timeBuf and valueBuf
		uncompressedSize := dataBuffer.Len()
		var compressedSize int = uncompressedSize
		pageHeader, pageHeaderErr := header.NewPageHeader(
			int32(uncompressedSize), int32(compressedSize),
			int32(valueCount), s.pageStatistics, s.time,
			s.minTimestamp, pageWriter.desc.GetTsDataType())
		if pageHeaderErr != nil {
			log.Error("init pageHeader error: ", pageHeaderErr)
		}
		pageBuf := pageWriter.pageBuf
		//pageHeader.PageHeaderToMemory(p.pageBuf, p.desc.GetTsDataType())
		binary.Write(pageBuf, binary.BigEndian, pageHeader.GetUncompressedSize())
		binary.Write(pageBuf, binary.BigEndian, pageHeader.GetCompressedSize())
		binary.Write(pageBuf, binary.BigEndian, pageHeader.GetNumberOfValues())
		binary.Write(pageBuf, binary.BigEndian, pageHeader.Max_timestamp())
		binary.Write(pageBuf, binary.BigEndian, pageHeader.Min_timestamp())
		statistics.Serialize(*(pageHeader.GetStatistics()), pageBuf, pageWriter.desc.GetTsDataType())

		pageBuf.Write(dataBuffer.Bytes())
		pageWriter.totalValueCount += int64(valueCount)
	} else {
		//this uncompressedSize should be calculate from timeBuf and valueBuf
		uncompressedSize := dataBuffer.Len()

		// write pageData to pageBuf
		//声明一个空的slice,容量为dataBuffer的长度
		dataSlice := make([]byte, dataBuffer.Len())
		//把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
		dataBuffer.Read(dataSlice)

		var compressedSize int
		var enc []byte
		aSlice := make([]byte, 0)
		enc = pageWriter.compressor.GetEncompressor(
			pageWriter.desc.GetCompresstionType()).Encompress(aSlice, dataSlice)
		compressedSize = len(enc)

		pageHeader, pageHeaderErr := header.NewPageHeader(
			int32(uncompressedSize), int32(compressedSize), int32(valueCount),
			s.pageStatistics, s.time, s.minTimestamp,
			pageWriter.desc.GetTsDataType())
		if pageHeaderErr != nil {
			log.Error("init pageHeader error: ", pageHeaderErr)
		}
		// write pageheader to pageBuf
		pageHeader.PageHeaderToMemory(pageWriter.pageBuf,
			pageWriter.desc.GetTsDataType())

		//// write pageData to pageBuf
		////声明一个空的slice,容量为dataBuffer的长度
		//dataSlice := make([]byte, dataBuffer.Len())
		////把buf的内容读入到timeSlice内,因为timeSlice容量为timeSize,所以只读了timeSize个过来
		pageWriter.pageBuf.Write(enc)
		pageWriter.totalValueCount += int64(valueCount)
	}

	// pageStatistics
	s.numOfPages += 1

	s.minTimestamp = -1
	s.valueCount = 0
	s.valueWriter.Reset()
	s.ResetPageStatistics()
	return
}

func (s *SeriesWriter) ResetPageStatistics() {
	s.pageStatistics = statistics.GetStatsByType(s.tsDataType)
	return
}

func NewSeriesWriter(dId string, d *sensorDescriptor.SensorDescriptor, pw *PageWriter, pst int) (*SeriesWriter, error) {
	vw, _ := NewValueWriter(d)
	return &SeriesWriter{
		deviceId:                   dId,
		desc:                       d,
		pageWriter:                 pw,
		psThres:                    pst,
		pageCountUpperBound:        conf.MaxNumberOfPointsInPage,
		minimumRecordCountForCheck: 1,
		valueCountForNextSizeCheck: 1,
		numOfPages:                 0,
		tsDataType:                 d.GetTsDataType(),
		seriesStatistics:           statistics.GetStatsByType(d.GetTsDataType()),
		pageStatistics:             statistics.GetStatsByType(d.GetTsDataType()),
		valueWriter:                *vw,
		minTimestamp:               -1,
		valueCount:                 0,
	}, nil
}
