package engine

import (
	"tsfile/file/metadata"
	"tsfile/timeseries/read"
	"tsfile/timeseries/query"
	"tsfile/timeseries/query/dataset"
	"tsfile/timeseries/read/reader/impl"
	"log"
	"fmt"
	"tsfile/common/constant"
)

type Engine struct {
	reader *read.TsFileSequenceReader
	fileMeta *metadata.FileMetaData
}

func (e *Engine) open(reader *read.TsFileSequenceReader) {
	e.reader = reader
	e.fileMeta = reader.ReadFileMetadata()
}

func (e *Engine) query(exp *query.QueryExpression) dataset.QueryDataSet{
	readerMap := make(map[string]*impl.SeriesReader)
	for _, path := range exp.Paths(){
		 readerMap[path] = e.constructReader(path)
	}
}

func (e *Engine) constructReader(path string) *impl.SeriesReader {
	dataType := e.getDataType(path)
	if dataType == constant.INVALID {
		log.Println(fmt.Sprintf("No such timeseries in this file : %s", path))
		return  nil
	}

	deviceMeta, ok := e.fileMeta.DeviceMap()[path]
	if !ok {
		log.Println(fmt.Sprintf("No such timeseries in this file : %s", path))
		return  nil
	}

	var offsets []int64
	var sizes []int
	for ele := deviceMeta.RowGroupMetadataList().Front(); ele != nil; ele = ele.Next() {
		if rowGroupMeta, ok := ele.Value.(metadata.RowGroupMetaData); ok {
			for c := rowGroupMeta.TimeSeriesChunkMetaDataList().Front(); c != nil; c = c.Next() {
				if chunkMeta, ok := c.Value.(metadata.ChunkMetaData); ok {
					chunkHeader := e.reader.ReadChunkHeaderAt(chunkMeta.FileOffsetOfCorrespondingData())
					for i := 0; i < chunkHeader.GetNumberOfPages(); i ++ {
						pageHeader := e.reader.ReadPageHeader(dataType)
						offsets = append(offsets, e.reader.Pos())
						sizes = append(sizes, pageHeader.GetCompressedSize())
					}
				}
			}
		}
	}

	return impl.NewSeriesReader(offsets, sizes, e.reader)
}

func (e* Engine) getDataType(path string) constant.TSDataType {
	if tsMeta, ok := e.fileMeta.TimeSeriesMetadataMap()[path]; ok {
		return tsMeta.DataType()
	}
	return constant.INVALID
}