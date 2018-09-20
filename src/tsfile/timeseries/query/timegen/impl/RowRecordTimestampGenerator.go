package impl

import (
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/filter"
	"tsfile/common/constant"
	"tsfile/timeseries/read/reader/impl/basic"
)

type RowRecordTimestampGenerator struct {
	reader reader.IRowRecordReader
	filter filter.Filter

	currTime int64
}

func (gen *RowRecordTimestampGenerator) Close() {
	gen.reader.Close()
}

func NewRowRecordTimestampGenerator(paths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *RowRecordTimestampGenerator{
	reader := basic.NewRecordReader(paths, readerMap)
	return &RowRecordTimestampGenerator{reader:reader, filter:filter, currTime:constant.INVALID_TIMESTAMP}
}

func (gen *RowRecordTimestampGenerator) fetch() {
	for gen.reader.HasNext() {
		record := gen.reader.Next()
		if gen.filter == nil || gen.filter.Satisfy(record) {
			gen.currTime = record.Timestamp()
			break
		}
	}
}

func (gen *RowRecordTimestampGenerator) HasNext() bool {
	if gen.currTime != constant.INVALID_TIMESTAMP {
		return true
	}
	gen.fetch()
	return gen.currTime != constant.INVALID_TIMESTAMP
}

func (gen *RowRecordTimestampGenerator) Next() int64 {
	ret := gen.currTime
	gen.currTime = constant.INVALID_TIMESTAMP
	gen.fetch()
	return ret
}


