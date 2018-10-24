package impl

import (
	"tsfile/common/constant"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/read/reader/impl/basic"
	"tsfile/common/log"
	"errors"
)

type RowRecordTimestampGenerator struct {
	reader reader.IRowRecordReader
	filter filter.Filter

	currTime int64
	exhausted bool
}

func (gen *RowRecordTimestampGenerator) Close() {
	gen.reader.Close()
}

func NewRowRecordTimestampGenerator(paths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *RowRecordTimestampGenerator {
	reader := basic.NewRecordReader(paths, readerMap)
	return &RowRecordTimestampGenerator{reader: reader, filter: filter, currTime: constant.INVALID_TIMESTAMP, exhausted:false}
}

func (gen *RowRecordTimestampGenerator) fetch() {
	for gen.reader.HasNext() {
		record, err := gen.reader.Next()
		if err != nil {
			log.Error("cannot read next RowRecord", err)
			gen.exhausted = true
			gen.currTime = constant.INVALID_TIMESTAMP
		}
		if gen.filter == nil || gen.filter.Satisfy(record) {
			gen.currTime = record.Timestamp()
			break
		}
	}
}

func (gen *RowRecordTimestampGenerator) HasNext() bool {
	if gen.exhausted {
		return false
	}
	if gen.currTime != constant.INVALID_TIMESTAMP {
		return true
	}
	gen.fetch()
	return gen.currTime != constant.INVALID_TIMESTAMP
}

func (gen *RowRecordTimestampGenerator) Next() (int64, error) {
	if gen.exhausted {
		return constant.INVALID_TIMESTAMP, errors.New("timestamp exhausted")
	}
	if gen.currTime == constant.INVALID_TIMESTAMP {
		gen.fetch()
		if gen.currTime == constant.INVALID_TIMESTAMP {
			return constant.INVALID_TIMESTAMP, errors.New("timestamp exhausted")
		}
	}
	ret := gen.currTime
	gen.currTime = constant.INVALID_TIMESTAMP
	return ret, nil
}
