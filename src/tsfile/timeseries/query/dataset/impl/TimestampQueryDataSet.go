package impl

import (
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/query/timegen"
	"tsfile/timeseries/read/reader/impl/seek"
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/query/timegen/impl"
	"tsfile/common/constant"
)

type TimestampQueryDataSet struct {
	tGen timegen.ITimestampGenerator
	r reader.ISeekableRowReader

	currTime int64
	current  *datatype.RowRecord
}

func NewTimestampQueryDataSet(selectPaths []string, conditionPaths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *TimestampQueryDataSet {
	tGen := impl.NewRowRecordTimestampGenerator(conditionPaths, readerMap, filter)
	r := seek.NewSeekableRowReader(selectPaths, readerMap)
	return &TimestampQueryDataSet{tGen:tGen, r:r, currTime:constant.INVALID_TIMESTAMP}
}

func (set *TimestampQueryDataSet) fetch() {
	if set.tGen.HasNext() {
		currTime := set.tGen.Next()
		if set.r.Seek(currTime) {
			set.current = set.r.Current()
		}
	}
}

func (set *TimestampQueryDataSet) HasNext() bool {
	if set.current != nil {
		return false
	}
	set.fetch()
	return set.current != nil
}

func (set *TimestampQueryDataSet) Next() *datatype.RowRecord {
	ret := set.current
	set.current = nil
	set.fetch()
	return ret
}

func (set *TimestampQueryDataSet) Close() {
	set.r.Close()
}

