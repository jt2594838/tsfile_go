package impl

import (
	"tsfile/common/constant"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/query/timegen"
	"tsfile/timeseries/query/timegen/impl"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/read/reader/impl/basic"
	"tsfile/timeseries/read/reader/impl/seek"
	"errors"
	"tsfile/common/log"
)

type TimestampQueryDataSet struct {
	tGen timegen.ITimestampGenerator
	rGen *basic.FilteredRowReader
	r    reader.ISeekableRowReader

	currTime int64
	current  *datatype.RowRecord
	exhausted bool
}

func NewTimestampQueryDataSet(selectPaths []string, conditionPaths []string,
	selectReaderMap map[string]reader.ISeekableTimeValuePairReader, conditionReaderMap map[string]reader.TimeValuePairReader, filter filter.Filter) *TimestampQueryDataSet {
	tGen := impl.NewRowRecordTimestampGenerator(conditionPaths, conditionReaderMap, filter)
	rGen := basic.NewFilteredRowReader(conditionPaths, conditionReaderMap, filter)
	r := seek.NewSeekableRowReader(selectPaths, selectReaderMap)
	return &TimestampQueryDataSet{tGen: tGen, rGen: rGen, r: r, currTime: constant.INVALID_TIMESTAMP, exhausted:false}
}

func (set *TimestampQueryDataSet) fetch() {
	//if set.tGen.HasNext() {
	//	currTime, err := set.tGen.Next()
	//	if err != nil {
	//		log.Error("cannot generate next timestamp", err)
	//		set.exhausted = true
	//		return
	//	}
	//	if set.r.Seek(currTime) {
	//		set.current = set.r.Current()
	//	}
	//}
	if set.rGen.HasNext() {
		currRecord, err := set.rGen.Next()
		if err != nil {
			log.Error("cannot generate next timestamp", err)
			set.exhausted = true
			return
		}
		if set.r.Seek(currRecord.Timestamp()) {
			set.current = set.r.Current()
		}
	} else {
		set.exhausted = true
	}
}

func (set *TimestampQueryDataSet) HasNext() bool {
	if set.exhausted {
		return false
	}
	if set.current != nil {
		return true
	}
	set.fetch()
	if  set.current != nil {
		return true
	} else {
		set.exhausted = true
		return false
	}
}

func (set *TimestampQueryDataSet) Next() (*datatype.RowRecord, error) {
	if set.exhausted {
		return nil, errors.New("Dataset exhausted!");
	}
	if set.current == nil {
		set.fetch()
	}
	ret := set.current
	if ret == nil {
		set.exhausted = true
		return nil, errors.New("Dataset exhausted!");
	}
	set.current = nil
	set.fetch()
	return ret, nil
}

func (set *TimestampQueryDataSet) Close() {
	set.r.Close()
}
