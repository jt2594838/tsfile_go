package impl

import (
	"tsfile/timeseries/read/reader"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader/impl"
)

type QueryDataSet struct {
	reader reader.IRowRecordReader
}

func NewQueryDataSet(paths []string, readerMap map[string]reader.ISeriesReader, filter filter.Filter) *QueryDataSet{
	rowReader := impl.NewFilteredRowReader(paths, readerMap, filter)
	dataSet := &QueryDataSet{reader:rowReader}
	return dataSet
}

func (set *QueryDataSet) HasNext() bool {
	return set.reader.HasNext()
}

func (set *QueryDataSet) Next() *datatype.RowRecord {
	return set.reader.Next()
}

func (set *QueryDataSet) Close()  {
	set.reader.Close()
}