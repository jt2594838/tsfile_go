package basic

import (
	"tsfile/timeseries/filter"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type FilteredRowReader struct {
	reader *RowRecordReader
	filter filter.Filter

	row *datatype.RowRecord
}

func (r *FilteredRowReader) fillCache() {
	for {
		if !r.reader.HasNext() {
			return
		} else {
			row := r.reader.Next()
			if r.filter == nil || r.filter.Satisfy(row) {
				r.row = row
				break
			}
		}
	}
}

func (r *FilteredRowReader) HasNext() bool {
	if r.row != nil {
		return true
	}
	r.fillCache()
	return r.row != nil
}

func (r *FilteredRowReader) Next() *datatype.RowRecord {
	ret := r.row
	r. row = nil
	r.fillCache()
	return ret
}

func (r *FilteredRowReader) Close() {
	r.reader.Close()
}

func NewFilteredRowReader(paths []string, readerMap map[string]reader.ISeriesReader, filter filter.Filter) *FilteredRowReader{
	rowReader := NewRecordReader(paths, readerMap)
	dataSet := &FilteredRowReader{reader:rowReader, filter:filter}
	return dataSet
}

