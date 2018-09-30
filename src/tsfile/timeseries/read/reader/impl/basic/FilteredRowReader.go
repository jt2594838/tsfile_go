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
			if row.Timestamp() > 200000 {
				//print("here")
			}
			if r.filter == nil || r.filter.Satisfy(row) {
				r.row = row
				//fmt.Printf("Row %v satisfies\n", row.Timestamp())
				break
			}
		}
	}
}

func (r *FilteredRowReader) HasNext() bool {
	if r.row == nil {
		r.fillCache()
	}
	return r.row != nil
}

func (r *FilteredRowReader) Next() *datatype.RowRecord {
	if r.row == nil {
		r.fillCache()
	}
	ret := r.row
	r. row = nil

	return ret
}

func (r *FilteredRowReader) Close() {
	r.reader.Close()
}

func NewFilteredRowReader(paths []string, readerMap map[string]reader.TimeValuePairReader, filter filter.Filter) *FilteredRowReader{
	rowReader := NewRecordReader(paths, readerMap)
	dataSet := &FilteredRowReader{reader:rowReader, filter:filter}
	return dataSet
}

