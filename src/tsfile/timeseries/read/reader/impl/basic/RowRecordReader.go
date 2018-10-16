package basic

import (
	"math"
	"tsfile/timeseries/read/datatype"
	"tsfile/timeseries/read/reader"
)

type RowRecordReader struct {
	paths     []string
	readerMap map[string]reader.TimeValuePairReader

	cacheList []*datatype.TimeValuePair
	row       *datatype.RowRecord
	currTime  int64
}

func NewRecordReader(paths []string, readerMap map[string]reader.TimeValuePairReader) *RowRecordReader {
	ret := &RowRecordReader{paths: paths, readerMap: readerMap}
	ret.row = datatype.NewRowRecordWithPaths(paths)
	ret.cacheList = make([]*datatype.TimeValuePair, len(paths))
	ret.currTime = math.MaxInt64

	return ret
}

func (r *RowRecordReader) fillCache() {
	// try filling the column caches and update the currTime
	for i, path := range r.paths {
		if r.cacheList[i] == nil && r.readerMap[path].HasNext() {
			tv := r.readerMap[path].Next()
			r.cacheList[i] = tv

		}
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp < r.currTime {
			r.currTime = r.cacheList[i].Timestamp
		}
	}
}

func (r *RowRecordReader) fillRow() {
	// fill the row cache using column caches
	for i, _ := range r.paths {
		if r.cacheList[i] != nil && r.cacheList[i].Timestamp == r.currTime {
			r.row.Values()[i] = r.cacheList[i].Value
			r.cacheList[i] = nil
		} else {
			r.row.Values()[i] = nil
		}
	}
	r.row.SetTimestamp(r.currTime)
}

func (r *RowRecordReader) HasNext() bool {
	if r.currTime != math.MaxInt64 {
		return true
	}
	r.fillCache()
	return r.currTime != math.MaxInt64
}

/*
	Notice: The return value is IMMUTABLE because the RowRecord is reused through out the iteration to reduce memory
	overhead. You can only copy the values in the RowRecord instead of copying the pointer of the return value.
*/
func (r *RowRecordReader) Next() *datatype.RowRecord {
	r.fillRow()
	r.currTime = math.MaxInt64
	r.fillCache()
	return r.row
}

func (r *RowRecordReader) Close() {
	for _, path := range r.paths {
		if r.readerMap[path] != nil {
			r.readerMap[path].Close()
		}
	}
}
